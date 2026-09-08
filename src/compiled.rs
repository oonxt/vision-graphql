//! Queries lowered and rendered once, executed many times.
//!
//! [`Engine::query`](crate::Engine::query) does everything per request: parse,
//! lower, render, execute. A [`CompiledQuery`] splits that in two. Compiling
//! resolves the schema, applies the scope policy and produces the SQL string;
//! executing supplies this request's variables and principal and runs it.
//!
//! What makes the split real is that variables stay symbolic through lowering
//! and rendering (see [`crate::ast::Val`]), so one compiled statement serves
//! every value of `$id` and every tenant. Compiling with the values already
//! substituted would give a different statement per request and buy nothing.
//!
//! ```no_run
//! # use vision_graphql::{Engine, policy::ScopePolicy, predicate::{col, principal}};
//! # async fn f(engine: Engine, schema: vision_graphql::Schema) -> vision_graphql::error::Result<()> {
//! let policy = ScopePolicy::builder()
//!     .allow("orders", col("user_id").eq(principal()))
//!     .validate(&schema)?;
//!
//! // once, at startup:
//! let q = engine.compile_scoped("query($id: Int!) { orders(where: {id: {_eq: $id}}) { id } }", &policy)?;
//!
//! // per request:
//! let principal = vision_graphql::predicate::Principal::new().set("principal", 7);
//! let data = engine.execute_scoped(&q, Some(serde_json::json!({"id": 1})), &principal).await?;
//! # let _ = data; Ok(())
//! # }
//! ```
//!
//! # What cannot be compiled
//!
//! A variable that decides the *shape* of the SQL rather than a value in it
//! cannot be deferred, and compiling such a query fails with
//! [`Error::NotCompilable`](crate::Error::NotCompilable) naming the position:
//!
//! - `where: $w` — a whole filter object, and likewise `order_by: $o`,
//!   `distinct_on: $d`; these decide which predicates and clauses exist.
//! - `_is_null: $b` — picks between `IS NULL` and `IS NOT NULL`.
//! - any variable inside an `insert` argument. A VALUES list's row count and
//!   column set come from the argument itself, so `objects: $rows` could never
//!   compile; this first cut does not thread variables into written-out rows
//!   either, so `objects: [{name: $n}]` is refused as well. An insert whose
//!   arguments are entirely written out does compile — there is just nothing
//!   left for it to defer.
//!
//! Everything else — comparison values, `_in` lists (including `_in: $ids`),
//! `limit` / `offset`, `_by_pk` arguments, `update`'s `where` and `_set`
//! values, `delete`'s `where` — compiles. Run the rest through
//! [`Engine::query`](crate::Engine::query), which is unaffected.
//!
//! A [`CompiledQuery`] runs on the pool, not inside
//! [`Engine::transaction`](crate::Engine::transaction); mutations that need a
//! transaction still go through [`TxClient`](crate::TxClient).

use crate::error::{Error, Result};
use crate::types::BindSpec;
use serde_json::{Map, Value};

/// A rendered statement plus the recipe for its parameters.
///
/// Cheap to clone-free share behind an `Arc`; executing takes `&self`.
///
/// An operation with `@choices` variables holds one *shape* per combination of
/// their values — a statement each, lowered and rendered exactly as a
/// single-shape operation is — and picks one per request by the values the
/// request supplies. See [`shapes`](Self::shapes).
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    /// One per combination of `@choices` values, in the order the combinations
    /// enumerate (first declared variable slowest). Exactly one, pinned to
    /// nothing, when the operation declares no `@choices`.
    pub(crate) shapes: Vec<Shape>,
    /// The `@choices` declarations, in declaration order: what the shapes are
    /// keyed by, and the set a request's value is checked against.
    pub(crate) choices: Vec<(String, Vec<Value>)>,
    /// Response key when the operation has exactly one root field, so typed
    /// execution can unwrap the data envelope.
    pub(crate) root_alias: Option<String>,
    /// Defaults the operation declared for its variables. Applied at execute
    /// time, since compiling happens before any request exists.
    pub(crate) defaults: Map<String, Value>,
    /// Whether a scope policy was applied when this was compiled.
    ///
    /// Tracked explicitly rather than inferred from whether any scope parameter
    /// survived: a policy that only marks tables `unrestricted` has no
    /// parameters, and inferring would let it be executed by the scoped path
    /// and the unscoped one interchangeably. Executing a scoped statement
    /// without a principal, or an unscoped one with, is refused.
    pub(crate) scoped: bool,
}

/// One statement of a [`CompiledQuery`]: the SQL for one combination of
/// `@choices` values, and the parameters it takes.
#[derive(Debug, Clone)]
pub(crate) struct Shape {
    /// The `@choices` values this shape was compiled for, by variable name.
    pub(crate) pinned: Map<String, Value>,
    pub(crate) sql: String,
    pub(crate) specs: Vec<BindSpec>,
}

impl CompiledQuery {
    /// The rendered SQL. Stable for the life of this value — that is the point
    /// of compiling — so it is what to `EXPLAIN`, log, or diff in review.
    ///
    /// For an operation with `@choices`, the SQL of the first combination of
    /// values; [`shapes`](Self::shapes) has every one.
    pub fn sql(&self) -> &str {
        &self.shapes[0].sql
    }

    /// Number of bound parameters the statement takes — the first shape's,
    /// for an operation with `@choices`.
    pub fn bind_count(&self) -> usize {
        self.shapes[0].specs.len()
    }

    /// Every statement this query may run, each with the `@choices` values it
    /// was compiled for. One entry, pinned to nothing, for an operation without
    /// `@choices`.
    pub fn shapes(&self) -> impl Iterator<Item = (&Map<String, Value>, &str)> {
        self.shapes.iter().map(|s| (&s.pinned, s.sql.as_str()))
    }

    /// How many statements [`shapes`](Self::shapes) holds.
    pub fn shape_count(&self) -> usize {
        self.shapes.len()
    }

    /// The `@choices` declarations, in declaration order: each variable with
    /// the values a request may supply for it.
    pub fn choices(&self) -> &[(String, Vec<Value>)] {
        &self.choices
    }

    /// Whether this was compiled against a scope policy, and so must be run
    /// with a principal.
    pub fn is_scoped(&self) -> bool {
        self.scoped
    }

    /// Default values this statement's operation declared, by variable name.
    /// A request that omits one of these still runs.
    pub fn defaults(&self) -> &Map<String, Value> {
        &self.defaults
    }

    /// Names of the GraphQL variables this statement reads, in placeholder
    /// order and with duplicates kept out. Useful for checking a request
    /// supplies what a persisted query needs before running it.
    ///
    /// `@choices` variables come first: they are read to pick the shape, so
    /// they are needed even where the chosen shape has no placeholder for
    /// them.
    pub fn variables(&self) -> Vec<String> {
        let mut out: Vec<String> = self.choices.iter().map(|(n, _)| n.clone()).collect();
        for shape in &self.shapes {
            for spec in &shape.specs {
                let mut found = Vec::new();
                match spec {
                    BindSpec::Scalar { val, .. } | BindSpec::Array { val, .. } => {
                        val.collect_vars(&mut found)
                    }
                    BindSpec::Count {
                        val: crate::ast::Count::Var { name, .. },
                        ..
                    } => found.push(name.clone()),
                    _ => {}
                }
                for name in found {
                    if !out.contains(&name) {
                        out.push(name);
                    }
                }
            }
        }
        out
    }

    /// The shape this request runs: the one compiled for the `@choices` values
    /// it supplies (or defaults to).
    ///
    /// A value outside the declared list is an error naming the variable, not
    /// a fallback to some shape: the request asked for an ordering or a
    /// predicate the document never offered, and answering with another would
    /// be answering a different question.
    pub(crate) fn shape_for(&self, variables: &Value) -> Result<&Shape> {
        if self.choices.is_empty() {
            return Ok(&self.shapes[0]);
        }
        let mut wanted: Vec<(&str, &Value)> = Vec::with_capacity(self.choices.len());
        for (name, values) in &self.choices {
            let v = variables
                .get(name)
                .or_else(|| self.defaults.get(name))
                .ok_or_else(|| Error::Variable {
                    name: name.clone(),
                    message: "not bound".into(),
                })?;
            if !values.contains(v) {
                return Err(Error::Variable {
                    name: name.clone(),
                    message: format!("{v} is not one of the values declared by @choices"),
                });
            }
            wanted.push((name, v));
        }
        self.shapes
            .iter()
            .find(|s| wanted.iter().all(|(n, v)| s.pinned.get(*n) == Some(v)))
            .ok_or_else(|| Error::Validate {
                path: "@choices".into(),
                message: "internal: no compiled shape for a declared combination of values"
                    .into(),
            })
    }
}

/// Lower, scope, bound and render `doc` once per combination of its
/// `@choices` values. What [`crate::Engine::compile`] does, minus the pool.
pub(crate) fn compile(
    doc: &async_graphql_parser::types::ExecutableDocument,
    operation_name: Option<&str>,
    policy: Option<&crate::policy::ScopePolicy>,
    schema: &crate::schema::Schema,
    limits: &crate::limits::ExecutionLimits,
) -> Result<CompiledQuery> {
    let contract = crate::parser::variable_contract(doc, operation_name)?;
    let combinations = choice_combinations(&contract.choices)?;
    let mut shapes = Vec::with_capacity(combinations.len());
    let mut root_alias = None;
    for pinned in combinations {
        // One full lowering per combination, with the chosen values
        // substituted as an eager lowering would and everything else left
        // symbolic. Each shape then goes through the same scope rewrite and
        // limits as a single-shape statement — it *is* one — so a pass added
        // to that pipeline covers every shape without knowing they exist.
        let pinned_value = Value::Object(pinned.clone());
        let mut op = crate::parser::lower_with(
            doc,
            crate::parser::Bindings::pinned(&pinned_value),
            operation_name,
            schema,
        )?;
        if let Some(policy) = policy {
            crate::scope::apply_scope(&mut op, &policy.symbolic(), schema)?;
        }
        root_alias = crate::engine::single_root_alias(&op).map(String::from);
        let (sql, specs) = crate::engine::prepare_symbolic(&mut op, schema, limits)?;
        shapes.push(Shape { pinned, sql, specs });
    }
    Ok(CompiledQuery {
        shapes,
        choices: contract.choices,
        root_alias,
        defaults: crate::parser::variable_defaults(doc, operation_name)?,
        scoped: policy.is_some(),
    })
}

/// Every combination of `@choices` values, as the object to pin each lowering
/// to. One empty object when there are no choices — the plain compile.
///
/// The product is bounded by [`crate::parser::MAX_SHAPES`]: each combination
/// is a full lowering held for the life of the statement, and a document whose
/// lists multiply past the bound is asking for a different design.
fn choice_combinations(choices: &[(String, Vec<Value>)]) -> Result<Vec<Map<String, Value>>> {
    let total = choices
        .iter()
        .try_fold(1usize, |n, (_, values)| n.checked_mul(values.len()))
        .filter(|n| *n <= crate::parser::MAX_SHAPES);
    if total.is_none() {
        let names: Vec<String> = choices.iter().map(|(n, _)| format!("${n}")).collect();
        return Err(Error::NotCompilable {
            path: names.join(", "),
            message: format!(
                "@choices multiply out to more than {} shapes; a compiled statement holds \
                 one per combination",
                crate::parser::MAX_SHAPES
            ),
        });
    }
    let mut out = vec![Map::new()];
    for (name, values) in choices {
        let mut next = Vec::with_capacity(out.len() * values.len());
        for base in &out {
            for v in values {
                let mut m = base.clone();
                m.insert(name.clone(), v.clone());
                next.push(m);
            }
        }
        out = next;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use crate::ast::Val;
    use crate::error::Error;
    use crate::parser::{lower_with, parse_document, Bindings};
    use crate::policy::ScopePolicy;
    use crate::predicate::{col, principal, Principal};
    use crate::schema::{PgType, Relation, Schema, Table};
    use crate::scope::apply_scope;
    use crate::sql::render;
    use crate::types::{resolve_binds, Bind, Inputs};
    use serde_json::{json, Value};

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation("orders", Relation::array("orders").on([("id", "user_id")])),
            )
            .table(
                Table::new("orders", "public", "orders")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .primary_key(&["id"]),
            )
            .build()
    }

    /// Compile without a policy: SQL plus the parameter recipe.
    fn compile(source: &str) -> crate::error::Result<(String, Vec<crate::types::BindSpec>)> {
        let schema = schema();
        let doc = parse_document(source)?;
        let op = lower_with(&doc, Bindings::symbolic(), None, &schema)?;
        render(&op, &schema)
    }

    fn binds(
        specs: &[crate::types::BindSpec],
        vars: serde_json::Value,
    ) -> crate::error::Result<Vec<Bind>> {
        resolve_binds(specs, &Inputs::variables(&vars))
    }

    /// The engine's compile, minus the pool: every shape, ready to pick from.
    fn compile_doc(source: &str) -> crate::error::Result<super::CompiledQuery> {
        let schema = schema();
        let doc = parse_document(source)?;
        super::compile(
            &doc,
            None,
            None,
            &schema,
            &crate::limits::ExecutionLimits::default(),
        )
    }

    #[test]
    fn choices_compile_one_shape_per_value_and_the_request_picks_one() {
        // `order_by: $sort` alone is the textbook NotCompilable; declaring
        // the values it may take is what makes it a bounded set of shapes.
        let q = compile_doc(
            r#"query($sort: [orders_order_by!]! @choices(values: [[{title: asc}], [{title: desc}], [{id: desc}]])) {
                 orders(order_by: $sort) { id }
               }"#,
        )
        .unwrap();
        assert_eq!(q.shape_count(), 3);
        let sqls: Vec<&str> = q.shapes().map(|(_, sql)| sql).collect();
        assert!(sqls[0].contains(r#""title" ASC"#), "{}", sqls[0]);
        assert!(sqls[1].contains(r#""title" DESC"#), "{}", sqls[1]);
        assert!(sqls[2].contains(r#""id" DESC"#), "{}", sqls[2]);
        assert_eq!(q.sql(), sqls[0]);
        assert_eq!(q.choices().len(), 1);
        assert_eq!(q.variables(), vec!["sort".to_string()]);

        // Picked by value, compared as JSON — key order and spelling of the
        // request's object do not matter, its content does.
        let shape = q.shape_for(&json!({"sort": [{"title": "desc"}]})).unwrap();
        assert_eq!(shape.sql, sqls[1]);
        let shape = q.shape_for(&json!({"sort": [{"id": "desc"}]})).unwrap();
        assert_eq!(shape.sql, sqls[2]);

        // A value the document never offered is refused, not mapped to the
        // nearest shape.
        let err = q.shape_for(&json!({"sort": [{"id": "asc"}]})).unwrap_err();
        assert!(
            matches!(&err, Error::Variable { name, message } if name == "sort" && message.contains("@choices")),
            "{err:?}"
        );
        let err = q.shape_for(&json!({})).unwrap_err();
        assert!(matches!(&err, Error::Variable { name, .. } if name == "sort"), "{err:?}");
    }

    #[test]
    fn choices_default_picks_a_shape_when_the_request_leaves_it_out() {
        let q = compile_doc(
            r#"query($sort: [orders_order_by!] @choices(values: [[{id: asc}], [{id: desc}]]) = [{id: desc}]) {
                 orders(order_by: $sort) { id }
               }"#,
        )
        .unwrap();
        let shape = q.shape_for(&json!({})).unwrap();
        assert!(shape.sql.contains(r#""id" DESC"#), "{}", shape.sql);
        // A default outside the list is a document error, not a request one.
        let err = compile_doc(
            r#"query($sort: [orders_order_by!] @choices(values: [[{id: asc}]]) = [{title: asc}]) {
                 orders(order_by: $sort) { id }
               }"#,
        )
        .unwrap_err();
        assert!(
            matches!(&err, Error::Variable { name, message } if name == "sort" && message.contains("default")),
            "{err:?}"
        );
    }

    #[test]
    fn choices_multiply_out_and_are_bounded() {
        let q = compile_doc(
            r#"query($sort: [orders_order_by!]! @choices(values: [[{id: asc}], [{id: desc}]]),
                     $null: Boolean! @choices(values: [true, false, null])) {
                 orders(order_by: $sort, where: {title: {_is_null: $null}}) { id }
               }"#,
        );
        // `_is_null: null` does not lower, so the null choice fails the whole
        // compile: a shape that cannot be built is not silently left out.
        let err = q.unwrap_err();
        assert!(matches!(err, Error::Validate { .. }), "{err:?}");

        let q = compile_doc(
            r#"query($sort: [orders_order_by!]! @choices(values: [[{id: asc}], [{id: desc}]]),
                     $null: Boolean! @choices(values: [true, false])) {
                 orders(order_by: $sort, where: {title: {_is_null: $null}}) { id }
               }"#,
        )
        .unwrap();
        assert_eq!(q.shape_count(), 4);
        assert_eq!(q.variables(), vec!["sort".to_string(), "null".to_string()]);
        let shape = q
            .shape_for(&json!({"sort": [{"id": "desc"}], "null": false}))
            .unwrap();
        assert!(shape.sql.contains("IS NOT NULL") && shape.sql.contains(r#""id" DESC"#), "{}", shape.sql);
        assert_eq!(shape.pinned, json!({"sort": [{"id": "desc"}], "null": false}).as_object().cloned().unwrap());

        // The bound. 257 values for one variable is one over.
        let values: Vec<String> = (0..=256).map(|n| n.to_string()).collect();
        let err = compile_doc(&format!(
            "query($n: Int! @choices(values: [{}])) {{ orders(limit: $n) {{ id }} }}",
            values.join(", ")
        ))
        .unwrap_err();
        assert!(
            matches!(&err, Error::NotCompilable { path, message } if path == "$n" && message.contains("256")),
            "{err:?}"
        );
    }

    #[test]
    fn a_choices_variable_in_a_value_position_is_pinned_per_shape() {
        // Nothing stops a bounded variable from also being an ordinary value;
        // it is then a literal of each shape rather than a placeholder.
        let q = compile_doc(
            r#"query($t: String! @choices(values: ["a", "b"])) {
                 orders(where: {title: {_eq: $t}}) { id }
               }"#,
        )
        .unwrap();
        assert_eq!(q.shape_count(), 2);
        for (pinned, _) in q.shapes() {
            let shape = q.shape_for(&Value::Object(pinned.clone())).unwrap();
            let binds = resolve_binds(&shape.specs, &Inputs::variables(&json!({}))).unwrap();
            assert_eq!(binds, vec![Bind::Text(pinned["t"].as_str().unwrap().into())]);
        }
        // Still listed as a variable the request must supply.
        assert_eq!(q.variables(), vec!["t".to_string()]);
    }

    #[test]
    fn optional_compiles_to_a_null_guard_around_the_comparison() {
        let q = compile_doc(
            r#"query($t: String @optional, $ids: [Int!] @optional) {
                 orders(where: {title: {_ilike: $t}, id: {_in: $ids}}) { id }
               }"#,
        )
        .unwrap();
        let sql = q.sql();
        assert!(
            sql.contains(r#"($1::text IS NULL OR o0."title" ILIKE $1::text)"#)
                || sql.contains(r#"($1::text IS NULL OR "#),
            "{sql}"
        );
        assert!(sql.contains("IS NULL OR") && sql.contains("= ANY ($2::int4[])"), "{sql}");
        // One placeholder each, used twice.
        assert_eq!(q.bind_count(), 2);
        assert_eq!(sql.matches("$1::").count(), 2, "{sql}");
        assert_eq!(sql.matches("$2::").count(), 2, "{sql}");
        assert_eq!(q.variables(), vec!["t".to_string(), "ids".to_string()]);

        let shape = q.shape_for(&json!({})).unwrap();
        // Leaving both out: nulls, which the SQL turns into TRUE.
        assert_eq!(
            resolve_binds(&shape.specs, &Inputs::variables(&json!({"t": null, "ids": null}))).unwrap(),
            vec![Bind::Null, Bind::Null]
        );
        // Supplying them: the ordinary binds.
        assert_eq!(
            resolve_binds(&shape.specs, &Inputs::variables(&json!({"t": "%a%", "ids": [1, 2]}))).unwrap(),
            vec![Bind::Text("%a%".into()), Bind::Int4Array(vec![Some(1), Some(2)])]
        );
        // Not supplied at all is still not bound: an optional filter is one the
        // request says nothing about *by passing null*, not one it may forget.
        let err = resolve_binds(&shape.specs, &Inputs::variables(&json!({"t": "x"}))).unwrap_err();
        assert!(matches!(&err, Error::Variable { name, .. } if name == "ids"), "{err:?}");
    }

    #[test]
    fn optional_is_transparent_on_the_eager_path() {
        let schema = schema();
        let with = parse_document(
            "query($t: String @optional) { orders(where: {title: {_eq: $t}}) { id } }",
        )
        .unwrap();
        let without =
            parse_document("query($t: String) { orders(where: {title: {_eq: $t}}) { id } }")
                .unwrap();
        let vars = json!({"t": "x"});
        let a = render(&lower_with(&with, Bindings::eager(&vars), None, &schema).unwrap(), &schema).unwrap();
        let b = render(&lower_with(&without, Bindings::eager(&vars), None, &schema).unwrap(), &schema).unwrap();
        assert_eq!(a.0, b.0);

        // A null: the comparison is gone, and no parameter is left behind.
        let vars = json!({"t": null});
        let (sql, specs) = render(&lower_with(&with, Bindings::eager(&vars), None, &schema).unwrap(), &schema).unwrap();
        assert!(sql.contains("TRUE") && !sql.contains("$1"), "{sql}");
        assert!(specs.is_empty());
        // Without the declaration the same request is still refused.
        let err = render(&lower_with(&without, Bindings::eager(&vars), None, &schema).unwrap(), &schema).unwrap_err();
        assert!(format!("{err}").contains("null"), "{err}");
    }

    #[test]
    fn optional_only_applies_as_a_whole_comparison_operand() {
        for (q, what) in [
            ("query($n: Int @optional) { orders(limit: $n) { id } }", "limit"),
            ("query($x: Int @optional) { orders(where: {id: {_in: [1, $x]}}) { id } }", "list element"),
            ("query($b: Boolean @optional) { orders(where: {title: {_is_null: $b}}) { id } }", "_is_null"),
            ("query($o: [orders_order_by!] @optional) { orders(order_by: $o) { id } }", "order_by"),
            ("query($w: orders_bool_exp @optional) { orders(where: $w) { id } }", "where"),
            ("mutation($t: String @optional) { update_orders(where: {id: {_eq: 1}}, _set: {title: $t}) { affected_rows } }", "_set"),
            ("query($id: Int @optional) { orders_by_pk(id: $id) { id } }", "by_pk"),
        ] {
            let err = compile_doc(q).unwrap_err();
            assert!(
                matches!(&err, Error::Validate { message, .. } if message.contains("@optional")),
                "{what}: {err:?}"
            );
            // Same verdict when the value is in hand.
            let doc = parse_document(q).unwrap();
            let vars = json!({"n": 1, "x": 1, "b": true, "o": [], "w": {}, "t": "a", "id": 1});
            let err = lower_with(&doc, Bindings::eager(&vars), None, &schema()).unwrap_err();
            assert!(
                matches!(&err, Error::Validate { message, .. } if message.contains("@optional")),
                "{what} (eager): {err:?}"
            );
        }
    }

    #[test]
    fn variable_directives_are_checked_where_they_are_declared() {
        for (q, expect) in [
            ("query($t: String! @optional) { orders(where: {title: {_eq: $t}}) { id } }", "non-null"),
            ("query($t: String @optional @optional) { orders(where: {title: {_eq: $t}}) { id } }", "twice"),
            ("query($t: String @optional(if: true)) { orders(where: {title: {_eq: $t}}) { id } }", "no arguments"),
            ("query($t: String @optional @choices(values: [\"a\"])) { orders(where: {title: {_eq: $t}}) { id } }", "contradict"),
            ("query($t: String @choices(values: [])) { orders(where: {title: {_eq: $t}}) { id } }", "at least one"),
            ("query($t: String @choices(values: [\"a\", \"a\"])) { orders(where: {title: {_eq: $t}}) { id } }", "twice"),
            ("query($t: String @choices(values: \"a\")) { orders(where: {title: {_eq: $t}}) { id } }", "must be a list"),
            ("query($t: String @choices(of: [\"a\"])) { orders(where: {title: {_eq: $t}}) { id } }", "'values'"),
            ("query($t: String @choices) { orders(where: {title: {_eq: $t}}) { id } }", "values"),
            ("query($t: String @choices(values: [$u]), $u: String) { orders(where: {title: {_eq: $t}}) { id } }", "literals"),
            ("query($t: String @deprecated) { orders(where: {title: {_eq: $t}}) { id } }", "not supported"),
        ] {
            let err = compile_doc(q).unwrap_err();
            let text = format!("{err}");
            assert!(text.contains(expect), "{q}\n  -> {text}");
        }
        // The two directives are for variable definitions only.
        let err = compile_doc("{ orders @choices(values: [1]) { id } }").unwrap_err();
        assert!(format!("{err}").contains("not supported"), "{err}");
        let err = compile_doc("query @optional { orders { id } }").unwrap_err();
        assert!(format!("{err}").contains("not supported"), "{err}");
    }

    #[test]
    fn a_choices_value_is_checked_on_the_eager_path_too() {
        // The same document must ask the same question under Engine::query:
        // a value outside the declared list is refused there as well.
        let schema = schema();
        let doc = parse_document(
            r#"query($sort: [orders_order_by!]! @choices(values: [[{id: asc}], [{id: desc}]])) {
                 orders(order_by: $sort) { id }
               }"#,
        )
        .unwrap();
        let vars = json!({"sort": [{"id": "desc"}]});
        lower_with(&doc, Bindings::eager(&vars), None, &schema).unwrap();
        let vars = json!({"sort": [{"title": "asc"}]});
        let err = lower_with(&doc, Bindings::eager(&vars), None, &schema).unwrap_err();
        assert!(
            matches!(&err, Error::Variable { name, message } if name == "sort" && message.contains("@choices")),
            "{err:?}"
        );
    }

    #[test]
    fn same_sql_for_every_variable_value() {
        let (sql, specs) =
            compile("query($id: Int!) { users(where: {id: {_eq: $id}}) { name } }").unwrap();
        assert!(sql.contains("$1"), "{sql}");
        assert_eq!(
            binds(&specs, json!({"id": 1})).unwrap(),
            vec![Bind::Int4(1)]
        );
        assert_eq!(
            binds(&specs, json!({"id": 9})).unwrap(),
            vec![Bind::Int4(9)]
        );
        // The point of the exercise: the statement itself never changed.
        let (sql2, _) =
            compile("query($id: Int!) { users(where: {id: {_eq: $id}}) { name } }").unwrap();
        assert_eq!(sql, sql2);
    }

    #[test]
    fn literal_values_are_converted_at_compile_time() {
        // A literal that cannot be an Int4 is caught when compiling, not on the
        // request that happens to run it.
        let err = compile("{ users(where: {id: {_eq: \"nope\"}}) { name } }").unwrap_err();
        assert!(matches!(err, Error::Validate { .. }), "{err:?}");
    }

    #[test]
    fn variable_type_errors_surface_at_execution() {
        let (_, specs) =
            compile("query($id: Int!) { users(where: {id: {_eq: $id}}) { name } }").unwrap();
        let err = binds(&specs, json!({"id": "nope"})).unwrap_err();
        assert!(format!("{err}").contains("expected an integer"), "{err}");
        assert!(
            format!("{err}").contains("where.id"),
            "the position is named: {err}"
        );
    }

    #[test]
    fn missing_variable_is_an_error_not_a_null() {
        let (_, specs) =
            compile("query($id: Int!) { users(where: {id: {_eq: $id}}) { name } }").unwrap();
        let err = binds(&specs, json!({})).unwrap_err();
        assert!(matches!(err, Error::Variable { .. }), "{err:?}");
    }

    #[test]
    fn variable_limit_becomes_a_bind_and_literal_limit_stays_inline() {
        let (sql, specs) = compile("query($n: Int!) { users(limit: $n) { id } }").unwrap();
        assert!(sql.contains("LIMIT $1::int8"), "{sql}");
        assert_eq!(
            binds(&specs, json!({"n": 25})).unwrap(),
            vec![Bind::Int8(25)]
        );

        let (sql, specs) = compile("{ users(limit: 25) { id } }").unwrap();
        assert!(sql.contains("LIMIT 25"), "{sql}");
        assert!(specs.is_empty());
    }

    #[test]
    fn whole_list_variable_compiles_to_one_array_bind() {
        let (sql, specs) =
            compile("query($ids: [Int!]) { users(where: {id: {_in: $ids}}) { id } }").unwrap();
        assert!(sql.contains("= ANY ($1::int4[])"), "{sql}");
        assert_eq!(
            binds(&specs, json!({"ids": [1, 2, 3]})).unwrap(),
            vec![Bind::Int4Array(vec![Some(1), Some(2), Some(3)])]
        );
        // An empty list at request time is still correct: `= ANY('{}')` is false.
        assert_eq!(
            binds(&specs, json!({"ids": []})).unwrap(),
            vec![Bind::Int4Array(vec![])]
        );
    }

    #[test]
    fn list_with_a_variable_element_compiles() {
        let (_, specs) =
            compile("query($x: Int!) { users(where: {id: {_in: [1, $x]}}) { id } }").unwrap();
        assert_eq!(
            binds(&specs, json!({"x": 7})).unwrap(),
            vec![Bind::Int4Array(vec![Some(1), Some(7)])]
        );
    }

    #[test]
    fn by_pk_and_update_set_take_variables() {
        let (_, specs) = compile("query($id: Int!) { users_by_pk(id: $id) { name } }").unwrap();
        assert_eq!(
            binds(&specs, json!({"id": 3})).unwrap(),
            vec![Bind::Int4(3)]
        );

        let (_, specs) = compile(
            "mutation($id: Int!, $name: String!) {
                 update_users(where: {id: {_eq: $id}}, _set: {name: $name}) { affected_rows }
             }",
        )
        .unwrap();
        let out = binds(&specs, json!({"id": 3, "name": "zoe"})).unwrap();
        assert!(out.contains(&Bind::Int4(3)), "{out:?}");
        assert!(out.contains(&Bind::Text("zoe".into())), "{out:?}");
    }

    #[test]
    fn structural_variables_are_refused_with_the_position_named() {
        for (source, needle) in [
            (
                "query($w: users_bool_exp) { users(where: $w) { id } }",
                "where",
            ),
            (
                "query($b: Boolean!) { users(where: {name: {_is_null: $b}}) { id } }",
                "_is_null",
            ),
            (
                "query($o: [users_order_by!]) { users(order_by: $o) { id } }",
                "order_by",
            ),
            (
                "query($d: [users_select_column!]) { users(distinct_on: $d) { id } }",
                "distinct_on",
            ),
            (
                "mutation($rows: [users_insert_input!]!) { insert_users(objects: $rows) { affected_rows } }",
                "objects",
            ),
        ] {
            let err = compile(source).unwrap_err();
            let Error::NotCompilable { path, .. } = &err else {
                panic!("expected NotCompilable for {source}, got {err:?}");
            };
            assert!(path.contains(needle), "path {path} should name {needle}");
        }
    }

    #[test]
    fn the_same_queries_still_run_eagerly() {
        // Everything symbolic lowering refuses must still work the old way.
        let schema = schema();
        let doc = parse_document("query($w: users_bool_exp) { users(where: $w) { id } }").unwrap();
        let vars = json!({"w": {"id": {"_eq": 4}}});
        let op = lower_with(&doc, Bindings::eager(&vars), None, &schema).unwrap();
        let (sql, specs) = render(&op, &schema).unwrap();
        assert!(sql.contains("$1"), "{sql}");
        assert_eq!(
            resolve_binds(&specs, &Inputs::none()).unwrap(),
            vec![Bind::Int4(4)]
        );
    }

    #[test]
    fn declared_defaults_apply_at_execute_time() {
        let schema = schema();
        let src = r#"query($t: String = "fallback") { orders(where: {title: {_eq: $t}}) { id } }"#;
        let doc = parse_document(src).unwrap();
        let op = lower_with(&doc, Bindings::symbolic(), None, &schema).unwrap();
        let (_sql, specs) = render(&op, &schema).unwrap();
        let defaults = crate::parser::variable_defaults(&doc, None).unwrap();

        // Nothing supplied: the default stands in.
        let empty = json!({});
        assert_eq!(
            resolve_binds(&specs, &Inputs::variables(&empty).with_defaults(&defaults)).unwrap(),
            vec![Bind::Text("fallback".into())]
        );
        // Supplied: the request wins.
        let given = json!({"t": "given"});
        assert_eq!(
            resolve_binds(&specs, &Inputs::variables(&given).with_defaults(&defaults)).unwrap(),
            vec![Bind::Text("given".into())]
        );
        // Supplied as null: still the request, not the default — an explicit
        // null is a value, not an absence. In a comparison it is now refused
        // rather than compared (see `types::null_comparison`), and that refusal
        // is itself the proof: falling back to the default would have succeeded
        // with "fallback".
        let null = json!({"t": null});
        let err =
            resolve_binds(&specs, &Inputs::variables(&null).with_defaults(&defaults)).unwrap_err();
        assert!(format!("{err}").contains("_is_null"), "{err}");
    }

    #[test]
    fn compiling_against_a_policy_defers_the_principal() {
        let schema = schema();
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(principal()))
            .validate(&schema)
            .unwrap();

        let doc = parse_document("query($t: String!) { orders(where: {title: {_eq: $t}}) { id } }")
            .unwrap();
        let mut op = lower_with(&doc, Bindings::symbolic(), None, &schema).unwrap();
        apply_scope(&mut op, &policy.symbolic(), &schema).unwrap();
        let (sql, specs) = render(&op, &schema).unwrap();

        // The predicate is in the statement; whose rows it admits is not.
        assert!(sql.contains("user_id"), "{sql}");
        let vars = json!({"t": "a-order-1"});
        for tenant in [1i64, 2] {
            let principal = Principal::new().set("principal", tenant);
            let out = resolve_binds(&specs, &Inputs::variables(&vars).with_principal(&principal))
                .unwrap();
            assert!(
                out.contains(&Bind::Int4(tenant as i32)),
                "tenant {tenant} not bound: {out:?}"
            );
        }
    }

    #[test]
    fn a_policy_compiled_statement_will_not_run_without_a_principal() {
        let schema = schema();
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(principal()))
            .validate(&schema)
            .unwrap();
        let doc = parse_document("{ orders { id } }").unwrap();
        let mut op = lower_with(&doc, Bindings::symbolic(), None, &schema).unwrap();
        apply_scope(&mut op, &policy.symbolic(), &schema).unwrap();
        let (_, specs) = render(&op, &schema).unwrap();

        let err = resolve_binds(&specs, &Inputs::none()).unwrap_err();
        assert!(format!("{err}").contains("principal"), "{err}");
    }

    #[test]
    fn tables_outside_the_policy_are_refused_at_compile_time() {
        let schema = schema();
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(principal()))
            .validate(&schema)
            .unwrap();
        let doc = parse_document("{ users { id } }").unwrap();
        let mut op = lower_with(&doc, Bindings::symbolic(), None, &schema).unwrap();
        let err = apply_scope(&mut op, &policy.symbolic(), &schema).unwrap_err();
        assert!(matches!(err, Error::ScopeDenied { .. }), "{err:?}");
    }

    #[test]
    fn scope_params_do_not_leak_into_the_reported_variables() {
        let schema = schema();
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(principal()))
            .validate(&schema)
            .unwrap();
        let doc = parse_document("query($t: String!) { orders(where: {title: {_eq: $t}}) { id } }")
            .unwrap();
        let mut op = lower_with(&doc, Bindings::symbolic(), None, &schema).unwrap();
        apply_scope(&mut op, &policy.symbolic(), &schema).unwrap();
        let (sql, specs) = render(&op, &schema).unwrap();
        let compiled = super::CompiledQuery {
            shapes: vec![super::Shape {
                pinned: Default::default(),
                sql,
                specs,
            }],
            choices: Vec::new(),
            root_alias: None,
            defaults: Default::default(),
            scoped: true,
        };
        assert_eq!(compiled.variables(), vec!["t".to_string()]);
    }

    #[test]
    fn a_written_out_insert_compiles_but_a_variable_one_does_not() {
        // Worth pinning down, because the module docs make a claim about it:
        // the row count and column set come from the argument, so literal rows
        // are compilable and any variable in the argument is not.
        assert!(
            compile("mutation { insert_users(objects: [{name: \"a\"}]) { affected_rows } }")
                .is_ok()
        );
        let err = compile(
            "mutation($n: String!) { insert_users(objects: [{name: $n}]) { affected_rows } }",
        )
        .unwrap_err();
        assert!(matches!(err, Error::NotCompilable { .. }), "{err:?}");
    }

    #[test]
    fn a_composite_without_variables_stays_a_plain_literal() {
        // `Val::collapse` keeps written-out lists indistinguishable from before.
        let v = Val::Array(vec![Val::Lit(json!(1)), Val::Lit(json!(2))]).collapse();
        assert_eq!(v, Val::Lit(json!([1, 2])));
        let v = Val::Array(vec![Val::Lit(json!(1)), Val::Var("x".into())]).collapse();
        assert!(!v.is_lit());
    }
}
