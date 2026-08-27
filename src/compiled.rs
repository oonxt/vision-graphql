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

use crate::types::BindSpec;

/// A rendered statement plus the recipe for its parameters.
///
/// Cheap to clone-free share behind an `Arc`; executing takes `&self`.
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    pub(crate) sql: String,
    pub(crate) specs: Vec<BindSpec>,
    /// Response key when the operation has exactly one root field, so typed
    /// execution can unwrap the data envelope.
    pub(crate) root_alias: Option<String>,
    /// Defaults the operation declared for its variables. Applied at execute
    /// time, since compiling happens before any request exists.
    pub(crate) defaults: serde_json::Map<String, serde_json::Value>,
    /// Whether a scope policy was applied when this was compiled.
    ///
    /// Tracked explicitly rather than inferred from whether any scope parameter
    /// survived: a policy that only marks tables `unrestricted` has no
    /// parameters, and inferring would let it be executed by the scoped path
    /// and the unscoped one interchangeably. Executing a scoped statement
    /// without a principal, or an unscoped one with, is refused.
    pub(crate) scoped: bool,
}

impl CompiledQuery {
    /// The rendered SQL. Stable for the life of this value — that is the point
    /// of compiling — so it is what to `EXPLAIN`, log, or diff in review.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Number of bound parameters the statement takes.
    pub fn bind_count(&self) -> usize {
        self.specs.len()
    }

    /// Whether this was compiled against a scope policy, and so must be run
    /// with a principal.
    pub fn is_scoped(&self) -> bool {
        self.scoped
    }

    /// Default values this statement's operation declared, by variable name.
    /// A request that omits one of these still runs.
    pub fn defaults(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.defaults
    }

    /// Names of the GraphQL variables this statement reads, in placeholder
    /// order and with duplicates kept out. Useful for checking a request
    /// supplies what a persisted query needs before running it.
    pub fn variables(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for spec in &self.specs {
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
        out
    }
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
    use serde_json::json;

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
        let op = lower_with(&doc, Bindings::Symbolic, None, &schema)?;
        render(&op, &schema)
    }

    fn binds(
        specs: &[crate::types::BindSpec],
        vars: serde_json::Value,
    ) -> crate::error::Result<Vec<Bind>> {
        resolve_binds(specs, &Inputs::variables(&vars))
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
        let op = lower_with(&doc, Bindings::Eager(&vars), None, &schema).unwrap();
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
        let op = lower_with(&doc, Bindings::Symbolic, None, &schema).unwrap();
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
        // Supplied as null: still the request — null is a value, not an absence.
        let null = json!({"t": null});
        assert_eq!(
            resolve_binds(&specs, &Inputs::variables(&null).with_defaults(&defaults)).unwrap(),
            vec![Bind::Null]
        );
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
        let mut op = lower_with(&doc, Bindings::Symbolic, None, &schema).unwrap();
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
        let mut op = lower_with(&doc, Bindings::Symbolic, None, &schema).unwrap();
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
        let mut op = lower_with(&doc, Bindings::Symbolic, None, &schema).unwrap();
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
        let mut op = lower_with(&doc, Bindings::Symbolic, None, &schema).unwrap();
        apply_scope(&mut op, &policy.symbolic(), &schema).unwrap();
        let (sql, specs) = render(&op, &schema).unwrap();
        let compiled = super::CompiledQuery {
            sql,
            specs,
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
