//! Bounds applied to a GraphQL document *before* it is parsed.
//!
//! Every other check in this crate runs on the parsed document or on the IR.
//! This one cannot: nesting an input value deeply enough overflows the stack
//! inside the parser itself, and a stack overflow in Rust aborts the process —
//! it is not a panic, so no `catch_unwind` at the request boundary can contain
//! it. One 16 KiB request would take down the server and every request in
//! flight with it.
//!
//! So the guard has to be a scan of the raw text, and it has to sit at the one
//! place every path funnels through: [`parse_document`](crate::parser::parse_document).
//!
//! ```text
//! { users(where: {_not: {_not: … × 2000 … }}) { id } }   ~16 KiB
//!     → fatal runtime error: stack overflow, aborting     (2 MiB stack)
//! ```
//!
//! 2 MiB is what a tokio worker thread gets by default, so that is the size
//! that matters for a server; an 8 MiB main thread only moves the cliff to
//! ~8000. Selection-set nesting is already bounded by the parser's own
//! recursion limit — it is input values (`where`, `_set`, `objects`) that have
//! no guard, which is why the depth counted here is over all bracket kinds
//! rather than just braces.
//!
//! The defaults are far above any hand-written or generated query and are meant
//! to be left alone; [`ParseLimits`] exists so a caller with an unusual
//! workload can raise them, and so an endpoint can lower them.

use crate::error::{Error, Result};

/// Limits on the raw text of a document.
///
/// Both are coarse by design: this runs on every request before parsing, so it
/// is a single pass over the bytes with no allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseLimits {
    /// Maximum nesting depth, counting `{`, `[` and `(` together.
    ///
    /// Selection sets, argument lists and input objects all contribute, so a
    /// query nesting five relations deep with a `where` on each sits around 20.
    /// [`DEFAULT_MAX_DEPTH`] leaves room for that and still stops the stack
    /// overflow by two orders of magnitude.
    pub max_depth: usize,
    /// Maximum length of the document in bytes.
    ///
    /// The depth check is what prevents the crash; this is the cheap cut that
    /// keeps a pathologically wide document (thousands of aliased fields, each
    /// legal on its own) from reaching the parser at all.
    pub max_bytes: usize,
}

/// See [`ParseLimits::max_depth`].
pub const DEFAULT_MAX_DEPTH: usize = 64;

/// See [`ParseLimits::max_bytes`].
pub const DEFAULT_MAX_BYTES: usize = 128 * 1024;

impl Default for ParseLimits {
    fn default() -> Self {
        ParseLimits {
            max_depth: DEFAULT_MAX_DEPTH,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl ParseLimits {
    /// Limits that reject nothing. For a caller whose documents are its own
    /// source code rather than request input.
    ///
    /// This re-opens the stack overflow described in the module docs — only use
    /// it where the document text cannot come from a client.
    pub fn unbounded() -> Self {
        ParseLimits {
            max_depth: usize::MAX,
            max_bytes: usize::MAX,
        }
    }

    /// Check `source` against these limits.
    ///
    /// One pass over the bytes. String literals (including block strings) and
    /// `#` comments are skipped, so a bracket inside `_eq: "{{{{"` does not
    /// count toward the depth. Unbalanced or unterminated input is not this
    /// function's business — it stops scanning and lets the parser produce the
    /// syntax error it would have produced anyway.
    pub fn check(&self, source: &str) -> Result<()> {
        if source.len() > self.max_bytes {
            return Err(Error::Limit {
                message: format!(
                    "document is {} bytes, over the {}-byte limit",
                    source.len(),
                    self.max_bytes
                ),
            });
        }

        // Byte-wise is sound here: every byte matched is ASCII, and no byte of a
        // multi-byte UTF-8 sequence can collide with one.
        let b = source.as_bytes();
        let mut i = 0;
        let mut depth = 0usize;
        while i < b.len() {
            match b[i] {
                b'#' => {
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                }
                b'"' => i = skip_string(b, i),
                b'{' | b'[' | b'(' => {
                    depth += 1;
                    if depth > self.max_depth {
                        return Err(Error::Limit {
                            message: format!(
                                "document nests deeper than the limit of {}",
                                self.max_depth
                            ),
                        });
                    }
                    i += 1;
                }
                b'}' | b']' | b')' => {
                    depth = depth.saturating_sub(1);
                    i += 1;
                }
                _ => i += 1,
            }
        }
        Ok(())
    }
}

/// Index just past the string literal starting at `i` (which must be a `"`), or
/// the end of input if it is unterminated.
fn skip_string(b: &[u8], i: usize) -> usize {
    if b[i..].starts_with(br#"""""#) {
        let mut i = i + 3;
        while i < b.len() {
            if b[i] == b'\\' {
                i += 2; // \""" is the only escape a block string has
                continue;
            }
            if b[i..].starts_with(br#"""""#) {
                return i + 3;
            }
            i += 1;
        }
        return b.len();
    }
    let mut i = i + 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    b.len()
}

#[cfg(test)]
mod exec_tests {
    use super::*;
    use crate::ast::{Count, Operation};
    use crate::schema::{PgType, Relation, Schema, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build()
    }

    fn lower(q: &str) -> Operation {
        crate::parser::parse_and_lower(q, &json!({}), None, &schema()).unwrap()
    }

    fn apply(q: &str, limits: ExecutionLimits) -> Result<Operation> {
        let mut op = lower(q);
        limits.apply(&mut op)?;
        Ok(op)
    }

    fn limit_of(op: &Operation) -> Option<Count> {
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        roots[0].args.limit.clone()
    }

    #[test]
    fn unbounded_limits_change_nothing() {
        let op = apply("{ users { id } }", ExecutionLimits::new()).unwrap();
        assert_eq!(limit_of(&op), None);
    }

    #[test]
    fn default_limit_fills_in_a_list_that_asked_for_none() {
        let op = apply("{ users { id } }", ExecutionLimits::new().default_limit(50)).unwrap();
        assert_eq!(limit_of(&op), Some(Count::Lit(50)));
    }

    #[test]
    fn default_limit_leaves_an_explicit_one_alone() {
        let op = apply(
            "{ users(limit: 3) { id } }",
            ExecutionLimits::new().default_limit(50),
        )
        .unwrap();
        assert_eq!(limit_of(&op), Some(Count::Lit(3)));
    }

    #[test]
    fn default_limit_reaches_array_relations_too() {
        let op = apply(
            "{ users { posts { id } } }",
            ExecutionLimits::new().default_limit(50),
        )
        .unwrap();
        let Operation::Query(roots) = &op else {
            panic!()
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!()
        };
        match &selection[0] {
            crate::ast::Field::Relation { args, .. } => {
                assert_eq!(args.limit, Some(Count::Lit(50)))
            }
            other => panic!("expected a relation, got {other:?}"),
        }
    }

    /// An aggregate answers with one row however many it reads, and capping it
    /// would quietly change what `count` counts.
    #[test]
    fn default_limit_does_not_touch_an_aggregate_or_a_by_pk() {
        let op = apply(
            "{ users_aggregate { aggregate { count } } }",
            ExecutionLimits::new().default_limit(50),
        )
        .unwrap();
        assert_eq!(limit_of(&op), None);

        let op = apply(
            "{ users_by_pk(id: 1) { id } }",
            ExecutionLimits::new().default_limit(50),
        )
        .unwrap();
        assert_eq!(limit_of(&op), None);
    }

    #[test]
    fn max_limit_rejects_a_literal_over_the_ceiling() {
        let err = apply(
            "{ users(limit: 5000) { id } }",
            ExecutionLimits::new().max_limit(100),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("over the limit of 100"), "{err}");
        apply(
            "{ users(limit: 100) { id } }",
            ExecutionLimits::new().max_limit(100),
        )
        .unwrap();
    }

    /// `limit: $n` has no value yet, so the ceiling has to travel with it.
    #[test]
    fn max_limit_follows_a_variable_to_where_it_resolves() {
        // Symbolic lowering: eager lowering would substitute the value and leave
        // no variable for a ceiling to travel with.
        let doc =
            crate::parser::parse_document("query($n: Int) { users(limit: $n) { id } }").unwrap();
        let mut op =
            crate::parser::lower_with(&doc, crate::parser::Bindings::Symbolic, None, &schema())
                .unwrap();
        ExecutionLimits::new()
            .max_limit(100)
            .apply(&mut op)
            .unwrap();

        let Operation::Query(roots) = &op else {
            panic!()
        };
        let count = roots[0].args.limit.clone().unwrap();
        let over = json!({"n": 5000});
        let err = count
            .resolve(&crate::types::Inputs::variables(&over), "users.limit")
            .unwrap_err();
        assert!(format!("{err}").contains("over the limit of 100"), "{err}");

        let ok = json!({"n": 5});
        assert_eq!(
            count
                .resolve(&crate::types::Inputs::variables(&ok), "users.limit")
                .unwrap(),
            5
        );
    }

    #[test]
    fn max_relation_depth_counts_relation_hops() {
        let limits = ExecutionLimits::new().max_relation_depth(2);
        apply("{ users { posts { user { id } } } }", limits).unwrap();
        let err = apply("{ users { posts { user { posts { id } } } } }", limits).unwrap_err();
        assert!(
            format!("{err}").contains("deeper than the limit of 2"),
            "{err}"
        );
    }

    /// Breadth is the other half: two hundred aliases of one relation nest one
    /// level and render two hundred correlated subqueries.
    #[test]
    fn max_table_reads_counts_breadth_not_just_depth() {
        let inner: String = (0..20).map(|i| format!("a{i}: posts {{ id }} ")).collect();
        let q = format!("{{ users {{ {inner} }} }}");
        let err = apply(&q, ExecutionLimits::new().max_table_reads(10)).unwrap_err();
        assert!(
            format!("{err}").contains("more than 10 table positions"),
            "{err}"
        );
        apply(&q, ExecutionLimits::new().max_table_reads(21)).unwrap();
    }

    #[test]
    fn an_exists_filter_and_an_order_by_hop_count_as_reads() {
        // root + EXISTS
        let err = apply(
            "{ users(where: {posts: {id: {_gt: 1}}}) { id } }",
            ExecutionLimits::new().max_table_reads(1),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("table positions"), "{err}");

        // root + order_by hop
        let err = apply(
            "{ posts(order_by: [{user: {id: asc}}]) { id } }",
            ExecutionLimits::new().max_table_reads(1),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("table positions"), "{err}");
    }

    #[test]
    fn introspection_reads_no_table() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .enable_introspection()
            .build();
        let mut op = crate::parser::parse_and_lower(
            "{ __type(name: \"users\") { name } }",
            &json!({}),
            None,
            &schema,
        )
        .unwrap();
        ExecutionLimits::new()
            .max_table_reads(0)
            .apply(&mut op)
            .unwrap();
    }

    #[test]
    fn a_nested_insert_counts_every_level() {
        let op = crate::parser::parse_and_lower(
            r#"mutation { insert_users(objects: [{id: 1, posts: {data: [{id: 2}]}}]) {
                 affected_rows } }"#,
            &json!({}),
            None,
            &schema(),
        );
        let mut op = op.unwrap();
        // The parent plus its nested child.
        let err = ExecutionLimits::new()
            .max_table_reads(1)
            .apply(&mut op)
            .unwrap_err();
        assert!(format!("{err}").contains("table positions"), "{err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth_of(source: &str) -> Result<()> {
        ParseLimits::default().check(source)
    }

    #[test]
    fn ordinary_queries_pass() {
        depth_of("{ users { id name } }").unwrap();
        depth_of(
            r#"query($id: Int!) {
                 users(where: {_and: [{id: {_eq: $id}}, {name: {_is_null: false}}]}) {
                   id
                   posts(order_by: [{id: asc}], limit: 5) { title comments { body } }
                 }
               }"#,
        )
        .unwrap();
    }

    #[test]
    fn deep_input_value_is_rejected() {
        let q = format!(
            "{{ users(where: {}{}{}) {{ id }} }}",
            "{_not: ".repeat(2000),
            "{id: {_eq: 1}}",
            "}".repeat(2000)
        );
        let err = depth_of(&q).unwrap_err();
        assert!(format!("{err}").contains("nests deeper"), "{err}");
    }

    #[test]
    fn depth_is_counted_across_bracket_kinds() {
        let limits = ParseLimits {
            max_depth: 3,
            ..Default::default()
        };
        limits.check("{ a(b: [1]) }").unwrap(); // { ( [  => 3
        let err = limits.check("{ a(b: [[1]]) }").unwrap_err(); // => 4
        assert!(format!("{err}").contains("nests deeper"), "{err}");
    }

    #[test]
    fn brackets_inside_strings_do_not_count() {
        let limits = ParseLimits {
            max_depth: 2,
            ..Default::default()
        };
        limits
            .check(r#"{ users(where: "{{{{{{{{") { id } }"#)
            .unwrap();
        limits
            .check(r#"{ users(where: "\"{{{{") { id } }"#)
            .unwrap();
        limits
            .check("{ users(where: \"\"\"{{{{{{\"\"\") { id } }")
            .unwrap();
    }

    #[test]
    fn brackets_inside_comments_do_not_count() {
        let limits = ParseLimits {
            max_depth: 2,
            ..Default::default()
        };
        limits.check("{ users { id } } # {{{{{{{{").unwrap();
        limits.check("# {{{{\n{ users { id } }").unwrap();
    }

    #[test]
    fn unterminated_string_is_left_to_the_parser() {
        // No panic, no false rejection: the parser reports the syntax error.
        ParseLimits::default()
            .check(r#"{ users(name: "oops) { id } }"#)
            .unwrap();
    }

    #[test]
    fn oversized_document_is_rejected() {
        let limits = ParseLimits {
            max_bytes: 16,
            ..Default::default()
        };
        let err = limits.check("{ users { id name active } }").unwrap_err();
        assert!(format!("{err}").contains("over the 16-byte limit"), "{err}");
    }

    #[test]
    fn unbounded_accepts_what_default_rejects() {
        let q = format!("{}{}", "{a(b: [".repeat(100), "]) }".repeat(100));
        assert!(ParseLimits::default().check(&q).is_err());
        ParseLimits::unbounded().check(&q).unwrap();
    }

    #[test]
    fn multibyte_text_does_not_confuse_the_scan() {
        let limits = ParseLimits {
            max_depth: 2,
            ..Default::default()
        };
        limits
            .check(r#"{ users(name: "中文｛括号｝") { id } }"#)
            .unwrap();
    }
}

/// What a single request may cost, checked on the lowered IR.
///
/// [`ParseLimits`] bounds the *document*; these bound the *statement* it turns
/// into, which is a different thing and not derivable from the first. A flat
/// document two hundred lines long, nesting nothing, still renders two hundred
/// correlated subqueries; and `{ users { id } }` is four words that reads a
/// whole table, builds the entire result as one JSON value in Postgres, and
/// hands it to the client in one piece.
///
/// The pass runs on the IR rather than on the parsed document because the IR is
/// the one thing both entry points share: the typed builder
/// ([`Engine::run`](crate::Engine::run)) never goes near the parser, so a check
/// living there would leave that path unbounded.
///
/// Every field is optional and unset by default: a library that silently capped
/// results would be worse than one that did not cap them, since the caller
/// cannot tell a capped answer from a complete one. Set them where requests come
/// from clients.
///
/// ```
/// # use vision_graphql::limits::ExecutionLimits;
/// let limits = ExecutionLimits::new()
///     .max_relation_depth(6)
///     .max_table_reads(40)
///     .default_limit(100)
///     .max_limit(1000);
/// # let _ = limits;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionLimits {
    max_relation_depth: Option<usize>,
    max_table_reads: Option<usize>,
    default_limit: Option<u64>,
    max_limit: Option<u64>,
}

impl ExecutionLimits {
    /// Limits that bound nothing. Start here and add what applies.
    pub fn new() -> Self {
        Self::default()
    }

    /// How deep relations may be followed. `{ users { posts { comments { … } } } }`
    /// is depth 3.
    ///
    /// Each level is a correlated subquery evaluated per row of the level above
    /// it, so cost compounds with depth in a way it does not with breadth.
    pub fn max_relation_depth(mut self, depth: usize) -> Self {
        self.max_relation_depth = Some(depth);
        self
    }

    /// How many table access points one request may have.
    ///
    /// Everything that reads a table counts once: each root field, each relation
    /// field at any depth, each `EXISTS` relation filter inside a `where`, each
    /// `order_by` hop, and each nested insert. That is the number of subqueries
    /// the statement will carry, which is the closest cheap proxy for what it
    /// will cost — and the thing a hundred aliases of one relation inflates
    /// while leaving depth at 1.
    pub fn max_table_reads(mut self, reads: usize) -> Self {
        self.max_table_reads = Some(reads);
        self
    }

    /// `limit` applied to any row list that did not ask for one.
    ///
    /// Applies to root list fields and array relations — the positions that can
    /// return unbounded rows. Not to `_by_pk` or object relations, which are one
    /// row by construction, and not to an aggregate, which reads many rows but
    /// answers with one: capping it would silently change what `count` counts.
    ///
    /// This one is a silent change to the answer, which is the trade it exists
    /// to make. A client that must know whether more rows exist should ask for
    /// `_aggregate { count }`.
    pub fn default_limit(mut self, n: u64) -> Self {
        self.default_limit = Some(n);
        self
    }

    /// Ceiling on any `limit` the request asks for. A larger one is an error
    /// rather than a quiet clamp — a truncated answer that looks complete is the
    /// failure worth avoiding here.
    ///
    /// A literal is checked when the limits are applied; `limit: $n` is checked
    /// when the variable resolves, so a compiled statement keeps the ceiling it
    /// was compiled under.
    pub fn max_limit(mut self, n: u64) -> Self {
        self.max_limit = Some(n);
        self
    }

    /// Whether anything is set. Used to skip the walk entirely.
    pub fn is_unbounded(&self) -> bool {
        *self == Self::default()
    }

    /// Apply to a lowered operation: rewrite what needs defaults, reject what is
    /// over a ceiling.
    pub fn apply(&self, op: &mut crate::ast::Operation) -> Result<()> {
        if self.is_unbounded() {
            return Ok(());
        }
        let mut reads = 0usize;
        match op {
            crate::ast::Operation::Query(roots) => {
                for root in roots.iter_mut() {
                    self.root(root, &mut reads)?;
                }
            }
            crate::ast::Operation::Mutation(fields) => {
                for f in fields.iter_mut() {
                    self.mutation(f, &mut reads)?;
                }
            }
        }
        Ok(())
    }

    fn count_read(&self, reads: &mut usize) -> Result<()> {
        *reads += 1;
        match self.max_table_reads {
            Some(max) if *reads > max => Err(Error::Limit {
                message: format!("request reads more than {max} table positions"),
            }),
            _ => Ok(()),
        }
    }

    fn check_depth(&self, depth: usize) -> Result<()> {
        match self.max_relation_depth {
            Some(max) if depth > max => Err(Error::Limit {
                message: format!("relations nest deeper than the limit of {max}"),
            }),
            _ => Ok(()),
        }
    }

    fn root(&self, root: &mut crate::ast::RootField, reads: &mut usize) -> Result<()> {
        use crate::ast::RootBody;
        // Introspection answers from memory and reads no table.
        if matches!(root.body, RootBody::Introspection(_)) {
            return Ok(());
        }
        self.count_read(reads)?;
        let list = matches!(root.body, RootBody::List { .. });
        self.args(&mut root.args, list, reads, 0)?;
        match &mut root.body {
            RootBody::List { selection } | RootBody::ByPk { selection, .. } => {
                self.fields(selection, reads, 0)?;
            }
            RootBody::Aggregate { nodes, .. } => {
                if let Some(fields) = nodes.as_mut() {
                    self.fields(fields, reads, 0)?;
                }
            }
            RootBody::Introspection(_) => {}
        }
        Ok(())
    }

    fn mutation(&self, mf: &mut crate::ast::MutationField, reads: &mut usize) -> Result<()> {
        use crate::ast::MutationField;
        self.count_read(reads)?;
        match mf {
            MutationField::Insert {
                objects, returning, ..
            } => {
                for o in objects.iter() {
                    self.insert_object(o, reads)?;
                }
                self.fields(returning, reads, 0)?;
            }
            MutationField::Update {
                where_, returning, ..
            }
            | MutationField::Delete {
                where_, returning, ..
            } => {
                self.bool_expr(where_, reads)?;
                self.fields(returning, reads, 0)?;
            }
            MutationField::UpdateByPk { selection, .. }
            | MutationField::DeleteByPk { selection, .. } => {
                self.fields(selection, reads, 0)?;
            }
        }
        Ok(())
    }

    fn insert_object(&self, obj: &crate::ast::InsertObject, reads: &mut usize) -> Result<()> {
        for nested in obj.nested_arrays.values() {
            self.count_read(reads)?;
            for row in &nested.rows {
                self.insert_object(row, reads)?;
            }
        }
        for nested in obj.nested_objects.values() {
            self.count_read(reads)?;
            self.insert_object(&nested.row, reads)?;
        }
        Ok(())
    }

    fn args(
        &self,
        args: &mut crate::ast::QueryArgs,
        list: bool,
        reads: &mut usize,
        depth: usize,
    ) -> Result<()> {
        if let Some(w) = args.where_.as_mut() {
            self.bool_expr(w, reads)?;
        }
        // An `order_by` that walks a relation reads that relation's table.
        for ob in &args.order_by {
            for _ in &ob.path {
                self.count_read(reads)?;
            }
        }
        if list {
            match args.limit.as_mut() {
                Some(crate::ast::Count::Lit(n)) => {
                    if let Some(max) = self.max_limit {
                        if *n > max {
                            return Err(Error::Limit {
                                message: format!("limit {n} is over the limit of {max}"),
                            });
                        }
                    }
                }
                Some(crate::ast::Count::Var { max, .. }) => {
                    // Tightest of what was already there and what applies here.
                    *max = match (*max, self.max_limit) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                }
                None => {
                    if let Some(default) = self.default_limit {
                        args.limit = Some(crate::ast::Count::Lit(default));
                    }
                }
            }
        }
        let _ = depth;
        Ok(())
    }

    fn fields(
        &self,
        fields: &mut [crate::ast::Field],
        reads: &mut usize,
        depth: usize,
    ) -> Result<()> {
        for f in fields.iter_mut() {
            let crate::ast::Field::Relation {
                args, selection, ..
            } = f
            else {
                continue;
            };
            self.count_read(reads)?;
            self.check_depth(depth + 1)?;
            // Whether this relation returns many rows is not visible in the IR —
            // the kind lives on the schema — so the default applies to any
            // relation that carries list arguments at all. An object relation
            // renders `row_to_json` over one row, where a `LIMIT` is harmless.
            self.args(args, true, reads, depth + 1)?;
            self.fields(selection, reads, depth + 1)?;
        }
        Ok(())
    }

    fn bool_expr(&self, expr: &mut crate::ast::BoolExpr, reads: &mut usize) -> Result<()> {
        use crate::ast::BoolExpr;
        match expr {
            BoolExpr::And(parts) | BoolExpr::Or(parts) => {
                for p in parts.iter_mut() {
                    self.bool_expr(p, reads)?;
                }
            }
            BoolExpr::Not(inner) => self.bool_expr(inner, reads)?,
            BoolExpr::Relation { inner, .. } => {
                // An EXISTS subquery reads a table like any other position.
                self.count_read(reads)?;
                self.bool_expr(inner, reads)?;
            }
            BoolExpr::Compare { .. } | BoolExpr::IsNull { .. } | BoolExpr::InList { .. } => {}
        }
        Ok(())
    }
}
