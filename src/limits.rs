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
use crate::schema::Schema;

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
    bind_row_counts: bool,
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

    /// Render `limit` and `offset` as bound parameters even when the query text
    /// spells them out.
    ///
    /// Off by default, and the default is the right one for an application whose
    /// queries it wrote itself: a literal renders inline so the statement reads
    /// the way the query does and can be `EXPLAIN`ed as-is, which is most of the
    /// point of [`Engine::compile`](crate::Engine::compile).
    ///
    /// It is the wrong default when the numbers come from a client. `limit: 1`,
    /// `limit: 2`, `limit: 3` are three statements, and a driver caches prepared
    /// statements per connection keyed on their text — sqlx keeps 100 by default
    /// — so a caller paging through results, or simply varying the number,
    /// evicts everything else and leaves prepared statements accumulating
    /// server-side. Bound, they are one statement whatever the page size.
    ///
    /// The cost is that `CompiledQuery::sql()` no longer shows the number.
    pub fn bind_row_counts(mut self, yes: bool) -> Self {
        self.bind_row_counts = yes;
        self
    }

    /// Whether anything is set. Used to skip the walk entirely.
    pub fn is_unbounded(&self) -> bool {
        *self == Self::default()
    }
    /// Apply to a lowered operation: rewrite what needs defaults, reject what is
    /// over a ceiling.
    ///
    /// Takes the schema because whether a relation returns one row or many is
    /// not in the IR, and a default `limit` means different things for each: on
    /// an object relation it would replace the `LIMIT 1` the renderer relies on
    /// to keep `row_to_json` over a single row.
    pub fn apply(&self, op: &mut crate::ast::Operation, schema: &Schema) -> Result<()> {
        if self.is_unbounded() {
            return Ok(());
        }
        let mut reads = 0usize;
        match op {
            crate::ast::Operation::Query(roots) => {
                for root in roots.iter_mut() {
                    self.root(root, schema, &mut reads)?;
                }
            }
            crate::ast::Operation::Mutation(fields) => {
                for f in fields.iter_mut() {
                    self.mutation(f, schema, &mut reads)?;
                }
            }
        }
        Ok(())
    }

    /// The cap `nodes` gets when the caller wrote none.
    ///
    /// `None` when there is no `nodes`, when the caller wrote a limit of their
    /// own — which they meant for the whole field, `count` included — or when
    /// no default is configured.
    fn nodes_default(
        &self,
        nodes: Option<&[crate::ast::Field]>,
        args: &crate::ast::QueryArgs,
    ) -> Option<crate::ast::Count> {
        if nodes.is_none() || args.limit.is_some() {
            return None;
        }
        self.default_limit.map(crate::ast::Count::Lit)
    }

    fn count_read(&self, reads: &mut usize) -> Result<()> {
        *reads += 1;
        match self.max_table_reads {
            Some(max) if *reads > max => Err(Error::CostLimit {
                message: format!("request reads more than {max} table positions"),
            }),
            _ => Ok(()),
        }
    }

    fn check_depth(&self, depth: usize) -> Result<()> {
        match self.max_relation_depth {
            Some(max) if depth > max => Err(Error::CostLimit {
                message: format!("relations nest deeper than the limit of {max}"),
            }),
            _ => Ok(()),
        }
    }

    fn root(
        &self,
        root: &mut crate::ast::RootField,
        schema: &Schema,
        reads: &mut usize,
    ) -> Result<()> {
        use crate::ast::RootBody;
        // Introspection answers from memory and reads no table.
        if matches!(root.body, RootBody::Introspection(_)) {
            return Ok(());
        }
        self.count_read(reads)?;
        // An `_aggregate` root renders `LIMIT` like any list, and its `nodes`
        // returns those rows — so the ceiling has to reach it, or appending
        // `_aggregate` is all it takes to walk around one. A *default* is filled
        // in only for `nodes`, and on a source of its own: `aggregate` and
        // `nodes` otherwise read one source, so a cap on it would decide what
        // `count` counted rather than how many rows came back.
        let (cap, fill_default) = match &root.body {
            RootBody::List { .. } => (true, true),
            // Never on the shared source: it decides what `count` counts. A
            // default meant for `nodes` goes to its own slot, below.
            RootBody::Aggregate { .. } => (true, false),
            RootBody::ByPk { .. } | RootBody::Introspection(_) => (false, false),
        };
        let table = schema.table(&root.table);
        self.args(&mut root.args, cap, fill_default, table, schema, reads, 0)?;
        if let RootBody::Aggregate {
            nodes, nodes_limit, ..
        } = &mut root.body
        {
            *nodes_limit = self.nodes_default(nodes.as_deref(), &root.args);
        }
        match &mut root.body {
            RootBody::List { selection } | RootBody::ByPk { selection, .. } => {
                self.fields(selection, table, schema, reads, 0)?;
            }
            RootBody::Aggregate { nodes, .. } => {
                if let Some(fields) = nodes.as_mut() {
                    self.fields(fields, table, schema, reads, 0)?;
                }
            }
            RootBody::Introspection(_) => {}
        }
        Ok(())
    }

    fn mutation(
        &self,
        mf: &mut crate::ast::MutationField,
        schema: &Schema,
        reads: &mut usize,
    ) -> Result<()> {
        use crate::ast::MutationField;
        self.count_read(reads)?;
        match mf {
            MutationField::Insert {
                table,
                objects,
                on_conflict,
                returning,
                scope_check,
                ..
            } => {
                let t = schema.table(table);
                self.insert_batch(objects, reads)?;
                if let Some(w) = on_conflict.as_mut().and_then(|oc| oc.where_.as_mut()) {
                    self.bool_expr(w, reads, 0)?;
                }
                self.predicate(scope_check, reads)?;
                self.fields(returning, t, schema, reads, 0)?;
            }
            MutationField::Update {
                table,
                where_,
                returning,
                scope_check,
                ..
            } => {
                let t = schema.table(table);
                self.bool_expr(where_, reads, 0)?;
                self.predicate(scope_check, reads)?;
                self.fields(returning, t, schema, reads, 0)?;
            }
            MutationField::Delete {
                table,
                where_,
                returning,
                ..
            } => {
                let t = schema.table(table);
                self.bool_expr(where_, reads, 0)?;
                self.fields(returning, t, schema, reads, 0)?;
            }
            MutationField::UpdateByPk {
                table,
                selection,
                scope,
                ..
            }
            | MutationField::DeleteByPk {
                table,
                selection,
                scope,
                ..
            } => {
                let t = schema.table(table);
                self.predicate(scope, reads)?;
                self.fields(selection, t, schema, reads, 0)?;
            }
        }
        Ok(())
    }

    /// A predicate injected before this pass runs — a scope check, an
    /// `on_conflict` filter — can carry a relation chain of its own, which
    /// renders as `EXISTS` like any other.
    fn predicate(&self, pred: &mut Option<crate::ast::BoolExpr>, reads: &mut usize) -> Result<()> {
        match pred.as_mut() {
            Some(p) => self.bool_expr(p, reads, 0),
            None => Ok(()),
        }
    }

    /// Nested inserts are counted per distinct relation, not per row.
    ///
    /// The renderer batches every parent row's children into one CTE per
    /// relation name, so counting per row made a fifty-row bulk insert look like
    /// fifty subqueries when it renders two — and rejected it.
    fn insert_batch(&self, objects: &[crate::ast::InsertObject], reads: &mut usize) -> Result<()> {
        use std::collections::BTreeMap;

        let mut children: BTreeMap<(&str, bool), Vec<&crate::ast::InsertObject>> = BTreeMap::new();
        for o in objects {
            for (name, nested) in &o.nested_arrays {
                children
                    .entry((name, true))
                    .or_default()
                    .extend(nested.rows.iter());
            }
            for (name, nested) in &o.nested_objects {
                children.entry((name, false)).or_default().push(&nested.row);
            }
        }
        for rows in children.into_values() {
            self.count_read(reads)?;
            let owned: Vec<crate::ast::InsertObject> = rows.into_iter().cloned().collect();
            self.insert_batch(&owned, reads)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn args(
        &self,
        args: &mut crate::ast::QueryArgs,
        cap: bool,
        fill_default: bool,
        table: Option<&std::sync::Arc<crate::schema::Table>>,
        schema: &Schema,
        reads: &mut usize,
        depth: usize,
    ) -> Result<()> {
        if let Some(w) = args.where_.as_mut() {
            self.bool_expr(w, reads, depth)?;
        }
        // An `order_by` that walks relations reads each one's table and nests one
        // correlated subquery per hop, so the chain answers to the depth limit
        // exactly as a selection does.
        for ob in &mut args.order_by {
            self.check_depth(depth + ob.path.len())?;
            for hop in &mut ob.path {
                self.count_read(reads)?;
                // `apply_scope` fills these in, and a scope predicate can carry
                // a relation chain of its own.
                if let Some(filter) = hop.filter.as_mut() {
                    self.bool_expr(filter, reads, depth)?;
                }
            }
        }
        if cap {
            use crate::ast::Count;
            match args.limit.as_mut() {
                Some(Count::Lit(n) | Count::Bound(n)) => {
                    if let Some(max) = self.max_limit {
                        if *n > max {
                            return Err(Error::CostLimit {
                                message: format!("limit {n} is over the limit of {max}"),
                            });
                        }
                    }
                }
                Some(Count::Var { max, .. }) => {
                    // Tightest of what was already there and what applies here.
                    *max = match (*max, self.max_limit) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                }
                None => {
                    if fill_default {
                        if let Some(default) = self.default_limit {
                            args.limit = Some(Count::Lit(default));
                        }
                    }
                }
            }
        }
        if self.bind_row_counts {
            // Both, and after the default was filled in: a default limit is the
            // same text on every request, but an offset the caller varies
            // fragments the statement cache exactly as a limit does.
            for slot in [args.limit.as_mut(), args.offset.as_mut()]
                .into_iter()
                .flatten()
            {
                if let crate::ast::Count::Lit(n) = slot {
                    *slot = crate::ast::Count::Bound(*n);
                }
            }
        }
        let _ = (table, schema);
        Ok(())
    }

    fn fields(
        &self,
        fields: &mut [crate::ast::Field],
        table: Option<&std::sync::Arc<crate::schema::Table>>,
        schema: &Schema,
        reads: &mut usize,
        depth: usize,
    ) -> Result<()> {
        for f in fields.iter_mut() {
            // A relation aggregate is a correlated subquery over the target
            // table like any relation field, and nests like one.
            if let crate::ast::Field::RelationAggregate {
                name,
                args,
                nodes,
                nodes_limit,
                ..
            } = f
            {
                self.count_read(reads)?;
                self.check_depth(depth + 1)?;
                let rel = table.and_then(|t| t.find_relation(name));
                let target = rel.and_then(|r| schema.table(&r.target_table));
                // Never on the shared source, which decides what `count`
                // counts; a default meant for `nodes` goes to its own slot.
                self.args(args, true, false, target, schema, reads, depth + 1)?;
                *nodes_limit = self.nodes_default(nodes.as_deref(), args);
                if let Some(node_fields) = nodes.as_mut() {
                    self.fields(node_fields, target, schema, reads, depth + 1)?;
                }
                continue;
            }
            let crate::ast::Field::Relation {
                name,
                args,
                selection,
                ..
            } = f
            else {
                continue;
            };
            self.count_read(reads)?;
            self.check_depth(depth + 1)?;
            // Only an array relation may return many rows. An object relation
            // renders `row_to_json` over a subquery the renderer caps at one
            // row, and a default filled in here would replace that cap — turning
            // a relation that matches two rows from a quiet single answer into a
            // failed statement.
            let rel = table.and_then(|t| t.find_relation(name));
            let target = rel.and_then(|r| schema.table(&r.target_table));
            let is_array = matches!(rel.map(|r| r.kind), Some(crate::schema::RelKind::Array));
            self.args(args, true, is_array, target, schema, reads, depth + 1)?;
            self.fields(selection, target, schema, reads, depth + 1)?;
        }
        Ok(())
    }

    /// A relation inside a `where` renders as `EXISTS` and nests exactly the way
    /// a selected relation does, so it answers to the same depth limit — which
    /// is the whole reason that limit exists.
    fn bool_expr(
        &self,
        expr: &mut crate::ast::BoolExpr,
        reads: &mut usize,
        depth: usize,
    ) -> Result<()> {
        use crate::ast::BoolExpr;
        match expr {
            BoolExpr::And(parts) | BoolExpr::Or(parts) => {
                for p in parts.iter_mut() {
                    self.bool_expr(p, reads, depth)?;
                }
            }
            BoolExpr::Not(inner) => self.bool_expr(inner, reads, depth)?,
            BoolExpr::Relation { inner, .. } => {
                self.count_read(reads)?;
                self.check_depth(depth + 1)?;
                self.bool_expr(inner, reads, depth + 1)?;
            }
            BoolExpr::Compare { .. } | BoolExpr::IsNull { .. } | BoolExpr::InList { .. } => {}
        }
        Ok(())
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
        limits.apply(&mut op, &schema())?;
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
            .apply(&mut op, &schema())
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
    fn bind_row_counts_moves_literals_out_of_the_sql() {
        let schema = schema();
        let render = |limits: ExecutionLimits| {
            let mut op = lower("{ users(limit: 10, offset: 5) { id } }");
            limits.apply(&mut op, &schema).unwrap();
            crate::sql::render_now(&op, &schema, &crate::types::Inputs::none()).unwrap()
        };

        // Default: inline, and the statement reads the way the query does.
        let (sql, binds) = render(ExecutionLimits::new());
        assert!(sql.contains("LIMIT 10"), "{sql}");
        assert!(sql.contains("OFFSET 5"), "{sql}");
        assert!(binds.is_empty(), "{binds:?}");

        // Bound: one statement whatever the numbers, and they travel as binds.
        let (sql, binds) = render(ExecutionLimits::new().bind_row_counts(true));
        assert!(!sql.contains("LIMIT 10"), "{sql}");
        assert!(sql.contains("LIMIT $"), "{sql}");
        assert!(sql.contains("OFFSET $"), "{sql}");
        assert_eq!(
            binds,
            vec![crate::types::Bind::Int8(10), crate::types::Bind::Int8(5)]
        );
    }

    /// The point of the option: two page sizes, one statement.
    #[test]
    fn bound_row_counts_give_every_page_size_the_same_sql() {
        let schema = schema();
        let limits = ExecutionLimits::new().bind_row_counts(true);
        let sql_for = |n: u64| {
            let mut op = lower(&format!("{{ users(limit: {n}) {{ id }} }}"));
            limits.apply(&mut op, &schema).unwrap();
            crate::sql::render_now(&op, &schema, &crate::types::Inputs::none())
                .unwrap()
                .0
        };
        assert_eq!(sql_for(1), sql_for(1000));
    }

    #[test]
    fn a_default_limit_is_bound_too_when_asked() {
        let schema = schema();
        let mut op = lower("{ users { id } }");
        ExecutionLimits::new()
            .default_limit(25)
            .bind_row_counts(true)
            .apply(&mut op, &schema)
            .unwrap();
        let (sql, binds) =
            crate::sql::render_now(&op, &schema, &crate::types::Inputs::none()).unwrap();
        assert!(sql.contains("LIMIT $"), "{sql}");
        assert_eq!(binds, vec![crate::types::Bind::Int8(25)]);
    }

    #[test]
    fn a_bound_count_is_still_subject_to_the_ceiling() {
        let err = apply(
            "{ users(limit: 500) { id } }",
            ExecutionLimits::new().max_limit(100).bind_row_counts(true),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("over the limit of 100"), "{err}");
    }

    /// `_aggregate` renders `LIMIT` and its `nodes` returns those rows, so a
    /// ceiling that stopped at `RootBody::List` was one suffix away from being
    /// bypassed entirely.
    #[test]
    fn an_aggregate_root_answers_to_the_row_ceiling() {
        let err = apply(
            "{ users_aggregate(limit: 100000) { nodes { id } } }",
            ExecutionLimits::new().max_limit(10),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("over the limit of 10"), "{err}");

        // Through a variable too.
        let doc = crate::parser::parse_document(
            "query($n: Int) { users_aggregate(limit: $n) { nodes { id } } }",
        )
        .unwrap();
        let mut op =
            crate::parser::lower_with(&doc, crate::parser::Bindings::Symbolic, None, &schema())
                .unwrap();
        ExecutionLimits::new()
            .max_limit(10)
            .apply(&mut op, &schema())
            .unwrap();
        let Operation::Query(roots) = &op else {
            panic!()
        };
        let over = json!({"n": 999});
        let err = roots[0]
            .args
            .limit
            .clone()
            .unwrap()
            .resolve(&crate::types::Inputs::variables(&over), "limit")
            .unwrap_err();
        assert!(format!("{err}").contains("over the limit of 10"), "{err}");
    }

    /// `aggregate` and `nodes` read one source, so a `LIMIT` on it decides what
    /// `count` counts. An injected default must therefore reach `nodes` alone —
    /// it exists to bound how many rows come back, not to change the answer.
    #[test]
    fn a_default_limit_bounds_aggregate_nodes_without_touching_the_count() {
        let schema = schema();
        let render = |q: &str, limits: ExecutionLimits| {
            let mut op = lower(q);
            limits.apply(&mut op, &schema).unwrap();
            crate::sql::render_now(&op, &schema, &crate::types::Inputs::none())
                .unwrap()
                .0
        };

        // Both selected: the count sees every row, `nodes` sees 25.
        let sql = render(
            "{ users_aggregate { aggregate { count } nodes { id } } }",
            ExecutionLimits::new().default_limit(25),
        );
        assert!(sql.contains("count(*)"), "{sql}");
        assert_eq!(sql.matches("LIMIT 25").count(), 1, "{sql}");
        // The counted source carries no limit of its own.
        let counted = sql.split("'nodes'").next().unwrap();
        assert!(
            !counted.contains("LIMIT"),
            "the count must see every row: {sql}"
        );

        // `nodes` alone is bounded the same way.
        let sql = render(
            "{ users_aggregate { nodes { id } } }",
            ExecutionLimits::new().default_limit(25),
        );
        assert!(sql.contains("LIMIT 25"), "{sql}");

        // A bare count is untouched.
        let op = apply(
            "{ users_aggregate { aggregate { count } } }",
            ExecutionLimits::new().default_limit(25),
        )
        .unwrap();
        assert_eq!(limit_of(&op), None);

        // A limit the caller wrote applies to the whole field, count included:
        // they asked about that many rows.
        let sql = render(
            "{ users_aggregate(limit: 5) { aggregate { count } nodes { id } } }",
            ExecutionLimits::new().default_limit(25),
        );
        assert!(sql.contains("LIMIT 5"), "{sql}");
        assert!(!sql.contains("LIMIT 25"), "{sql}");
    }

    /// An object relation renders `row_to_json` over a subquery the renderer
    /// caps at one row. Filling a default in there replaces that cap, and a
    /// relation matching two rows then fails the whole statement.
    #[test]
    fn a_default_limit_leaves_object_relations_alone() {
        let op = apply(
            "{ posts { id user { id } } }",
            ExecutionLimits::new().default_limit(50),
        )
        .unwrap();
        let Operation::Query(roots) = &op else {
            panic!()
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!()
        };
        let rel = selection
            .iter()
            .find_map(|f| match f {
                crate::ast::Field::Relation { name, args, .. } if name == "user" => Some(args),
                _ => None,
            })
            .unwrap();
        assert_eq!(rel.limit, None, "the renderer's LIMIT 1 must survive");
    }

    /// The renderer batches every parent row's children into one CTE per
    /// relation, so counting per row rejected ordinary bulk inserts.
    #[test]
    fn a_bulk_insert_counts_relations_not_rows() {
        let rows: String = (0..50)
            .map(|i| format!("{{id: {i}, posts: {{data: [{{id: {i}}}]}}}}, "))
            .collect();
        let mut op = crate::parser::parse_and_lower(
            &format!("mutation {{ insert_users(objects: [{rows}]) {{ affected_rows }} }}"),
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        // One read for the insert, one for the batched `posts` CTE.
        ExecutionLimits::new()
            .max_table_reads(2)
            .apply(&mut op, &schema())
            .unwrap();
    }

    #[test]
    fn relation_depth_is_checked_in_where_chains_and_order_by_paths() {
        let limits = ExecutionLimits::new().max_relation_depth(2);
        // EXISTS nests one correlated subquery per hop, same as a selection.
        let err = apply(
            "{ users(where: {posts: {user: {posts: {id: {_gt: 1}}}}}) { id } }",
            limits,
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("deeper than the limit of 2"),
            "{err}"
        );
        apply(
            "{ users(where: {posts: {user: {id: {_gt: 1}}}}) { id } }",
            limits,
        )
        .unwrap();
    }

    #[test]
    fn an_on_conflict_filter_counts_as_a_read() {
        let mut op = crate::parser::parse_and_lower(
            r#"mutation { insert_users(objects: [{id: 1}], on_conflict: {
                 constraint: "users_pkey", update_columns: ["id"],
                 where: {posts: {id: {_gt: 1}}}
               }) { affected_rows } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let err = ExecutionLimits::new()
            .max_table_reads(1)
            .apply(&mut op, &schema())
            .unwrap_err();
        assert!(format!("{err}").contains("table positions"), "{err}");
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
            .apply(&mut op, &schema)
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
            .apply(&mut op, &schema())
            .unwrap_err();
        assert!(format!("{err}").contains("table positions"), "{err}");
    }
}
