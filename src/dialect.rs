//! The SQL a backend spells differently.
//!
//! The renderer in [`crate::sql`] is one body of code; where PostgreSQL and
//! SQLite disagree on a spelling — a typed placeholder, an `_in` over a bound
//! list, how a row becomes a JSON object — the renderer asks the [`Dialect`]
//! it was given rather than writing the PostgreSQL form inline.
//!
//! An enum rather than a trait, deliberately: every seam is a `match` on the
//! dialect, so adding a variant fails to compile until every seam has been
//! answered for it. A trait with default methods would let a new backend
//! inherit PostgreSQL's spelling at whichever seam nobody remembered — SQL
//! that runs and returns the wrong shape, which is the kind of bug this crate
//! hunts hardest.
//!
//! The seams return `impl Display` rather than `String`: the renderer runs
//! once per request on the uncompiled path and writes each fragment straight
//! into the statement, so a fragment is formatted in place, not allocated and
//! copied.
//!
//! # What SQLite changes
//!
//! Three things about SQLite shape this file, and the renderer's use of it:
//!
//! - **JSON is text with a subtype, and the subtype does not survive a
//!   derived table.** `json_object('k', (SELECT json_group_array(…)))` nests
//!   the array as JSON, but the same value read back out of `FROM (SELECT
//!   …) r` is a string, escaped into the response. So a row wrapper on SQLite
//!   builds the object by hand from the row's columns and wraps each JSON
//!   column in `json()` again (`Dialect::value_as_json`); `row_to_json(r)`
//!   has no counterpart.
//! - **A boolean is `0` or `1`.** Even the literal `true` renders as `1`
//!   inside `json_object`. A `Bool` column is turned back into a JSON boolean
//!   at the same place.
//! - **No arrays.** A bound list is JSON text read by `json_each`, which keeps
//!   `_in` one placeholder — the property a compiled statement depends on.

use crate::schema::ColumnType;
use crate::types::Bind;
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};

/// Which database's SQL the renderer produces. Chosen by the
/// [`Backend`](crate::Backend) the engine runs on.
///
/// Non-exhaustive for code outside this crate: a backend added later must
/// not break a downstream `match`, while inside the crate every seam still
/// has to answer for it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    /// SQLite 3.44 or later. Available with the `sqlite` feature; the
    /// variant itself is always present so a [`Schema`](crate::Schema) can
    /// say which backend it describes.
    Sqlite,
}

/// How a response value has to be treated to land in the JSON with its type
/// intact. Only SQLite tells them apart; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonKind {
    /// A number or a string: the same in SQL and in JSON.
    Plain,
    /// Already JSON — a `json`/`jsonb` column, a path read, a nested
    /// relation — that a derived table would have flattened to a string.
    Json,
    /// A boolean column, which SQLite holds as `0`/`1`.
    Bool,
}

/// The [`JsonKind`] of a column's values.
pub(crate) fn json_kind(ty: &ColumnType) -> JsonKind {
    match ty {
        ColumnType::Bool => JsonKind::Bool,
        ColumnType::Json | ColumnType::Jsonb => JsonKind::Json,
        _ => JsonKind::Plain,
    }
}

/// Double-quote an identifier, doubling any embedded quote. The same in
/// every dialect this crate targets.
pub(crate) fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a string for a single-quoted SQL literal.
pub(crate) fn escape_string_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Write `s` as a single-quoted SQL literal, escaping as it goes.
fn write_quoted(f: &mut Formatter<'_>, s: &str) -> fmt::Result {
    f.write_str("'")?;
    let mut rest = s;
    while let Some(i) = rest.find('\'') {
        f.write_str(&rest[..i])?;
        f.write_str("''")?;
        rest = &rest[i + 1..];
    }
    f.write_str(rest)?;
    f.write_str("'")
}

/// The PostgreSQL type keyword for a [`ColumnType`], as used in a cast
/// (`$1::int4`). Also what error messages name a column-less value by,
/// whichever dialect is rendering: the variants are PostgreSQL's names.
pub(crate) fn pg_type_name(ty: &ColumnType) -> Cow<'static, str> {
    Cow::Borrowed(match ty {
        ColumnType::Bool => "bool",
        ColumnType::Int2 => "int2",
        ColumnType::Int4 => "int4",
        ColumnType::Int8 => "int8",
        ColumnType::Float4 => "float4",
        ColumnType::Float8 => "float8",
        ColumnType::Text => "text",
        ColumnType::Varchar => "varchar",
        ColumnType::Uuid => "uuid",
        ColumnType::Numeric => "numeric",
        ColumnType::Timestamp => "timestamp",
        ColumnType::TimestampTz => "timestamptz",
        ColumnType::Json => "json",
        ColumnType::Jsonb => "jsonb",
        ColumnType::Date => "date",
        ColumnType::Time => "time",
        ColumnType::Enum { schema, name } => {
            return Cow::Owned(format!("{}.{}", quote_ident(schema), quote_ident(name)));
        }
    })
}

/// A JSON path in SQLite's spelling: `$.key[0]."a key"`. A component that is
/// all digits indexes an array, as it does for PostgreSQL's `#>` — with one
/// difference the README records: PostgreSQL also accepts it as an object
/// key, SQLite does not.
fn sqlite_json_path(path: &[String]) -> String {
    let mut out = String::from("$");
    for comp in path {
        if !comp.is_empty() && comp.bytes().all(|b| b.is_ascii_digit()) {
            out.push('[');
            out.push_str(comp);
            out.push(']');
        } else if !comp.is_empty() && comp.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            out.push('.');
            out.push_str(comp);
        } else {
            out.push_str(".\"");
            out.push_str(&comp.replace('"', "\\\""));
            out.push('"');
        }
    }
    out
}

/// A fragment that formats itself on demand.
struct Fragment<F>(F);

impl<F: Fn(&mut Formatter<'_>) -> fmt::Result> Display for Fragment<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        (self.0)(f)
    }
}

impl Dialect {
    /// A placeholder for a value of column type `ty`.
    ///
    /// PostgreSQL gets an explicit cast: sqlx prepares the statement with the
    /// types of its first execution's parameters, and a compiled statement
    /// first run with a null variable would otherwise fix the wrong type.
    /// SQLite types values, not parameters, and gets a numbered placeholder
    /// — numbered rather than `?`, so a placeholder used twice (the optional
    /// forms) is one parameter and the binds stay in step with the numbers.
    pub(crate) fn param<'a>(self, n: usize, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::{}", pg_type_name(ty)),
            Dialect::Sqlite => write!(f, "?{n}"),
        })
    }

    /// A typed SQL NULL, for an inserted column the object left out.
    pub(crate) fn null_of<'a>(self, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "NULL::{}", pg_type_name(ty)),
            Dialect::Sqlite => f.write_str("NULL"),
        })
    }

    /// A boolean placeholder (`_is_null: $b`).
    pub(crate) fn bool_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::boolean"),
            Dialect::Sqlite => write!(f, "?{n}"),
        })
    }

    /// A `limit` / `offset` placeholder.
    pub(crate) fn count_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::int8"),
            Dialect::Sqlite => write!(f, "?{n}"),
        })
    }

    /// A placeholder carrying a JSON document the renderer answered itself
    /// (introspection riding along in a data query).
    pub(crate) fn json_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::json"),
            Dialect::Sqlite => write!(f, "json(?{n})"),
        })
    }

    /// A placeholder for a bound list of `ty`, as a value: what `IS NULL` is
    /// asked of when the list is optional.
    pub(crate) fn list_param<'a>(self, n: usize, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::{}[]", pg_type_name(ty)),
            Dialect::Sqlite => write!(f, "?{n}"),
        })
    }

    /// `lhs` is (or, `negated`, is not) one of the bound list `n`. One
    /// placeholder whatever the list's length, so the SQL text of a compiled
    /// statement does not depend on the request.
    pub(crate) fn in_list<'a>(
        self,
        lhs: impl Display + 'a,
        n: usize,
        ty: &'a ColumnType,
        negated: bool,
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => {
                let pred = if negated { "<> ALL" } else { "= ANY" };
                write!(f, "{lhs} {pred} ({})", self.list_param(n, ty))
            }
            Dialect::Sqlite => {
                let pred = if negated { "NOT IN" } else { "IN" };
                write!(f, "{lhs} {pred} (SELECT value FROM json_each(?{n}))")
            }
        })
    }

    /// `lhs <op> rhs`, in the dialect's spelling of the operator.
    ///
    /// SQLite's `LIKE` has no default escape character (PostgreSQL's is `\`),
    /// so one is named; and it has no `ILIKE`, so case-insensitive matching
    /// lower-cases both sides — ASCII only, where PostgreSQL folds Unicode.
    /// Case-sensitive `LIKE` itself is a per-connection pragma there; see
    /// [`crate::sqlite::connect_options`].
    pub(crate) fn compare<'a>(
        self,
        lhs: impl Display + 'a,
        op: crate::ast::CmpOp,
        rhs: impl Display + 'a,
    ) -> impl Display + 'a {
        use crate::ast::CmpOp;
        Fragment(move |f: &mut Formatter<'_>| {
            let plain = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
                CmpOp::Like | CmpOp::ILike | CmpOp::NLike | CmpOp::NILike => {
                    return match (self, op) {
                        (Dialect::Postgres, CmpOp::Like) => write!(f, "{lhs} LIKE {rhs}"),
                        (Dialect::Postgres, CmpOp::ILike) => write!(f, "{lhs} ILIKE {rhs}"),
                        (Dialect::Postgres, CmpOp::NLike) => write!(f, "{lhs} NOT LIKE {rhs}"),
                        (Dialect::Postgres, CmpOp::NILike) => {
                            write!(f, "{lhs} NOT ILIKE {rhs}")
                        }
                        (Dialect::Sqlite, CmpOp::Like) => {
                            write!(f, "{lhs} LIKE {rhs} ESCAPE '\\'")
                        }
                        (Dialect::Sqlite, CmpOp::NLike) => {
                            write!(f, "{lhs} NOT LIKE {rhs} ESCAPE '\\'")
                        }
                        (Dialect::Sqlite, CmpOp::ILike) => {
                            write!(f, "lower({lhs}) LIKE lower({rhs}) ESCAPE '\\'")
                        }
                        (Dialect::Sqlite, CmpOp::NILike) => {
                            write!(f, "lower({lhs}) NOT LIKE lower({rhs}) ESCAPE '\\'")
                        }
                        _ => unreachable!("only the pattern operators reach here"),
                    };
                }
            };
            write!(f, "{lhs} {plain} {rhs}")
        })
    }

    /// Opens a JSON object built from alternating `'key', value` arguments;
    /// the caller closes the parenthesis.
    pub(crate) fn json_object_open(self) -> &'static str {
        match self {
            Dialect::Postgres => "json_build_object(",
            Dialect::Sqlite => "json_object(",
        }
    }

    /// A row's value, as the JSON it should be in the response.
    ///
    /// The identity on PostgreSQL, where every column type has a JSON
    /// rendering of its own. On SQLite a boolean is `0`/`1` and a JSON column
    /// is text, and both are put right here — at the point the value enters
    /// `json_object`, which is the only place SQLite will take the hint.
    pub(crate) fn value_as_json<'a>(
        self,
        expr: impl Display + 'a,
        kind: JsonKind,
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match (self, kind) {
            (Dialect::Postgres, _) | (Dialect::Sqlite, JsonKind::Plain) => write!(f, "{expr}"),
            (Dialect::Sqlite, JsonKind::Json) => write!(f, "json({expr})"),
            // Anything but 0, 1 and NULL is a value the column's declared
            // type does not admit — text in a BOOLEAN column, say. Not null,
            // not a guess at truthiness ('true' is falsy to SQLite): an
            // error, raised the one way SQLite has outside a trigger,
            // which the executor recognises and names.
            (Dialect::Sqlite, JsonKind::Bool) => write!(
                f,
                "CASE WHEN {expr} IS NULL THEN NULL WHEN {expr} = 1 THEN json('true') \
                 WHEN {expr} = 0 THEN json('false') ELSE json('not a boolean') END"
            ),
        })
    }

    /// The JSON object for one row of the derived table aliased `row_alias`,
    /// whose response keys and kinds are `shape`.
    fn row_object<'a>(
        self,
        row_alias: &'a str,
        shape: &'a [(String, JsonKind)],
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "row_to_json({row_alias})"),
            Dialect::Sqlite => {
                f.write_str("json_object(")?;
                for (i, (key, kind)) in shape.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write_quoted(f, key)?;
                    f.write_str(", ")?;
                    let col = format!("{row_alias}.{}", quote_ident(key));
                    write!(f, "{}", self.value_as_json(col, *kind))?;
                }
                f.write_str(")")
            }
        })
    }

    /// Opens `(SELECT <rows of the following derived table as a JSON array>
    /// FROM (`; the caller renders the inner select and closes with
    /// `) <row_alias>)`. An empty result is `[]`, not null.
    ///
    /// `shape` is the inner select's output columns as response keys, with
    /// what each holds; PostgreSQL does not need it (`row_to_json` reads the
    /// row), SQLite builds the object from it.
    pub(crate) fn rows_list_open<'a>(
        self,
        row_alias: &'a str,
        shape: &'a [(String, JsonKind)],
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(
                f,
                "(SELECT coalesce(json_agg({}), '[]'::json) FROM (",
                self.row_object(row_alias, shape)
            ),
            // Rows arrive in the derived table's order and json_group_array
            // takes them as they come, as json_agg does on PostgreSQL. Zero
            // rows is `[]` without help.
            Dialect::Sqlite => write!(
                f,
                "(SELECT json_group_array({}) FROM (",
                self.row_object(row_alias, shape)
            ),
        })
    }

    /// As [`rows_list_open`](Self::rows_list_open) for the one row of a
    /// `_by_pk` or an object relation: a JSON object, or null when there is
    /// no row.
    pub(crate) fn row_object_open<'a>(
        self,
        row_alias: &'a str,
        shape: &'a [(String, JsonKind)],
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres | Dialect::Sqlite => {
                write!(f, "(SELECT {} FROM (", self.row_object(row_alias, shape))
            }
        })
    }

    /// Opens an aggregation of the following JSON objects into an array;
    /// closed by [`json_agg_close`](Self::json_agg_close). Empty is `[]`.
    pub(crate) fn json_agg_open(self) -> &'static str {
        match self {
            Dialect::Postgres => "coalesce(json_agg(",
            Dialect::Sqlite => "json_group_array(",
        }
    }

    pub(crate) fn json_agg_close(self) -> &'static str {
        match self {
            Dialect::Postgres => "), '[]'::json)",
            Dialect::Sqlite => ")",
        }
    }

    /// The literal empty JSON array, typed so it nests as JSON.
    pub(crate) fn empty_json_array(self) -> &'static str {
        match self {
            Dialect::Postgres => "'[]'::json",
            Dialect::Sqlite => "json('[]')",
        }
    }

    /// The literal empty JSON object.
    pub(crate) fn empty_json_object(self) -> &'static str {
        match self {
            Dialect::Postgres => "'{}'::json",
            Dialect::Sqlite => "json('{}')",
        }
    }

    /// A string literal that nests into JSON as a string: `__typename` and
    /// friends. PostgreSQL needs the cast, or `row_to_json` sees an
    /// `unknown`-typed constant.
    pub(crate) fn text_literal<'a>(self, s: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => {
                write_quoted(f, s)?;
                f.write_str("::text")
            }
            Dialect::Sqlite => write_quoted(f, s),
        })
    }

    /// Read inside a JSON column along the path bound as parameter `n` (see
    /// [`json_path_bind`](Self::json_path_bind)). The result keeps its JSON
    /// type so it nests unchanged — on SQLite through `->`, whose result is
    /// JSON where `json_extract`'s is SQL.
    pub(crate) fn json_path<'a>(self, col: &'a str, n: usize) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "{col} #> ${n}::text[]"),
            Dialect::Sqlite => write!(f, "{col} -> ?{n}"),
        })
    }

    /// The bind carrying a JSON path's components.
    pub(crate) fn json_path_bind(self, path: &[String]) -> Bind {
        match self {
            Dialect::Postgres => Bind::TextArray(path.iter().map(|c| Some(c.clone())).collect()),
            Dialect::Sqlite => Bind::Text(sqlite_json_path(path)),
        }
    }

    /// `ASC` / `DESC` with where NULLs go.
    ///
    /// The two databases disagree on the default: PostgreSQL sorts NULLs last
    /// on `ASC` and first on `DESC`, SQLite the other way round. A document
    /// that says nothing gets PostgreSQL's answer on both, spelled out where
    /// it would otherwise differ — the same `order_by` sorting the same rows
    /// two ways depending on the backend is a wrong answer that looks right.
    pub(crate) fn order_dir(
        self,
        direction: crate::ast::OrderDir,
        nulls: Option<crate::ast::NullsOrder>,
    ) -> &'static str {
        use crate::ast::{NullsOrder, OrderDir};
        match (direction, nulls, self) {
            (OrderDir::Asc, None, Dialect::Postgres) => " ASC",
            (OrderDir::Desc, None, Dialect::Postgres) => " DESC",
            (OrderDir::Asc, None, Dialect::Sqlite) => " ASC NULLS LAST",
            (OrderDir::Desc, None, Dialect::Sqlite) => " DESC NULLS FIRST",
            (OrderDir::Asc, Some(NullsOrder::First), _) => " ASC NULLS FIRST",
            (OrderDir::Asc, Some(NullsOrder::Last), _) => " ASC NULLS LAST",
            (OrderDir::Desc, Some(NullsOrder::First), _) => " DESC NULLS FIRST",
            (OrderDir::Desc, Some(NullsOrder::Last), _) => " DESC NULLS LAST",
        }
    }

    /// Whether a row wrapper builds the row's JSON object by hand and so
    /// needs the row's shape ([`rows_list_open`](Self::rows_list_open)).
    pub(crate) fn builds_row_objects(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite => true,
        }
    }

    /// Whether `count((a, b))` — a row constructor as the counted value — is
    /// SQL here. SQLite has row values but rejects one as an aggregate's
    /// argument, and has no other one-expression spelling of "distinct
    /// pairs", so `count(columns: [a, b])` is refused there.
    pub(crate) fn counts_tuples(self) -> bool {
        match self {
            Dialect::Postgres => true,
            Dialect::Sqlite => false,
        }
    }

    /// Whether `OFFSET` may only follow a `LIMIT`. SQLite's grammar says so;
    /// the renderer supplies `LIMIT -1` (no limit) in front of a bare offset.
    pub(crate) fn offset_needs_limit(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite => true,
        }
    }

    /// Whether `distinct_on` has to be spelled as a `row_number()` window
    /// over a derived table, for want of `DISTINCT ON`.
    pub(crate) fn distinct_on_by_window(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite => true,
        }
    }

    /// Whether the database has this aggregate function. SQLite ships no
    /// statistical aggregates; the type system publishes none for it, and the
    /// renderer refuses one so the builder path cannot reach a function the
    /// database would report as unknown.
    pub(crate) fn supports_agg(self, func: crate::ast::AggFunc) -> bool {
        use crate::ast::AggFunc;
        match (self, func) {
            (Dialect::Postgres, _) => true,
            (Dialect::Sqlite, AggFunc::Sum | AggFunc::Avg | AggFunc::Max | AggFunc::Min) => true,
            (
                Dialect::Sqlite,
                AggFunc::Stddev
                | AggFunc::StddevPop
                | AggFunc::StddevSamp
                | AggFunc::Variance
                | AggFunc::VarPop
                | AggFunc::VarSamp,
            ) => false,
        }
    }

    /// Whether a mutation is a sequence of statements ([`crate::plan`])
    /// rather than one statement of data-modifying CTEs. SQLite allows no
    /// DML inside a CTE.
    pub(crate) fn mutations_by_plan(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_literal_escapes_embedded_quotes() {
        assert_eq!(
            Dialect::Postgres.text_literal("it's 'q'").to_string(),
            "'it''s ''q'''::text"
        );
        assert_eq!(Dialect::Postgres.text_literal("").to_string(), "''::text");
        assert_eq!(Dialect::Sqlite.text_literal("it's").to_string(), "'it''s'");
    }

    #[test]
    fn sqlite_json_paths() {
        let p = |s: &[&str]| sqlite_json_path(&s.iter().map(|c| c.to_string()).collect::<Vec<_>>());
        assert_eq!(p(&["tags", "0"]), "$.tags[0]");
        assert_eq!(p(&["a b", "c\"d"]), "$.\"a b\".\"c\\\"d\"");
        assert_eq!(p(&[]), "$");
    }

    #[test]
    fn sqlite_spells_the_postgres_null_order_out() {
        use crate::ast::{NullsOrder, OrderDir};
        assert_eq!(
            Dialect::Sqlite.order_dir(OrderDir::Asc, None),
            " ASC NULLS LAST"
        );
        assert_eq!(
            Dialect::Sqlite.order_dir(OrderDir::Desc, None),
            " DESC NULLS FIRST"
        );
        assert_eq!(Dialect::Postgres.order_dir(OrderDir::Asc, None), " ASC");
        assert_eq!(
            Dialect::Postgres.order_dir(OrderDir::Desc, Some(NullsOrder::Last)),
            " DESC NULLS LAST"
        );
    }
}
