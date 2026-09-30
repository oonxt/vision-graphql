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
//!
//! # What MySQL changes
//!
//! MySQL 8 has a real JSON type that survives derived tables, so the SQLite
//! re-wrapping is not needed; what it lacks is elsewhere:
//!
//! - **`JSON_ARRAYAGG` takes no `ORDER BY`**, and does not keep the order of
//!   a derived table it aggregates (it happens to when a `LIMIT` forces the
//!   table to be materialised, which is worse than never). The one spelling
//!   that orders by contract is the window form: the inner select numbers
//!   its rows in the order asked for (`ROW_NUMBER() OVER (ORDER BY …)`), and
//!   the aggregate runs as a window over the whole frame ordered by that
//!   number. See [`Dialect::rows_list_open`] and
//!   [`Dialect::numbers_rows_for_order`].
//! - **Placeholders are anonymous.** `?` only, no `?3`: a parameter used
//!   twice has to be bound twice. The renderer writes numbered placeholders
//!   as for SQLite and [`Dialect::anonymise_placeholders`] rewrites the
//!   statement once it is complete, repeating the bind specs in occurrence
//!   order.
//! - **A `TINYINT(1)` is `0`/`1`** inside `JSON_OBJECT`, as on SQLite; and a
//!   `DATETIME` renders as `2026-01-02 03:04:05.000000`, a `TIMESTAMP` in
//!   the session's time zone. Both are put right at the point the value
//!   enters the JSON ([`Dialect::value_as_json`]) so the response carries
//!   what PostgreSQL's would: a JSON boolean, an ISO 8601 string, UTC with
//!   its offset.
//! - **`JSON_OBJECT` sorts its keys.** The response comes back with the keys
//!   of every object in MySQL's order, not the selection's; the engine puts
//!   them back in selection order after decoding (see
//!   [`crate::sql::KeyOrder`]).
//! - **Collation decides case.** `LIKE` is case-insensitive under the
//!   default collations, so `_like` names a binary collation on its pattern.

use crate::schema::ColumnType;
use crate::types::{Bind, BindSpec};
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter, Write as _};

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
    /// MySQL 8.0.19 or later. Available with the `mysql` feature; the
    /// variant is always present, as `Sqlite` is. MariaDB is not MySQL
    /// here — its JSON is text and it lacks `JSON_TABLE` and `MEMBER OF` —
    /// and [`crate::mysql::verify`] refuses it.
    MySql,
}

/// How a response value has to be treated to land in the JSON with its type
/// intact. PostgreSQL treats them all alike; see the module docs for what
/// SQLite and MySQL do with each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonKind {
    /// A number or a string: the same in SQL and in JSON.
    Plain,
    /// Already JSON — a `json`/`jsonb` column, a path read, a nested
    /// relation — that a derived table would have flattened to a string.
    Json,
    /// A boolean column, which SQLite and MySQL hold as `0`/`1`.
    Bool,
    /// A timestamp without a zone, which MySQL prints with a space where
    /// ISO 8601 has a `T`.
    Timestamp,
    /// A timestamp with a zone, which MySQL prints in the session's zone
    /// with no offset to say so.
    TimestampTz,
}

/// The [`JsonKind`] of a column's values.
pub(crate) fn json_kind(ty: &ColumnType) -> JsonKind {
    match ty {
        ColumnType::Bool => JsonKind::Bool,
        ColumnType::Json | ColumnType::Jsonb => JsonKind::Json,
        ColumnType::Timestamp => JsonKind::Timestamp,
        ColumnType::TimestampTz => JsonKind::TimestampTz,
        _ => JsonKind::Plain,
    }
}

/// Quote an identifier: double quotes, doubling any embedded quote, or
/// backticks on MySQL, whose double quotes delimit a string unless the
/// session says otherwise — and a session that does not say makes
/// `SELECT "id" FROM t` return the word `id` on every row.
pub(crate) fn quote_ident(s: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Postgres | Dialect::Sqlite => format!("\"{}\"", s.replace('"', "\"\"")),
        Dialect::MySql => format!("`{}`", s.replace('`', "``")),
    }
}

/// Escape a string for a single-quoted SQL literal. MySQL reads a backslash
/// as an escape character (unless the session's `sql_mode` says otherwise,
/// which [`crate::mysql::verify`] refuses), so it is doubled there.
pub(crate) fn escape_string_literal(s: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Postgres | Dialect::Sqlite => s.replace('\'', "''"),
        Dialect::MySql => s.replace('\\', "\\\\").replace('\'', "''"),
    }
}

/// Write `s` as a single-quoted SQL literal, escaping as it goes.
fn write_quoted(f: &mut Formatter<'_>, s: &str, dialect: Dialect) -> fmt::Result {
    let backslashes = matches!(dialect, Dialect::MySql);
    f.write_str("'")?;
    for c in s.chars() {
        match c {
            '\'' => f.write_str("''")?,
            '\\' if backslashes => f.write_str("\\\\")?,
            c => f.write_char(c)?,
        }
    }
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
            return Cow::Owned(format!(
                "{}.{}",
                quote_ident(schema, Dialect::Postgres),
                quote_ident(name, Dialect::Postgres)
            ));
        }
    })
}

/// A JSON path in the `$.key[0]."a key"` spelling SQLite and MySQL share. A
/// component that is all digits indexes an array, as it does for
/// PostgreSQL's `#>` — with one difference the README records: PostgreSQL
/// also accepts it as an object key, these two do not.
fn dollar_json_path(path: &[String]) -> String {
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

/// The column a dialect that [numbers rows for
/// order](Dialect::numbers_rows_for_order) adds to a derived table: the
/// row's position under the `ORDER BY` asked for. Prefixed and unquoted;
/// not a name a column can be exposed as through the GraphQL path.
pub(crate) const ORDER_NUMBER: &str = "__vision_graphql_ord";

/// The MySQL type `JSON_TABLE` reads a bound list's elements as: what an
/// `_in` compares the column against. `TEXT` rather than `CHAR(n)`, which
/// truncates a longer element and then matches nothing, silently.
fn mysql_json_table_type(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Bool => "TINYINT",
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => "BIGINT",
        ColumnType::Float4 | ColumnType::Float8 => "DOUBLE",
        ColumnType::Numeric => "DECIMAL(65,30)",
        ColumnType::Text
        | ColumnType::Varchar
        | ColumnType::Uuid
        | ColumnType::Enum { .. }
        | ColumnType::Json
        | ColumnType::Jsonb => "TEXT",
        ColumnType::Timestamp | ColumnType::TimestampTz => "DATETIME(6)",
        ColumnType::Date => "DATE",
        ColumnType::Time => "TIME(6)",
    }
}

/// A MySQL `DATETIME`/`TIMESTAMP` as the ISO 8601 string PostgreSQL's JSON
/// carries: `T` between date and time, the fraction only when there is one,
/// and for a `TIMESTAMP` — stored as UTC, read back in the session's zone —
/// converted to UTC and suffixed `+00:00`.
fn mysql_iso_timestamp(f: &mut Formatter<'_>, expr: &dyn Display, zoned: bool) -> fmt::Result {
    let (value, suffix) = if zoned {
        (
            format!("CONVERT_TZ({expr}, @@session.time_zone, '+00:00')"),
            "+00:00",
        )
    } else {
        (expr.to_string(), "")
    };
    write!(
        f,
        "CASE WHEN {expr} IS NULL THEN NULL WHEN MICROSECOND({value}) = 0 THEN \
         DATE_FORMAT({value}, '%Y-%m-%dT%H:%i:%s{suffix}') ELSE \
         DATE_FORMAT({value}, '%Y-%m-%dT%H:%i:%s.%f{suffix}') END"
    )
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
    /// MySQL is written the same way and rewritten once the statement is
    /// complete ([`anonymise_placeholders`]); a decimal is cast so the
    /// comparison is exact (a string against a `DECIMAL` column compares as
    /// doubles), and a JSON value so it compares as JSON.
    pub(crate) fn param<'a>(self, n: usize, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::{}", pg_type_name(ty)),
            Dialect::Sqlite => write!(f, "?{n}"),
            Dialect::MySql => match ty {
                ColumnType::Numeric => write!(f, "CAST(?{n} AS DECIMAL(65,30))"),
                ColumnType::Json | ColumnType::Jsonb => write!(f, "CAST(?{n} AS JSON)"),
                _ => write!(f, "?{n}"),
            },
        })
    }

    /// A typed SQL NULL, for an inserted column the object left out.
    pub(crate) fn null_of<'a>(self, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "NULL::{}", pg_type_name(ty)),
            Dialect::Sqlite | Dialect::MySql => f.write_str("NULL"),
        })
    }

    /// A boolean placeholder (`_is_null: $b`).
    pub(crate) fn bool_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::boolean"),
            Dialect::Sqlite | Dialect::MySql => write!(f, "?{n}"),
        })
    }

    /// A `limit` / `offset` placeholder.
    pub(crate) fn count_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::int8"),
            Dialect::Sqlite | Dialect::MySql => write!(f, "?{n}"),
        })
    }

    /// A placeholder carrying a JSON document the renderer answered itself
    /// (introspection riding along in a data query).
    pub(crate) fn json_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::json"),
            Dialect::Sqlite => write!(f, "json(?{n})"),
            Dialect::MySql => write!(f, "CAST(?{n} AS JSON)"),
        })
    }

    /// A placeholder for a bound list of `ty`, as a value: what `IS NULL` is
    /// asked of when the list is optional, and what the key-set operators
    /// take. On MySQL a bound list is JSON text, and this is it as JSON.
    pub(crate) fn list_param<'a>(self, n: usize, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::{}[]", pg_type_name(ty)),
            Dialect::Sqlite => write!(f, "?{n}"),
            Dialect::MySql => write!(f, "CAST(?{n} AS JSON)"),
        })
    }

    /// `lhs` is (or, `negated`, is not) one of the bound list `n`. One
    /// placeholder whatever the list's length, so the SQL text of a compiled
    /// statement does not depend on the request.
    ///
    /// MySQL reads the list through `JSON_TABLE`, typed as the column is: a
    /// JSON string against a `DATETIME` column would otherwise compare as a
    /// JSON value against a JSON datetime, which is never equal.
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
            Dialect::MySql => {
                let pred = if negated { "NOT IN" } else { "IN" };
                let value = match ty {
                    ColumnType::Json | ColumnType::Jsonb => "CAST(jt.v AS JSON)",
                    _ => "jt.v",
                };
                write!(
                    f,
                    "{lhs} {pred} (SELECT {value} FROM JSON_TABLE(?{n}, '$[*]' COLUMNS (v {} \
                     PATH '$')) AS jt)",
                    mysql_json_table_type(ty)
                )
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
    ///
    /// MySQL's `LIKE` follows the column's collation, case-insensitive under
    /// the defaults, so `_like` names a binary collation on the pattern (the
    /// column side converts to it); `_ilike` lower-cases both sides, which
    /// MySQL's `LOWER` does for Unicode. The jsonb operators are spelled with
    /// the JSON functions, by the JSON type of the column's value, so that a
    /// key test on an array asks about its elements as PostgreSQL's does.
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
                        (Dialect::MySql, CmpOp::Like) => {
                            write!(f, "{lhs} LIKE {rhs} COLLATE utf8mb4_bin")
                        }
                        (Dialect::MySql, CmpOp::NLike) => {
                            write!(f, "{lhs} NOT LIKE {rhs} COLLATE utf8mb4_bin")
                        }
                        (Dialect::MySql, CmpOp::ILike) => {
                            write!(f, "LOWER({lhs}) LIKE LOWER({rhs})")
                        }
                        (Dialect::MySql, CmpOp::NILike) => {
                            write!(f, "LOWER({lhs}) NOT LIKE LOWER({rhs})")
                        }
                        _ => unreachable!("only the pattern operators reach here"),
                    };
                }
                // The operators rather than `jsonb_exists` and friends: a GIN
                // index on the column serves `@>`, `?`, `?|` and `?&`, and
                // does not serve the functions. A `?` here is no placeholder —
                // PostgreSQL's are `$n` and nothing rewrites them.
                CmpOp::Contains
                | CmpOp::ContainedIn
                | CmpOp::HasKey
                | CmpOp::HasKeysAny
                | CmpOp::HasKeysAll => match self {
                    Dialect::Postgres => match op {
                        CmpOp::Contains => "@>",
                        CmpOp::ContainedIn => "<@",
                        CmpOp::HasKey => "?",
                        CmpOp::HasKeysAny => "?|",
                        CmpOp::HasKeysAll => "?&",
                        _ => unreachable!("only the jsonb operators reach here"),
                    },
                    Dialect::Sqlite => {
                        unreachable!("refused by supports_cmp before rendering")
                    }
                    Dialect::MySql => {
                        return match op {
                            CmpOp::Contains => write!(f, "JSON_CONTAINS({lhs}, {rhs})"),
                            CmpOp::ContainedIn => write!(f, "JSON_CONTAINS({rhs}, {lhs})"),
                            // `?` on an object asks about its keys, on an
                            // array about its string elements, on a string
                            // about the string itself; anything else is
                            // false, and a null is null.
                            CmpOp::HasKey => write!(
                                f,
                                "CASE WHEN {lhs} IS NULL THEN NULL \
                                 WHEN JSON_TYPE({lhs}) = 'OBJECT' THEN \
                                 JSON_CONTAINS_PATH({lhs}, 'one', CONCAT('$.', JSON_QUOTE({rhs}))) \
                                 WHEN JSON_TYPE({lhs}) = 'ARRAY' THEN \
                                 JSON_CONTAINS({lhs}, JSON_QUOTE({rhs})) \
                                 WHEN JSON_TYPE({lhs}) = 'STRING' THEN JSON_UNQUOTE({lhs}) = {rhs} \
                                 ELSE FALSE END"
                            ),
                            CmpOp::HasKeysAny => write!(
                                f,
                                "CASE WHEN {lhs} IS NULL THEN NULL \
                                 WHEN JSON_TYPE({lhs}) = 'OBJECT' THEN \
                                 JSON_OVERLAPS(JSON_KEYS({lhs}), {rhs}) \
                                 WHEN JSON_TYPE({lhs}) = 'ARRAY' THEN JSON_OVERLAPS({lhs}, {rhs}) \
                                 WHEN JSON_TYPE({lhs}) = 'STRING' THEN JSON_CONTAINS({rhs}, {lhs}) \
                                 ELSE FALSE END"
                            ),
                            CmpOp::HasKeysAll => write!(
                                f,
                                "CASE WHEN {lhs} IS NULL THEN NULL \
                                 WHEN JSON_TYPE({lhs}) = 'OBJECT' THEN \
                                 JSON_CONTAINS(JSON_KEYS({lhs}), {rhs}) \
                                 WHEN JSON_TYPE({lhs}) = 'ARRAY' THEN JSON_CONTAINS({lhs}, {rhs}) \
                                 WHEN JSON_TYPE({lhs}) = 'STRING' THEN \
                                 JSON_CONTAINS(JSON_ARRAY({lhs}), {rhs}) \
                                 ELSE FALSE END"
                            ),
                            _ => unreachable!("only the jsonb operators reach here"),
                        };
                    }
                },
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
            Dialect::MySql => "JSON_OBJECT(",
        }
    }

    /// A row's value, as the JSON it should be in the response.
    ///
    /// The identity on PostgreSQL, where every column type has a JSON
    /// rendering of its own. On SQLite a boolean is `0`/`1` and a JSON column
    /// is text, and both are put right here — at the point the value enters
    /// `json_object`, which is the only place SQLite will take the hint. On
    /// MySQL a boolean is `0`/`1` too, and a timestamp is spelled the way
    /// MySQL spells one; see the module docs.
    pub(crate) fn value_as_json<'a>(
        self,
        expr: impl Display + 'a,
        kind: JsonKind,
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match (self, kind) {
            (Dialect::Postgres, _)
            | (Dialect::Sqlite, JsonKind::Plain | JsonKind::Timestamp | JsonKind::TimestampTz)
            | (Dialect::MySql, JsonKind::Plain | JsonKind::Json) => write!(f, "{expr}"),
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
            // A `TINYINT(1)` holds -128..127 and the type enforces nothing
            // narrower; MySQL's own reading of it as a boolean is "not
            // zero", which is what a client that wrote it expects back.
            (Dialect::MySql, JsonKind::Bool) => write!(
                f,
                "CASE WHEN {expr} IS NULL THEN NULL WHEN {expr} THEN CAST('true' AS JSON) \
                 ELSE CAST('false' AS JSON) END"
            ),
            (Dialect::MySql, JsonKind::Timestamp) => mysql_iso_timestamp(f, &expr, false),
            (Dialect::MySql, JsonKind::TimestampTz) => mysql_iso_timestamp(f, &expr, true),
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
            Dialect::Sqlite | Dialect::MySql => {
                f.write_str(self.json_object_open())?;
                for (i, (key, kind)) in shape.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write_quoted(f, key, self)?;
                    f.write_str(", ")?;
                    let col = format!("{row_alias}.{}", quote_ident(key, self));
                    write!(f, "{}", self.value_as_json(col, *kind))?;
                }
                f.write_str(")")
            }
        })
    }

    /// Opens `(SELECT <rows of the following derived table as a JSON array>
    /// FROM (`; the caller renders the inner select and closes with
    /// [`rows_list_close`](Self::rows_list_close). An empty result is `[]`,
    /// not null.
    ///
    /// `shape` is the inner select's output columns as response keys, with
    /// what each holds; PostgreSQL does not need it (`row_to_json` reads the
    /// row), SQLite and MySQL build the object from it. On MySQL the inner
    /// select also carries [`ORDER_NUMBER`], which the window aggregates by.
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
            // The window form: every row of the frame gets the whole array,
            // in frame order, and the outer LIMIT 1 keeps one. Zero rows is
            // zero rows, hence the COALESCE outside the subquery.
            Dialect::MySql => write!(
                f,
                "COALESCE((SELECT JSON_ARRAYAGG({}) OVER (ORDER BY {row_alias}.{ORDER_NUMBER} \
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (",
                self.row_object(row_alias, shape)
            ),
        })
    }

    /// Closes what [`rows_list_open`](Self::rows_list_open) opened.
    pub(crate) fn rows_list_close<'a>(self, row_alias: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres | Dialect::Sqlite => write!(f, ") {row_alias})"),
            Dialect::MySql => write!(f, ") {row_alias} LIMIT 1), JSON_ARRAY())"),
        })
    }

    /// As [`rows_list_open`](Self::rows_list_open) for the one row of a
    /// `_by_pk` or an object relation: a JSON object, or null when there is
    /// no row. Closed with `) <row_alias>)`.
    pub(crate) fn row_object_open<'a>(
        self,
        row_alias: &'a str,
        shape: &'a [(String, JsonKind)],
    ) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres | Dialect::Sqlite | Dialect::MySql => {
                write!(f, "(SELECT {} FROM (", self.row_object(row_alias, shape))
            }
        })
    }

    /// Opens an aggregation of the following JSON objects into an array;
    /// closed by [`json_agg_close`](Self::json_agg_close). Empty is `[]`.
    /// The rows arrive in their source's order on PostgreSQL and SQLite; a
    /// dialect that [numbers rows](Self::numbers_rows_for_order) reads
    /// `nodes` from a source of its own instead
    /// ([`nodes_list_open`](Self::nodes_list_open)).
    pub(crate) fn json_agg_open(self) -> &'static str {
        match self {
            Dialect::Postgres => "coalesce(json_agg(",
            Dialect::Sqlite => "json_group_array(",
            Dialect::MySql => "COALESCE(JSON_ARRAYAGG(",
        }
    }

    pub(crate) fn json_agg_close(self) -> &'static str {
        match self {
            Dialect::Postgres => "), '[]'::json)",
            Dialect::Sqlite => ")",
            Dialect::MySql => "), JSON_ARRAY())",
        }
    }

    /// Whether `nodes` under an aggregate has to read the source on its own,
    /// apart from the aggregate functions. MySQL: the ordered aggregation is
    /// a window function, and a window beside `count(*)` runs over the one
    /// grouped row.
    pub(crate) fn nodes_need_own_source(self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Sqlite => false,
            Dialect::MySql => true,
        }
    }

    /// `nodes` over a source of its own, in three parts around the object
    /// and the source: `<open> object <mid> source <close>`.
    pub(crate) fn nodes_list_open(self) -> &'static str {
        match self {
            Dialect::Postgres => "(SELECT coalesce(json_agg(",
            Dialect::Sqlite => "(SELECT json_group_array(",
            Dialect::MySql => "COALESCE((SELECT JSON_ARRAYAGG(",
        }
    }

    pub(crate) fn nodes_list_mid<'a>(self, alias: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => f.write_str("), '[]'::json) FROM ("),
            Dialect::Sqlite => f.write_str(") FROM ("),
            Dialect::MySql => write!(
                f,
                ") OVER (ORDER BY {alias}.{ORDER_NUMBER} ROWS BETWEEN UNBOUNDED PRECEDING AND \
                 UNBOUNDED FOLLOWING) FROM ("
            ),
        })
    }

    pub(crate) fn nodes_list_close<'a>(self, alias: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres | Dialect::Sqlite => write!(f, ") {alias})"),
            Dialect::MySql => write!(f, ") {alias} LIMIT 1), JSON_ARRAY())"),
        })
    }

    /// The literal empty JSON array, typed so it nests as JSON.
    pub(crate) fn empty_json_array(self) -> &'static str {
        match self {
            Dialect::Postgres => "'[]'::json",
            Dialect::Sqlite => "json('[]')",
            Dialect::MySql => "JSON_ARRAY()",
        }
    }

    /// The literal empty JSON object.
    pub(crate) fn empty_json_object(self) -> &'static str {
        match self {
            Dialect::Postgres => "'{}'::json",
            Dialect::Sqlite => "json('{}')",
            Dialect::MySql => "JSON_OBJECT()",
        }
    }

    /// A string literal that nests into JSON as a string: `__typename` and
    /// friends. PostgreSQL needs the cast, or `row_to_json` sees an
    /// `unknown`-typed constant.
    pub(crate) fn text_literal<'a>(self, s: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => {
                write_quoted(f, s, self)?;
                f.write_str("::text")
            }
            Dialect::Sqlite | Dialect::MySql => write_quoted(f, s, self),
        })
    }

    /// Read inside a JSON column along the path bound as parameter `n` (see
    /// [`json_path_bind`](Self::json_path_bind)). The result keeps its JSON
    /// type so it nests unchanged — on SQLite through `->`, whose result is
    /// JSON where `json_extract`'s is SQL; on MySQL through `JSON_EXTRACT`,
    /// whose `->` shorthand takes only a literal path.
    pub(crate) fn json_path<'a>(self, col: &'a str, n: usize) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "{col} #> ${n}::text[]"),
            Dialect::Sqlite => write!(f, "{col} -> ?{n}"),
            Dialect::MySql => write!(f, "JSON_EXTRACT({col}, ?{n})"),
        })
    }

    /// The bind carrying a JSON path's components.
    pub(crate) fn json_path_bind(self, path: &[String]) -> Bind {
        match self {
            Dialect::Postgres => Bind::TextArray(path.iter().map(|c| Some(c.clone())).collect()),
            Dialect::Sqlite | Dialect::MySql => Bind::Text(dollar_json_path(path)),
        }
    }

    /// `ASC` / `DESC` with where NULLs go, for a dialect that spells it.
    ///
    /// The databases disagree on the default: PostgreSQL sorts NULLs last
    /// on `ASC` and first on `DESC`, SQLite and MySQL the other way round. A
    /// document that says nothing gets PostgreSQL's answer everywhere,
    /// spelled out where it would otherwise differ — the same `order_by`
    /// sorting the same rows two ways depending on the backend is a wrong
    /// answer that looks right.
    fn order_dir(
        self,
        direction: crate::ast::OrderDir,
        nulls: Option<crate::ast::NullsOrder>,
    ) -> &'static str {
        use crate::ast::{NullsOrder, OrderDir};
        match (direction, nulls, self) {
            (OrderDir::Asc, None, Dialect::Postgres) => " ASC",
            (OrderDir::Desc, None, Dialect::Postgres) => " DESC",
            (OrderDir::Asc, None, Dialect::Sqlite | Dialect::MySql) => " ASC NULLS LAST",
            (OrderDir::Desc, None, Dialect::Sqlite | Dialect::MySql) => " DESC NULLS FIRST",
            (OrderDir::Asc, Some(NullsOrder::First), _) => " ASC NULLS FIRST",
            (OrderDir::Asc, Some(NullsOrder::Last), _) => " ASC NULLS LAST",
            (OrderDir::Desc, Some(NullsOrder::First), _) => " DESC NULLS FIRST",
            (OrderDir::Desc, Some(NullsOrder::Last), _) => " DESC NULLS LAST",
        }
    }

    /// One `ORDER BY` term: `expr`, its direction and where its NULLs go.
    ///
    /// MySQL has no `NULLS FIRST|LAST`; the term becomes two, `expr IS NULL`
    /// sorted so that the NULLs land where asked, then `expr` itself.
    pub(crate) fn order_term<'a>(
        self,
        expr: &'a str,
        direction: crate::ast::OrderDir,
        nulls: Option<crate::ast::NullsOrder>,
    ) -> impl Display + 'a {
        use crate::ast::{NullsOrder, OrderDir};
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres | Dialect::Sqlite => {
                write!(f, "{expr}{}", self.order_dir(direction, nulls))
            }
            Dialect::MySql => {
                let nulls_last = match (direction, nulls) {
                    (_, Some(NullsOrder::Last)) => true,
                    (_, Some(NullsOrder::First)) => false,
                    (OrderDir::Asc, None) => true,
                    (OrderDir::Desc, None) => false,
                };
                let dir = match direction {
                    OrderDir::Asc => "ASC",
                    OrderDir::Desc => "DESC",
                };
                let nulls_dir = if nulls_last { "ASC" } else { "DESC" };
                write!(f, "{expr} IS NULL {nulls_dir}, {expr} {dir}")
            }
        })
    }

    /// Whether a row wrapper builds the row's JSON object by hand and so
    /// needs the row's shape ([`rows_list_open`](Self::rows_list_open)).
    pub(crate) fn builds_row_objects(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite | Dialect::MySql => true,
        }
    }

    /// Whether the rows of an ordered derived table have to carry their
    /// position ([`ORDER_NUMBER`]) for the aggregation over them to keep the
    /// order: MySQL's `JSON_ARRAYAGG` does not otherwise.
    pub(crate) fn numbers_rows_for_order(self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Sqlite => false,
            Dialect::MySql => true,
        }
    }

    /// How `count(columns: [a, b])` wraps its columns, or `None` where the
    /// dialect has no one-expression spelling of it and the count is refused.
    ///
    /// PostgreSQL counts the row constructor `(a, b)`, NULL only when every
    /// field is. SQLite has row values but rejects one as an aggregate's
    /// argument. MySQL has `COUNT(DISTINCT a, b)` — distinct pairs, bare —
    /// and nothing for the plain count of pairs.
    pub(crate) fn count_tuple(self, distinct: bool) -> Option<(&'static str, &'static str)> {
        match (self, distinct) {
            (Dialect::Postgres, _) => Some(("(", ")")),
            (Dialect::Sqlite, _) => None,
            (Dialect::MySql, true) => Some(("", "")),
            (Dialect::MySql, false) => None,
        }
    }

    /// The `LIMIT` to write in front of a bare `OFFSET`, for a grammar that
    /// allows no offset without one. SQLite reads `-1` as no limit; MySQL's
    /// manual suggests the largest value the column takes.
    pub(crate) fn no_limit(self) -> Option<&'static str> {
        match self {
            Dialect::Postgres => None,
            Dialect::Sqlite => Some("-1"),
            Dialect::MySql => Some("18446744073709551615"),
        }
    }

    /// Whether `distinct_on` has to be spelled as a `row_number()` window
    /// over a derived table, for want of `DISTINCT ON`.
    pub(crate) fn distinct_on_by_window(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite | Dialect::MySql => true,
        }
    }

    /// Whether the database has this aggregate function. SQLite ships no
    /// statistical aggregates; the type system publishes none for it, and the
    /// renderer refuses one so the builder path cannot reach a function the
    /// database would report as unknown.
    pub(crate) fn supports_agg(self, func: crate::ast::AggFunc) -> bool {
        use crate::ast::AggFunc;
        match (self, func) {
            (Dialect::Postgres | Dialect::MySql, _) => true,
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

    /// Whether the dialect implements a comparison operator on a type that
    /// [`cmp_applies`](crate::type_system::cmp_applies) allows it on.
    ///
    /// SQLite has no JSON type, only text and functions over it, and nothing
    /// that compares two JSON values structurally:
    ///
    /// - `=` is text equality. `json()` normalises whitespace but keeps key
    ///   order, and so does the binary `jsonb()`: `{"a":1,"b":2}` and
    ///   `{"b":2,"a":1}` differ, as do `1.0` and `1`, where PostgreSQL's
    ///   `jsonb` calls both pairs equal. `_eq`, `_neq`, `_in` and `_nin` on a
    ///   JSON column would match when the stored text happens to be spelled
    ///   as the engine spells the operand, and silently not otherwise.
    /// - `@>` recurses through objects and arrays with rules of its own (an
    ///   array contains a bare scalar it holds, at the top level only). The
    ///   key tests go with it, so the jsonb operators come and go as one
    ///   family.
    ///
    /// A structural comparison can be assembled from `json_tree`, at the cost
    /// of expanding both sides per row and of matching PostgreSQL's edge
    /// cases one by one; until that is done, none of these is published on
    /// SQLite, and the renderer refuses each from the builder.
    ///
    /// MySQL's JSON is binary and compares structurally, as `jsonb` does,
    /// and has functions for each operator ([`compare`](Self::compare)).
    pub(crate) fn supports_cmp(self, op: crate::ast::CmpOp, ty: &ColumnType) -> bool {
        use crate::ast::CmpOp;
        let json = matches!(ty, ColumnType::Json | ColumnType::Jsonb);
        match (self, op) {
            (Dialect::Postgres | Dialect::MySql, _) => true,
            (Dialect::Sqlite, CmpOp::Eq | CmpOp::Neq) => !json,
            (
                Dialect::Sqlite,
                CmpOp::Gt
                | CmpOp::Gte
                | CmpOp::Lt
                | CmpOp::Lte
                | CmpOp::Like
                | CmpOp::ILike
                | CmpOp::NLike
                | CmpOp::NILike,
            ) => true,
            (
                Dialect::Sqlite,
                CmpOp::Contains
                | CmpOp::ContainedIn
                | CmpOp::HasKey
                | CmpOp::HasKeysAny
                | CmpOp::HasKeysAll,
            ) => false,
        }
    }

    /// Whether the engine writes to this database at all. MySQL: not yet —
    /// the plan identifies rows by rowid and reads them back with
    /// `RETURNING`, neither of which MySQL has — so no mutation field is
    /// published for it and the renderer refuses one reached anyway.
    pub(crate) fn supports_mutations(self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Sqlite => true,
            Dialect::MySql => false,
        }
    }

    /// Whether a mutation is a sequence of statements ([`crate::plan`])
    /// rather than one statement of data-modifying CTEs. SQLite and MySQL
    /// allow no DML inside a CTE.
    pub(crate) fn mutations_by_plan(self) -> bool {
        match self {
            Dialect::Postgres => false,
            Dialect::Sqlite | Dialect::MySql => true,
        }
    }

    /// Whether the dialect's placeholders are anonymous (`?`), so that a
    /// statement rendered with numbered ones has to go through
    /// [`anonymise_placeholders`] before it is executed.
    pub(crate) fn anonymous_placeholders(self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Sqlite => false,
            Dialect::MySql => true,
        }
    }

    /// Whether the database returns a JSON object's keys in an order of its
    /// own rather than the one they were given in, so that the response has
    /// to be put back in selection order after decoding.
    pub(crate) fn reorders_keys(self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Sqlite => false,
            Dialect::MySql => true,
        }
    }
}

/// Rewrite a statement's numbered placeholders (`?3`) as anonymous ones
/// (`?`), returning the bind specs in the order the placeholders occur — a
/// spec repeated for every occurrence of its number. What a dialect with
/// [anonymous placeholders](Dialect::anonymous_placeholders) executes.
///
/// A `?` inside a string literal or a quoted identifier is text, not a
/// placeholder, and is left alone: a response key or a type name the
/// renderer quoted could spell one.
pub(crate) fn anonymise_placeholders(sql: &str, specs: &[BindSpec]) -> (String, Vec<BindSpec>) {
    let mut out = String::with_capacity(sql.len());
    let mut binds = Vec::with_capacity(specs.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // A literal: up to the closing quote, honouring '' and \x. The
            // renderer writes both escapes, and a doubled quote inside a
            // backslash-escaped one is not a case it produces.
            q @ (b'\'' | b'`' | b'"') => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && q == b'\'' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        if bytes.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                let end = i.min(bytes.len());
                out.push_str(&sql[start..end]);
            }
            b'?' => {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                let n: usize = sql[start..end]
                    .parse()
                    .expect("the renderer numbers every placeholder");
                binds.push(specs[n - 1].clone());
                out.push('?');
                i = end;
            }
            _ => {
                // Copy one char, whatever its width.
                let c = sql[i..].chars().next().expect("in bounds");
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    (out, binds)
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
        // MySQL reads a backslash as an escape; the others read it as itself.
        assert_eq!(Dialect::Sqlite.text_literal("a\\b").to_string(), "'a\\b'");
        assert_eq!(
            Dialect::MySql.text_literal("it's a\\b").to_string(),
            "'it''s a\\\\b'"
        );
        assert_eq!(
            escape_string_literal("it's a\\b", Dialect::MySql),
            "it''s a\\\\b"
        );
    }

    #[test]
    fn identifiers_are_quoted_the_dialects_way() {
        assert_eq!(quote_ident("a\"b", Dialect::Postgres), "\"a\"\"b\"");
        assert_eq!(quote_ident("a\"b", Dialect::Sqlite), "\"a\"\"b\"");
        assert_eq!(quote_ident("a`b", Dialect::MySql), "`a``b`");
        assert_eq!(quote_ident("a\"b", Dialect::MySql), "`a\"b`");
    }

    #[test]
    fn dollar_json_paths() {
        let p = |s: &[&str]| dollar_json_path(&s.iter().map(|c| c.to_string()).collect::<Vec<_>>());
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

    #[test]
    fn mysql_orders_nulls_with_a_leading_term() {
        use crate::ast::{NullsOrder, OrderDir};
        let t = |d, n| Dialect::MySql.order_term("t.x", d, n).to_string();
        assert_eq!(t(OrderDir::Asc, None), "t.x IS NULL ASC, t.x ASC");
        assert_eq!(t(OrderDir::Desc, None), "t.x IS NULL DESC, t.x DESC");
        assert_eq!(
            t(OrderDir::Desc, Some(NullsOrder::Last)),
            "t.x IS NULL ASC, t.x DESC"
        );
        assert_eq!(
            t(OrderDir::Asc, Some(NullsOrder::First)),
            "t.x IS NULL DESC, t.x ASC"
        );
        assert_eq!(
            Dialect::Postgres
                .order_term("t.x", OrderDir::Desc, None)
                .to_string(),
            "t.x DESC"
        );
    }

    #[test]
    fn anonymising_repeats_a_reused_placeholder_and_skips_literals() {
        use crate::types::Bind;
        let specs = vec![
            BindSpec::Fixed(Bind::Int8(1)),
            BindSpec::Fixed(Bind::Text("two".into())),
        ];
        let (sql, binds) = anonymise_placeholders(
            "SELECT '?1 ''?2', `?2`, \"?1\" FROM t WHERE (?2 IS NULL OR a = ?2) AND b = ?1 \
             AND c = 'x\\'?1'",
            &specs,
        );
        assert_eq!(
            sql,
            "SELECT '?1 ''?2', `?2`, \"?1\" FROM t WHERE (? IS NULL OR a = ?) AND b = ? \
             AND c = 'x\\'?1'"
        );
        let as_text: Vec<String> = binds.iter().map(|b| format!("{b:?}")).collect();
        assert_eq!(
            as_text,
            vec![
                format!("{:?}", specs[1]),
                format!("{:?}", specs[1]),
                format!("{:?}", specs[0]),
            ]
        );
    }
}
