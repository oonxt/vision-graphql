//! The SQL a backend spells differently.
//!
//! The renderer in [`crate::sql`] is one body of code; where PostgreSQL and
//! another database disagree on a spelling — a typed placeholder, an `_in`
//! over a bound list, how a row becomes a JSON object — the renderer asks the
//! [`Dialect`] it was given rather than writing the PostgreSQL form inline.
//!
//! An enum rather than a trait, deliberately: every seam is a `match` on the
//! dialect, so adding a variant fails to compile until every seam has been
//! answered for it. A trait with default methods would let a new backend
//! inherit PostgreSQL's spelling at whichever seam nobody remembered — SQL
//! that runs and returns the wrong shape, which is the kind of bug this crate
//! hunts hardest.

use crate::schema::ColumnType;
use crate::types::Bind;
use std::borrow::Cow;

/// Which database's SQL the renderer produces. Chosen by the
/// [`Backend`](crate::Backend) the engine runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
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

impl Dialect {
    /// A placeholder for a value of column type `ty`.
    ///
    /// PostgreSQL gets an explicit cast: sqlx prepares the statement with the
    /// types of its first execution's parameters, and a compiled statement
    /// first run with a null variable would otherwise fix the wrong type.
    pub(crate) fn param(self, n: usize, ty: &ColumnType) -> String {
        match self {
            Dialect::Postgres => format!("${n}::{}", pg_type_name(ty)),
        }
    }

    /// A typed SQL NULL, for an inserted column the object left out.
    pub(crate) fn null_of(self, ty: &ColumnType) -> String {
        match self {
            Dialect::Postgres => format!("NULL::{}", pg_type_name(ty)),
        }
    }

    /// A boolean placeholder (`_is_null: $b`).
    pub(crate) fn bool_param(self, n: usize) -> String {
        match self {
            Dialect::Postgres => format!("${n}::boolean"),
        }
    }

    /// A `limit` / `offset` placeholder.
    pub(crate) fn count_param(self, n: usize) -> String {
        match self {
            Dialect::Postgres => format!("${n}::int8"),
        }
    }

    /// A placeholder carrying a JSON document the renderer answered itself
    /// (introspection riding along in a data query).
    pub(crate) fn json_param(self, n: usize) -> String {
        match self {
            Dialect::Postgres => format!("${n}::json"),
        }
    }

    /// A placeholder for a bound list of `ty`, as a value: what `IS NULL` is
    /// asked of when the list is optional.
    pub(crate) fn list_param(self, n: usize, ty: &ColumnType) -> String {
        match self {
            Dialect::Postgres => format!("${n}::{}[]", pg_type_name(ty)),
        }
    }

    /// `lhs` is (or, `negated`, is not) one of the bound list `n`. One
    /// placeholder whatever the list's length, so the SQL text of a compiled
    /// statement does not depend on the request.
    pub(crate) fn in_list(self, lhs: &str, n: usize, ty: &ColumnType, negated: bool) -> String {
        match self {
            Dialect::Postgres => {
                let pred = if negated { "<> ALL" } else { "= ANY" };
                format!("{lhs} {pred} ({})", self.list_param(n, ty))
            }
        }
    }

    /// The SQL spelling of a comparison operator.
    pub(crate) fn cmp(self, op: crate::ast::CmpOp) -> &'static str {
        use crate::ast::CmpOp;
        match (self, op) {
            (_, CmpOp::Eq) => "=",
            (_, CmpOp::Neq) => "<>",
            (_, CmpOp::Gt) => ">",
            (_, CmpOp::Gte) => ">=",
            (_, CmpOp::Lt) => "<",
            (_, CmpOp::Lte) => "<=",
            (Dialect::Postgres, CmpOp::Like) => "LIKE",
            (Dialect::Postgres, CmpOp::ILike) => "ILIKE",
            (Dialect::Postgres, CmpOp::NLike) => "NOT LIKE",
            (Dialect::Postgres, CmpOp::NILike) => "NOT ILIKE",
        }
    }

    /// Opens a JSON object built from alternating `'key', value` arguments;
    /// the caller closes the parenthesis.
    pub(crate) fn json_object_open(self) -> &'static str {
        match self {
            Dialect::Postgres => "json_build_object(",
        }
    }

    /// Opens `(SELECT <rows of the following derived table as a JSON array>
    /// FROM (`; the caller renders the inner select and closes with
    /// `) <row_alias>)`. An empty result is `[]`, not null.
    pub(crate) fn rows_list_open(self, row_alias: &str) -> String {
        match self {
            Dialect::Postgres => {
                format!("(SELECT coalesce(json_agg(row_to_json({row_alias})), '[]'::json) FROM (")
            }
        }
    }

    /// As [`rows_list_open`](Self::rows_list_open) for the one row of a
    /// `_by_pk` or an object relation: a JSON object, or null when there is
    /// no row.
    pub(crate) fn row_object_open(self, row_alias: &str) -> String {
        match self {
            Dialect::Postgres => format!("(SELECT row_to_json({row_alias}) FROM ("),
        }
    }

    /// Opens an aggregation of the following JSON objects into an array;
    /// closed by [`json_agg_close`](Self::json_agg_close). Empty is `[]`.
    pub(crate) fn json_agg_open(self) -> &'static str {
        match self {
            Dialect::Postgres => "coalesce(json_agg(",
        }
    }

    pub(crate) fn json_agg_close(self) -> &'static str {
        match self {
            Dialect::Postgres => "), '[]'::json)",
        }
    }

    /// The literal empty JSON array, typed so it nests as JSON.
    pub(crate) fn empty_json_array(self) -> &'static str {
        match self {
            Dialect::Postgres => "'[]'::json",
        }
    }

    /// The literal empty JSON object.
    pub(crate) fn empty_json_object(self) -> &'static str {
        match self {
            Dialect::Postgres => "'{}'::json",
        }
    }

    /// A string literal that nests into JSON as a string: `__typename` and
    /// friends. PostgreSQL needs the cast, or `row_to_json` sees an
    /// `unknown`-typed constant.
    pub(crate) fn text_literal(self, s: &str) -> String {
        match self {
            Dialect::Postgres => format!("'{}'::text", escape_string_literal(s)),
        }
    }

    /// Read inside a JSON column along the path bound as parameter `n` (see
    /// [`json_path_bind`](Self::json_path_bind)). The result keeps its JSON
    /// type so it nests unchanged.
    pub(crate) fn json_path(self, col: &str, n: usize) -> String {
        match self {
            Dialect::Postgres => format!("{col} #> ${n}::text[]"),
        }
    }

    /// The bind carrying a JSON path's components.
    pub(crate) fn json_path_bind(self, path: &[String]) -> Bind {
        match self {
            Dialect::Postgres => Bind::TextArray(path.iter().map(|c| Some(c.clone())).collect()),
        }
    }
}
