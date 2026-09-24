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
//!
//! The seams return `impl Display` rather than `String`: the renderer runs
//! once per request on the uncompiled path and writes each fragment straight
//! into the statement, so a fragment is formatted in place, not allocated and
//! copied.

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
    pub(crate) fn param<'a>(self, n: usize, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::{}", pg_type_name(ty)),
        })
    }

    /// A typed SQL NULL, for an inserted column the object left out.
    pub(crate) fn null_of<'a>(self, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "NULL::{}", pg_type_name(ty)),
        })
    }

    /// A boolean placeholder (`_is_null: $b`).
    pub(crate) fn bool_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::boolean"),
        })
    }

    /// A `limit` / `offset` placeholder.
    pub(crate) fn count_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::int8"),
        })
    }

    /// A placeholder carrying a JSON document the renderer answered itself
    /// (introspection riding along in a data query).
    pub(crate) fn json_param(self, n: usize) -> impl Display {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::json"),
        })
    }

    /// A placeholder for a bound list of `ty`, as a value: what `IS NULL` is
    /// asked of when the list is optional.
    pub(crate) fn list_param<'a>(self, n: usize, ty: &'a ColumnType) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "${n}::{}[]", pg_type_name(ty)),
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
        })
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
    pub(crate) fn rows_list_open<'a>(self, row_alias: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(
                f,
                "(SELECT coalesce(json_agg(row_to_json({row_alias})), '[]'::json) FROM ("
            ),
        })
    }

    /// As [`rows_list_open`](Self::rows_list_open) for the one row of a
    /// `_by_pk` or an object relation: a JSON object, or null when there is
    /// no row.
    pub(crate) fn row_object_open<'a>(self, row_alias: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "(SELECT row_to_json({row_alias}) FROM ("),
        })
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
    pub(crate) fn text_literal<'a>(self, s: &'a str) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => {
                write_quoted(f, s)?;
                f.write_str("::text")
            }
        })
    }

    /// Read inside a JSON column along the path bound as parameter `n` (see
    /// [`json_path_bind`](Self::json_path_bind)). The result keeps its JSON
    /// type so it nests unchanged.
    pub(crate) fn json_path<'a>(self, col: &'a str, n: usize) -> impl Display + 'a {
        Fragment(move |f: &mut Formatter<'_>| match self {
            Dialect::Postgres => write!(f, "{col} #> ${n}::text[]"),
        })
    }

    /// The bind carrying a JSON path's components.
    pub(crate) fn json_path_bind(self, path: &[String]) -> Bind {
        match self {
            Dialect::Postgres => Bind::TextArray(path.iter().map(|c| Some(c.clone())).collect()),
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
    }
}
