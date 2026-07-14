//! Schema introspection from a live PostgreSQL connection.

use crate::error::Result;
use crate::schema::PgType;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct IntrospectedDb {
    /// Keyed by `(schema_name, table_name)`.
    pub tables: BTreeMap<(String, String), IntrospectedTable>,
}

#[derive(Debug)]
pub struct IntrospectedTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<IntrospectedColumn>,
    pub primary_key: Vec<String>,
    pub unique_constraints: BTreeMap<String, Vec<String>>,
    pub foreign_keys: Vec<IntrospectedForeignKey>,
    /// The relation is not a base table, so it must not be handed mutation
    /// roots: a view (Postgres auto-updates a simple one straight through to the
    /// table behind it) or a materialized view (Postgres refuses to write one at
    /// all — it can only be REFRESHed).
    ///
    /// Plain views reach us through `information_schema.columns`, which lists
    /// their columns like any other relation's, so they are indistinguishable
    /// from base tables until `information_schema.tables.table_type` says
    /// otherwise. Materialized views are absent from `information_schema`
    /// entirely and come from a separate `pg_catalog` pass.
    pub read_only: bool,
}

#[derive(Debug)]
pub struct IntrospectedColumn {
    pub name: String,
    pub pg_type: PgType,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct IntrospectedForeignKey {
    pub constraint_name: String,
    pub from_columns: Vec<String>,
    pub to_schema: String,
    pub to_table: String,
    pub to_columns: Vec<String>,
}

pub fn data_type_to_pg_type(data_type: &str) -> Option<PgType> {
    match data_type {
        "integer" => Some(PgType::Int4),
        "bigint" => Some(PgType::Int8),
        "text" => Some(PgType::Text),
        "character varying" => Some(PgType::Varchar),
        "boolean" => Some(PgType::Bool),
        "real" => Some(PgType::Float4),
        "double precision" => Some(PgType::Float8),
        "numeric" => Some(PgType::Numeric),
        "uuid" => Some(PgType::Uuid),
        "timestamp without time zone" => Some(PgType::Timestamp),
        "timestamp with time zone" => Some(PgType::TimestampTz),
        "jsonb" => Some(PgType::Jsonb),
        "date" => Some(PgType::Date),
        "time without time zone" => Some(PgType::Time),
        _ => None,
    }
}

/// Drop a type modifier from a `format_type` result so it matches the spelling
/// `information_schema.data_type` uses, which is what `data_type_to_pg_type`
/// expects.
///
/// The modifier is not always a suffix — Postgres renders it mid-name for the
/// datetime types (`timestamp(3) with time zone`), so trimming from the end is
/// not enough.
fn strip_type_modifier(t: &str) -> String {
    let (Some(open), Some(close)) = (t.find('('), t.find(')')) else {
        return t.to_string();
    };
    if close < open {
        return t.to_string();
    }
    let mut out = String::with_capacity(t.len());
    out.push_str(&t[..open]);
    out.push_str(&t[close + 1..]);
    out.trim().to_string()
}

pub async fn introspect(pool: &PgPool) -> Result<IntrospectedDb> {
    let mut db = IntrospectedDb::default();

    // information_schema columns are domain types (sql_identifier etc.);
    // cast to text so decoding stays driver-agnostic.
    let rows = sqlx::query(
        r#"
        SELECT c.table_schema::text, c.table_name::text, c.column_name::text,
               c.data_type::text, c.is_nullable::text,
               c.udt_schema::text, c.udt_name::text,
               t.typtype = 'e' AS is_enum
        FROM information_schema.columns c
        LEFT JOIN pg_namespace n ON n.nspname = c.udt_schema
        LEFT JOIN pg_type t ON t.typnamespace = n.oid AND t.typname = c.udt_name
        WHERE c.table_schema = 'public'
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        let dtype: String = row.get(3);
        let is_nullable: String = row.get(4);
        let udt_schema: Option<String> = row.get(5);
        let udt_name: Option<String> = row.get(6);
        let is_enum: Option<bool> = row.get(7);
        let pg_type = match (&*dtype, udt_schema, udt_name, is_enum) {
            ("USER-DEFINED", Some(schema), Some(name), Some(true)) => {
                Some(PgType::Enum { schema, name })
            }
            _ => data_type_to_pg_type(&dtype),
        };
        let Some(pg_type) = pg_type else {
            tracing::warn!(
                target: "vision_graphql::introspect",
                table = %tname,
                column = %cname,
                data_type = %dtype,
                "skipping column with unsupported type"
            );
            continue;
        };
        let entry = db
            .tables
            .entry((schema.clone(), tname.clone()))
            .or_insert_with(|| IntrospectedTable {
                schema: schema.clone(),
                name: tname.clone(),
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: BTreeMap::new(),
                foreign_keys: Vec::new(),
                read_only: false,
            });
        entry.columns.push(IntrospectedColumn {
            name: cname,
            pg_type,
            nullable: is_nullable == "YES",
        });
    }

    // Which of those relations are views. `information_schema.columns` does not
    // distinguish them, so without this a view is indistinguishable from a base
    // table and would be handed mutation roots that write through to whatever
    // sits behind it.
    let rows = sqlx::query(
        r#"
        SELECT t.table_schema::text, t.table_name::text
        FROM information_schema.tables t
        WHERE t.table_schema = 'public' AND t.table_type = 'VIEW'
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.read_only = true;
        }
    }

    // Materialized views. They are a Postgres extension, so `information_schema`
    // does not know about them at all — no amount of relaxing the queries above
    // will surface one. Their columns have to come from `pg_catalog` directly.
    //
    // `format_type` is used rather than a `data_type` column so the spelling
    // matches what `data_type_to_pg_type` already understands; enums are detected
    // from `typtype` instead, exactly as the information_schema pass does.
    let rows = sqlx::query(
        r#"
        SELECT n.nspname::text, c.relname::text, a.attname::text,
               format_type(a.atttypid, a.atttypmod)::text,
               (NOT a.attnotnull) AS nullable,
               (t.typtype = 'e') AS is_enum,
               tn.nspname::text, t.typname::text
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
        WHERE c.relkind = 'm'
          AND n.nspname = 'public'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY n.nspname, c.relname, a.attnum
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        let dtype: String = row.get(3);
        let nullable: bool = row.get(4);
        let is_enum: bool = row.get(5);
        let udt_schema: String = row.get(6);
        let udt_name: String = row.get(7);

        let pg_type = if is_enum {
            Some(PgType::Enum {
                schema: udt_schema,
                name: udt_name,
            })
        } else {
            data_type_to_pg_type(&strip_type_modifier(&dtype))
        };
        let Some(pg_type) = pg_type else {
            tracing::warn!(
                target: "vision_graphql::introspect",
                table = %tname,
                column = %cname,
                data_type = %dtype,
                "skipping materialized-view column with unsupported type"
            );
            continue;
        };
        let entry = db
            .tables
            .entry((schema.clone(), tname.clone()))
            .or_insert_with(|| IntrospectedTable {
                schema: schema.clone(),
                name: tname.clone(),
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: BTreeMap::new(),
                foreign_keys: Vec::new(),
                // Postgres cannot write a materialized view at all; it can only
                // be REFRESHed.
                read_only: true,
            });
        entry.columns.push(IntrospectedColumn {
            name: cname,
            pg_type,
            nullable,
        });
    }

    let rows = sqlx::query(
        r#"
        SELECT tc.table_schema::text, tc.table_name::text, kcu.column_name::text
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
        ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.primary_key.push(cname);
        }
    }

    let rows = sqlx::query(
        r#"
        SELECT tc.table_schema::text, tc.table_name::text, tc.constraint_name::text,
               kcu.column_name::text
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY') AND tc.table_schema = 'public'
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let constraint: String = row.get(2);
        let cname: String = row.get(3);
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.unique_constraints
                .entry(constraint)
                .or_default()
                .push(cname);
        }
    }

    let rows = sqlx::query(
        r#"
        SELECT
            tc.table_schema::text,
            tc.table_name::text,
            tc.constraint_name::text,
            kcu.column_name::text,
            ccu.table_schema::text AS foreign_schema,
            ccu.table_name::text AS foreign_table,
            ccu.column_name::text AS foreign_column,
            kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        JOIN information_schema.referential_constraints rc
          ON tc.constraint_name = rc.constraint_name
         AND tc.table_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
          ON rc.unique_constraint_name = ccu.constraint_name
         AND rc.unique_constraint_schema = ccu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#,
    )
    .fetch_all(pool)
    .await?;
    let mut fk_acc: BTreeMap<(String, String, String), IntrospectedForeignKey> = BTreeMap::new();
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let constraint: String = row.get(2);
        let cname: String = row.get(3);
        let f_schema: String = row.get(4);
        let f_table: String = row.get(5);
        let f_col: String = row.get(6);
        let key = (schema.clone(), tname.clone(), constraint.clone());
        let fk = fk_acc.entry(key).or_insert_with(|| IntrospectedForeignKey {
            constraint_name: constraint.clone(),
            from_columns: Vec::new(),
            to_schema: f_schema.clone(),
            to_table: f_table.clone(),
            to_columns: Vec::new(),
        });
        fk.from_columns.push(cname);
        fk.to_columns.push(f_col);
    }
    for ((schema, tname, _), fk) in fk_acc {
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.foreign_keys.push(fk);
        }
    }

    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `format_type` (used for the materialized-view pass) renders the modifier
    /// mid-name for the datetime types, so a suffix trim would leave
    /// `timestamp with time zone` mangled and the column would map to nothing
    /// and be silently dropped.
    #[test]
    fn strips_type_modifiers_wherever_they_appear() {
        // Suffix modifier.
        assert_eq!(
            strip_type_modifier("character varying(50)"),
            "character varying"
        );
        assert_eq!(strip_type_modifier("numeric(10,2)"), "numeric");
        // Infix modifier — the case a suffix trim gets wrong.
        assert_eq!(
            strip_type_modifier("timestamp(3) with time zone"),
            "timestamp with time zone"
        );
        assert_eq!(
            strip_type_modifier("time(6) without time zone"),
            "time without time zone"
        );
        // No modifier at all: unchanged.
        assert_eq!(strip_type_modifier("integer"), "integer");
        assert_eq!(
            strip_type_modifier("timestamp with time zone"),
            "timestamp with time zone"
        );

        // And the stripped spellings are exactly what the mapper understands.
        assert_eq!(
            data_type_to_pg_type(&strip_type_modifier("timestamp(3) with time zone")),
            Some(PgType::TimestampTz)
        );
        assert_eq!(
            data_type_to_pg_type(&strip_type_modifier("character varying(50)")),
            Some(PgType::Varchar)
        );
    }

    #[test]
    fn maps_known_types() {
        assert_eq!(data_type_to_pg_type("integer"), Some(PgType::Int4));
        assert_eq!(data_type_to_pg_type("text"), Some(PgType::Text));
        assert_eq!(data_type_to_pg_type("boolean"), Some(PgType::Bool));
        assert_eq!(
            data_type_to_pg_type("timestamp with time zone"),
            Some(PgType::TimestampTz)
        );
    }

    #[test]
    fn unknown_type_returns_none() {
        assert_eq!(data_type_to_pg_type("hstore"), None);
    }
}
