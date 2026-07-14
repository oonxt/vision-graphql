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
    /// True when Postgres reports this relation as a VIEW rather than a BASE
    /// TABLE. Views arrive here because `information_schema.columns` lists their
    /// columns like any other relation's; the distinction only shows up in
    /// `information_schema.tables.table_type`.
    pub is_view: bool,
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
                is_view: false,
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
            t.is_view = true;
        }
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
