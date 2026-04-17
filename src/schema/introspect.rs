//! Schema introspection from a live PostgreSQL connection.

use crate::error::Result;
use crate::schema::PgType;
use deadpool_postgres::Pool;
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
        _ => None,
    }
}

pub async fn introspect(pool: &Pool) -> Result<IntrospectedDb> {
    let client = pool.get().await?;
    let mut db = IntrospectedDb::default();

    let rows = client
        .query(
            r#"
            SELECT table_schema, table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_schema, table_name, ordinal_position
            "#,
            &[],
        )
        .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        let dtype: String = row.get(3);
        let is_nullable: String = row.get(4);
        let Some(pg_type) = data_type_to_pg_type(&dtype) else {
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
            });
        entry.columns.push(IntrospectedColumn {
            name: cname,
            pg_type,
            nullable: is_nullable == "YES",
        });
    }

    let rows = client
        .query(
            r#"
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
            ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
            "#,
            &[],
        )
        .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.primary_key.push(cname);
        }
    }

    let rows = client
        .query(
            r#"
            SELECT tc.table_schema, tc.table_name, tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY') AND tc.table_schema = 'public'
            ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
            "#,
            &[],
        )
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

    let rows = client
        .query(
            r#"
            SELECT
                tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                kcu.column_name,
                ccu.table_schema AS foreign_schema,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column,
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
            &[],
        )
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
        let fk = fk_acc
            .entry(key)
            .or_insert_with(|| IntrospectedForeignKey {
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
