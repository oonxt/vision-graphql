//! Schema introspection from a live MySQL connection.
//!
//! Everything comes from `information_schema`, for the database the
//! connection is on (`DATABASE()`), and lands in the same [`IntrospectedDb`]
//! PostgreSQL introspection fills, so the merge into a [`SchemaBuilder`] is
//! shared. Other databases on the server are not walked: a foreign key into
//! one is left out, as a cross-database relation would be a read of a table
//! the schema does not have.
//!
//! # Types
//!
//! MySQL's types map onto the engine's the way a PostgreSQL user would
//! expect them to, with the conventions MySQL itself follows:
//!
//! - `TINYINT(1)` — what `BOOLEAN` declares — is [`ColumnType::Bool`]; any
//!   other `TINYINT` is a small integer.
//! - Every unsigned integer is [`ColumnType::Int8`], whatever its width: an
//!   `INT UNSIGNED` holds more than an `Int` promises, and a `BIGINT
//!   UNSIGNED` more than an `Int8` does — a value above `i64::MAX` reaches
//!   the response intact (it is a JSON number) but cannot be bound as a
//!   filter value.
//! - `JSON` is [`ColumnType::Jsonb`]: MySQL's JSON is binary, normalised
//!   and compared structurally, which is what `jsonb` means here, and what
//!   lets the jsonb operators be published for it.
//! - `DATETIME` is a [`ColumnType::Timestamp`] and `TIMESTAMP` a
//!   [`ColumnType::TimestampTz`]: the second is stored as UTC and read back
//!   in the session's zone, which the renderer converts back
//!   ([`Dialect::value_as_json`](crate::dialect::Dialect::value_as_json)).
//! - `ENUM` is text. `DECIMAL` is [`ColumnType::Numeric`], exact.
//!
//! What has no mapping is left out and recorded in
//! [`IntrospectedDb::skipped_columns`]: `SET`, `BIT`, the binary and blob
//! types, and the spatial ones.
//!
//! # Keys
//!
//! MySQL names every primary key `PRIMARY`; it is registered as
//! `<table>_pkey`, PostgreSQL's default, so an `on_conflict` written
//! against PostgreSQL says the same word here. Every unique index is a
//! unique constraint to `information_schema`, and is recorded as one.
//!
//! [`SchemaBuilder`]: crate::schema::SchemaBuilder

use super::introspect::{
    IntrospectedColumn, IntrospectedDb, IntrospectedForeignKey, IntrospectedTable, SkippedColumn,
};
use crate::error::{Error, Result};
use crate::schema::ColumnType;
use sqlx::mysql::MySqlPool;
use sqlx::Row;
use std::collections::BTreeMap;

/// Map a column's `information_schema` description to the engine's type, or
/// `None` for one the engine leaves out. `data_type` is `DATA_TYPE`
/// (`tinyint`, `varchar`), `column_type` is `COLUMN_TYPE`, which carries the
/// width and signedness (`tinyint(1)`, `int unsigned`).
pub fn data_type_to_column_type(data_type: &str, column_type: &str) -> Option<ColumnType> {
    let column_type = column_type.to_ascii_lowercase();
    let unsigned = column_type.contains("unsigned");
    Some(match data_type.to_ascii_lowercase().as_str() {
        "tinyint" if column_type.starts_with("tinyint(1)") => ColumnType::Bool,
        "tinyint" | "smallint" | "year" if !unsigned => ColumnType::Int2,
        "mediumint" | "int" | "integer" if !unsigned => ColumnType::Int4,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => ColumnType::Int8,
        "float" => ColumnType::Float4,
        "double" => ColumnType::Float8,
        "decimal" | "numeric" => ColumnType::Numeric,
        "char" | "varchar" | "enum" => ColumnType::Varchar,
        "tinytext" | "text" | "mediumtext" | "longtext" => ColumnType::Text,
        "json" => ColumnType::Jsonb,
        "datetime" => ColumnType::Timestamp,
        "timestamp" => ColumnType::TimestampTz,
        "date" => ColumnType::Date,
        "time" => ColumnType::Time,
        // SET, BIT, BINARY, VARBINARY, the BLOBs, the spatial types.
        _ => return None,
    })
}

/// The database the pool is connected to: the one schema walked.
async fn current_database(pool: &MySqlPool) -> Result<String> {
    let row = sqlx::query("SELECT DATABASE() AS db")
        .fetch_one(pool)
        .await?;
    let db: Option<String> = row.try_get("db")?;
    db.ok_or_else(|| {
        Error::Schema(
            "the MySQL connection names no database; put one in the URL (mysql://…/name)".into(),
        )
    })
}

pub async fn introspect(pool: &MySqlPool) -> Result<IntrospectedDb> {
    crate::mysql::verify(pool).await?;
    let schema = current_database(pool).await?;

    let mut db = IntrospectedDb {
        tables: BTreeMap::new(),
        skipped_columns: Vec::new(),
        schemas: vec![schema.clone()],
    };

    let relations = sqlx::query(
        "SELECT TABLE_NAME AS name, TABLE_TYPE AS kind FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for rel in &relations {
        let name: String = rel.try_get("name")?;
        let kind: String = rel.try_get("kind")?;
        db.tables.insert(
            (schema.clone(), name.clone()),
            IntrospectedTable {
                schema: schema.clone(),
                name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: BTreeMap::new(),
                unique_indexes: BTreeMap::new(),
                foreign_keys: Vec::new(),
                read_only: kind == "VIEW",
            },
        );
    }

    let columns = sqlx::query(
        "SELECT TABLE_NAME AS tbl, COLUMN_NAME AS col, DATA_TYPE AS data_type, \
         COLUMN_TYPE AS column_type, IS_NULLABLE AS nullable \
         FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
         ORDER BY TABLE_NAME, ORDINAL_POSITION",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for c in &columns {
        let tbl: String = c.try_get("tbl")?;
        let col: String = c.try_get("col")?;
        let data_type: String = c.try_get("data_type")?;
        let column_type: String = c.try_get("column_type")?;
        let nullable: String = c.try_get("nullable")?;
        let Some(table) = db.tables.get_mut(&(schema.clone(), tbl.clone())) else {
            continue;
        };
        let Some(ty) = data_type_to_column_type(&data_type, &column_type) else {
            db.skipped_columns.push(SkippedColumn {
                schema: schema.clone(),
                table: tbl,
                column: col,
                data_type: column_type,
            });
            continue;
        };
        table.columns.push(IntrospectedColumn {
            name: col,
            ty,
            nullable: nullable == "YES",
        });
    }

    // Primary and unique keys. `information_schema` lists every unique index
    // as a UNIQUE constraint, so there is nothing to file under
    // `unique_indexes`.
    let keys = sqlx::query(
        "SELECT tc.TABLE_NAME AS tbl, tc.CONSTRAINT_NAME AS name, tc.CONSTRAINT_TYPE AS kind, \
         kcu.COLUMN_NAME AS col \
         FROM information_schema.TABLE_CONSTRAINTS tc \
         JOIN information_schema.KEY_COLUMN_USAGE kcu \
           ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA \
          AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
          AND kcu.TABLE_NAME = tc.TABLE_NAME \
         WHERE tc.TABLE_SCHEMA = ? AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE') \
         ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for k in &keys {
        let tbl: String = k.try_get("tbl")?;
        let name: String = k.try_get("name")?;
        let kind: String = k.try_get("kind")?;
        let col: String = k.try_get("col")?;
        let Some(table) = db.tables.get_mut(&(schema.clone(), tbl)) else {
            continue;
        };
        // A key over a column the engine left out pins nothing it can name.
        if !table.columns.iter().any(|c| c.name == col) {
            continue;
        }
        if kind == "PRIMARY KEY" {
            table.primary_key.push(col);
        } else {
            table.unique_constraints.entry(name).or_default().push(col);
        }
    }
    for table in db.tables.values_mut() {
        if !table.read_only && !table.primary_key.is_empty() {
            let name = format!("{}_pkey", table.name);
            table
                .unique_constraints
                .insert(name, table.primary_key.clone());
        }
    }

    let fks = sqlx::query(
        "SELECT TABLE_NAME AS tbl, CONSTRAINT_NAME AS name, COLUMN_NAME AS col, \
         REFERENCED_TABLE_SCHEMA AS to_schema, REFERENCED_TABLE_NAME AS to_tbl, \
         REFERENCED_COLUMN_NAME AS to_col \
         FROM information_schema.KEY_COLUMN_USAGE \
         WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL \
         ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for fk in &fks {
        let tbl: String = fk.try_get("tbl")?;
        let name: String = fk.try_get("name")?;
        let col: String = fk.try_get("col")?;
        let to_schema: String = fk.try_get("to_schema")?;
        let to_tbl: String = fk.try_get("to_tbl")?;
        let to_col: String = fk.try_get("to_col")?;
        if to_schema != schema {
            continue;
        }
        let Some(table) = db.tables.get_mut(&(schema.clone(), tbl)) else {
            continue;
        };
        let entry = match table
            .foreign_keys
            .iter_mut()
            .find(|f| f.constraint_name == name)
        {
            Some(f) => f,
            None => {
                table.foreign_keys.push(IntrospectedForeignKey {
                    constraint_name: name,
                    from_columns: Vec::new(),
                    to_schema,
                    to_table: to_tbl,
                    to_columns: Vec::new(),
                });
                table.foreign_keys.last_mut().expect("just pushed")
            }
        };
        entry.from_columns.push(col);
        entry.to_columns.push(to_col);
    }

    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_follow_mysqls_conventions() {
        let m = data_type_to_column_type;
        assert_eq!(m("tinyint", "tinyint(1)"), Some(ColumnType::Bool));
        assert_eq!(m("tinyint", "tinyint"), Some(ColumnType::Int2));
        assert_eq!(m("tinyint", "tinyint(4)"), Some(ColumnType::Int2));
        assert_eq!(m("smallint", "smallint"), Some(ColumnType::Int2));
        assert_eq!(m("int", "int"), Some(ColumnType::Int4));
        assert_eq!(m("int", "int unsigned"), Some(ColumnType::Int8));
        assert_eq!(m("bigint", "bigint"), Some(ColumnType::Int8));
        assert_eq!(m("bigint", "bigint unsigned"), Some(ColumnType::Int8));
        assert_eq!(m("float", "float"), Some(ColumnType::Float4));
        assert_eq!(m("double", "double"), Some(ColumnType::Float8));
        assert_eq!(m("decimal", "decimal(10,2)"), Some(ColumnType::Numeric));
        assert_eq!(m("varchar", "varchar(50)"), Some(ColumnType::Varchar));
        assert_eq!(m("char", "char(36)"), Some(ColumnType::Varchar));
        assert_eq!(m("enum", "enum('a','b')"), Some(ColumnType::Varchar));
        assert_eq!(m("text", "text"), Some(ColumnType::Text));
        assert_eq!(m("longtext", "longtext"), Some(ColumnType::Text));
        assert_eq!(m("json", "json"), Some(ColumnType::Jsonb));
        assert_eq!(m("datetime", "datetime(6)"), Some(ColumnType::Timestamp));
        assert_eq!(m("timestamp", "timestamp"), Some(ColumnType::TimestampTz));
        assert_eq!(m("date", "date"), Some(ColumnType::Date));
        assert_eq!(m("time", "time(3)"), Some(ColumnType::Time));
        assert_eq!(m("year", "year"), Some(ColumnType::Int2));
        assert_eq!(m("set", "set('a','b')"), None);
        assert_eq!(m("bit", "bit(1)"), None);
        assert_eq!(m("blob", "blob"), None);
        assert_eq!(m("varbinary", "varbinary(16)"), None);
        assert_eq!(m("geometry", "geometry"), None);
    }
}
