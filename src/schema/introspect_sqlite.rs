//! Schema introspection from a live SQLite connection.
//!
//! Everything comes from `sqlite_master` and the `pragma_*` table-valued
//! functions, and lands in the same [`IntrospectedDb`] PostgreSQL
//! introspection fills, so the merge into a [`SchemaBuilder`] is shared. The
//! one schema is `main`; attached databases are not walked.
//!
//! # Declared types
//!
//! SQLite stores whatever it is given; a column's declared type is a hint
//! (its *affinity*) unless the table is `STRICT`. The mapping below reads the
//! declaration the way SQLite's own affinity rules do — `INT` anywhere in the
//! name means integer, `CHAR`/`CLOB`/`TEXT` means text — and adds the names
//! people actually write for the types SQLite lacks (`BOOLEAN`, `JSON`,
//! `DATETIME`, `UUID`). What has no mapping is left out and recorded in
//! [`IntrospectedDb::skipped_columns`]:
//!
//! - `NUMERIC` / `DECIMAL`: SQLite holds them as `REAL` and a sum loses digits
//!   past the fifteenth. Declare the column `REAL` to publish it as a float,
//!   knowingly.
//! - `BLOB`, and a column with no declared type at all.
//!
//! Every integer is [`ColumnType::Int8`]: SQLite integers are 64-bit whatever
//! the declaration says, and publishing a 32-bit `Int` for a column that can
//! hold more would be a promise the database does not keep.
//!
//! [`SchemaBuilder`]: crate::schema::SchemaBuilder

use super::introspect::{
    IntrospectedColumn, IntrospectedDb, IntrospectedForeignKey, IntrospectedTable, SkippedColumn,
};
use crate::error::{Error, Result};
use crate::schema::ColumnType;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::collections::BTreeMap;

/// The schema name every SQLite table is filed under.
pub const SCHEMA: &str = "main";

/// What introspection found, plus what the shared [`IntrospectedDb`] has no
/// place for.
#[derive(Debug)]
pub struct SqliteIntrospection {
    pub db: IntrospectedDb,
    /// Tables that are not `STRICT` — see
    /// [`Table::loosely_typed`](crate::schema::Table::loosely_typed).
    pub loosely_typed: Vec<String>,
}

/// Map a declared column type to the engine's, or `None` for one the engine
/// leaves out. See the module docs for the rules.
pub fn declared_type_to_column_type(declared: &str) -> Option<ColumnType> {
    // `VARCHAR(255)` → `VARCHAR`; SQLite ignores the length and so does this.
    let base = declared
        .split('(')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    if base.is_empty() {
        return None;
    }
    let has = |needle: &str| base.contains(needle);
    Some(if has("JSONB") {
        ColumnType::Jsonb
    } else if has("JSON") {
        ColumnType::Json
    } else if has("BOOL") {
        ColumnType::Bool
    } else if has("INT") {
        ColumnType::Int8
    } else if has("CHAR") || has("CLOB") || has("TEXT") {
        ColumnType::Text
    } else if has("REAL") || has("FLOA") || has("DOUB") {
        ColumnType::Float8
    } else if has("UUID") {
        ColumnType::Uuid
    } else if has("DATETIME") || has("TIMESTAMP") {
        ColumnType::Timestamp
    } else if has("DATE") {
        ColumnType::Date
    } else if has("TIME") {
        ColumnType::Time
    } else {
        // NUMERIC, DECIMAL, BLOB, ANY and anything else.
        return None;
    })
}

/// Whether the `CREATE TABLE` text declares the table `STRICT`: the keyword
/// comes after the closing parenthesis, beside `WITHOUT ROWID`.
fn is_strict(create_sql: &str) -> bool {
    match create_sql.rfind(')') {
        Some(i) => create_sql[i + 1..].to_ascii_uppercase().contains("STRICT"),
        None => false,
    }
}

pub async fn introspect(pool: &SqlitePool) -> Result<SqliteIntrospection> {
    crate::sqlite::verify(pool).await?;

    let mut db = IntrospectedDb {
        tables: BTreeMap::new(),
        skipped_columns: Vec::new(),
        schemas: vec![SCHEMA.to_string()],
    };
    let mut loosely_typed = Vec::new();

    let relations = sqlx::query(
        "SELECT name, type, sql FROM sqlite_master \
         WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await?;

    for rel in &relations {
        let name: String = rel.try_get("name")?;
        let kind: String = rel.try_get("type")?;
        let create_sql: Option<String> = rel.try_get("sql")?;
        let is_view = kind == "view";

        let mut columns = Vec::new();
        // pk > 0 is the column's 1-based position in the primary key.
        let mut pk: Vec<(i64, String)> = Vec::new();
        let cols = sqlx::query("SELECT name, type, \"notnull\", pk FROM pragma_table_info(?1)")
            .bind(&name)
            .fetch_all(pool)
            .await?;
        for c in &cols {
            let col: String = c.try_get("name")?;
            let declared: String = c.try_get("type")?;
            let not_null: i64 = c.try_get("notnull")?;
            let pk_pos: i64 = c.try_get("pk")?;
            let Some(ty) = declared_type_to_column_type(&declared) else {
                db.skipped_columns.push(SkippedColumn {
                    schema: SCHEMA.to_string(),
                    table: name.clone(),
                    column: col,
                    data_type: if declared.is_empty() {
                        "(no declared type)".to_string()
                    } else {
                        declared
                    },
                });
                continue;
            };
            if pk_pos > 0 {
                pk.push((pk_pos, col.clone()));
            }
            columns.push(IntrospectedColumn {
                name: col,
                ty,
                // A primary key column is never null in practice: an INTEGER
                // PRIMARY KEY is the rowid, and the engine's `_by_pk` treats
                // the key as a value either way.
                nullable: not_null == 0 && pk_pos == 0,
            });
        }
        pk.sort();
        let primary_key: Vec<String> = pk.into_iter().map(|(_, c)| c).collect();

        let mut unique_constraints = BTreeMap::new();
        let mut unique_indexes = BTreeMap::new();
        // SQLite has no name for a primary key. PostgreSQL's default,
        // `<table>_pkey`, gives `on_conflict` one to say — the same word a
        // document written against PostgreSQL would use.
        if !is_view && !primary_key.is_empty() {
            unique_constraints.insert(format!("{name}_pkey"), primary_key.clone());
        }
        if !is_view {
            let indexes =
                sqlx::query("SELECT name, \"unique\", origin, partial FROM pragma_index_list(?1)")
                    .bind(&name)
                    .fetch_all(pool)
                    .await?;
            for ix in &indexes {
                let ix_name: String = ix.try_get("name")?;
                let unique: i64 = ix.try_get("unique")?;
                let origin: String = ix.try_get("origin")?;
                let partial: i64 = ix.try_get("partial")?;
                // A partial index pins nothing about the rows it leaves out;
                // the primary key is already known.
                if unique != 1 || partial != 0 || origin == "pk" {
                    continue;
                }
                let key_cols = sqlx::query("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
                    .bind(&ix_name)
                    .fetch_all(pool)
                    .await?;
                let mut cols_of = Vec::with_capacity(key_cols.len());
                let mut expression = false;
                for kc in &key_cols {
                    match kc.try_get::<Option<String>, _>("name")? {
                        Some(c) => cols_of.push(c),
                        // An expression index has no column name to pin.
                        None => expression = true,
                    }
                }
                if expression || cols_of.is_empty() {
                    continue;
                }
                match origin.as_str() {
                    "u" => unique_constraints.insert(ix_name, cols_of),
                    _ => unique_indexes.insert(ix_name, cols_of),
                };
            }
        }

        let mut foreign_keys = Vec::new();
        if !is_view {
            let fks = sqlx::query(
                "SELECT id, seq, \"table\", \"from\", \"to\" FROM pragma_foreign_key_list(?1) \
                 ORDER BY id, seq",
            )
            .bind(&name)
            .fetch_all(pool)
            .await?;
            // One constraint per `id`; `seq` orders its column pairs.
            let mut by_id: BTreeMap<i64, IntrospectedForeignKey> = BTreeMap::new();
            for fk in &fks {
                let id: i64 = fk.try_get("id")?;
                let to_table: String = fk.try_get("table")?;
                let from: String = fk.try_get("from")?;
                // `to` is null when the reference names no column, meaning the
                // target's primary key. Resolved after every table is known.
                let to: Option<String> = fk.try_get("to")?;
                let entry = by_id.entry(id).or_insert_with(|| IntrospectedForeignKey {
                    constraint_name: format!("{name}_fk{id}"),
                    from_columns: Vec::new(),
                    to_schema: SCHEMA.to_string(),
                    to_table: to_table.clone(),
                    to_columns: Vec::new(),
                });
                entry.from_columns.push(from);
                if let Some(to) = to {
                    entry.to_columns.push(to);
                }
            }
            foreign_keys.extend(by_id.into_values());
        }

        if !is_view && !create_sql.as_deref().is_some_and(is_strict) {
            loosely_typed.push(name.clone());
        }

        db.tables.insert(
            (SCHEMA.to_string(), name.clone()),
            IntrospectedTable {
                schema: SCHEMA.to_string(),
                name,
                columns,
                primary_key,
                unique_constraints,
                unique_indexes,
                foreign_keys,
                read_only: is_view,
            },
        );
    }

    // SQLite matches table names without regard to case: `REFERENCES users`
    // finds `CREATE TABLE Users`. The merge matches by exact string, so the
    // target is spelled here the way the table was created.
    let canonical: BTreeMap<String, String> = db
        .tables
        .values()
        .map(|t| (t.name.to_lowercase(), t.name.clone()))
        .collect();
    // Then the implicit `REFERENCES t` targets, now that every primary key is
    // known. A reference that names no columns to a table with no primary key
    // of matching width is refused: SQLite would refuse the write, and the
    // relation it would derive could not be right.
    let pks: BTreeMap<String, Vec<String>> = db
        .tables
        .values()
        .map(|t| (t.name.clone(), t.primary_key.clone()))
        .collect();
    for t in db.tables.values_mut() {
        for fk in &mut t.foreign_keys {
            if let Some(real) = canonical.get(&fk.to_table.to_lowercase()) {
                fk.to_table = real.clone();
            }
            if fk.to_columns.is_empty() {
                match pks.get(&fk.to_table) {
                    Some(pk) if pk.len() == fk.from_columns.len() => {
                        fk.to_columns = pk.clone();
                    }
                    _ => {
                        return Err(Error::Schema(format!(
                            "foreign key {} on {} references {} without naming columns, and \
                             that table has no primary key of matching width",
                            fk.constraint_name, t.name, fk.to_table
                        )));
                    }
                }
            }
        }
    }

    Ok(SqliteIntrospection { db, loosely_typed })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_types_follow_affinity_plus_the_usual_names() {
        let m = declared_type_to_column_type;
        assert_eq!(m("INTEGER"), Some(ColumnType::Int8));
        assert_eq!(m("int"), Some(ColumnType::Int8));
        assert_eq!(m("BIGINT"), Some(ColumnType::Int8));
        assert_eq!(m("VARCHAR(255)"), Some(ColumnType::Text));
        assert_eq!(m("text"), Some(ColumnType::Text));
        assert_eq!(m("BOOLEAN"), Some(ColumnType::Bool));
        assert_eq!(m("REAL"), Some(ColumnType::Float8));
        assert_eq!(m("DOUBLE PRECISION"), Some(ColumnType::Float8));
        assert_eq!(m("JSON"), Some(ColumnType::Json));
        assert_eq!(m("JSONB"), Some(ColumnType::Jsonb));
        assert_eq!(m("DATETIME"), Some(ColumnType::Timestamp));
        assert_eq!(m("TIMESTAMP"), Some(ColumnType::Timestamp));
        assert_eq!(m("DATE"), Some(ColumnType::Date));
        assert_eq!(m("TIME"), Some(ColumnType::Time));
        assert_eq!(m("UUID"), Some(ColumnType::Uuid));
        assert_eq!(m("NUMERIC"), None);
        assert_eq!(m("DECIMAL(10,2)"), None);
        assert_eq!(m("BLOB"), None);
        assert_eq!(m(""), None);
        assert_eq!(m("ANY"), None);
    }

    #[test]
    fn strict_is_read_off_the_tail_of_the_create_statement() {
        assert!(is_strict("CREATE TABLE t(id INTEGER) STRICT"));
        assert!(is_strict(
            "CREATE TABLE t(id INTEGER) WITHOUT ROWID, STRICT"
        ));
        assert!(is_strict("create table t(id integer) strict"));
        assert!(!is_strict("CREATE TABLE t(id INTEGER)"));
        // The word inside the body is a column name, not the table option.
        assert!(!is_strict("CREATE TABLE t(strict INTEGER)"));
    }
}
