//! TOML configuration overlay for Schema.

use crate::error::{Error, Result};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverlay {
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverlay>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableOverlay {
    #[serde(default)]
    pub expose_as: Option<String>,
    #[serde(default)]
    pub hide_columns: Vec<String>,
    #[serde(default)]
    pub relations: Vec<RelationOverlay>,
    /// Override what introspection decided about mutability. Introspection marks
    /// views and materialized views read-only; set `read_only = false` for a view
    /// fronted by INSTEAD OF triggers, or `true` to freeze a base table. Absent
    /// means "keep whatever introspection found".
    #[serde(default)]
    pub read_only: Option<bool>,
    /// Declare a logical primary key. Views and materialized views have no
    /// constraints, so introspection finds no PK for them and `_by_pk` is
    /// unavailable — this is how you say "`id` identifies a row in this view".
    ///
    /// Nothing enforces uniqueness; the columns are used to build the `WHERE`
    /// of a `_by_pk` lookup, so a non-unique choice just means the lookup
    /// returns whichever row Postgres reaches first.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationOverlay {
    pub name: String,
    pub kind: RelationKindOverlay,
    pub target: String,
    pub mapping: Vec<(String, String)>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RelationKindOverlay {
    Object,
    Array,
}

pub fn parse(source: &str) -> Result<ConfigOverlay> {
    toml::from_str(source).map_err(|e| Error::Schema(format!("TOML parse error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_overlay() {
        let s = r#"
            [tables.users]
            expose_as = "profiles"
            hide_columns = ["password_hash"]

            [[tables.users.relations]]
            name = "followers"
            kind = "array"
            target = "users"
            mapping = [["id", "followed_id"]]
        "#;
        let cfg = parse(s).unwrap();
        let users = cfg.tables.get("users").unwrap();
        assert_eq!(users.expose_as.as_deref(), Some("profiles"));
        assert_eq!(users.hide_columns, vec!["password_hash".to_string()]);
        assert_eq!(users.relations.len(), 1);
        assert_eq!(users.relations[0].kind, RelationKindOverlay::Array);
    }

    #[test]
    fn rejects_unknown_fields() {
        let s = r#"
            [tables.users]
            unknown_field = 1
        "#;
        assert!(parse(s).is_err());
    }
}
