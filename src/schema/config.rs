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
