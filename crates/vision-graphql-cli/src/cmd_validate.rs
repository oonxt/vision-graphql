//! Offline structural validation of a schema.toml.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::path::Path;
use vision_graphql::schema::config::parse;

pub fn run(path: &Path) -> Result<()> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let cfg = parse(&text)
        .with_context(|| format!("parsing {}", path.display()))?;

    let mut issues: Vec<String> = Vec::new();

    // Within-overlay expose_as collisions.
    let mut counts: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (key, overlay) in &cfg.tables {
        if let Some(new) = &overlay.expose_as {
            counts.entry(new.as_str()).or_default().push(key.as_str());
        }
    }
    for (exposed, sources) in &counts {
        if sources.len() > 1 {
            issues.push(format!(
                "expose_as collision: {} <- [{}]",
                exposed,
                sources.join(", ")
            ));
        }
    }

    // Empty mappings + structural sanity.
    for (key, overlay) in &cfg.tables {
        for rel in &overlay.relations {
            if rel.mapping.is_empty() {
                issues.push(format!(
                    "{}.{}: relation mapping must be non-empty",
                    key, rel.name
                ));
            }
        }
    }

    if issues.is_empty() {
        println!("OK: {}", path.display());
        Ok(())
    } else {
        for i in &issues {
            eprintln!("{i}");
        }
        bail!("{} structural issues found", issues.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(contents: &str) -> tempfile_polyfill::PathHolder {
        tempfile_polyfill::write_temp(contents)
    }

    #[test]
    fn rejects_unknown_field() {
        let f = temp_file(
            r#"
            [tables.users]
            unknown_field = 1
        "#,
        );
        let err = run(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("parsing"));
    }

    #[test]
    fn rejects_duplicate_expose_as() {
        let f = temp_file(
            r#"
            [tables.users]
            expose_as = "people"

            [tables.profiles]
            expose_as = "people"
        "#,
        );
        let err = run(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("structural issues"));
    }

    #[test]
    fn rejects_empty_mapping() {
        let f = temp_file(
            r#"
            [[tables.users.relations]]
            name = "x"
            kind = "array"
            target = "users"
            mapping = []
        "#,
        );
        let err = run(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("structural issues"));
    }

    #[test]
    fn accepts_clean_overlay() {
        let f = temp_file(
            r#"
            [tables.users]
            expose_as = "profiles"
            hide_columns = ["secret"]

            [[tables.users.relations]]
            name = "followers"
            kind = "array"
            target = "users"
            mapping = [["id", "followed_id"]]
        "#,
        );
        run(f.path()).unwrap();
    }

    /// Tiny tempfile shim so we don't depend on the `tempfile` crate.
    mod tempfile_polyfill {
        use std::io::Write;
        use std::path::{Path, PathBuf};

        pub struct PathHolder {
            pub path: PathBuf,
        }
        impl PathHolder {
            pub fn path(&self) -> &Path {
                &self.path
            }
        }
        impl Drop for PathHolder {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }

        pub fn write_temp(contents: &str) -> PathHolder {
            let mut p = std::env::temp_dir();
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            p.push(format!("vision-gql-test-{nanos}-{}.toml", std::process::id()));
            let mut f = std::fs::File::create(&p).expect("temp file");
            f.write_all(contents.as_bytes()).expect("write");
            PathHolder { path: p }
        }
    }
}
