//! Glob-based include/ignore filter for table names.

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};

/// Decides which table names should be processed.
///
/// Build with [`TableFilter::new`]. Apply with [`TableFilter::keep`].
#[derive(Debug)]
pub struct TableFilter {
    include: Option<GlobSet>,
    ignore: Option<GlobSet>,
}

impl TableFilter {
    pub fn new(include: Option<&[String]>, ignore: Option<&[String]>) -> Result<Self> {
        Ok(Self {
            include: compile(include)?,
            ignore: compile(ignore)?,
        })
    }

    pub fn keep(&self, name: &str) -> bool {
        let included = match &self.include {
            Some(set) => set.is_match(name),
            None => true,
        };
        let ignored = matches!(&self.ignore, Some(set) if set.is_match(name));
        included && !ignored
    }
}

fn compile(patterns: Option<&[String]>) -> Result<Option<GlobSet>> {
    let Some(pats) = patterns else {
        return Ok(None);
    };
    if pats.is_empty() {
        return Ok(None);
    }
    let mut b = GlobSetBuilder::new();
    for p in pats {
        let g = Glob::new(p).with_context(|| format!("invalid glob pattern: {p}"))?;
        b.add(g);
    }
    Ok(Some(b.build().context("compiling glob set")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).into()).collect()
    }

    #[test]
    fn empty_filter_keeps_everything() {
        let f = TableFilter::new(None, None).unwrap();
        assert!(f.keep("users"));
        assert!(f.keep("audit_log"));
    }

    #[test]
    fn include_only_restricts() {
        let f = TableFilter::new(Some(&s(&["users", "post*"])), None).unwrap();
        assert!(f.keep("users"));
        assert!(f.keep("posts"));
        assert!(f.keep("post_tags"));
        assert!(!f.keep("audit"));
    }

    #[test]
    fn ignore_only_excludes() {
        let f = TableFilter::new(None, Some(&s(&["audit_*", "_temp_*"]))).unwrap();
        assert!(f.keep("users"));
        assert!(!f.keep("audit_log"));
        assert!(!f.keep("_temp_x"));
    }

    #[test]
    fn include_then_ignore() {
        let f = TableFilter::new(Some(&s(&["*"])), Some(&s(&["audit_*"]))).unwrap();
        assert!(f.keep("users"));
        assert!(!f.keep("audit_log"));
    }

    #[test]
    fn invalid_glob_fails_construction() {
        let err = TableFilter::new(Some(&s(&["users[unclosed"])), None).unwrap_err();
        assert!(format!("{err:#}").contains("invalid glob pattern"));
    }
}
