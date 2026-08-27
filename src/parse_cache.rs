//! Bounded cache of parsed GraphQL documents.
//!
//! Parsing the GraphQL text is the most expensive step of turning a request
//! into SQL — roughly 70% of it — and it is the only step that does not depend
//! on the request's variables or on the caller's scope. So it is the one step
//! that can be shared across requests without a key that includes those.
//!
//! The key is the full source string, not a hash of it: a hash collision would
//! run a *different* caller's query, which is a security bug, not a cache miss.
//!
//! Eviction is two-generation rather than true LRU: entries land in `hot`, and
//! when `hot` fills it demotes wholesale to `cold` (dropping the previous
//! `cold`). A hit in `cold` promotes back to `hot`. That keeps every operation
//! O(1) with no bookkeeping per entry, and a query that keeps being used never
//! falls out. The cost is looser eviction than LRU — at most `2 * capacity`
//! documents are retained.

use crate::error::Result;
use crate::limits::ParseLimits;
use crate::parser::parse_document_with;
use async_graphql_parser::types::ExecutableDocument;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Documents kept in the hot generation before it is demoted. At most twice
/// this many documents are retained overall.
pub const DEFAULT_CAPACITY: usize = 256;

/// Sources longer than this are parsed on every request and never stored, so a
/// caller sending huge one-off documents cannot pin `2 * capacity` of them in
/// memory. Hand-written queries are orders of magnitude below the limit.
const MAX_CACHED_SOURCE: usize = 16 * 1024;

/// Thread-safe, bounded cache of `source → parsed document`.
///
/// Cloning shares one cache: [`Engine`](crate::Engine) hands the same instance
/// to every handle it spawns.
#[derive(Debug)]
pub struct ParseCache {
    capacity: usize,
    limits: ParseLimits,
    inner: Mutex<Generations>,
}

#[derive(Debug, Default)]
struct Generations {
    hot: HashMap<Box<str>, Arc<ExecutableDocument>>,
    cold: HashMap<Box<str>, Arc<ExecutableDocument>>,
}

impl Default for ParseCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl ParseCache {
    /// A cache holding up to `capacity` documents in its hot generation.
    /// `capacity == 0` disables caching: every call parses.
    pub fn new(capacity: usize) -> Self {
        Self::with_limits(capacity, ParseLimits::default())
    }

    /// A cache that also carries the [`ParseLimits`] every document going
    /// through it is checked against.
    ///
    /// The limits live here rather than on [`Engine`](crate::Engine) because
    /// this is what sits in front of the parser on every path, cached or not.
    pub fn with_limits(capacity: usize, limits: ParseLimits) -> Self {
        Self {
            capacity,
            limits,
            inner: Mutex::new(Generations::default()),
        }
    }

    /// The limits documents are checked against.
    pub fn limits(&self) -> &ParseLimits {
        &self.limits
    }

    /// Parsed form of `source`, parsing only on a miss.
    ///
    /// Parsing happens outside the lock, so a burst of concurrent misses for
    /// the same source may parse it more than once — wasted work, never a
    /// wrong answer — instead of serialising every caller behind one parse.
    pub fn get(&self, source: &str) -> Result<Arc<ExecutableDocument>> {
        if self.capacity == 0 || source.len() > MAX_CACHED_SOURCE {
            return parse_document_with(source, &self.limits).map(Arc::new);
        }
        if let Some(doc) = self.lookup(source) {
            return Ok(doc);
        }
        let doc = Arc::new(parse_document_with(source, &self.limits)?);
        self.insert(source, doc.clone());
        Ok(doc)
    }

    fn lookup(&self, source: &str) -> Option<Arc<ExecutableDocument>> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(doc) = g.hot.get(source) {
            return Some(doc.clone());
        }
        // A cold hit is still live traffic: move it back to hot so it survives
        // the next demotion.
        let (key, doc) = g.cold.remove_entry(source)?;
        g.hot.insert(key, doc.clone());
        Some(doc)
    }

    fn insert(&self, source: &str, doc: Arc<ExecutableDocument>) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.hot.len() >= self.capacity {
            g.cold = std::mem::take(&mut g.hot);
        }
        g.hot.insert(source.into(), doc);
    }

    /// Documents currently retained across both generations. Test/observability
    /// helper; not a hit-rate metric.
    pub fn len(&self) -> usize {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.hot.len() + g.cold.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every cached document. The schema is not part of the key, so a
    /// caller that swaps schemas does not need this — parsing is
    /// schema-independent — but it is here for tests and for reclaiming memory.
    pub fn clear(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.hot.clear();
        g.cold.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q: &str = "{ users { id } }";

    /// The document that aborted the process before the pre-parse guard existed
    /// — a stack overflow inside the parser, on a stack the size tokio gives a
    /// worker thread. Reaching the assertion at all is most of the test.
    fn over_deep() -> String {
        format!(
            "{{ users(where: {}{}{}) {{ id }} }}",
            "{_not: ".repeat(2000),
            "{id: {_eq: 1}}",
            "}".repeat(2000)
        )
    }

    #[test]
    fn over_deep_document_is_rejected_and_not_cached() {
        let cache = ParseCache::new(8);
        let err = cache.get(&over_deep()).unwrap_err();
        assert!(matches!(err, crate::Error::Limit { .. }), "{err}");
        assert!(cache.is_empty());
    }

    #[test]
    fn limits_apply_on_the_uncached_path_too() {
        // capacity 0 skips the cache entirely; the guard must still run.
        let err = ParseCache::new(0).get(&over_deep()).unwrap_err();
        assert!(matches!(err, crate::Error::Limit { .. }), "{err}");
    }

    #[test]
    fn custom_limits_are_honoured() {
        let cache = ParseCache::with_limits(
            8,
            crate::ParseLimits {
                max_depth: 2,
                ..Default::default()
            },
        );
        assert!(cache.get("{ users { id } }").is_ok());
        let err = cache.get("{ users { posts { id } } }").unwrap_err();
        assert!(matches!(err, crate::Error::Limit { .. }), "{err}");
    }

    #[test]
    fn second_get_returns_the_same_document() {
        let c = ParseCache::default();
        let a = c.get(Q).unwrap();
        let b = c.get(Q).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "expected the cached document back");
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn distinct_sources_do_not_collide() {
        let c = ParseCache::default();
        let a = c.get("{ users { id } }").unwrap();
        let b = c.get("{ posts { id } }").unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn parse_errors_are_not_cached() {
        let c = ParseCache::default();
        assert!(c.get("{ users { id }").is_err());
        assert!(c.is_empty());
    }

    #[test]
    fn capacity_zero_disables_caching() {
        let c = ParseCache::new(0);
        let a = c.get(Q).unwrap();
        let b = c.get(Q).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert!(c.is_empty());
    }

    #[test]
    fn hot_demotes_to_cold_and_retention_stays_bounded() {
        let c = ParseCache::new(2);
        for i in 0..6 {
            c.get(&format!("{{ t{i} {{ id }} }}")).unwrap();
        }
        assert!(c.len() <= 4, "retained {} documents", c.len());
    }

    #[test]
    fn cold_hit_promotes_and_survives_the_next_demotion() {
        let c = ParseCache::new(2);
        let first = c.get("{ a { id } }").unwrap();
        c.get("{ b { id } }").unwrap();
        // Demote: hot {a,b} → cold, hot {c}.
        c.get("{ c { id } }").unwrap();
        // Cold hit on `a` promotes it back into hot...
        assert!(Arc::ptr_eq(&c.get("{ a { id } }").unwrap(), &first));
        // ...so the next demotion drops {b}, not {a}.
        c.get("{ d { id } }").unwrap();
        c.get("{ e { id } }").unwrap();
        assert!(Arc::ptr_eq(&c.get("{ a { id } }").unwrap(), &first));
    }

    #[test]
    fn oversized_sources_are_parsed_but_not_stored() {
        let c = ParseCache::default();
        let big = format!("{{ users {{ id {} }} }}", "a".repeat(MAX_CACHED_SOURCE));
        assert!(big.len() > MAX_CACHED_SOURCE);
        c.get(&big).unwrap();
        assert!(c.is_empty());
    }

    #[test]
    fn cache_is_shared_across_threads() {
        let c = Arc::new(ParseCache::default());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = c.clone();
                std::thread::spawn(move || c.get(Q).unwrap())
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(c.len(), 1);
    }
}
