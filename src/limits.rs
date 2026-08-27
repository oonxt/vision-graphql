//! Bounds applied to a GraphQL document *before* it is parsed.
//!
//! Every other check in this crate runs on the parsed document or on the IR.
//! This one cannot: nesting an input value deeply enough overflows the stack
//! inside the parser itself, and a stack overflow in Rust aborts the process —
//! it is not a panic, so no `catch_unwind` at the request boundary can contain
//! it. One 16 KiB request would take down the server and every request in
//! flight with it.
//!
//! So the guard has to be a scan of the raw text, and it has to sit at the one
//! place every path funnels through: [`parse_document`](crate::parser::parse_document).
//!
//! ```text
//! { users(where: {_not: {_not: … × 2000 … }}) { id } }   ~16 KiB
//!     → fatal runtime error: stack overflow, aborting     (2 MiB stack)
//! ```
//!
//! 2 MiB is what a tokio worker thread gets by default, so that is the size
//! that matters for a server; an 8 MiB main thread only moves the cliff to
//! ~8000. Selection-set nesting is already bounded by the parser's own
//! recursion limit — it is input values (`where`, `_set`, `objects`) that have
//! no guard, which is why the depth counted here is over all bracket kinds
//! rather than just braces.
//!
//! The defaults are far above any hand-written or generated query and are meant
//! to be left alone; [`ParseLimits`] exists so a caller with an unusual
//! workload can raise them, and so an endpoint can lower them.

use crate::error::{Error, Result};

/// Limits on the raw text of a document.
///
/// Both are coarse by design: this runs on every request before parsing, so it
/// is a single pass over the bytes with no allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseLimits {
    /// Maximum nesting depth, counting `{`, `[` and `(` together.
    ///
    /// Selection sets, argument lists and input objects all contribute, so a
    /// query nesting five relations deep with a `where` on each sits around 20.
    /// [`DEFAULT_MAX_DEPTH`] leaves room for that and still stops the stack
    /// overflow by two orders of magnitude.
    pub max_depth: usize,
    /// Maximum length of the document in bytes.
    ///
    /// The depth check is what prevents the crash; this is the cheap cut that
    /// keeps a pathologically wide document (thousands of aliased fields, each
    /// legal on its own) from reaching the parser at all.
    pub max_bytes: usize,
}

/// See [`ParseLimits::max_depth`].
pub const DEFAULT_MAX_DEPTH: usize = 64;

/// See [`ParseLimits::max_bytes`].
pub const DEFAULT_MAX_BYTES: usize = 128 * 1024;

impl Default for ParseLimits {
    fn default() -> Self {
        ParseLimits {
            max_depth: DEFAULT_MAX_DEPTH,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl ParseLimits {
    /// Limits that reject nothing. For a caller whose documents are its own
    /// source code rather than request input.
    ///
    /// This re-opens the stack overflow described in the module docs — only use
    /// it where the document text cannot come from a client.
    pub fn unbounded() -> Self {
        ParseLimits {
            max_depth: usize::MAX,
            max_bytes: usize::MAX,
        }
    }

    /// Check `source` against these limits.
    ///
    /// One pass over the bytes. String literals (including block strings) and
    /// `#` comments are skipped, so a bracket inside `_eq: "{{{{"` does not
    /// count toward the depth. Unbalanced or unterminated input is not this
    /// function's business — it stops scanning and lets the parser produce the
    /// syntax error it would have produced anyway.
    pub fn check(&self, source: &str) -> Result<()> {
        if source.len() > self.max_bytes {
            return Err(Error::Limit {
                message: format!(
                    "document is {} bytes, over the {}-byte limit",
                    source.len(),
                    self.max_bytes
                ),
            });
        }

        // Byte-wise is sound here: every byte matched is ASCII, and no byte of a
        // multi-byte UTF-8 sequence can collide with one.
        let b = source.as_bytes();
        let mut i = 0;
        let mut depth = 0usize;
        while i < b.len() {
            match b[i] {
                b'#' => {
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                }
                b'"' => i = skip_string(b, i),
                b'{' | b'[' | b'(' => {
                    depth += 1;
                    if depth > self.max_depth {
                        return Err(Error::Limit {
                            message: format!(
                                "document nests deeper than the limit of {}",
                                self.max_depth
                            ),
                        });
                    }
                    i += 1;
                }
                b'}' | b']' | b')' => {
                    depth = depth.saturating_sub(1);
                    i += 1;
                }
                _ => i += 1,
            }
        }
        Ok(())
    }
}

/// Index just past the string literal starting at `i` (which must be a `"`), or
/// the end of input if it is unterminated.
fn skip_string(b: &[u8], i: usize) -> usize {
    if b[i..].starts_with(br#"""""#) {
        let mut i = i + 3;
        while i < b.len() {
            if b[i] == b'\\' {
                i += 2; // \""" is the only escape a block string has
                continue;
            }
            if b[i..].starts_with(br#"""""#) {
                return i + 3;
            }
            i += 1;
        }
        return b.len();
    }
    let mut i = i + 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    b.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth_of(source: &str) -> Result<()> {
        ParseLimits::default().check(source)
    }

    #[test]
    fn ordinary_queries_pass() {
        depth_of("{ users { id name } }").unwrap();
        depth_of(
            r#"query($id: Int!) {
                 users(where: {_and: [{id: {_eq: $id}}, {name: {_is_null: false}}]}) {
                   id
                   posts(order_by: [{id: asc}], limit: 5) { title comments { body } }
                 }
               }"#,
        )
        .unwrap();
    }

    #[test]
    fn deep_input_value_is_rejected() {
        let q = format!(
            "{{ users(where: {}{}{}) {{ id }} }}",
            "{_not: ".repeat(2000),
            "{id: {_eq: 1}}",
            "}".repeat(2000)
        );
        let err = depth_of(&q).unwrap_err();
        assert!(format!("{err}").contains("nests deeper"), "{err}");
    }

    #[test]
    fn depth_is_counted_across_bracket_kinds() {
        let limits = ParseLimits {
            max_depth: 3,
            ..Default::default()
        };
        limits.check("{ a(b: [1]) }").unwrap(); // { ( [  => 3
        let err = limits.check("{ a(b: [[1]]) }").unwrap_err(); // => 4
        assert!(format!("{err}").contains("nests deeper"), "{err}");
    }

    #[test]
    fn brackets_inside_strings_do_not_count() {
        let limits = ParseLimits {
            max_depth: 2,
            ..Default::default()
        };
        limits
            .check(r#"{ users(where: "{{{{{{{{") { id } }"#)
            .unwrap();
        limits
            .check(r#"{ users(where: "\"{{{{") { id } }"#)
            .unwrap();
        limits
            .check("{ users(where: \"\"\"{{{{{{\"\"\") { id } }")
            .unwrap();
    }

    #[test]
    fn brackets_inside_comments_do_not_count() {
        let limits = ParseLimits {
            max_depth: 2,
            ..Default::default()
        };
        limits.check("{ users { id } } # {{{{{{{{").unwrap();
        limits.check("# {{{{\n{ users { id } }").unwrap();
    }

    #[test]
    fn unterminated_string_is_left_to_the_parser() {
        // No panic, no false rejection: the parser reports the syntax error.
        ParseLimits::default()
            .check(r#"{ users(name: "oops) { id } }"#)
            .unwrap();
    }

    #[test]
    fn oversized_document_is_rejected() {
        let limits = ParseLimits {
            max_bytes: 16,
            ..Default::default()
        };
        let err = limits.check("{ users { id name active } }").unwrap_err();
        assert!(format!("{err}").contains("over the 16-byte limit"), "{err}");
    }

    #[test]
    fn unbounded_accepts_what_default_rejects() {
        let q = format!("{}{}", "{a(b: [".repeat(100), "]) }".repeat(100));
        assert!(ParseLimits::default().check(&q).is_err());
        ParseLimits::unbounded().check(&q).unwrap();
    }

    #[test]
    fn multibyte_text_does_not_confuse_the_scan() {
        let limits = ParseLimits {
            max_depth: 2,
            ..Default::default()
        };
        limits
            .check(r#"{ users(name: "中文｛括号｝") { id } }"#)
            .unwrap();
    }
}
