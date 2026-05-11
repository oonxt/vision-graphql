//! TOML template rendering and helpers (URL redaction, header metadata).

/// Strip the password component of a postgres URL so it is safe to print.
///
/// Returns the original input unchanged when it cannot be parsed as a URL.
pub fn redact_url(raw: &str) -> String {
    let scheme_sep = match raw.find("://") {
        Some(i) => i + 3,
        None => return raw.to_string(),
    };
    let (scheme, rest) = raw.split_at(scheme_sep);
    let (authority, tail) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (userinfo, hostpart) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    match userinfo {
        Some(ui) => {
            let user = ui.split(':').next().unwrap_or("");
            if user.is_empty() {
                format!("{scheme}{hostpart}{tail}")
            } else {
                format!("{scheme}{user}@{hostpart}{tail}")
            }
        }
        None => raw.to_string(),
    }
}

/// Metadata embedded in the header of a generated schema.toml.
pub struct HeaderMeta {
    pub tool_version: String,
    pub timestamp_iso8601: String,
    pub redacted_source_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password_keeps_user() {
        let s = redact_url("postgres://alice:supersecret@db.example.com:5432/myapp");
        assert_eq!(s, "postgres://alice@db.example.com:5432/myapp");
    }

    #[test]
    fn no_userinfo_passes_through() {
        let s = redact_url("postgres://db.example.com:5432/myapp");
        assert_eq!(s, "postgres://db.example.com:5432/myapp");
    }

    #[test]
    fn user_only_no_password() {
        let s = redact_url("postgres://alice@db.example.com/myapp");
        assert_eq!(s, "postgres://alice@db.example.com/myapp");
    }

    #[test]
    fn unparseable_returned_unchanged() {
        let s = redact_url("not-a-url");
        assert_eq!(s, "not-a-url");
    }

    #[test]
    fn no_path_redacts() {
        let s = redact_url("postgres://alice:pw@host:5432");
        assert_eq!(s, "postgres://alice@host:5432");
    }
}
