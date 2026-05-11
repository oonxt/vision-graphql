//! Configure tracing-subscriber from -v/-q flags.

use tracing::Level;

pub fn level_from_flags(verbose: u8, quiet: bool) -> Option<Level> {
    if quiet {
        None
    } else {
        match verbose {
            0 => Some(Level::WARN),
            1 => Some(Level::DEBUG),
            _ => Some(Level::TRACE),
        }
    }
}

pub fn install(verbose: u8, quiet: bool) {
    let Some(level) = level_from_flags(verbose, quiet) else {
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_overrides_verbose() {
        assert!(level_from_flags(2, true).is_none());
    }

    #[test]
    fn defaults_to_warn() {
        assert_eq!(level_from_flags(0, false), Some(Level::WARN));
    }

    #[test]
    fn one_v_is_debug() {
        assert_eq!(level_from_flags(1, false), Some(Level::DEBUG));
    }

    #[test]
    fn two_v_is_trace() {
        assert_eq!(level_from_flags(2, false), Some(Level::TRACE));
        assert_eq!(level_from_flags(5, false), Some(Level::TRACE));
    }
}
