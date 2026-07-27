//! `HIVE_*` aliases for `BUZZ_*` environment variables (Hive plan §8 Phase 6).
//!
//! The branding sweep keeps `BUZZ_*` as the canonical, documented names for
//! now, but every relay setting can equivalently be provided as `HIVE_*`
//! (same suffix). Precedence: `BUZZ_*` wins when both are set — existing
//! deployments keep exactly today's behavior, and a stray `HIVE_*` in the
//! environment can never override explicit `BUZZ_*` configuration.
//!
//! Non-`BUZZ_`-prefixed settings (`DATABASE_URL`, `REDIS_URL`, `RELAY_URL`,
//! …) are deliberately not aliased — they are protocol-conventional names,
//! not product branding.

use std::env::VarError;

/// Read `buzz_name` from the environment, falling back to its `HIVE_*` alias.
///
/// `buzz_name` must be the canonical `BUZZ_*` spelling; the alias is derived
/// by prefix substitution (`BUZZ_PROFILE` → `HIVE_PROFILE`). Returns the same
/// `Result` shape as [`std::env::var`], so call sites are drop-in.
pub fn var(buzz_name: &str) -> Result<String, VarError> {
    debug_assert!(
        buzz_name.starts_with("BUZZ_"),
        "env_alias::var takes canonical BUZZ_* names, got {buzz_name}"
    );
    match std::env::var(buzz_name) {
        Err(VarError::NotPresent) => {
            let alias = format!("HIVE_{}", buzz_name.trim_start_matches("BUZZ_"));
            std::env::var(alias)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::var;

    // Each test uses a unique variable name: `std::env` is process-global and
    // the test harness runs threads in parallel.

    #[test]
    fn buzz_name_wins_when_both_set() {
        std::env::set_var("BUZZ_ALIAS_TEST_BOTH", "buzz");
        std::env::set_var("HIVE_ALIAS_TEST_BOTH", "hive");
        assert_eq!(var("BUZZ_ALIAS_TEST_BOTH").as_deref(), Ok("buzz"));
    }

    #[test]
    fn hive_alias_fills_in_when_buzz_absent() {
        std::env::set_var("HIVE_ALIAS_TEST_ONLY", "hive");
        assert_eq!(var("BUZZ_ALIAS_TEST_ONLY").as_deref(), Ok("hive"));
    }

    #[test]
    fn absent_everywhere_is_not_present() {
        assert!(matches!(
            var("BUZZ_ALIAS_TEST_UNSET"),
            Err(std::env::VarError::NotPresent)
        ));
    }
}
