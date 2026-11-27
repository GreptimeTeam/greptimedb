// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Values for the MySQL system variables that GreptimeDB reports but does not
//! actually implement.
//!
//! Two consumers need the same numbers: the MySQL protocol layer, which answers
//! `@@name` and `SHOW VARIABLES`, and `information_schema.session_variables`,
//! which reports a session as rows. Keeping the definitions here rather than in
//! either consumer is what lets them agree: `catalog` cannot depend on `servers`
//! (the dependency runs the other way), and duplicating the table in both would
//! let the two drift apart.

use common_version;

/// The `sql_mode` GreptimeDB reports.
///
/// GreptimeDB does not implement MySQL's strict-mode semantics; this string
/// exists so that clients which probe `sql_mode` to decide how to build their
/// statements get a plausible, stable answer.
pub const SQL_MODE: &str = "ONLY_FULL_GROUP_BY STRICT_TRANS_TABLES NO_ZERO_IN_DATE NO_ZERO_DATE ERROR_FOR_DIVISION_BY_ZERO NO_ENGINE_SUBSTITUTION";

/// Every variable this module defines, as `(name, value)` pairs in the order
/// `information_schema.session_variables` reports them.
///
/// Only canonical names appear here. The protocol layer additionally accepts
/// the `session.`-prefixed aliases below; the table reports one row per
/// variable, so the aliases are resolved there instead of duplicated here.
pub fn compatibility_variables() -> Vec<(&'static str, &'static str)> {
    vec![
        ("transaction_isolation", "REPEATABLE-READ"),
        ("transaction_read_only", "0"),
        ("max_allowed_packet", "134217728"),
        ("interactive_timeout", "31536000"),
        ("wait_timeout", "31536000"),
        ("net_write_timeout", "31536000"),
        ("sql_mode", SQL_MODE),
        ("version_comment", common_version::product_name()),
    ]
}

/// Resolves a MySQL variable name to the value GreptimeDB reports for it.
///
/// Accepts the canonical names from [`compatibility_variables`] plus the
/// aliases MySQL clients send: the `session.` scope prefix, and the pre-5.7.20
/// `tx_isolation` spelling of `transaction_isolation`.
///
/// Returns `None` for names this module does not define, leaving the caller to
/// decide what an unknown variable means.
pub fn value_of(name: &str) -> Option<&'static str> {
    match name {
        "tx_isolation"
        | "session.tx_isolation"
        | "transaction_isolation"
        | "session.transaction_isolation" => Some("REPEATABLE-READ"),
        "transaction_read_only" | "session.transaction_read_only" => Some("0"),
        "max_allowed_packet" => Some("134217728"),
        "interactive_timeout" | "wait_timeout" | "net_write_timeout" => Some("31536000"),
        "sql_mode" | "session.sql_mode" => Some(SQL_MODE),
        "version_comment" => Some(common_version::product_name()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_reported_variable_resolves() {
        for (canonical, expected) in compatibility_variables() {
            assert_eq!(Some(expected), value_of(canonical), "{canonical}");
        }
    }

    #[test]
    fn test_session_prefixed_aliases_resolve() {
        for canonical in ["transaction_isolation", "transaction_read_only", "sql_mode"] {
            assert!(
                value_of(&format!("session.{canonical}")).is_some(),
                "session.{canonical}"
            );
        }
    }

    #[test]
    fn test_tx_isolation_alias() {
        // The old spelling is not a `session.` alias of a canonical name, so it
        // needs its own arm.
        assert_eq!(Some("REPEATABLE-READ"), value_of("tx_isolation"));
        assert_eq!(Some("REPEATABLE-READ"), value_of("session.tx_isolation"));
    }

    #[test]
    fn test_unknown_variable_has_no_value() {
        assert_eq!(None, value_of("some_unsupported_var"));
        assert_eq!(None, value_of(""));
    }
}
