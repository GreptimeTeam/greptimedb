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

use crate::session_config::{Error, InvalidConfigValueSnafu};

/// Returns the canonical name for a supported option key.
pub fn canonical_query_option_name(key: &str) -> Option<&'static str> {
    let key = key.to_ascii_lowercase();
    match key.as_str() {
        "query_parallelism" | "query.parallelism" => Some("query.parallelism"),
        "query_fallback" | "allow_query_fallback" | "query.allow_query_fallback" => {
            Some("query.allow_query_fallback")
        }
        "query.enable_remote_dynamic_filter_pushdown" => {
            Some("query.enable_remote_dynamic_filter_pushdown")
        }
        _ => DATAFUSION_OPTIONS
            .iter()
            .find(|name| **name == key)
            .copied(),
    }
}

const DATAFUSION_OPTIONS: &[&str] = &[
    "datafusion.optimizer.repartition_joins",
    "datafusion.optimizer.repartition_aggregations",
    "datafusion.optimizer.repartition_sorts",
    "datafusion.optimizer.repartition_windows",
    "datafusion.optimizer.enable_round_robin_repartition",
    "datafusion.optimizer.prefer_existing_sort",
    "datafusion.optimizer.prefer_hash_join",
    "datafusion.optimizer.join_reordering",
    "datafusion.optimizer.enable_topk_aggregation",
    "datafusion.optimizer.enable_dynamic_filter_pushdown",
];

/// Validate a query option, returning its canonical name and normalized value.
pub fn parse_query_option(key: &str, value: &str) -> Result<Option<(String, String)>, Error> {
    let key_lower = key.to_ascii_lowercase();
    let canonical = match canonical_query_option_name(key) {
        Some(name) => name,
        None if key_lower.starts_with("query.") || key_lower.starts_with("datafusion.") => {
            return InvalidConfigValueSnafu {
                name: key,
                value,
                hint: "Unknown or unsupported query option",
            }
            .fail();
        }
        None => return Ok(None),
    };

    let normalized = if canonical == "query.parallelism" {
        match value.parse::<u16>() {
            Ok(n @ 1..=1024) => n.to_string(),
            _ => {
                return InvalidConfigValueSnafu {
                    name: key,
                    value,
                    hint: "query.parallelism must be between 1 and 1024",
                }
                .fail();
            }
        }
    } else if canonical.starts_with("query.") {
        match value.to_ascii_lowercase().as_str() {
            "true" => "true".to_string(),
            "false" => "false".to_string(),
            _ => {
                return InvalidConfigValueSnafu {
                    name: key,
                    value,
                    hint: "Expected true or false",
                }
                .fail();
            }
        }
    } else {
        let mut options = datafusion_common::config::ConfigOptions::default();
        if let Err(error) = options.set(canonical, value) {
            return InvalidConfigValueSnafu {
                name: key,
                value,
                hint: error.to_string(),
            }
            .fail();
        }
        value.to_ascii_lowercase()
    };
    Ok(Some((canonical.to_string(), normalized)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_and_validates_query_options() {
        assert_eq!(
            parse_query_option("QUERY_PARALLELISM", "1024").unwrap(),
            Some(("query.parallelism".into(), "1024".into()))
        );
        assert!(parse_query_option("query.parallelism", "0").is_err());
        assert!(parse_query_option("query.parallelism", "1025").is_err());
        assert_eq!(
            parse_query_option("query_fallback", "TRUE").unwrap(),
            Some(("query.allow_query_fallback".into(), "true".into()))
        );
        assert!(parse_query_option("query.allow_query_fallback", "yes").is_err());
        assert!(parse_query_option("datafusion.optimizer.repartition_joins", "nope").is_err());
        assert!(parse_query_option("datafusion.execution.batch_size", "12").is_err());
        assert_eq!(
            parse_query_option("flow.return_region_seq", "true").unwrap(),
            None
        );
    }
}
