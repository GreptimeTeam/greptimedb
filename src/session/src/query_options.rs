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

use datafusion_common::config::{ConfigField, ConfigOptions, Visit};

use crate::hint_registry::{
    HintCategory, HintDefault, HintDefinition, HintMetadata, HintRegistry, HintValueType,
};
use crate::session_config::{Error, InvalidConfigValueSnafu};

/// Query validation limits supplied by public or internal entry points.
#[derive(Debug, Clone, Copy)]
pub struct QueryParseOptions {
    pub max_parallelism: u16,
}

/// Canonical name of the maximum query parallelism setting.
pub const QUERY_PARALLELISM: &str = "query.parallelism";
/// Canonical name of the distributed query fallback setting.
pub const QUERY_ALLOW_QUERY_FALLBACK: &str = "query.allow_query_fallback";
/// Canonical name of the remote dynamic filter pushdown setting.
pub const QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN: &str =
    "query.enable_remote_dynamic_filter_pushdown";

/// Default used when a request does not override remote dynamic filter pushdown.
pub const DEFAULT_REMOTE_DYNAMIC_FILTER_PUSHDOWN: bool = true;

const QUERY_PARALLELISM_ALIAS: &str = "query_parallelism";
const QUERY_FALLBACK_ALIAS: &str = "query_fallback";
const ALLOW_QUERY_FALLBACK_ALIAS: &str = "allow_query_fallback";
/// Public namespace of supported DataFusion optimizer settings.
pub const DATAFUSION_OPTIMIZER_PREFIX: &str = "datafusion.optimizer.";

/// A public query option shared by parsing and hint discovery.
pub struct QueryOptionDefinition {
    /// Canonical setting name.
    pub name: &'static str,
    /// Accepted alternative names.
    pub aliases: &'static [&'static str],
    /// Public scalar value type.
    pub value_type: HintValueType,
    /// Purpose of the setting.
    pub description: &'static str,
    /// Fixed fallback when the query engine does not supply an effective default.
    pub default: Option<fn() -> String>,
}

/// Query options accepted through both session settings and protocol hints.
pub const QUERY_OPTIONS: &[QueryOptionDefinition] = &[
    QueryOptionDefinition {
        name: QUERY_PARALLELISM,
        aliases: &[QUERY_PARALLELISM_ALIAS],
        value_type: HintValueType::Integer,
        description: "Maximum query parallelism.",
        default: None,
    },
    QueryOptionDefinition {
        name: QUERY_ALLOW_QUERY_FALLBACK,
        aliases: &[QUERY_FALLBACK_ALIAS, ALLOW_QUERY_FALLBACK_ALIAS],
        value_type: HintValueType::Boolean,
        description: "Allow query fallback when distributed pushdown fails.",
        default: None,
    },
    QueryOptionDefinition {
        name: QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN,
        aliases: &[],
        value_type: HintValueType::Boolean,
        description: "Enable remote dynamic filter pushdown.",
        default: Some(|| DEFAULT_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string()),
    },
];

/// Resolves an omitted query option from its effective configuration or registered fallback.
/// Optimizer options without an exposed engine default remain unspecified.
pub fn default_query_option(name: &str, configured: Option<&str>) -> HintDefault {
    let value = configured.map(str::to_string).or_else(|| {
        QUERY_OPTIONS
            .iter()
            .find(|option| option.name == name)
            .and_then(|option| option.default)
            .map(|default| default())
    });
    let description = value
        .is_none()
        .then_some("The query handler does not expose its configured default.");
    HintDefault { value, description }
}

/// Returns a canonical query option alias or a DataFusion optimizer option name.
pub fn canonical_query_option_name(key: &str) -> Option<String> {
    let key = key.to_ascii_lowercase();
    QUERY_OPTIONS
        .iter()
        .find(|option| option.name == key || option.aliases.contains(&key.as_str()))
        .map(|option| option.name.to_string())
        .or_else(|| key.starts_with(DATAFUSION_OPTIMIZER_PREFIX).then_some(key))
}

/// Registers query options for an entry point's target and default environment.
/// Names, aliases, value domains and parsers are identical for HTTP and SET.
pub fn definitions<T, D>(
    apply: fn(&mut T, &str, String),
    default: fn(&D, &str) -> HintDefault,
) -> Vec<HintDefinition<T, D, QueryParseOptions>> {
    let mut definitions = Vec::new();
    for option in QUERY_OPTIONS {
        let mut metadata = HintMetadata::new(option.name, option.value_type, option.description);
        metadata.aliases = option.aliases.to_vec();
        definitions.push(query_definition(
            metadata,
            HintCategory::Query,
            apply,
            default,
        ));
    }
    let mut visitor = OptimizerDefinitions {
        definitions,
        apply,
        default,
    };
    ConfigOptions::default()
        .optimizer
        .visit(&mut visitor, "datafusion.optimizer", "");
    visitor.definitions
}

fn query_definition<T, D>(
    metadata: HintMetadata,
    category: HintCategory,
    apply: fn(&mut T, &str, String),
    default: fn(&D, &str) -> HintDefault,
) -> HintDefinition<T, D, QueryParseOptions> {
    HintDefinition {
        metadata,
        category,
        parse: parse_registered_query_option,
        apply,
        default,
        reject_conflicts: true,
    }
}

fn parse_registered_query_option(
    key: &str,
    value: &str,
    options: QueryParseOptions,
) -> Result<String, Error> {
    match parse_query_option(key, value, options.max_parallelism)? {
        Some((_, value)) => Ok(value),
        None => InvalidConfigValueSnafu {
            name: key,
            value,
            hint: "Not a supported query option",
        }
        .fail(),
    }
}

struct OptimizerDefinitions<T, D> {
    definitions: Vec<HintDefinition<T, D, QueryParseOptions>>,
    apply: fn(&mut T, &str, String),
    default: fn(&D, &str) -> HintDefault,
}

impl<T, D> Visit for OptimizerDefinitions<T, D> {
    fn some<V: std::fmt::Display>(&mut self, key: &str, _value: V, description: &'static str) {
        let value_type = match std::any::type_name::<V>().trim_start_matches('&') {
            "bool" => HintValueType::Boolean,
            "usize" | "u64" | "u32" | "u16" | "u8" | "i64" | "i32" => HintValueType::Integer,
            "f64" | "f32" => HintValueType::Number,
            _ => HintValueType::String,
        };
        self.definitions.push(query_definition(
            HintMetadata::new(key, value_type, description),
            HintCategory::Optimizer,
            self.apply,
            self.default,
        ));
    }

    fn none(&mut self, key: &str, description: &'static str) {
        self.definitions.push(query_definition(
            HintMetadata::new(key, HintValueType::String, description),
            HintCategory::Optimizer,
            self.apply,
            self.default,
        ));
    }
}

/// Maximum query parallelism accepted through public query-option inputs.
pub const MAX_PUBLIC_PARALLELISM: u16 = 1024;

/// Validate a query option, returning its canonical name and normalized value.
pub fn parse_query_option(
    key: &str,
    value: &str,
    max_parallelism: u16,
) -> Result<Option<(String, String)>, Error> {
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

    let normalized = if canonical == QUERY_PARALLELISM {
        match value.parse::<u16>() {
            Ok(n) if (1..=max_parallelism).contains(&n) => n.to_string(),
            _ => {
                return InvalidConfigValueSnafu {
                    name: key,
                    value,
                    hint: format!("query.parallelism must be between 1 and {max_parallelism}"),
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
        let set_result = options.set(&canonical, value).or_else(|error| {
            if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                options.set(&canonical, &value.to_ascii_lowercase())
            } else {
                Err(error)
            }
        });
        if let Err(error) = set_result {
            return InvalidConfigValueSnafu {
                name: key,
                value,
                hint: error.to_string(),
            }
            .fail();
        }
        match options
            .entries()
            .into_iter()
            .find(|entry| entry.key == canonical)
            .and_then(|entry| entry.value)
        {
            Some(value) => value,
            None => {
                return InvalidConfigValueSnafu {
                    name: key,
                    value,
                    hint: "No matching DataFusion configuration entry",
                }
                .fail();
            }
        }
    };
    Ok(Some((canonical, normalized)))
}

/// Validates query hints as one batch while preserving unrelated extensions.
pub fn normalize_query_option_batch(
    hints: impl IntoIterator<Item = (String, String)>,
    max_parallelism: u16,
) -> Result<Vec<(String, String)>, Error> {
    static REGISTRY: std::sync::LazyLock<HintRegistry<(), (), QueryParseOptions>> =
        std::sync::LazyLock::new(|| {
            HintRegistry::new(definitions(
                |_, _, _| {},
                |_, name| default_query_option(name, None),
            ))
        });
    let batch = REGISTRY.parse_grouped(hints, QueryParseOptions { max_parallelism })?;
    let normalized = batch.into_normalized();
    for (name, value) in &normalized {
        if REGISTRY.find(name).is_none() {
            parse_query_option(name, value, max_parallelism)?;
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_definition_categories_and_parsers() {
        let registry = HintRegistry::new(definitions(
            |_: &mut (), _, _| {},
            |_: &(), name| default_query_option(name, None),
        ));
        registry.validate_definitions().unwrap();
        for definition in registry.definitions() {
            let name = &definition.metadata.name;
            let expected = if name.starts_with(DATAFUSION_OPTIMIZER_PREFIX) {
                HintCategory::Optimizer
            } else {
                HintCategory::Query
            };
            assert_eq!(definition.category, expected, "{name}");
            for value in ["", "0", "1", "1024", "2048", "TRUE", "false", "invalid"] {
                let registered = registry
                    .parse(
                        name,
                        value,
                        QueryParseOptions {
                            max_parallelism: MAX_PUBLIC_PARALLELISM,
                        },
                    )
                    .map(|hint| {
                        hint.map(|hint| (hint.name().to_string(), hint.value().to_string()))
                    })
                    .map_err(|error| error.to_string());
                let original = parse_query_option(name, value, MAX_PUBLIC_PARALLELISM)
                    .map_err(|error| error.to_string());
                assert_eq!(registered, original, "{name}={value}");
            }
        }
        assert_eq!(
            registry
                .find("datafusion.optimizer.max_passes")
                .unwrap()
                .category,
            HintCategory::Optimizer
        );
    }

    #[test]
    fn test_defaults_prefer_configured_values() {
        assert_eq!(
            default_query_option(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, None)
                .value
                .as_deref(),
            Some("true")
        );
        assert_eq!(
            default_query_option(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, Some("false"))
                .value
                .as_deref(),
            Some("false")
        );
        assert!(
            default_query_option(QUERY_PARALLELISM, None)
                .value
                .is_none()
        );
    }

    #[test]
    fn test_query_alias_batch_and_unknown_namespace() {
        let input = vec![
            ("query_fallback".to_string(), "TRUE".to_string()),
            ("custom".to_string(), "raw".to_string()),
        ];
        assert_eq!(
            normalize_query_option_batch(input, MAX_PUBLIC_PARALLELISM).unwrap(),
            vec![
                (QUERY_ALLOW_QUERY_FALLBACK.to_string(), "true".to_string()),
                ("custom".to_string(), "raw".to_string())
            ]
        );
        for name in [
            "query.unknown",
            "datafusion.execution.batch_size",
            "datafusion.optimizer.unknown",
        ] {
            assert!(
                normalize_query_option_batch(
                    [(name.to_string(), "true".to_string())],
                    MAX_PUBLIC_PARALLELISM
                )
                .is_err()
            );
        }
        assert!(
            normalize_query_option_batch(
                [
                    ("query_fallback".to_string(), "true".to_string()),
                    (QUERY_ALLOW_QUERY_FALLBACK.to_string(), "false".to_string())
                ],
                MAX_PUBLIC_PARALLELISM
            )
            .is_err()
        );
    }
}
