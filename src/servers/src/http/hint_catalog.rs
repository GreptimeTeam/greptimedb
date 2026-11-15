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

//! Public hint discovery derived from the executable protocol registrations.
//! Defaults describe omitted hints, never overrides on the discovery request.

use std::collections::HashMap;

use axum::Json;
use axum::extract::State;
use serde::Serialize;
use serde_json::Value;
use session::hint_registry::HintValueType;
use snafu::{ResultExt, ensure};

use crate::error::{InvalidParameterSnafu, ParseJsonSnafu, Result};
use crate::hints::protocol::Protocol;
use crate::hints::registry;
use crate::http::ApiState;

/// Public hints grouped by purpose, in registration order.
#[derive(Serialize)]
pub(crate) struct HintCatalogResponse {
    groups: Vec<HintGroup>,
}

#[derive(Serialize)]
struct HintGroup {
    name: &'static str,
    description: &'static str,
    hints: Vec<Hint>,
}

/// Lists public hints using the same authentication as other SQL routes.
pub(crate) async fn list_hints(State(state): State<ApiState>) -> Result<Json<HintCatalogResponse>> {
    Ok(Json(build_catalog(&state.sql_handler.hint_defaults()?)?))
}

#[derive(Serialize)]
struct Hint {
    name: String,
    name_is_pattern: bool,
    aliases: Vec<&'static str>,
    #[serde(rename = "type")]
    value_type: &'static str,
    values: Option<&'static [&'static str]>,
    description: String,
    applies_to: Vec<Protocol>,
    deprecated: Option<&'static str>,
    default: Option<Value>,
    default_description: Option<&'static str>,
}

fn build_catalog(defaults: &HashMap<String, String>) -> Result<HintCatalogResponse> {
    let mut groups: Vec<HintGroup> = Vec::new();
    for registration in registry::registrations() {
        let definition = &registration.definition;
        let metadata = &definition.metadata;
        if !metadata.discoverable {
            continue;
        }
        let applies_to = registration.protocols();
        if applies_to.is_empty() {
            continue;
        }
        let default = (definition.default)(defaults, &metadata.name);
        let hint = Hint {
            name: metadata.name.clone(),
            name_is_pattern: metadata.matches_pattern.is_some(),
            aliases: metadata.aliases.clone(),
            value_type: match metadata.value_type {
                HintValueType::Enum(_) => "enum",
                value_type => value_type.as_str(),
            },
            values: metadata.value_type.allowed_values(),
            description: metadata.description.clone(),
            applies_to,
            deprecated: metadata.deprecated,
            default: default
                .value
                .map(|value| default_value(metadata.value_type, value))
                .transpose()?,
            default_description: default.description,
        };
        let name = registration.definition.category.as_str();
        if let Some(group) = groups.iter_mut().find(|group| group.name == name) {
            group.hints.push(hint);
        } else {
            groups.push(HintGroup {
                name,
                description: registration.definition.category.description(),
                hints: vec![hint],
            });
        }
    }
    Ok(HintCatalogResponse { groups })
}

fn default_value(value_type: HintValueType, value: String) -> Result<Value> {
    match value_type {
        HintValueType::String | HintValueType::Enum(_) => Ok(Value::String(value)),
        HintValueType::Boolean => serde_json::from_str::<bool>(&value)
            .map(Value::Bool)
            .context(ParseJsonSnafu),
        HintValueType::Integer | HintValueType::Number => {
            let number =
                serde_json::from_str::<serde_json::Number>(&value).context(ParseJsonSnafu)?;
            ensure!(
                value_type != HintValueType::Integer || number.is_i64() || number.is_u64(),
                InvalidParameterSnafu {
                    reason: format!("Expected an integer hint default, got {value:?}")
                }
            );
            Ok(Value::Number(number))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use datafusion_common::config::ConfigOptions;
    use session::{hints, query_options};
    use store_api::mito_engine_options;
    use table::requests;

    use super::*;

    const MAX_PASSES: &str = "datafusion.optimizer.max_passes";

    fn configured_defaults() -> HashMap<String, String> {
        HashMap::from([
            (
                query_options::QUERY_PARALLELISM.to_string(),
                "7".to_string(),
            ),
            (
                query_options::QUERY_ALLOW_QUERY_FALLBACK.to_string(),
                "true".to_string(),
            ),
            (MAX_PASSES.to_string(), "9".to_string()),
            (
                requests::AUTO_CREATE_TABLE_KEY.to_string(),
                "false".to_string(),
            ),
        ])
    }

    fn find_hint<'a>(catalog: &'a HintCatalogResponse, name: &str) -> &'a Hint {
        catalog
            .groups
            .iter()
            .flat_map(|group| &group.hints)
            .find(|hint| hint.name == name)
            .unwrap()
    }

    #[test]
    fn test_hint_catalog_configured_defaults() {
        let defaults = configured_defaults();
        let catalog = build_catalog(&defaults).unwrap();
        for (name, default) in &defaults {
            assert_eq!(
                find_hint(&catalog, name).default,
                Some(serde_json::from_str(default).unwrap())
            );
        }
        let comment = find_hint(&catalog, requests::COMMENT_KEY);
        let ttl = find_hint(&catalog, requests::TTL_KEY);
        assert_eq!(comment.default, None);
        assert_eq!(ttl.default, None);
        assert_ne!(comment.default_description, ttl.default_description);
    }

    #[test]
    fn test_hint_catalog_scalar_defaults_preserve_types() {
        for (value_type, input, expected) in [
            (HintValueType::Boolean, "false", serde_json::json!(false)),
            (HintValueType::Integer, "1024", serde_json::json!(1024)),
            (HintValueType::Number, "0.25", serde_json::json!(0.25)),
            (HintValueType::String, "false", serde_json::json!("false")),
            (HintValueType::String, "001", serde_json::json!("001")),
            (
                HintValueType::Enum(&["leader"]),
                "leader",
                serde_json::json!("leader"),
            ),
        ] {
            assert_eq!(
                default_value(value_type, input.to_string()).unwrap(),
                expected
            );
        }
        for (value_type, input) in [
            (HintValueType::Boolean, "invalid"),
            (HintValueType::Boolean, "1"),
            (HintValueType::Integer, "1.5"),
            (HintValueType::Integer, "true"),
            (HintValueType::Number, "false"),
            (HintValueType::Number, "[]"),
            (HintValueType::Number, "null"),
        ] {
            assert!(
                default_value(value_type, input.to_string()).is_err(),
                "{value_type:?}: {input}"
            );
        }
    }

    #[test]
    fn test_hint_catalog_defaults_and_parser_agree() {
        let mut defaults: HashMap<_, _> = ConfigOptions::default()
            .entries()
            .into_iter()
            .filter(|entry| {
                entry
                    .key
                    .starts_with(query_options::DATAFUSION_OPTIMIZER_PREFIX)
            })
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .collect();
        defaults.extend(configured_defaults());
        let catalog = build_catalog(&defaults).unwrap();
        for hint in catalog.groups.iter().flat_map(|group| &group.hints) {
            if hint.name.starts_with("query.")
                || hint
                    .name
                    .starts_with(query_options::DATAFUSION_OPTIMIZER_PREFIX)
            {
                let value = hint.default.as_ref().unwrap().to_string();
                for key in std::iter::once(hint.name.as_str()).chain(hint.aliases.iter().copied()) {
                    assert_eq!(
                        query_options::parse_query_option(
                            key,
                            &value,
                            query_options::MAX_PUBLIC_PARALLELISM
                        )
                        .unwrap(),
                        Some((hint.name.clone(), value.clone()))
                    );
                }
                assert!(matches!(hint.value_type, "boolean" | "integer"));
            }
        }
    }

    #[test]
    fn test_hint_catalog_definition_coverage() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        let names: BTreeSet<_> = catalog
            .groups
            .iter()
            .flat_map(|group| &group.hints)
            .map(|hint| hint.name.as_str())
            .collect();
        assert_eq!(
            names.len(),
            catalog
                .groups
                .iter()
                .map(|group| group.hints.len())
                .sum::<usize>()
        );
        assert_eq!(
            catalog
                .groups
                .iter()
                .map(|group| group.name)
                .collect::<Vec<_>>(),
            ["query", "optimizer", "ingestion"]
        );
        assert!(
            names
                .iter()
                .all(|name| !hints::is_reserved_extension_key(name))
        );
        let optimizer: BTreeSet<_> = ConfigOptions::default()
            .entries()
            .into_iter()
            .filter(|entry| {
                entry
                    .key
                    .starts_with(query_options::DATAFUSION_OPTIMIZER_PREFIX)
            })
            .map(|entry| entry.key)
            .collect();
        assert_eq!(
            optimizer,
            names
                .iter()
                .filter(|name| name.starts_with(query_options::DATAFUSION_OPTIMIZER_PREFIX))
                .map(|name| name.to_string())
                .collect()
        );
        for name in requests::VALID_TABLE_OPTION_KEYS
            .iter()
            .chain(requests::SEMANTIC_OPTION_KEYS.iter())
        {
            if [
                requests::FILE_TABLE_LOCATION_KEY,
                requests::FILE_TABLE_FORMAT_KEY,
                requests::FILE_TABLE_PATTERN_KEY,
            ]
            .contains(name)
            {
                assert!(!names.contains(name));
            } else {
                assert!(names.contains(name), "missing {name}");
            }
        }
        for name in mito_engine_options::COMPACTION_OPTION_KEYS {
            assert!(
                catalog
                    .groups
                    .iter()
                    .flat_map(|group| &group.hints)
                    .any(|hint| hint.name == name || hint.aliases.contains(&name)),
                "missing {name}"
            );
        }
        // Internal context values are not public hints.
        for name in [
            common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
            "flow.scheduled_time_millis",
        ] {
            assert!(!names.contains(name));
        }
    }

    #[test]
    fn test_hint_categories_follow_owners() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        for (name, expected) in [
            (crate::http::jaeger::JAEGER_QUERY_TABLE_NAME_KEY, "query"),
            (requests::TTL_KEY, "ingestion"),
            (mito_engine_options::MERGE_MODE_KEY, "ingestion"),
            (MAX_PASSES, "optimizer"),
        ] {
            let group = catalog
                .groups
                .iter()
                .find(|group| group.hints.iter().any(|hint| hint.name == name))
                .unwrap();
            assert_eq!(group.name, expected, "{name}");
        }
        #[cfg(feature = "enterprise")]
        assert!(
            catalog
                .groups
                .iter()
                .find(|group| group.name == "query")
                .unwrap()
                .hints
                .iter()
                .any(|hint| hint.name == hints::READ_PREFERENCE_HINT)
        );
    }

    #[test]
    fn test_hint_catalog_enterprise_boundary() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        let entry = catalog
            .groups
            .iter()
            .flat_map(|group| &group.hints)
            .find(|hint| hint.name == session::hints::READ_PREFERENCE_HINT);
        #[cfg(feature = "enterprise")]
        {
            let entry = entry.unwrap();
            assert_eq!(entry.default, Some(serde_json::json!("leader")));
        }
        #[cfg(not(feature = "enterprise"))]
        {
            assert!(entry.is_none());
            let json = serde_json::to_string(&catalog).unwrap();
            assert!(!json.contains("read_preference"));
            assert!(!json.contains("read replicas"));
            assert!(!json.contains("x-greptime-read-preference"));
        }
    }

    #[test]
    fn test_hint_catalog_enum_values() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        for hint in catalog.groups.iter().flat_map(|group| &group.hints) {
            if requests::is_semantic_option_key(&hint.name) {
                assert_eq!(hint.values, requests::semantic_option_values(&hint.name));
                if let Some(values) = hint.values {
                    for value in values {
                        assert!(requests::validate_semantic_option(&hint.name, value));
                    }
                }
            }
        }
        for (name, values) in [
            (
                mito_engine_options::MERGE_MODE_KEY,
                &["last_row", "last_non_null"][..],
            ),
            (
                mito_engine_options::SST_FORMAT_KEY,
                &["primary_key", "flat"][..],
            ),
            (
                mito_engine_options::COMPACTION_TYPE,
                &[mito_engine_options::COMPACTION_TYPE_TWCS][..],
            ),
        ] {
            let hint = catalog
                .groups
                .iter()
                .flat_map(|group| &group.hints)
                .find(|hint| hint.name == name)
                .unwrap();
            assert_eq!(hint.value_type, "enum");
            assert_eq!(hint.values, Some(values));
        }
        #[cfg(feature = "enterprise")]
        {
            let hint = catalog
                .groups
                .iter()
                .flat_map(|group| &group.hints)
                .find(|hint| hint.name == hints::READ_PREFERENCE_HINT)
                .unwrap();
            assert_eq!(hint.values, Some(session::READ_PREFERENCE_VALUES));
            for value in hint.values.unwrap() {
                assert!(value.parse::<session::ReadPreference>().is_ok());
            }
        }
    }

    #[test]
    fn test_hint_catalog_unknown_defaults_and_alias_metadata() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        let json = serde_json::to_value(&catalog).unwrap();
        let parallelism = find_hint(&catalog, query_options::QUERY_PARALLELISM);
        assert_eq!(parallelism.aliases, ["query_parallelism"]);
        assert_eq!(parallelism.default, None);
        assert!(!parallelism.applies_to.is_empty());
        assert!(json.get("legacy_headers_deprecation").is_none());
        assert!(!json.to_string().contains("legacy_headers"));
        assert!(
            catalog
                .groups
                .iter()
                .flat_map(|group| &group.hints)
                .filter(|hint| hint.default.is_none())
                .all(|hint| hint.default_description.is_some())
        );
    }

    #[test]
    fn test_hint_catalog_protocols_and_order_follow_bindings() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        for group in &catalog.groups {
            let registrations: Vec<_> = registry::registrations()
                .iter()
                .filter(|registration| {
                    registration.definition.category.as_str() == group.name
                        && registration.definition.metadata.discoverable
                        && !registration.protocols().is_empty()
                })
                .collect();
            assert_eq!(
                group
                    .hints
                    .iter()
                    .map(|hint| hint.name.as_str())
                    .collect::<Vec<_>>(),
                registrations
                    .iter()
                    .map(|registration| registration.definition.metadata.name.as_str())
                    .collect::<Vec<_>>()
            );
            for (hint, registration) in group.hints.iter().zip(registrations) {
                let protocols = registration.protocols();
                assert!(!protocols.is_empty());
                assert_eq!(hint.applies_to, protocols);
            }
        }
    }

    #[test]
    fn test_hint_catalog_open_entity_namespace() {
        let catalog = build_catalog(&HashMap::new()).unwrap();
        let patterns: Vec<_> = catalog
            .groups
            .iter()
            .flat_map(|group| &group.hints)
            .filter(|hint| hint.name_is_pattern)
            .collect();
        assert_eq!(
            patterns
                .iter()
                .map(|hint| hint.name.as_str())
                .collect::<Vec<_>>(),
            [
                registry::SEMANTIC_ENTITY_ID_PATTERN,
                registry::SEMANTIC_ENTITY_DESCRIPTIVE_PATTERN,
                registry::SEMANTIC_ENTITY_SCOPE_PATTERN,
            ]
        );
        for hint in patterns {
            assert_eq!(hint.value_type, "string");
            assert!(hint.values.is_none());
            assert!(hint.applies_to.contains(&Protocol::Influxdb));
        }
        assert!(!find_hint(&catalog, requests::TTL_KEY).name_is_pattern);
        assert!(
            catalog
                .groups
                .iter()
                .flat_map(|group| &group.hints)
                .all(|hint| hint.name != requests::SEMANTIC_ENTITY_SERVICE_ID)
        );
        let json = serde_json::to_value(catalog).unwrap();
        let pattern_names: Vec<_> = json["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["hints"].as_array().unwrap())
            .filter(|hint| hint["name_is_pattern"] == true)
            .map(|hint| hint["name"].as_str().unwrap())
            .collect();
        assert_eq!(pattern_names.len(), 3);
    }
}
