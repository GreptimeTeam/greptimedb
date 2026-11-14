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

//! Protocol hint registrations shared by application and discovery.

use std::collections::HashMap;
use std::sync::OnceLock;

use session::context::QueryContext;
use session::hint_registry::{
    HintCategory, HintDefault, HintDefinition, HintMetadata, HintRegistry, HintValueType,
};
use session::query_options::{self, QueryParseOptions};
use session::session_config::{Error, InvalidConfigValueSnafu};
use store_api::mito_engine_options;
use table::requests;

use crate::hints::compatibility::{self, HeaderCompatibility, InvalidHeaderPolicy};
use crate::hints::protocol::{self, Endpoint, Protocol, Transport};

pub(crate) type Definition =
    HintDefinition<QueryContext, HashMap<String, String>, QueryParseOptions>;
pub(crate) type Registry = HintRegistry<QueryContext, HashMap<String, String>, QueryParseOptions>;

/// One executable registration; discovery derives applicability from its bindings.
pub(crate) struct ProtocolHint {
    pub(crate) definition: Definition,
    pub(crate) endpoints: &'static [Endpoint],
    /// Historical transport-wide parsing/application, distinct from effective protocol use.
    compatibility_transports: &'static [Transport],
    pub(crate) legacy_header: Option<compatibility::LegacyHeader>,
    pub(crate) dedicated_http_header: Option<HeaderCompatibility>,
}

impl ProtocolHint {
    fn with_legacy_header(mut self, header: &'static str) -> Self {
        self.legacy_header = Some(compatibility::LegacyHeader {
            name: header,
            http_only: false,
        });
        self
    }

    fn with_dedicated_header(mut self, header: HeaderCompatibility) -> Self {
        self.dedicated_http_header = Some(header);
        self
    }

    /// Returns distinct protocols in binding order, without a second metadata list.
    pub(crate) fn protocols(&self) -> Vec<Protocol> {
        let mut protocols = Vec::new();
        for endpoint in self.endpoints {
            let protocol = endpoint.protocol();
            if !protocols.contains(&protocol) {
                protocols.push(protocol);
            }
        }
        protocols
    }
}

/// All definitions and executable protocol bindings in registration order.
pub(crate) fn registrations() -> &'static [ProtocolHint] {
    static REGISTRATIONS: OnceLock<Vec<ProtocolHint>> = OnceLock::new();
    REGISTRATIONS.get_or_init(|| {
        let mut definitions: Vec<_> = query_options::definitions(apply_extension, query_default)
            .into_iter()
            .map(|definition| ProtocolHint {
                definition,
                endpoints: protocol::QUERY_ENDPOINTS,
                compatibility_transports: &[],
                legacy_header: None,
                dedicated_http_header: None,
            })
            .collect();
        register_ingestion_hints(&mut definitions);
        register_table_hints(&mut definitions);
        register_semantic_hints(&mut definitions);
        register_read_preference(&mut definitions);
        definitions
    })
}

/// Returns a prebuilt registry containing the hooks bound to this entry point.
pub(crate) fn for_endpoint(endpoint: Endpoint) -> &'static Registry {
    static REGISTRIES: OnceLock<HashMap<Endpoint, Registry>> = OnceLock::new();
    let registries = REGISTRIES.get_or_init(|| {
        let endpoints = registrations()
            .iter()
            .flat_map(|hint| hint.endpoints.iter().copied())
            .chain([
                Endpoint::Http(Protocol::Other),
                Endpoint::Grpc(Protocol::Other),
            ]);
        let mut registries = HashMap::new();
        for endpoint in endpoints {
            registries
                .entry(endpoint)
                .or_insert_with(|| build_registry(Some(endpoint)));
        }
        registries
    });
    registries.get(&endpoint).unwrap_or_else(|| {
        let fallback = match endpoint {
            Endpoint::Http(_) => Endpoint::Http(Protocol::Other),
            Endpoint::Grpc(_) => Endpoint::Grpc(Protocol::Other),
        };
        &registries[&fallback]
    })
}

/// Dedicated HTTP sources retain their original context hooks on every route.
pub(crate) fn dedicated_http_registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        HintRegistry::new(
            registrations()
                .iter()
                .filter(|hint| hint.dedicated_http_header.is_some())
                .map(|hint| copy_definition(&hint.definition))
                .collect(),
        )
    })
}

/// Selects only hints supported by a specialized metadata endpoint. Every
/// occurrence is retained for the common validation and last-value-wins policy.
pub(crate) fn select_metadata_hints(
    endpoint: Endpoint,
    hints: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let registry = for_endpoint(endpoint);
    hints
        .into_iter()
        .filter(|(name, _)| registry.find(name).is_some())
        .collect()
}

/// Unscoped definitions used by transport compatibility tests.
#[cfg(test)]
pub(crate) fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| build_registry(None))
}

fn build_registry(endpoint: Option<Endpoint>) -> Registry {
    HintRegistry::new(
        registrations()
            .iter()
            .filter(|hint| {
                endpoint.is_none_or(|endpoint| {
                    hint.endpoints.contains(&endpoint)
                        || (!endpoint.is_metadata_only()
                            && hint
                                .compatibility_transports
                                .contains(&endpoint.transport()))
                })
            })
            .map(|hint| copy_definition(&hint.definition))
            .collect(),
    )
}

fn copy_definition(definition: &Definition) -> Definition {
    Definition {
        metadata: definition.metadata.clone(),
        category: definition.category,
        parse: definition.parse,
        apply: definition.apply,
        default: definition.default,
        reject_conflicts: definition.reject_conflicts,
    }
}

fn parse_skip_wal(name: &str, value: &str, _: QueryParseOptions) -> Result<String, Error> {
    value
        .parse::<bool>()
        .map(|_| value.to_string())
        .map_err(|_| {
            InvalidConfigValueSnafu {
                name,
                value,
                hint: "Expected true or false",
            }
            .build()
        })
}

fn hint(
    name: &str,
    value_type: HintValueType,
    description: impl Into<String>,
    endpoints: &'static [Endpoint],
    default: fn(&HashMap<String, String>, &str) -> HintDefault,
) -> ProtocolHint {
    let metadata = HintMetadata::new(name, value_type, description);
    ProtocolHint {
        endpoints,
        compatibility_transports: &[],
        legacy_header: None,
        dedicated_http_header: None,
        definition: Definition {
            metadata,
            category: HintCategory::Ingestion,
            parse: retain_table_option,
            apply: apply_extension,
            default,
            reject_conflicts: false,
        },
    }
}

fn register_ingestion_hints(definitions: &mut Vec<ProtocolHint>) {
    definitions.push(hint(
        requests::AUTO_CREATE_TABLE_KEY,
        HintValueType::Boolean,
        "Allow ingestion to create missing tables; cannot override a disabled frontend setting.",
        protocol::auto_create_endpoints(),
        auto_create_default,
    ).with_legacy_header(compatibility::AUTO_CREATE_TABLE_HEADER));
    definitions.push(
        hint(
            crate::http::prom_store::PHYSICAL_TABLE_PARAM,
            HintValueType::String,
            "Target physical metric table for OTLP metrics or native RowInserts; native column inserts do not use this hint.",
            &[
                Endpoint::Http(Protocol::OtlpMetrics),
                Endpoint::Grpc(Protocol::NativeInsert),
            ],
            physical_table_default,
        )
        .with_legacy_header(compatibility::PHYSICAL_TABLE_HEADER),
    );
    definitions.push(
        hint(
            mito_engine_options::APPEND_MODE_KEY,
            HintValueType::Boolean,
            "Set append mode during ordinary table creation; logical tables do not use this override, and log/trace tables force append mode.",
            protocol::PHYSICAL_TABLE_ENDPOINTS,
            append_mode_default,
        )
        .with_legacy_header(compatibility::APPEND_MODE_HEADER),
    );
    definitions.push(
        hint(
            mito_engine_options::MERGE_MODE_KEY,
            HintValueType::Enum(MERGE_MODE_VALUES),
            "Set merge mode during ordinary table creation; append mode takes precedence on InfluxDB tables, and logical/log/trace tables do not use this override.",
            protocol::PHYSICAL_TABLE_ENDPOINTS,
            merge_mode_default,
        )
        .with_legacy_header(compatibility::MERGE_MODE_HEADER),
    );
    definitions.push(hint(
        requests::TRACE_TABLE_PARTITIONS_HINT_KEY,
        HintValueType::Integer,
        "Partition count for automatically created trace tables.",
        &[Endpoint::Http(Protocol::OtlpTraces)],
        trace_partitions_default,
    ));
    let mut jaeger = hint(
        crate::http::jaeger::JAEGER_QUERY_TABLE_NAME_KEY,
        HintValueType::String,
        "Trace table to query; an explicit table request parameter takes precedence.",
        &[Endpoint::Http(Protocol::Jaeger)],
        jaeger_table_default,
    );
    jaeger.definition.category = HintCategory::Query;
    definitions.push(jaeger);
    let skip_wal = hint(
        session::hints::INSERT_SKIP_WAL_HINT,
        HintValueType::Boolean,
        "Skip WAL for this write request only; independent of table-level skip_wal.",
        protocol::WRITE_ENDPOINTS,
        |_, _| HintDefault {
            value: Some(false.to_string()),
            description: None,
        },
    );
    let mut skip_wal = skip_wal.with_dedicated_header(HeaderCompatibility {
        name: crate::http::header::constants::GREPTIME_INSERT_SKIP_WAL_HEADER_NAME,
        invalid: InvalidHeaderPolicy::Reject("must be true or false"),
    });
    skip_wal.compatibility_transports = &[Transport::Grpc];
    skip_wal.legacy_header = Some(compatibility::LegacyHeader {
        name: compatibility::INSERT_SKIP_WAL_HEADER,
        http_only: true,
    });
    skip_wal.definition.parse = parse_skip_wal;
    skip_wal.definition.apply = |context, _, value| {
        // The registered parser has already validated the strict boolean domain.
        context.set_skip_wal(value == "true");
    };
    definitions.push(skip_wal);
}

fn register_table_hints(definitions: &mut Vec<ProtocolHint>) {
    for name in requests::VALID_TABLE_OPTION_KEYS {
        let default = match name {
            requests::COMMENT_KEY
            | requests::FILE_TABLE_LOCATION_KEY
            | requests::FILE_TABLE_FORMAT_KEY
            | requests::FILE_TABLE_PATTERN_KEY => table_metadata_default,
            requests::OTLP_METRIC_COMPAT_KEY => otlp_compat_default,
            _ => table_option_default,
        };
        let mut definition = hint(
            name,
            table_option_type(name),
            format!("Set the {name} table option during automatic creation."),
            protocol::INGESTION_ENDPOINTS,
            default,
        );
        definition.legacy_header = match name {
            requests::TTL_KEY => Some(compatibility::LegacyHeader {
                name: compatibility::TTL_HEADER,
                http_only: false,
            }),
            requests::SKIP_WAL_KEY => Some(compatibility::LegacyHeader {
                name: compatibility::SKIP_WAL_HEADER,
                http_only: false,
            }),
            _ => None,
        };
        if matches!(
            name,
            requests::FILE_TABLE_LOCATION_KEY
                | requests::FILE_TABLE_FORMAT_KEY
                | requests::FILE_TABLE_PATTERN_KEY
        ) {
            definition.definition.metadata.discoverable = false;
        }
        if name == requests::OTLP_METRIC_COMPAT_KEY {
            definition.definition.metadata.deprecated = Some(
                "Retained as metadata for compatibility; OTLP metrics always use Prometheus-compatible conversion.",
            );
        }
        definitions.push(definition);
    }
    for name in mito_engine_options::COMPACTION_OPTION_KEYS {
        if name == mito_engine_options::TWCS_TRIGGER_FILE_NUM {
            continue;
        }
        let mut definition = hint(
            name,
            table_option_type(name),
            format!(
                "Set the {name} compaction option when automatically creating a non-logical table."
            ),
            protocol::INGESTION_ENDPOINTS,
            compaction_default,
        );
        if name == mito_engine_options::TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM {
            // TableOptions rejects unequal wire values for these aliases before
            // numeric parsing. Preserve that rule when canonicalizing names.
            definition.definition.reject_conflicts = true;
            definition
                .definition
                .metadata
                .aliases
                .push(mito_engine_options::TWCS_TRIGGER_FILE_NUM);
        }
        definitions.push(definition);
    }
}

fn register_semantic_hints(definitions: &mut Vec<ProtocolHint>) {
    for name in requests::SEMANTIC_OPTION_KEYS.iter().copied() {
        let value_type = requests::semantic_option_values(name)
            .map(HintValueType::Enum)
            .unwrap_or(HintValueType::String);
        definitions.push(hint(
            name,
            value_type,
            semantic_description(name),
            protocol::INGESTION_ENDPOINTS,
            semantic_default,
        ));
    }
    for (name, matches) in [
        (
            SEMANTIC_ENTITY_ID_PATTERN,
            matches_entity_id as fn(&str) -> bool,
        ),
        (
            SEMANTIC_ENTITY_DESCRIPTIVE_PATTERN,
            matches_entity_descriptive,
        ),
        (SEMANTIC_ENTITY_SCOPE_PATTERN, matches_entity_scope),
    ] {
        let mut definition = hint(
            name,
            HintValueType::String,
            "Entity column metadata during automatic table creation; <type> is a lowercase, dot-separated entity type.",
            protocol::INGESTION_ENDPOINTS,
            semantic_default,
        );
        definition.definition.metadata.matches_pattern = Some(matches);
        definitions.push(definition);
    }
}

fn semantic_description(name: &str) -> &'static str {
    match name {
        requests::SEMANTIC_SIGNAL_TYPE | requests::SEMANTIC_SOURCE => {
            "Semantic identity during automatic table creation; protocol-stamped identity overrides request values."
        }
        requests::SEMANTIC_SOURCE_VERSION => {
            "Semantic source version during automatic table creation; Prometheus remote write stamps its wire version."
        }
        requests::SEMANTIC_METRIC_METADATA_QUALITY => {
            "Metric metadata quality during automatic table creation; Prometheus remote write and protocol-derived per-table metadata take precedence."
        }
        requests::SEMANTIC_PIPELINE => {
            "Semantic pipeline metadata, not pipeline selection; built-in OTLP data models may overwrite it during automatic table creation."
        }
        _ => {
            "Semantic table metadata during automatic creation; protocol-derived per-table metadata takes precedence."
        }
    }
}

fn matches_entity_id(name: &str) -> bool {
    matches!(
        requests::parse_entity_option_key(name),
        Some((_, requests::EntityRole::Id))
    )
}

fn matches_entity_descriptive(name: &str) -> bool {
    matches!(
        requests::parse_entity_option_key(name),
        Some((_, requests::EntityRole::Descriptive))
    )
}

fn matches_entity_scope(name: &str) -> bool {
    matches!(
        requests::parse_entity_option_key(name),
        Some((_, requests::EntityRole::Scope))
    )
}

fn register_read_preference(definitions: &mut Vec<ProtocolHint>) {
    // OSS retains its existing parser but must not advertise enterprise routing.
    let mut definition = hint(
        session::hints::READ_PREFERENCE_HINT,
        HintValueType::String,
        "",
        protocol::QUERY_ENDPOINTS,
        read_preference_default,
    );
    definition.compatibility_transports = &[Transport::Http, Transport::Grpc];
    definition.definition.metadata.discoverable = false;
    #[cfg(feature = "enterprise")]
    {
        definition.definition.metadata.discoverable = true;
        definition.definition.metadata.value_type =
            HintValueType::Enum(session::READ_PREFERENCE_VALUES);
        definition.definition.metadata.description = "Read routing preference for enterprise read replicas; x-greptime-read-preference takes precedence.".to_string();
    }
    let mut definition = definition
        .with_legacy_header(compatibility::READ_PREFERENCE_HEADER)
        .with_dedicated_header(HeaderCompatibility {
            name: crate::http::header::constants::GREPTIME_DB_HEADER_READ_PREFERENCE,
            invalid: InvalidHeaderPolicy::Ignore,
        });
    definition.definition.parse = parse_read_preference;
    definition.definition.apply = apply_read_preference;
    definition.definition.category = HintCategory::Query;
    definitions.push(definition);
}

fn table_option_type(name: &str) -> HintValueType {
    match name {
        requests::SKIP_WAL_KEY
        | mito_engine_options::COMPACTION_OVERRIDE
        | mito_engine_options::TWCS_REMOTE_COMPACTION
        | mito_engine_options::TWCS_FALLBACK_TO_LOCAL => HintValueType::Boolean,
        requests::REPARTITION_PARTITION_NUM_HINT_KEY
        | mito_engine_options::TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM
        | mito_engine_options::TWCS_ACTIVE_WINDOW_L1_MERGE_TRIGGER
        | mito_engine_options::TWCS_INACTIVE_WINDOW_TRIGGER_FILE_NUM
        | mito_engine_options::TWCS_INACTIVE_WINDOW_L1_MERGE_TRIGGER => HintValueType::Integer,
        mito_engine_options::SST_FORMAT_KEY => HintValueType::Enum(SST_FORMAT_VALUES),
        requests::FILE_TABLE_FORMAT_KEY => HintValueType::Enum(FILE_FORMAT_VALUES),
        mito_engine_options::COMPACTION_TYPE => HintValueType::Enum(COMPACTION_TYPE_VALUES),
        _ => HintValueType::String,
    }
}

/// Ingestion owns validation at consumption time, including ignored invalid metadata.
fn retain_table_option(_: &str, value: &str, _: QueryParseOptions) -> Result<String, Error> {
    Ok(value.to_string())
}

fn parse_read_preference(name: &str, value: &str, _: QueryParseOptions) -> Result<String, Error> {
    value
        .parse::<session::ReadPreference>()
        .map(|_| value.to_string())
        .map_err(|_| {
            InvalidConfigValueSnafu {
                name,
                value,
                hint: "Invalid read_preference hint",
            }
            .build()
        })
}

fn apply_extension(context: &mut QueryContext, name: &str, value: String) {
    context.set_extension(name, value);
}

fn apply_read_preference(context: &mut QueryContext, _: &str, value: String) {
    // ParsedHint can only be constructed after the registered parser succeeds.
    if let Ok(preference) = value.parse::<session::ReadPreference>() {
        context.set_read_preference(preference);
    }
}

fn contextual(description: &'static str) -> HintDefault {
    HintDefault {
        value: None,
        description: Some(description),
    }
}

fn configured(
    defaults: &HashMap<String, String>,
    name: &str,
    description: &'static str,
) -> HintDefault {
    match defaults.get(name) {
        Some(value) => HintDefault {
            value: Some(value.clone()),
            description: None,
        },
        None => contextual(description),
    }
}

fn query_default(defaults: &HashMap<String, String>, name: &str) -> HintDefault {
    query_options::default_query_option(name, defaults.get(name).map(String::as_str))
}

fn auto_create_default(defaults: &HashMap<String, String>, name: &str) -> HintDefault {
    configured(
        defaults,
        name,
        "Controlled by the frontend auto_create_table setting.",
    )
}

fn physical_table_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("Determined by the protocol's physical-table configuration or request parameter.")
}

fn append_mode_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("Depends on the table model; log and trace ingestion force append mode.")
}

fn merge_mode_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("Depends on the table model and append_mode; commonly last_non_null or last_row.")
}

fn trace_partitions_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("Uses the trace table creation policy when omitted.")
}

fn jaeger_table_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    HintDefault {
        value: Some(common_catalog::consts::TRACE_TABLE_NAME.to_string()),
        description: None,
    }
}

fn table_metadata_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("No table metadata value is supplied.")
}

fn otlp_compat_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("OTLP metrics stamp prom; other ingestion paths may leave this metadata unset.")
}

fn table_option_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual(
        "No table-option override; the table model, database options and storage configuration determine behavior.",
    )
}

fn compaction_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("No override; uses the datanode compaction configuration and table creation policy.")
}

fn semantic_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    contextual("Derived from the ingestion protocol and payload, or left unset.")
}

fn read_preference_default(_: &HashMap<String, String>, _: &str) -> HintDefault {
    HintDefault {
        value: Some(
            session::ReadPreference::default()
                .to_string()
                .to_ascii_lowercase(),
        ),
        description: None,
    }
}

pub(crate) const SEMANTIC_ENTITY_ID_PATTERN: &str = "greptime.semantic.entity.<type>.id";
pub(crate) const SEMANTIC_ENTITY_DESCRIPTIVE_PATTERN: &str =
    "greptime.semantic.entity.<type>.descriptive";
pub(crate) const SEMANTIC_ENTITY_SCOPE_PATTERN: &str = "greptime.semantic.entity.<type>.scope";
const MERGE_MODE_VALUES: &[&str] = &["last_row", "last_non_null"];
const SST_FORMAT_VALUES: &[&str] = &["primary_key", "flat"];
const COMPACTION_TYPE_VALUES: &[&str] = &[mito_engine_options::COMPACTION_TYPE_TWCS];
const FILE_FORMAT_VALUES: &[&str] = &["csv", "json", "parquet", "orc"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registration_names_and_headers_are_unique() {
        registry().validate_definitions().unwrap();
        let mut headers = std::collections::HashSet::new();
        for registration in registrations() {
            if let Some(header) = &registration.legacy_header {
                assert!(headers.insert(header.name), "{}", header.name);
            }
            if let Some(header) = &registration.dedicated_http_header {
                assert!(headers.insert(header.name), "{}", header.name);
            }
        }
    }

    #[test]
    fn test_transports_share_parsing_and_duplicate_rules() {
        let options = QueryParseOptions {
            max_parallelism: query_options::MAX_PUBLIC_PARALLELISM,
        };
        for input in [
            vec![("INSERT_SKIP_WAL", "false"), ("insert_skip_wal", "true")],
            vec![("insert_skip_wal", "invalid"), ("insert_skip_wal", "true")],
            vec![("QUERY_PARALLELISM", "2"), ("query.parallelism", "2")],
            vec![("query_parallelism", "2"), ("query.parallelism", "3")],
            vec![("READ_PREFERENCE", "leader")],
        ] {
            let parse = |endpoint| {
                for_endpoint(endpoint)
                    .parse_batch(
                        input
                            .iter()
                            .map(|(key, value)| (key.to_string(), value.to_string())),
                        options,
                    )
                    .map(|hints| {
                        hints
                            .into_iter()
                            .map(|hint| (hint.name().to_string(), hint.value().to_string()))
                            .collect::<Vec<_>>()
                    })
                    .map_err(|error| error.to_string())
            };
            assert_eq!(
                parse(Endpoint::Http(Protocol::Sql)),
                parse(Endpoint::Grpc(Protocol::Sql)),
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_twcs_alias_conflicts_match_table_options() {
        let legacy = mito_engine_options::TWCS_TRIGGER_FILE_NUM;
        let canonical = mito_engine_options::TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM;
        for endpoint in [
            Endpoint::Http(Protocol::Influxdb),
            Endpoint::Grpc(Protocol::NativeInsert),
        ] {
            for (first, second) in [(legacy, canonical), (canonical, legacy)] {
                for (first_value, second_value) in [("4", "4"), ("4", "8"), ("01", "1")] {
                    let input = vec![
                        (first.to_string(), first_value.to_string()),
                        (second.to_string(), second_value.to_string()),
                    ];
                    let mut table_options = input.iter().cloned().collect();
                    let expected =
                        mito_engine_options::normalize_twcs_trigger_options(&mut table_options);
                    let parsed = crate::hints::parse_endpoint(endpoint, input);
                    assert_eq!(
                        parsed.is_ok(),
                        expected.is_ok(),
                        "{endpoint:?}: {first}={first_value}, {second}={second_value}"
                    );
                    if let Ok(parsed) = parsed {
                        let mut context = QueryContext::with("greptime", "public");
                        parsed.apply(&mut context);
                        assert_eq!(context.extension(canonical), Some(first_value));
                        assert_eq!(context.extension(legacy), None);
                    }
                }
            }
        }
    }

    #[test]
    fn test_endpoint_binding_controls_application() {
        let input = [(
            session::hints::INSERT_SKIP_WAL_HINT.to_string(),
            "true".to_string(),
        )];
        for (endpoint, expected) in [
            (Endpoint::Http(Protocol::Influxdb), Some("true")),
            (Endpoint::Grpc(Protocol::NativeInsert), Some("true")),
            (Endpoint::Grpc(Protocol::OtelArrow), Some("true")),
            (Endpoint::Http(Protocol::Jaeger), None),
        ] {
            let registry = for_endpoint(endpoint);
            let batch = registry
                .parse_grouped(
                    input.clone(),
                    QueryParseOptions {
                        max_parallelism: query_options::MAX_PUBLIC_PARALLELISM,
                    },
                )
                .unwrap();
            let (parsed, extensions) = batch.into_parts();
            let mut context = QueryContext::with("greptime", "public");
            for (name, value) in extensions {
                context.set_extension(name, value);
            }
            registry.apply(&mut context, parsed);
            assert_eq!(context.skip_wal(), expected.is_some(), "{endpoint:?}");
            // Unsupported keys retain the legacy passthrough behavior.
            assert_eq!(
                context.extension(session::hints::INSERT_SKIP_WAL_HINT),
                expected.is_none().then_some("true")
            );
        }
    }

    #[test]
    fn test_discovery_is_derived_from_executable_bindings() {
        for hint in registrations() {
            assert!(
                !hint.endpoints.is_empty(),
                "{}",
                hint.definition.metadata.name
            );
            for endpoint in hint.endpoints {
                let registry = for_endpoint(*endpoint);
                registry.validate_definitions().unwrap();
                if hint.definition.metadata.matches_pattern.is_none() {
                    assert!(registry.find(&hint.definition.metadata.name).is_some());
                } else {
                    assert!(registry.definitions().iter().any(|definition| {
                        definition.metadata.name == hint.definition.metadata.name
                    }));
                }
                assert!(hint.protocols().contains(&endpoint.protocol()));
            }
        }
        let jaeger = registrations()
            .iter()
            .find(|hint| {
                hint.definition.metadata.name == crate::http::jaeger::JAEGER_QUERY_TABLE_NAME_KEY
            })
            .unwrap();
        assert_eq!(jaeger.protocols(), [Protocol::Jaeger]);
        assert!(
            for_endpoint(Endpoint::Http(Protocol::Sql))
                .find(&jaeger.definition.metadata.name)
                .is_none()
        );
    }

    #[test]
    fn test_specialized_metadata_preserves_every_registered_occurrence() {
        let input = [
            (session::hints::INSERT_SKIP_WAL_HINT, "false"),
            (requests::AUTO_CREATE_TABLE_KEY, "false"),
            (session::hints::INSERT_SKIP_WAL_HINT, "invalid"),
            (requests::AUTO_CREATE_TABLE_KEY, "true"),
            (requests::TTL_KEY, "1d"),
            ("INSERT_SKIP_WAL", "true"),
        ]
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .to_vec();
        assert_eq!(
            select_metadata_hints(Endpoint::Grpc(Protocol::OtelArrow), input.clone()),
            [input[0].clone(), input[2].clone(), input[5].clone()],
        );
        assert_eq!(
            select_metadata_hints(Endpoint::Grpc(Protocol::BulkInsert), input.clone()),
            vec![
                input[0].clone(),
                input[1].clone(),
                input[2].clone(),
                input[3].clone(),
                input[5].clone()
            ],
        );
        assert!(
            for_endpoint(Endpoint::Grpc(Protocol::BulkInsert))
                .find(requests::TTL_KEY)
                .is_none()
        );
    }

    #[test]
    fn test_entity_patterns_apply_concrete_names_and_keep_unknown_passthrough() {
        let registry = for_endpoint(Endpoint::Http(Protocol::Influxdb));
        let mut input = Vec::new();
        for entity in ["service", "k8s.pod", "service.instance", "custom_1"] {
            for role in ["id", "descriptive", "scope"] {
                let name = format!("greptime.semantic.entity.{entity}.{role}");
                let definition = registry.find(&name).unwrap();
                assert!(definition.metadata.matches_pattern.is_some());
                input.push((name, "column".to_string()));
            }
        }
        let invalid = [
            SEMANTIC_ENTITY_ID_PATTERN,
            "greptime.semantic.entity..id",
            "greptime.semantic.entity.Service.id",
            "greptime.semantic.entity.service.unknown",
        ];
        for name in invalid {
            assert!(registry.find(name).is_none(), "{name}");
            input.push((name.to_string(), "retained".to_string()));
        }
        let batch = registry
            .parse_grouped(
                input.clone(),
                QueryParseOptions {
                    max_parallelism: query_options::MAX_PUBLIC_PARALLELISM,
                },
            )
            .unwrap();
        let (parsed, extensions) = batch.into_parts();
        assert_eq!(extensions.len(), invalid.len());
        let mut context = QueryContext::with("greptime", "public");
        for (name, value) in extensions {
            context.set_extension(name, value);
        }
        registry.apply(&mut context, parsed);
        for (name, value) in input {
            assert_eq!(context.extension(&name), Some(value.as_str()));
        }
    }

    #[test]
    fn test_known_unbound_hints_keep_legacy_extensions() {
        // Endpoint bindings select active hooks, not a whitelist of extensions.
        for (endpoint, name, value) in [
            (
                Endpoint::Http(Protocol::Sql),
                crate::http::prom_store::PHYSICAL_TABLE_PARAM,
                "metrics",
            ),
            (
                Endpoint::Http(Protocol::Logs),
                mito_engine_options::APPEND_MODE_KEY,
                "false",
            ),
            (
                Endpoint::Grpc(Protocol::NativeInsert),
                requests::TRACE_TABLE_PARTITIONS_HINT_KEY,
                "8",
            ),
            (
                Endpoint::Http(Protocol::Jaeger),
                query_options::QUERY_PARALLELISM,
                "4",
            ),
        ] {
            assert!(
                registrations()
                    .iter()
                    .any(|hint| hint.definition.metadata.name == name)
            );
            assert!(for_endpoint(endpoint).find(name).is_none());
            let input = vec![(name.to_string(), value.to_string())];
            let parsed = crate::hints::parse_endpoint(endpoint, input).unwrap();
            let mut context = QueryContext::with("greptime", "public");
            parsed.apply(&mut context);
            assert_eq!(context.extension(name), Some(value), "{endpoint:?}: {name}");
        }
    }

    #[test]
    fn test_native_query_and_row_insert_consumers_are_bound() {
        // Native SQL, logical plans and insert-into plans share the SQL endpoint;
        // RowInserts reads physical_table before choosing logical-table insertion.
        let query = for_endpoint(Endpoint::Grpc(Protocol::Sql));
        for name in [
            query_options::QUERY_PARALLELISM,
            query_options::QUERY_ALLOW_QUERY_FALLBACK,
            query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN,
            session::hints::READ_PREFERENCE_HINT,
        ] {
            assert!(query.find(name).is_some(), "{name}");
        }
        let rows = for_endpoint(Endpoint::Grpc(Protocol::NativeInsert));
        assert!(
            rows.find(crate::http::prom_store::PHYSICAL_TABLE_PARAM)
                .is_some()
        );
        for endpoint in protocol::PHYSICAL_TABLE_ENDPOINTS {
            for name in [
                mito_engine_options::APPEND_MODE_KEY,
                mito_engine_options::MERGE_MODE_KEY,
            ] {
                assert!(
                    for_endpoint(*endpoint).find(name).is_some(),
                    "{endpoint:?}: {name}"
                );
            }
        }
    }

    #[test]
    fn test_transport_compatibility_keeps_typed_validation_outside_consumers() {
        for (endpoint, name) in [
            (
                Endpoint::Http(Protocol::Influxdb),
                session::hints::READ_PREFERENCE_HINT,
            ),
            (
                Endpoint::Http(Protocol::Other),
                session::hints::READ_PREFERENCE_HINT,
            ),
            (
                Endpoint::Grpc(Protocol::NativeInsert),
                session::hints::READ_PREFERENCE_HINT,
            ),
            (
                Endpoint::Grpc(Protocol::Other),
                session::hints::INSERT_SKIP_WAL_HINT,
            ),
        ] {
            let input = vec![(name.to_string(), "invalid".to_string())];
            let result = crate::hints::parse_endpoint(endpoint, input);
            assert!(result.is_err(), "{endpoint:?}: {name}");
        }
        let mut context = QueryContext::with("greptime", "public");
        crate::hints::parse_endpoint(
            Endpoint::Grpc(Protocol::Other),
            vec![(
                session::hints::INSERT_SKIP_WAL_HINT.to_string(),
                "true".to_string(),
            )],
        )
        .unwrap()
        .apply(&mut context);
        assert!(context.skip_wal());
        let read_preference = registrations()
            .iter()
            .find(|hint| hint.definition.metadata.name == session::hints::READ_PREFERENCE_HINT)
            .unwrap();
        assert!(!read_preference.protocols().contains(&Protocol::Influxdb));
        for protocol in [Protocol::OtelArrow, Protocol::BulkInsert] {
            let endpoint = Endpoint::Grpc(protocol);
            assert!(
                for_endpoint(endpoint)
                    .find(session::hints::READ_PREFERENCE_HINT)
                    .is_none()
            );
            assert!(
                select_metadata_hints(
                    endpoint,
                    vec![(
                        session::hints::READ_PREFERENCE_HINT.to_string(),
                        "invalid".to_string()
                    ),]
                )
                .is_empty()
            );
        }
    }
}
