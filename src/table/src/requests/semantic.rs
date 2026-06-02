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

//! Table semantic layer vocabulary.
//!
//! A thin layer of semantic metadata attached to a table via `table_options`, so
//! machine consumers (LLM agents, alert/dashboard builders, MCP servers, ETL) can
//! align a table with the observability concept it stands for without guessing
//! from column names. See `docs/rfcs/2026-05-28-table-semantic-layer.md`.
//!
//! All public table-option keys share the [`SEMANTIC_PREFIX`] namespace and are
//! string-valued. [`is_semantic_option_key`] gates them through
//! [`crate::requests::validate_table_option`], so they are accepted both on the
//! ingestion auto-create path and on explicit `CREATE TABLE ... WITH (...)` DDL.

/// Reserved prefix for every public semantic table-option key.
pub const SEMANTIC_PREFIX: &str = "greptime.semantic.";

/// Internal `QueryContext` extension key carrying the per-table semantic index
/// (a `{table_name -> {semantic_key: value}}` JSON blob) from the ingestion
/// encode path to the auto-create site. Deliberately OUTSIDE [`SEMANTIC_PREFIX`]
/// so it is not a valid table option and never leaks into a table's options.
pub const SEMANTIC_PER_TABLE_INDEX_KEY: &str = "greptime.internal.semantic.per_table_index";

// ---- Common keys (all signals) ----

/// Signal kind: one of [`SIGNAL_TYPE_TRACE`] / [`SIGNAL_TYPE_LOG`] /
/// [`SIGNAL_TYPE_METRIC`] / [`SIGNAL_TYPE_EVENT`].
pub const SEMANTIC_SIGNAL_TYPE: &str = "greptime.semantic.signal_type";
/// Ingestion ecosystem, e.g. [`SOURCE_OPENTELEMETRY`] / [`SOURCE_PROMETHEUS`].
pub const SEMANTIC_SOURCE: &str = "greptime.semantic.source";
/// Optional protocol or SDK version string, e.g. `v2` (Prom remote write), `1.30.0`.
pub const SEMANTIC_SOURCE_VERSION: &str = "greptime.semantic.source_version";
/// Internal ingestion pipeline / data model, e.g. `greptime_trace_v1`.
pub const SEMANTIC_PIPELINE: &str = "greptime.semantic.pipeline";

// ---- Trace keys ----

/// Semantic-conventions version the rows conform to (e.g. `otel-semconv-1.27`),
/// or [`SEMANTIC_VALUE_UNKNOWN`] / [`SEMANTIC_VALUE_MIXED`] when not single-valued.
pub const SEMANTIC_TRACE_CONVENTIONS: &str = "greptime.semantic.trace.conventions";
/// Whether `span_events` are preserved on the table.
pub const SEMANTIC_TRACE_HAS_EVENTS: &str = "greptime.semantic.trace.has_events";
/// Whether `span_links` are preserved on the table.
pub const SEMANTIC_TRACE_HAS_LINKS: &str = "greptime.semantic.trace.has_links";

// ---- Metric keys (populated in Phase 2) ----

/// Instrument kind: `counter` / `gauge` / `histogram` / `summary` /
/// `updown_counter` / `gauge_histogram` / `info` / `stateset`.
pub const SEMANTIC_METRIC_TYPE: &str = "greptime.semantic.metric.type";
/// UCUM unit, e.g. `s`, `By`, `{request}`.
pub const SEMANTIC_METRIC_UNIT: &str = "greptime.semantic.metric.unit";
/// `cumulative` / `delta` (OTel only).
pub const SEMANTIC_METRIC_TEMPORALITY: &str = "greptime.semantic.metric.temporality";
/// `true` / `false` for sum / counter typed data.
pub const SEMANTIC_METRIC_MONOTONIC: &str = "greptime.semantic.metric.monotonic";
/// [`METADATA_QUALITY_DECLARED`] when the protocol stated the type, or
/// [`METADATA_QUALITY_INFERRED`] when guessed from a name suffix.
pub const SEMANTIC_METRIC_METADATA_QUALITY: &str = "greptime.semantic.metric.metadata_quality";
/// Pre-translation OTel metric name when the table name was Prometheus-ised.
pub const SEMANTIC_METRIC_ORIGINAL_NAME: &str = "greptime.semantic.metric.original_name";

// ---- Log keys (populated in Phase 3) ----

/// `otlp` / `syslog` / `custom` — which mapping to use for `severity_number`.
pub const SEMANTIC_LOG_SEVERITY_SCHEME: &str = "greptime.semantic.log.severity_scheme";
/// `string` / `json` / `mixed` — how to parse `body`.
pub const SEMANTIC_LOG_BODY_FORMAT: &str = "greptime.semantic.log.body_format";

// ---- Resource / scope preservation keys (populated in Phase 3) ----

/// JSON array string of resource attributes promoted to first-class columns.
pub const SEMANTIC_RESOURCE_ATTRIBUTES_PRESERVED: &str =
    "greptime.semantic.resource.attributes_preserved";
/// `true` / `false` — whether any resource attribute was dropped at ingest.
pub const SEMANTIC_RESOURCE_ATTRIBUTES_DROPPED: &str =
    "greptime.semantic.resource.attributes_dropped";
/// `true` / `false` — whether `scope.name` / `scope.version` survive on the row.
pub const SEMANTIC_SCOPE_PRESERVED: &str = "greptime.semantic.scope.preserved";

// ---- Value constants ----

pub const SIGNAL_TYPE_TRACE: &str = "trace";
pub const SIGNAL_TYPE_LOG: &str = "log";
pub const SIGNAL_TYPE_METRIC: &str = "metric";
pub const SIGNAL_TYPE_EVENT: &str = "event";

pub const SOURCE_OPENTELEMETRY: &str = "opentelemetry";
pub const SOURCE_PROMETHEUS: &str = "prometheus";

pub const METADATA_QUALITY_DECLARED: &str = "declared";
pub const METADATA_QUALITY_INFERRED: &str = "inferred";

/// Sentinel for a key that cannot be determined at stamp time.
pub const SEMANTIC_VALUE_UNKNOWN: &str = "unknown";
/// Sentinel for a single-valued key that saw conflicting sources.
pub const SEMANTIC_VALUE_MIXED: &str = "mixed";

/// Every recognised public semantic table-option key. The set is a closed
/// whitelist: keys under [`SEMANTIC_PREFIX`] that are not listed here are rejected,
/// so an unknown key like `greptime.semantic.unknown_key` does not silently land
/// in a table's options. Adding a key to the vocabulary means adding it here.
pub const SEMANTIC_OPTION_KEYS: &[&str] = &[
    SEMANTIC_SIGNAL_TYPE,
    SEMANTIC_SOURCE,
    SEMANTIC_SOURCE_VERSION,
    SEMANTIC_PIPELINE,
    SEMANTIC_TRACE_CONVENTIONS,
    SEMANTIC_TRACE_HAS_EVENTS,
    SEMANTIC_TRACE_HAS_LINKS,
    SEMANTIC_METRIC_TYPE,
    SEMANTIC_METRIC_UNIT,
    SEMANTIC_METRIC_TEMPORALITY,
    SEMANTIC_METRIC_MONOTONIC,
    SEMANTIC_METRIC_METADATA_QUALITY,
    SEMANTIC_METRIC_ORIGINAL_NAME,
    SEMANTIC_LOG_SEVERITY_SCHEME,
    SEMANTIC_LOG_BODY_FORMAT,
    SEMANTIC_RESOURCE_ATTRIBUTES_PRESERVED,
    SEMANTIC_RESOURCE_ATTRIBUTES_DROPPED,
    SEMANTIC_SCOPE_PRESERVED,
];

/// Returns true if `key` is a recognised semantic table-option key (whitelist).
///
/// Note this is membership, not a prefix test: unknown keys under
/// [`SEMANTIC_PREFIX`] are rejected, and the internal
/// [`SEMANTIC_PER_TABLE_INDEX_KEY`] (outside the prefix) never matches.
pub fn is_semantic_option_key(key: &str) -> bool {
    SEMANTIC_OPTION_KEYS.contains(&key)
}

/// Validates a `greptime.semantic.*` option's `value` against its allowed domain.
///
/// Open-value keys (unit, original_name, version, pipeline, conventions, the
/// preserved-attributes list) accept any non-empty string. Closed-domain keys
/// accept a fixed set, plus the `unknown` sentinel, plus `mixed` for the keys
/// where one long-lived table can legitimately see multiple values. Keys not in
/// [`SEMANTIC_OPTION_KEYS`] are rejected.
pub fn validate_semantic_option(key: &str, value: &str) -> bool {
    match key {
        SEMANTIC_SOURCE_VERSION
        | SEMANTIC_PIPELINE
        | SEMANTIC_METRIC_UNIT
        | SEMANTIC_METRIC_ORIGINAL_NAME
        | SEMANTIC_TRACE_CONVENTIONS
        | SEMANTIC_RESOURCE_ATTRIBUTES_PRESERVED => !value.is_empty(),

        SEMANTIC_SIGNAL_TYPE => matches!(value, "trace" | "log" | "metric" | "event" | "unknown"),
        SEMANTIC_SOURCE => matches!(
            value,
            "opentelemetry"
                | "prometheus"
                | "elasticsearch"
                | "loki"
                | "custom"
                | "mixed"
                | "unknown"
        ),
        SEMANTIC_METRIC_TYPE => matches!(
            value,
            "counter"
                | "gauge"
                | "histogram"
                | "summary"
                | "updown_counter"
                | "gauge_histogram"
                | "info"
                | "stateset"
                | "mixed"
                | "unknown"
        ),
        SEMANTIC_METRIC_TEMPORALITY => {
            matches!(value, "cumulative" | "delta" | "mixed" | "unknown")
        }
        SEMANTIC_METRIC_MONOTONIC
        | SEMANTIC_TRACE_HAS_EVENTS
        | SEMANTIC_TRACE_HAS_LINKS
        | SEMANTIC_RESOURCE_ATTRIBUTES_DROPPED
        | SEMANTIC_SCOPE_PRESERVED => matches!(value, "true" | "false" | "unknown"),
        SEMANTIC_METRIC_METADATA_QUALITY => matches!(value, "declared" | "inferred" | "unknown"),
        SEMANTIC_LOG_SEVERITY_SCHEME => matches!(value, "otlp" | "syslog" | "custom" | "unknown"),
        SEMANTIC_LOG_BODY_FORMAT => matches!(value, "string" | "json" | "mixed" | "unknown"),

        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_semantic_option_key() {
        assert!(is_semantic_option_key(SEMANTIC_SIGNAL_TYPE));
        assert!(is_semantic_option_key(SEMANTIC_METRIC_TYPE));

        // Unknown keys under the prefix are not whitelisted.
        assert!(!is_semantic_option_key("greptime.semantic.future.key"));
        assert!(!is_semantic_option_key("greptime.semantic.unknown_key"));
        // Near-misses must not match.
        assert!(!is_semantic_option_key("greptime.semanticx"));
        assert!(!is_semantic_option_key("semantic.signal_type"));
        assert!(!is_semantic_option_key("table_data_model"));
        // The internal transport key must never be treated as a table option.
        assert!(!is_semantic_option_key(SEMANTIC_PER_TABLE_INDEX_KEY));
    }

    #[test]
    fn test_validate_semantic_option() {
        // Enum keys reject out-of-domain values.
        assert!(validate_semantic_option(SEMANTIC_SIGNAL_TYPE, "metric"));
        assert!(!validate_semantic_option(SEMANTIC_SIGNAL_TYPE, "spans"));
        assert!(validate_semantic_option(SEMANTIC_METRIC_TYPE, "counter"));
        assert!(validate_semantic_option(SEMANTIC_METRIC_TYPE, "mixed"));
        assert!(!validate_semantic_option(SEMANTIC_METRIC_TYPE, "bogus"));

        // Booleans, sentinels, open values.
        assert!(validate_semantic_option(SEMANTIC_TRACE_HAS_EVENTS, "true"));
        assert!(!validate_semantic_option(SEMANTIC_TRACE_HAS_EVENTS, "yes"));
        assert!(validate_semantic_option(
            SEMANTIC_METRIC_TEMPORALITY,
            "unknown"
        ));
        assert!(validate_semantic_option(SEMANTIC_METRIC_UNIT, "By"));
        assert!(!validate_semantic_option(SEMANTIC_METRIC_UNIT, ""));

        // Unknown key is rejected regardless of value.
        assert!(!validate_semantic_option(
            "greptime.semantic.future.key",
            "x"
        ));

        // Drift guard: every value stamped by the ingestion path must validate.
        assert!(validate_semantic_option(
            SEMANTIC_SIGNAL_TYPE,
            SIGNAL_TYPE_TRACE
        ));
        assert!(validate_semantic_option(
            SEMANTIC_SIGNAL_TYPE,
            SIGNAL_TYPE_METRIC
        ));
        assert!(validate_semantic_option(
            SEMANTIC_SIGNAL_TYPE,
            SIGNAL_TYPE_LOG
        ));
        assert!(validate_semantic_option(
            SEMANTIC_SOURCE,
            SOURCE_OPENTELEMETRY
        ));
        assert!(validate_semantic_option(SEMANTIC_SOURCE, SOURCE_PROMETHEUS));
        assert!(validate_semantic_option(
            SEMANTIC_METRIC_METADATA_QUALITY,
            METADATA_QUALITY_INFERRED
        ));
        assert!(validate_semantic_option(
            SEMANTIC_TRACE_CONVENTIONS,
            SEMANTIC_VALUE_UNKNOWN
        ));
        // An empty value never validates, for any whitelisted key.
        for key in SEMANTIC_OPTION_KEYS {
            assert!(
                !validate_semantic_option(key, ""),
                "empty value should never validate for {key}"
            );
        }
    }
}
