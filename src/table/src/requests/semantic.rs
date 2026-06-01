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

/// Returns true if `key` is a public semantic table-option key.
///
/// The internal [`SEMANTIC_PER_TABLE_INDEX_KEY`] is intentionally NOT matched:
/// it lives outside [`SEMANTIC_PREFIX`] so it can never be a valid table option.
pub fn is_semantic_option_key(key: &str) -> bool {
    key.starts_with(SEMANTIC_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_semantic_option_key() {
        assert!(is_semantic_option_key(SEMANTIC_SIGNAL_TYPE));
        assert!(is_semantic_option_key(SEMANTIC_METRIC_TYPE));
        assert!(is_semantic_option_key("greptime.semantic.future.key"));

        // Near-misses must not match.
        assert!(!is_semantic_option_key("greptime.semanticx"));
        assert!(!is_semantic_option_key("greptime.semantic"));
        assert!(!is_semantic_option_key("semantic.signal_type"));
        assert!(!is_semantic_option_key("table_data_model"));

        // The internal transport key must never be treated as a table option.
        assert!(!is_semantic_option_key(SEMANTIC_PER_TABLE_INDEX_KEY));
    }
}
