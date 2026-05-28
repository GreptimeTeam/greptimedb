---
Feature Name: Table Semantic Layer
Tracking Issue: TBD
Date: 2026-05-28
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Attach a thin layer of semantic metadata to each table so machine consumers — LLM agents, alert generators, dashboard builders, MCP servers, ETL pipelines — can align it with the observability concepts they already know (OTel instrument kinds, Prometheus naming conventions, UCUM units, semantic conventions, severity numbers, OTel ↔ Prometheus translation rules).

The mechanism reuses what already exists in `table_options` (the same slot that today carries `table_data_model` and `otlp_metric_compat`): a reserved `greptime.semantic.*` namespace, plus standard SQL column `COMMENT` for field-level supplements, plus an `information_schema.semantic_tables` view as the discovery entry point. No new protocol, no new DDL keyword.

Per-table identity only. Cross-table relationships are deferred.

# Motivation

GreptimeDB already ingests OTLP metrics / traces / logs and Prometheus remote write. Each protocol carries rich metadata on the wire (instrument kind, temporality, unit, scope, resource, semantic-conventions version), and most of it is dropped when rows land in a table:

- An `opentelemetry_traces` table looks like any wide table; signal type, source, and field provenance must be guessed from naming.
- The OTel-to-Prometheus translation in v0.16+ actively drops scope attributes and most resource attributes; the table never records *what was dropped*.
- Prometheus remote write v1 metadata is unreliable by protocol, but downstream tables do not flag whether `counter` typing was *declared* or *inferred* from the `_total` suffix.
- Mixed-temporality data (OTel delta + Prometheus cumulative in the same table) is unrecoverable from schema alone.

The audience is broader than LLM agents. Alert generators need to choose between `rate()` and absolute thresholds, and need units to pick sensible bounds. Dashboard builders pick visualisations by signal type. MCP servers surface a structured tool catalog instead of free-text descriptions. ETL pipelines need lineage to know whether a `service_name` column is `resource.service.name` or a free-form label. All of them currently guess from column names; the metadata to remove the guess already exists at ingest time, we just do not preserve it.

# Goals

1. Tag every ingested table with a stable identity using existing SQL surfaces — no new protocol, no new DDL keyword.
2. Record the lossy transformations the ingestion path performs (dropped attributes, scope handling, type inference vs. declaration).
3. Expose one `information_schema` view as the consumer-facing discovery entry point.
4. Keep the layer optional and additive — tables without these options keep working unchanged.

# Non-Goals

- Cross-table relationship modelling. Deferred to a follow-up RFC.
- Bespoke storage. Reuse `table_options` and column `COMMENT`.
- Semantic enforcement at query time. The layer is descriptive, not coercive.
- New wire protocol. Upstream standardisation is mentioned only as a future direction.

# Proposal

## Three mechanisms

1. **`greptime.semantic.*` table options** — table-level identity and lineage. Carried inside the existing `table_options` blob. This is the same slot that today carries `table_data_model = 'greptime_trace_v1'` and `otlp_metric_compat = 'prom'`, so the mechanism is generalising what the OTLP trace auto-create path already does.
2. **Column `COMMENT`** — column-level supplements ("this column is `resource.service.name`"; "this column carries delta values"). Standard SQL.
3. **`information_schema.semantic_tables` view** — a denormalised projection of the options, registered through the existing `with_extra_table_factories()` hook. Tables without a `greptime.semantic.*` option do not appear in the view.

## Vocabulary

All keys are flat strings under the `greptime.semantic.` prefix; values are strings; unknown keys are tolerated so the vocabulary can grow without coordinated rollouts.

**Common (all signals)**

| Key | Example |
| --- | --- |
| `greptime.semantic.signal_type` | `trace` / `log` / `metric` / `event` |
| `greptime.semantic.source` | `opentelemetry` / `prometheus` / `jaeger` / `loki` / `custom` |
| `greptime.semantic.source_protocol` | `otlp/http` / `prom_remote_write_v2` / `scrape` |
| `greptime.semantic.source_version` | `1.30.0` (optional) |
| `greptime.semantic.pipeline` | `greptime_trace_v1` (subsumes the existing `table_data_model` value) |

**Trace**: `trace.conventions` (e.g. `otel-semconv-1.27`, lifted from `schema_url`), `trace.has_events`, `trace.has_links`.

**Metric** — v1 assumes one metric type per table, which is how both Prom RW and the post-v0.16 OTel ingestion path land data today; mixed-type tables are a follow-up.

| Key | Example |
| --- | --- |
| `greptime.semantic.metric.type` | `counter` / `gauge` / `histogram` / `summary` / `updown_counter` / `gauge_histogram` / `info` / `stateset` |
| `greptime.semantic.metric.unit` | UCUM, e.g. `s`, `By`, `{request}` |
| `greptime.semantic.metric.temporality` | `cumulative` / `delta` (OTel only) |
| `greptime.semantic.metric.monotonic` | `true` / `false` |
| `greptime.semantic.metric.metadata_quality` | `declared` (OTLP / Prom RW v2 / exposition) or `inferred` (Prom RW v1, name-suffix guess) |
| `greptime.semantic.metric.original_name` | Pre-translation OTel name when the table name was Prometheus-ised |

`metadata_quality = inferred` is the load-bearing field for confidence-aware tooling: an inferred counter should be re-checked before betting on `rate()`-style semantics.

**Log**: `log.severity_scheme` (`otlp` / `syslog` / `custom`), `log.body_format` (`string` / `json` / `mixed`).

**Resource / scope preservation**: `resource.attributes_preserved` (JSON array string of attrs promoted to columns), `resource.attributes_dropped` (boolean), `scope.preserved` (boolean). These answer the most common downstream question: "is this data missing because it was dropped, or because it lives on a different column than I think?" List-shaped values use JSON array strings rather than comma-separated text to avoid escaping and ordering ambiguity.

## Conflict and update semantics

Two design decisions worth pinning down up front, because they constrain everything else:

- **Conflict.** Some table-level keys (`trace.conventions` lifted from `schema_url`, `metric.temporality`, ...) cannot represent the truth when a long-lived table sees rows from multiple sources. v1 records `mixed` or `unknown` rather than a fictitious single value. Downstream consumers must treat any single-valued semantic key as best-effort, not strong evidence.
- **Update.** Semantic options are stamped at table creation. v1 does not specify an update path; promoting `metadata_quality` from `inferred` to `declared`, refreshing `resource.attributes_preserved`, or revising `trace.conventions` on later writes is deferred. If real usage shows update is needed, it lands as a separate RFC.

## `information_schema.semantic_tables`

A consumer's first SQL on connect:

```sql
SELECT table_schema, table_name, signal_type, source, pipeline
FROM information_schema.semantic_tables;
```

returns one row per semantic-tagged table. The view exposes a stable set of core columns (`signal_type`, `source`, `source_protocol`, `source_version`, `pipeline`) plus a `semantic_options` JSON column carrying the rest of the `greptime.semantic.*` keys verbatim. Future keys appear inside `semantic_options` without forcing a view-schema change; only widely-used keys are ever promoted to first-class columns.

# Implementation Plan

Four phases, each independently shippable.

1. **Identity.** Stamp `signal_type` and `source` on every auto-create path. The OTLP paths already have natural injection points; Prom remote write is the one non-trivial path because metric-engine logical tables share physical storage (see Open Question 2).
2. **Metric specifics.** Add type / unit / temporality / monotonic / metadata_quality / original_name at OTel metric and Prom RW ingestion sites; the data is already at hand inside the OTel translator.
3. **Resource / scope lineage.** Record what the OTel-to-Prometheus translation kept and dropped.
4. **`information_schema.semantic_tables` view + documentation** as a stable user-facing contract.

# Relationship to OpenTelemetry standardisation

OTel today standardises what producers emit and how agents are managed; the read side — what a backend exposes back to clients — is deliberately vendor turf. OTLP is one-way; OpAMP is agent management; OTEP-0243 (App Telemetry Schema) is producer-side; `schema_url` is producer-stated with no reverse. Adjacent precedents — Prometheus `/api/v1/metadata`, Loki labels API, Tempo tags, Jaeger services, ad-hoc MCP servers — are all vendor-specific.

This is a real gap. The shape we propose locally (signal-agnostic, `schema_url`-aware, structured around a small vocabulary) is deliberately close to what a future upstream OTEP for a backend-catalog read API could look like, with Weaver's *Resolved Telemetry Schema* as the natural data model. We do not commit to driving such an OTEP here; we do commit to keeping the local shape close enough that a future upstream proposal does not force a breaking migration.

# Alternatives

- **New DDL syntax (`SEMANTIC trace WITH (...)`).** Cleaner-looking but non-standard and forces every client to learn it. The metadata is not interesting enough to justify a new keyword.
- **Dedicated `_semantic` system table.** Doubles the storage path for what is static per-table KV and adds lifecycle questions (drop, backfill). A view over `table_options` covers the same access pattern.
- **Column comments only.** Discovery (`WHERE signal_type = 'trace'`) becomes a full-text problem. Comments are good for column-level supplements, not for identity.
- **Encode everything into the table name.** What we do today. Every new field becomes a new naming convention.

# Open Questions

1. **Namespace prefix.** `greptime.semantic.*` vs. bare `semantic.*`. v1 picks the vendored prefix; alias or migrate if a community standard later emerges.
2. **Prom RW injection point.** Metric-engine logical tables share physical storage, so per-logical-table options need a hook that does not exist as cleanly as the OTLP trace branch. A short spike before Phase 1 lands for Prom RW.
3. **Mixed-type metric tables.** When ingestion modes that pack multiple metric types into one table appear, `metric.type` migrates from table-level to row-level. v1 leaves a `metric.type = 'mixed'` marker and punts.
4. **Stability surface.** Top-level keys (`signal_type`, `source`) are stable; sub-namespaces (`metric.*`, ...) are evolving until v1.0 of the layer is declared.

# Future Work

- **Cross-table relationships.** Paired trace/services tables, metric/info pairing, JOIN hints. Its own RFC.
- **Backfill** for tables created before this feature shipped.
- **Upstream proposal.** Carry the shape into a community proposal — likely an OTEP for an OTLP-Catalog read API plus an MCP binding — informed by Greptime's local usage data.

# References

OpenTelemetry:
- [OTLP specification](https://opentelemetry.io/docs/specs/otlp/)
- [OTel Schemas (`schema_url`)](https://opentelemetry.io/docs/specs/otel/schemas/)
- [Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [OTEP-0243: App Telemetry Schema](https://github.com/open-telemetry/oteps/blob/main/text/0243-app-telemetry-schema-vision-roadmap.md)
- [OpAMP specification](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md)
- [Weaver: Resolved Telemetry Schema](https://github.com/open-telemetry/weaver)
- [2025 Stability Proposal](https://opentelemetry.io/blog/2025/stability-proposal-announcement/)

Prometheus / OpenMetrics:
- [Prometheus Remote Write 1.0](https://prometheus.io/docs/specs/prw/remote_write_spec/)
- [Prometheus Remote Write 2.0](https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/)
- [Prometheus exposition formats](https://prometheus.io/docs/instrumenting/exposition_formats/)
- [Prometheus HTTP API: `/api/v1/metadata`](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata)

Units and conventions:
- [UCUM — Unified Code for Units of Measure](https://ucum.org/)

GreptimeDB:
- [OTLP ingestion guide](https://docs.greptime.com/user-guide/ingest-data/for-observability/opentelemetry/)
- [Trace data model](https://docs.greptime.com/user-guide/traces/data-model/)
