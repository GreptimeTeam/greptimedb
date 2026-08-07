---
Feature Name: Native Histogram Support and Compatibility Decisions
Tracking Issue: TBD
Date: 2026-08-04
Author: codex
---

# Summary

GreptimeDB stores a Prometheus native histogram in one Struct-valued field and
evaluates it as a first-class PromQL sample. This document records the supported
protocols, the storage invariant, and the compatibility decisions needed to make
that path predictable.

Native histograms are experimental. Prometheus Remote Write 2.0 is the only
supported native-histogram ingestion protocol. Remote Write 1.0 histogram
payloads are rejected instead of being acknowledged and dropped.
Native-histogram Remote Read is deferred; the existing Remote Read path
continues to return scalar samples only.

# Goals

1. Prevent silent native-histogram data loss at protocol boundaries.
2. Keep one stable persisted representation for integer, float, exponential,
   and custom-bucket native histograms.
3. Match Prometheus query behavior where it is observable and practical.
4. State intentional limitations explicitly so incomplete behavior is not
   mistaken for support.

# Non-Goals

- Supporting native histograms in Remote Write 1.0.
- Returning native histograms through Prometheus Remote Read.
- Persisting Remote Write metric metadata.
- Ingesting OTLP exponential histograms.
- Persisting exemplars.
- Removing mixed float/histogram handling from PromQL expressions.
- Propagating PromQL annotations produced on datanodes back to the frontend.

# Data Model and Invariants

Each native histogram is stored in the configured native-histogram field
(`greptime_native_histogram` by default), a Struct whose children preserve the
wire-level schema, zero threshold, sum, reset hint, start timestamp, custom
bounds, spans, and either the integer or float count family.
Integer bucket deltas are converted to absolute integer counts for storage;
float bucket counts are already absolute. No separate sample-kind discriminator
is stored because the populated count family identifies it.

Within one resolved catalog, schema, and physical-table routing context, a
metric name has exactly one persisted sample kind:

- a float metric uses `greptime_value`;
- a native-histogram metric uses the configured native-histogram field.

Labels do not change that choice. Remote Write 2.0 rejects a request when two
label sets use different sample kinds for the same metric, and the existing
table schema rejects kind changes across requests. Prometheus metadata type is
not used for this decision: a classic histogram family is described as a
histogram but is represented by float-valued `_bucket`, `_sum`, and `_count`
series.

The storage invariant does not remove the need for mixed-sample PromQL support.
An expression can combine different metrics or branches, for example with
`or`, and therefore produce a vector containing float samples and histogram
samples from different series.

# Protocol Decisions

## Remote Write 1.0

Remote Write 1.0 scalar samples remain supported. Any `TimeSeries.histograms`
field causes the complete request to fail with an invalid-arguments response.
Rejecting at protobuf decode time prevents both histogram-only data loss and
partial ingestion of a request that mixes scalar and histogram series. Clients
that send native histograms must use Remote Write 2.0.

## Remote Write 2.0

Remote Write 2.0 accepts integer and float native histograms while
`http.experimental_enable_prometheus_native_histogram` is enabled. Supported
exponential schemas are `-4` through `8`; schema `-53` represents native
histograms with custom buckets.

Exponential spans may end at the schema's overflow bucket but must not continue
beyond it. The overflow index is `(1024 << schema) + 1` for non-negative schemas
and `(1024 >> -schema) + 1` for negative schemas. Inputs beyond that index are
rejected rather than clamped or merged into infinity.

Exemplars and declared metric metadata are accepted on the wire but are not
persisted by this feature.

## Remote Read

Native-histogram Remote Read is deferred. GreptimeDB's existing Remote Read path
continues to negotiate `SAMPLES` responses and serialize scalar samples only.

A follow-up can map the stored Struct into `TimeSeries.histograms`. It must
preserve integer counts without passing through `f64`, retain the stored
histogram fields, and decide sampled versus streamed response coverage. The
sampled protobuf has no start-timestamp field, so that omission must remain
explicit if sampled responses are implemented.

## OTLP

OTLP exponential histograms are not converted in this version. The ingestion
branch intentionally remains deferred until it can map temporality, scale,
reset behavior, attributes, and rejected-point reporting into the canonical
native-histogram path. The code carries an explicit TODO rather than a partial
encoder.

# PromQL Compatibility Decisions

## Staleness

A native histogram is stale only when its `sum` has Prometheus's stale-NaN bit
pattern. Ordinary NaN sums remain samples. Range selectors remove stale markers
before range functions run. Instant selectors treat the marker as the end of
the series for lookback purposes and do not resurrect an older sample.

## Start timestamps

Reset detection already uses consecutive start timestamps in addition to
bucket and count monotonicity. Counter `rate` and `increase` also use the first
histogram's start timestamp as a synthetic zero when it is nonzero and strictly
inside the query window before the first sample. This permits the same
single-sample calculation as Prometheus and prevents left extrapolation past the
known start. When no usable start timestamp exists, a positive histogram-count
increase lets counter extrapolation infer a zero point from the first count to
cap left extrapolation; a synthetic zero uses its known timestamp instead of
that heuristic.

This behavior is native-histogram-only. GreptimeDB does not persist start
timestamps for float samples, so float `rate` and `increase` still require two
samples. Warnings for overlapping start timestamps are deferred.

## Arithmetic and averages

Histogram addition and subtraction always reconcile layouts, including empty
histograms. Mixing exponential and custom layouts fails; custom layouts are
reconciled according to their shared bounds. The only layout bypass is local to
counter rate: when the first-to-second pair is a reset, the first sample's
layout is irrelevant and the second sample starts the accumulated segment.

`avg` and `avg_over_time` use a mergeable weighted running mean. This prevents a
finite mean from becoming infinity solely because the intermediate sum
overflowed. Native-histogram aggregates are not currently split into datanode
partial aggregation and frontend state merging, so this state does not cross
nodes or versions. If distributed stepping is added later, its state must be
versioned before rolling upgrades can safely mix implementations.

The rejected alternative is Prometheus's direct-sum/Kahan-compensation design
with an overflow-triggered mode change. It offers closer rounding parity but
requires additional state and substantially more merge logic.

## Equality, changes, and resets

Equality follows Prometheus's represented-layout semantics. Histograms compare
equal only when their represented bucket-index sequences and bucket value bit
patterns match. An explicitly represented zero bucket therefore differs from an
omitted bucket, and `changes()` observes that layout change. Redundant
zero-length span encodings are normalized, malformed layouts compare unequal,
and reset hints and start timestamps remain excluded.

Custom bucket bounds use ordinary floating-point equality rather than bitwise
equality, matching Prometheus's `CustomBucketBoundsMatch`. Histogram payload
values continue to use bitwise equality so NaN payloads behave deterministically.

`resets()` counts ordinary counter resets and, unlike Prometheus, also counts
each transition between gauge and non-gauge native histograms as one reset.

## Annotations

Warnings and infos produced by frontend-executed PromQL functions are returned
through the corresponding Prometheus JSON response fields. Annotations produced
while a pushed-down plan executes on a datanode are not yet transported back to
the frontend. The sample-dropping behavior is unchanged, but those remote
annotations remain silent until a query-result transport is defined for them.

## Standard deviation and bucket bounds

Custom-bucket standard deviation and variance use the arithmetic midpoint
`(lower + upper) / 2` for every bucket. Underflow and overflow buckets therefore
naturally produce infinite or NaN estimates. Exponential buckets retain the
signed geometric midpoint, with zero used for a bucket spanning zero.

Boundary calculation returns the last finite boundary as `f64::MAX`, the
overflow boundary as positive infinity, and rejects indices beyond the
overflow bucket. Widened integer arithmetic prevents large negative-schema
indices from wrapping into an unrelated finite boundary.

# Metric Metadata

The Prometheus `/metadata` endpoint currently derives metric names from logical
table names and reads type and unit from semantic table options stamped at table
creation. Native-histogram tables without a declared type fall back to
`histogram`. Help is always empty, and unit is absent for tables without a
semantic unit option.

Persisting declared Remote Write metadata is separate work. Reusing table
options alone is insufficient because metadata-only writes, later updates, and
existing tables do not pass through auto-create. An in-memory registry would
lose data on restart. The recommended follow-up is a persistent registry keyed
by catalog, schema, and metric family, populated by Remote Write 1.0 metadata-only
requests and Remote Write 2.0 per-series metadata. This follows the update
boundary already recorded in
[Table Semantic Layer](2026-05-28-table-semantic-layer.md).

# Testing and Compatibility

Compatibility coverage includes protocol rejection, kind exclusivity,
stale-marker selector semantics, synthetic-zero rates, incompatible empty
layouts, overflow-safe averages, layout-sensitive equality, infinite custom
midpoints, and exponential overflow indices for schemas `-4`, `0`, and `8`.
Behavioral coverage also verifies frontend PromQL warning and info responses.

This work does not change GreptimeDB's persisted Struct, protobuf dependencies,
or public configuration. Existing native-histogram data remains readable.

# Future Work

- Native-histogram Remote Read, including exact integer round-trips and streamed
  chunks with start timestamps.
- OTLP exponential-histogram conversion with partial-success reporting.
- Persistent Remote Write metadata and accurate help/unit updates.
- Native-histogram exemplars and exemplar query APIs.
- Start-timestamp overlap annotations.
- Datanode-to-frontend PromQL annotation propagation.
- Versioned two-phase native-histogram aggregation state.
- Exact Prometheus summation compensation if measured precision differences
  justify the extra aggregate state.
