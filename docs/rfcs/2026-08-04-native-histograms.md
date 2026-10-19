---
Feature Name: Native Histogram Support and Compatibility Decisions
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8887
Date: 2026-08-04
Author: codex
---

# Summary

GreptimeDB stores native histograms in one Prometheus-compatible Struct-valued
field and evaluates them as first-class PromQL samples. Prometheus Remote Write
2.0 supplies native histograms directly. OTLP `ExponentialHistogram` is an
ingestion transport: accepted cumulative points are normalized into the same
Struct before persistence and are queried only as native histograms.

Prometheus Remote Write 2.0 native histograms and cumulative OTLP/HTTP
exponential histograms are enabled by default.
Remote Write 1.0 histogram payloads are rejected instead of being acknowledged and dropped.
Native-histogram Remote Read is deferred; the existing Remote Read path
continues to return scalar samples only.

# Goals

1. Prevent silent native-histogram data loss at protocol boundaries.
2. Keep one stable Prometheus-compatible persisted representation across
   Prometheus and OTLP ingestion.
3. Match Prometheus query behavior where it is observable and practical.
4. State intentional limitations explicitly so incomplete behavior is not
   mistaken for support.

# Non-Goals

- Supporting native histograms in Remote Write 1.0.
- Returning native histograms through Prometheus Remote Read.
- Persisting Remote Write metric metadata.
- Persisting exemplars.
- Converting OTLP delta values to cumulative values during ingestion. Supported
  delta metrics are stored as interval values without per-series accumulation
  state; this also applies to the planned exponential-histogram support.
- Providing an OTLP-specific histogram query surface or reconstructing and
  re-exporting the original OTLP point after persistence.
- Removing mixed float/histogram handling from PromQL expressions.
- Propagating PromQL annotations produced on datanodes back to the frontend.

# Data Model and Invariants

Each accepted native histogram, whether received directly through Remote Write
2.0 or normalized from OTLP `ExponentialHistogram`, is stored in the configured
native-histogram field (`greptime_native_histogram` by default) as the same
canonical Struct. The persistence boundary admits only values accepted by the
shared native-histogram validation.

## Prometheus Remote Write 2.0

Remote Write 2.0 native histograms enter the persistence path in Prometheus
format. The Struct preserves the validated schema, zero threshold, sum, reset
hint, start timestamp, custom bounds, spans, and either the integer or float
count family. Integer bucket deltas are converted to absolute integer counts
for storage; float bucket counts are already absolute. No separate sample-kind
discriminator is stored because the populated count family identifies it.

## OTLP ExponentialHistogram

OTLP `ExponentialHistogram` does not introduce a second persisted format.
Accepted cumulative points are normalized into a valid Prometheus native
histogram and then pass through the same validator and Struct encoder. The
Struct does not retain the raw OTLP point or a source-protocol discriminator,
and queries use only the native-histogram PromQL behavior. Detailed conversion
and rejection rules are listed under [OTLP](#otlp).

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

Remote Write 2.0 accepts integer and float native histograms by default. Supported
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

OTLP/HTTP exponential histograms are accepted by default.
OTel Arrow exponential histograms are rejected because
the current Arrow wire format omits `zero_threshold`; accepting them would
silently change the distribution. Cumulative and delta temporality are accepted;
unspecified and unrecognized temporality are rejected before processing points.
Explicit OTLP histograms keep their existing `_bucket`, `_sum`, and `_count`
representation. All delta metrics store raw interval values with
`otlp_aggregation_temporality="delta"`; ingestion never accumulates across points
or requests. Explicit histogram bucket counts are prefix-summed within a point.
Tagged float series retain GreptimeDB's automatic delta `rate()`/`increase()`
extension. Native histograms use the explicit queries below instead.
Delta-to-cumulative conversion is outside this feature.

OTLP scales `-4` through `8` map directly to Prometheus schemas. Higher scales
are downscaled to schema `8`: dense counts that collide are merged before the
OTLP lower-bound index is shifted by one to the Prometheus upper-bound index.
This preserves count mass but irreversibly loses distinctions between source
buckets that merge. Lower scales are rejected. OTLP counts always populate the
integer histogram family. The transmitted non-negative finite zero threshold
and zero count are preserved; the Struct start timestamp is stored in
milliseconds. The reset hint is gauge for delta and unknown for cumulative samples.
Point timestamps and attributes
retain their existing mode-specific conversion rules. Legacy mode retains its
normalized name, attribute rules, and nanosecond row timestamp. Nonlegacy metric-engine conversion retains nanoseconds until alignment to the
destination time-index unit: seconds, milliseconds, microseconds, or nanoseconds.
New logical tables inherit their backing table's unit; auto-created physical
tables default to milliseconds. Nonlegacy ordinary insertion uses milliseconds.
Both modes write the same canonical Struct.

An absent sum is stored as an ordinary quiet NaN, so the sample remains
selectable while `histogram_sum` is unknown. Any NaN sum on a point without
`NoRecordedValue` is normalized to the same value, even if its payload has the
Prometheus stale-marker bits. Only an exponential-histogram point carrying OTLP
`NoRecordedValue` becomes an empty schema-0 integer histogram with the canonical
Prometheus stale-NaN sum; its attributes and timestamps remain. This
interpretation intentionally does not change gauges, sums, or explicit histograms.

Invalid points are skipped while unrelated valid points continue. Mixed
accepted/rejected OTLP/HTTP requests return partial success; a request with only
rejected points returns `InvalidArgument`. OTel Arrow uses an `OK` batch status
for mixed batches and `INVALID_ARGUMENT` when all points are rejected. Rejection
details are bounded, and metric metadata is emitted only for a metric that
produced an accepted row.

Zero-count buckets are omitted after downscaling, splitting nonzero runs into
sparse spans without changing integer counts. Rejected data points increment
`greptime_servers_otlp_exponential_histogram_rejected_data_points_total`, labeled
by `reason`: `unspecified_temporality` or `invalid_data_point`. Attribute and
schema rejections use `invalid_data_point`; other metric families do not
increment this histogram-specific counter. Each rejected point is counted once, including when
resource descriptors are enabled. Minimum, maximum, and exemplars are not persisted.

### Raw-delta exponential histograms

Each stored delta sample retains its interval count, sum, zero count and buckets.
The temporality tag separates delta and cumulative series under the same table
name. Query raw deltas explicitly:

```promql
sum_over_time(request_duration{otlp_aggregation_temporality="delta"}[3m])
sum_over_time(request_duration{otlp_aggregation_temporality="delta"}[3m]) / 180
histogram_count(sum_over_time(request_duration{otlp_aggregation_temporality="delta"}[3m]))
```

Samples are selected by their endpoint timestamp in the existing left-open,
right-closed range. Each selected sample contributes its entire interval,
including intervals crossing the left boundary. A single sample contributes its
full value; an empty window emits no sample. Counts `10,20,15` at seconds
`60,120,180` give count `45` at second `180`, or `0.25` after division by `180`.
Missing intervals do not shorten the divisor. There is no prorating, reset
correction, extrapolation, synthetic zero insertion or inferred coverage.

Native-histogram `rate()` and `increase()` retain counter behavior, regardless
of the tag; they are not supported queries for raw deltas. Converting a histogram
to a float can enter the existing scalar-delta extension, so the explicit
expressions above are the supported examples. Existing sum/division UDFs and
serialization are reused without a new wire format.

Under default storage options, identical table/tags/stored timestamps overwrite
with the last point, without adding buckets. Timestamp alignment can collapse
endpoints at coarser units; interval starts do not form part of the key. Preserve
source order through default resolution and row merging. Existing storage
options continue to apply.

Metrics recovery is point-based across gauges, sums, explicit histograms,
summaries and exponential histograms. Point-local validation precedes
existing-schema validation, which precedes simultaneous detection of fresh
logical/physical conflicts. Reject the union of contributors to all conflicts;
remove every output of a rejected point. Defaults distinguish omitted columns
from explicit nulls. Auto-DDL configuration and hints remain authoritative;
metric-engine timestamp alignment remains permitted with auto-DDL disabled.
Only surviving points contribute metadata, descriptor windows and original-name
mode-cache entries. Metadata describes actual outputs, excluding absent sums
and empty quantile outputs.

Detected request-wide legacy/nonlegacy conflicts still fail the request before
conversion. The existing partial-cache-hit shortcut can skip catalog resolution
for uncached names; survivor filtering does not resolve this limitation.
Authentication, authorization, admission, catalog and primary-write failures
retain their failure paths. Synchronous acceptance waits for writes; eligible
asynchronous batching acknowledges enqueue, and later flush errors retain their
existing metrics. Descriptor failures after primary acceptance remain warnings,
including a partial-success warning with zero rejected points. No write is
retried in smaller subsets after an ambiguous result.

Prometheus compatibility follows raw-delta ingestion with a gauge reset hint
and explicit summation. GreptimeDB retains its temporality label, transmitted
zero threshold, NaN for absent sums, canonical stale representation, integer
bounds, sparse compaction and existing arithmetic. Equivalent distributions
need not have identical layouts or floating-point results. Start timestamps
remain stored but explicit summation does not use them for counter correction.
Public metadata is `unknown` for freshly created delta/mixed-temporality tables;
existing creation-time options are not refreshed by later writes.

Upgrade receiving ingestion frontends before enabling delta senders. Mixed-version
query execution is unverified; this feature introduces no new query UDF or format.

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

Compatibility coverage includes protocol rejection, OTLP scale conversion and
partial-success behavior, kind exclusivity,
stale-marker selector semantics, synthetic-zero rates, incompatible empty
layouts, overflow-safe averages, layout-sensitive equality, infinite custom
midpoints, and exponential overflow indices for schemas `-4`, `0`, and `8`.
Behavioral coverage also verifies frontend PromQL warning and info responses.

This work does not change GreptimeDB's persisted Struct or protobuf
dependencies. It adds the disabled-by-default public OTLP gate; existing
native-histogram data remains readable.

# Future Work

- Native-histogram Remote Read, including exact integer round-trips and streamed
  chunks with start timestamps.
- Raw-delta OTLP exponential-histogram storage and temporality-aware native
  histogram queries, as described above; no delta-to-cumulative conversion.
- Persistent Remote Write metadata and accurate help/unit updates.
- Native-histogram exemplars and exemplar query APIs.
- Start-timestamp overlap annotations.
- Datanode-to-frontend PromQL annotation propagation.
- Versioned two-phase native-histogram aggregation state.
- Exact Prometheus summation compensation if measured precision differences
  justify the extra aggregate state.

## Configuration migration

The `prom_store.experimental_enable_prometheus_native_histogram` and
`otlp.experimental_enable_exponential_histogram` options have been removed.
Remove them from existing configurations; both ingestion paths are always enabled
when their protocol is enabled. Old option values are ignored, including `false`.
