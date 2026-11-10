# Metric exemplars

Remote Write v1/v2 and OTLP metric ingestion store exemplars in the ordinary
Mito table `greptime_metrics_exemplars` in the source metric's schema. The table uses
append mode and sparse primary keys. Inherited series labels become nullable
string tag columns; exemplar-owned labels are stored in `exemplar_labels`
as JSON2.

Each exemplar keeps its own observation timestamp. Remote Write v1/v2 supply
milliseconds, which are converted to nanoseconds; OTLP supplies nanoseconds
directly. These timestamps are independent of the associated metric sample's
timestamp. The query API exposes millisecond precision while SQL exposes the
stored precision.

Each row also stores `__table_id` (`UINT32 NOT NULL`) and `__tsid`
(`UINT64 NOT NULL`) as the first two primary-key columns. They identify the
source logical metric table and its series. Their column IDs use the metric
engine's physical-region reserved IDs: `0x80000004` and `0x80000003`, respectively.
The TSID uses the metric engine's hash of sorted source labels, including empty
values and excluding exemplar-owned labels. Metric names and inherited labels
remain stored because a TSID cannot reconstruct them.

Exemplar ingestion requires metric-engine ingestion and a logical metric-engine
source table. Accompanying samples may create that source through normal metric
ingestion. Exemplar-only writes require an existing source; Remote Write rejects
missing tables and other engines. OTLP skips and counts exemplars for sources
outside the metric engine while retaining valid metric rows. Metric and exemplar
writes remain non-atomic, so one may succeed while the other fails. When preparing
a combined write, both use the same captured source table identity even if its
name is later reused.

OTel Arrow currently uses ordinary-engine ingestion, so it skips exemplars
while writing valid metric rows successfully.

## Validation and query results

Invalid exemplar-owned labels, out-of-range exemplar timestamps, and invalid
OTLP exemplar IDs or missing values cause that exemplar to be dropped. OTLP
histogram exemplars that cannot be assigned to an emitted bucket are also
dropped. Valid samples and sibling exemplars are retained. The
`greptime_servers_exemplars_dropped_total` counter records these drops, with
`protocol` values `prometheus_v1`, `prometheus_v2`, and `otlp`.

The combined names and values of exemplar-owned labels are limited to 128
Unicode characters for all three protocols. OTLP's dedicated `trace_id` and
`span_id` contribute 63 characters when both are present, leaving 65 for
other labels. Source-series validation, malformed request encoding, invalid
Remote Write v2 symbol references, permissions, admission, and storage errors
still fail the request. The Remote Write v2 exemplar written-count header
reports confirmed writes and excludes dropped exemplars.

`/v1/prometheus/api/v1/query_exemplars` filters by series and time before
collecting observations. It extracts selectors from the PromQL expression;
operators and functions are not evaluated. For example, `rate(requests[5m])`
selects `requests` exemplars within the explicit `start` and `end` bounds.

If SQL or another writer stored NULL required values or non-string exemplar
label values, the endpoint skips those rows and returns a `warnings` entry with
the skipped count. Valid observations retain their values, labels, and
deduplication behavior. Schema, decoding, storage, memory-limit, and cancellation
errors still fail the query.

Query `start` and `end` accept numeric Unix seconds or RFC3339 timestamps using
the existing PromQL timestamp handling. Numeric bounds round to milliseconds;
RFC3339 bounds floor to milliseconds, including before the Unix epoch. The
normalized `start` must not exceed `end`, and bounds outside the signed
nanosecond storage range are rejected. Both bounds are inclusive: `start=end`
selects the entire named millisecond, including stored OTLP observations within
that millisecond.

Response timestamps are numeric Unix seconds floored to millisecond precision.
Storage, ordering, and exact deduplication retain the stored timestamp precision,
so distinct observations may have the same response timestamp. Use SQL to read
the stored precision. Automatically created exemplar tables use nanoseconds;
compatible pre-created tables retain their configured timestamp unit.

Selectors use `__name__` for the metric name and inherited series labels such
as `job`. Storage columns such as `__table_id`, `__tsid`, `greptime_metric`,
and `greptime_value` are not exposed as series labels; matchers on them follow
missing-label semantics.
For example, `{__name__="requests", greptime_metric=""}` matches `requests`,
while `{greptime_metric="requests"}` matches nothing. Exemplar-owned labels
also remain separate from the series-label selector namespace.

## Capacity and retention

Automatic creation starts with one region per schema. The table accumulates
the union of inherited label names across the schema's metrics; new names
require schema alterations. Automatic creation does not configure inverted
indexes or partitioning. Size the table for the combined exemplar workload
and configure retention explicitly.

For distributed deployments, a compatible table can be created before the
first exemplar write. For example:

```sql
CREATE TABLE greptime_metrics_exemplars (
    __table_id INT UNSIGNED NOT NULL,
    __tsid BIGINT UNSIGNED NOT NULL,
    greptime_timestamp TIMESTAMP(9) TIME INDEX,
    greptime_metric STRING,
    greptime_value DOUBLE,
    exemplar_labels JSON2,
    PRIMARY KEY (__table_id, __tsid, greptime_metric)
)
PARTITION ON COLUMNS (greptime_metric) (
    greptime_metric < 'm',
    greptime_metric >= 'm'
)
WITH (
    'append_mode' = 'true',
    'primary_key_encoding' = 'sparse',
    'ttl' = '7d'
);
```

Choose boundaries and retention for the actual workload. This example does
not balance metric names by hash, and a single busy metric remains in one
partition. Pre-created inherited label columns must be nullable string tags
without non-NULL defaults, so omitted source labels remain absent. Additional
inherited labels can still be added automatically. Writes to pre-created tables
use the table's timestamp precision, including when automatic creation is disabled.

Pre-created tables must use the identity types and primary-key prefix shown
above; creation assigns the reserved column IDs. Older exemplar table schemas
without that prefix are rejected. Ingestion does not rewrite or migrate their
existing data.

## Permissions

When table privileges are enabled, the exemplar endpoint checks access to
`greptime_metrics_exemplars` and to source metrics resolved from metric-name equality
matchers. Selectors without a resolved metric name use the permission
checker's unresolved-target policy.

SQL access follows the shared table's privileges: granting `SELECT` on
`greptime_metrics_exemplars` grants access to exemplars of every metric stored there.
Source-metric grants do not filter SQL rows in this table. Use separate schemas
and grants when exemplar data requires separate access boundaries; endpoint
checks alone do not provide per-metric isolation for direct SQL access.
