# Metric exemplars

Remote Write v1/v2 and OTLP metric ingestion store exemplars in the ordinary
Mito table `metrics_exemplars` in the source metric's schema. The table uses
append mode and sparse primary keys. Inherited series labels become nullable
string tag columns; exemplar-owned labels are stored in `exemplar_labels`
as JSON2.

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
still fail the request. Remote Write v2 written-count headers report confirmed
writes and exclude dropped exemplars.

`/v1/prometheus/api/v1/query_exemplars` filters by series and time before
collecting observations. If SQL or another writer stored NULL required values
or non-string exemplar label values, the endpoint skips those rows and returns
a `warnings` entry with the skipped count. Valid observations retain their
timestamps, values, labels, and deduplication behavior. Schema, decoding,
storage, memory-limit, and cancellation errors still fail the query.

Selectors use `__name__` for the metric name and inherited series labels such
as `job`. Storage columns such as `greptime_metric` and `greptime_value` are
not exposed as series labels; matchers on them follow missing-label semantics.
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
CREATE TABLE metrics_exemplars (
    greptime_timestamp TIMESTAMP(9) TIME INDEX,
    greptime_metric STRING PRIMARY KEY,
    greptime_value DOUBLE,
    exemplar_labels JSON2
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

## Permissions

When table privileges are enabled, the exemplar endpoint checks access to
`metrics_exemplars` and to source metrics resolved from metric-name equality
matchers. Selectors without a resolved metric name use the permission
checker's unresolved-target policy.

SQL access follows the shared table's privileges: granting `SELECT` on
`metrics_exemplars` grants access to exemplars of every metric stored there.
Source-metric grants do not filter SQL rows in this table. Use separate schemas
and grants when exemplar data requires separate access boundaries; endpoint
checks alone do not provide per-metric isolation for direct SQL access.
