---
Feature Name: Entity Relationships and Graph Query
Tracking Issue: TBD
Date: 2026-06-25
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Give GreptimeDB a way to model, derive, and query the **relationships between
the entities** that telemetry describes — the service that calls another
service, the process that runs on a host, the pod that belongs to a node — and
to traverse that graph to pull back the metrics, logs, and traces attached to
each entity.

This is the follow-up RFC promised in the Future Work of
[`2026-05-28-table-semantic-layer.md`](2026-05-28-table-semantic-layer.md). The
semantic layer gave every table a *per-table identity*; this RFC adds the
*edges between tables and the entities inside them*.

Two load-bearing decisions:

1. **GreptimeDB does not become a graph database.** Entities and relationships
   are exposed as tables; the graph is a *logical view* over data already
   stored, queried with SQL (later with the ISO SQL/PGQ `GRAPH_TABLE`/`MATCH`
   operator). No separate graph store, no second copy of the data.
2. **The graph is derived at read time.** `greptime_private.semantic_entities`
   and `greptime_private.semantic_relationships` are computed tables: scanning
   them derives rows from the declaring telemetry tables on the fly. Nothing
   has to be materialised for the graph to work, and a newly-appeared entity is
   visible the instant its first row lands.

# Motivation

The driving use case is **incident localisation**. An alert fires; an
LLM-assisted agent can write a genuinely useful root-cause analysis *if and
only if* it is handed the right context:

1. The **alert entity** — a set of labels (e.g. `instance="api-server-1",
   service="users"`) — and the telemetry attached to it.
2. The **upstream/downstream entities** it depends on or is depended on by (in
   microservice architectures the call chain is empirically the highest-signal
   context), and *their* telemetry.
3. Correlated events: deploys, config changes, and increasingly the **agent
   events** of AI agents acting on the system.

GreptimeDB already stores the raw data — and stores it **all-in-one**: metrics,
logs, traces, and agent events land in a single database, queried by one
engine. What it lacks is the link layer: the metadata is scattered per signal,
and there is no way to query it as a graph. Today an agent must guess the
topology from column names and join by hand.

All-in-one is the structural lever. In split stacks an entity catalog sits
*above* the stores and stitches results in application code; here, correlation
is a join inside one query engine, and the entity graph is a view over the
same tables. The audience is broader than one product — alert routing,
dashboards, impact analysis, MCP servers all need *"given this entity, what is
related to it, and how do I get its telemetry?"* — and OpenTelemetry settles
entity identity but explicitly defers entity relationships and the read-side
catalog to backends: unclaimed territory.

# Goals

1. Declare, per table, **which entities a table describes** and **which columns
   form each entity's identity**, reusing the existing `greptime.semantic.*`
   surface — no new DDL keyword.
2. Represent **relationships between entities** as first-class, time-ranged
   facts with a small typed vocabulary and an explicit **provenance**
   (declared vs. derived-from-trace vs. derived-from-attribute vs.
   agent-inferred).
3. **Derive** the high-value edges automatically at read time: the service call
   graph from trace tables, and host/containment edges from identities
   co-declared on the same rows.
4. **Query** the graph with bounded-hop traversal and a standard SQL surface,
   so an agent can ask "what is related to this entity" in one statement and
   join straight back to the telemetry tables.
5. Correlate **metrics, logs, traces, and agent events** across the shared join
   keys (entity, trace context, business key, time), exploiting the all-in-one
   store so correlation is a single-engine join.
6. Keep every layer **optional and additive**. Tables without entity
   declarations keep working; the computed tables stream empty until something
   declares an entity.

# Non-Goals

- **Becoming a graph database.** No native graph storage, no graph-tuned
  on-disk format.
- **A general-purpose property-graph OLTP engine.** We optimise for
  observability topology: bounded-hop traversal over a relationship set that is
  small relative to the raw telemetry.
- **Reconciling a canonical CMDB.** Identity is the OTel "minimally sufficient
  id" derived from labels; conflicts follow the same best-effort rule as the
  semantic layer.
- **A new relationship wire protocol.** Relationships are derived or declared
  locally; upstream standardisation is a future direction.
- **The diagnosis agent / RCA logic itself.** This RFC makes the substrate
  *queryable*; agents live in the layer above. Agent events are *stored and
  correlated* here, not *produced* here.

# Background

The design distils a survey of OpenTelemetry and the entity/topology models of
Datadog, New Relic, Backstage, ServiceNow CSDM, Grafana Asserts/Tempo, and
eBPF tools (Hubble/Pixie), plus SQL/PGQ implementations (DuckPGQ, Spanner
Graph). The facts that shaped it:

- **OTel settles entity identity, not relationships.** An entity is
  `(type, identifying attributes, descriptive attributes)` under the
  *Minimally Sufficient Id* rule (a service is `service.name`, disambiguated
  by `service.namespace` when set; a process is `(pid, start_time)`).
  Relationships are explicitly deferred; the de-facto mechanism today is join
  by shared identifying attribute — which maps cleanly onto a relational model.
- **No vendor treats derived topology as permanent** — edges TTL out (New
  Relic ~75 min) and span pairing runs in bounded windows; for a TSDB the
  natural form is edges as time-ranged facts.
- **The service graph is span pairing** (`client`/`server` on
  `trace_id`+`parent_span_id`, RED aggregation — the OTel Collector
  `servicegraph` connector). Pairing needs trace-locality; a database holding
  the whole trace table gets it for free as a self-join.
- **SQL/PGQ adds graph query without a graph store**: property graphs are
  view-like objects over existing tables, `MATCH` compiles to joins; DuckPGQ
  proves the model inside an analytical columnar engine. Apache AGE (own
  per-label storage) is the model this RFC rejects.

| Decision | Borrowed from |
| --- | --- |
| Entity = `(type, identifying attrs)`, identity ≠ description | OTel Entity model |
| Join keys from shared identifying attributes; edge semantics from co-declaration | OTel de-facto; relational FK |
| Small typed, inverse-paired edge vocabulary | Backstage / New Relic / ServiceNow |
| Single `calls` edge qualified by attributes (`connection_type`, RED) | Tempo / servicegraph |
| **Provenance** on every edge; declared edges survive derivation | Datadog |
| Edges as **time-ranged facts with TTL** | New Relic / Datadog / TSDB-native |
| Call edges by span pairing (self-join on `trace_id`) | Tempo / servicegraph |
| Relationships as a first-class queryable relation, bounded-hop contract | New Relic / Netflix |
| Graph as a logical view, `GRAPH_TABLE`/`MATCH`, transient CSR | SQL/PGQ / DuckPGQ |

# Design

## One graph at two resolutions

Metrics, logs, traces, and agent events are four shapes of the same thing — *an
observation, at a time, about an entity, optionally within a causal context* —
joined by a small set of shared keys:

| Join key | Question | Strength | Carried by |
| --- | --- | --- | --- |
| entity identity | who / where | coarse, always present | all signals |
| `trace_id` / `span_id` | why / request flow | exact, request-scoped | spans; logs may carry; metrics via exemplar |
| business key (`session.id`, order/user id) | which instance | exact, cross-cutting | instrumentation-dependent |
| `ts` | when | weak, fallback | all signals |

The same observations form one graph at two resolutions:

- **Coarse — the entity graph**: services, hosts, pods and their dependencies.
  Slow-moving and small; this is what the computed tables expose. The
  entity-level `calls` edge is the aggregation of per-request span pairs under
  the entity-identity map.
- **Fine — the trace causality graph**: spans, the logs emitted inside them,
  the exemplars that sampled them. Fast-moving and huge — one graph per
  request. These links are 100–1000× the volume of entity edges and are
  **never turned into graph rows**; they stay a query pattern over
  already-indexed join keys (`trace_id`/`span_id`/time) within a bounded
  window.

Agent events need no new signal type: they ride the existing `trace`/`event`
signals with GenAI semantic conventions and join the graph through ordinary
entity declarations (`agent`, `session`, `model`, `tool`). Agent-*asserted*
edges carry `provenance = 'agent'` and `confidence < 1.0` — an LLM-inferred
edge must stay visibly distinct from observed structure on an RCA path.

## Overview: four layers

```
                          ┌─────────────────────────────────────────────┐
  Query (Layer 4)         │  SQL joins / WITH RECURSIVE today;           │
                          │  GRAPH_TABLE ( og MATCH ... ) later          │
                          └───────────────┬─────────────────────────────┘
                                          │ reads
  Exposure (Layer 2)      ┌───────────────▼────────────────────┐
                          │  greptime_private.semantic_entities │  computed,
                          │  greptime_private.semantic_relation-│  read-only
                          │  ships                              │  tables
                          └───────▲────────────────────────────┘
                                  │ derives at read time
  Derivation (Layer 3)    ┌───────┴────────────────────────────┐
                          │  typed DataFusion plans:            │
                          │  registry DISTINCT, trace self-join │
                          └───────▲────────────────────────────┘
                                  │ reads declarations + telemetry
  Declaration (Layer 1)   ┌───────┴────────────────────────────┐
                          │  greptime.semantic.entity.* table   │
                          │  options on the telemetry tables    │
                          └────────────────────────────────────┘
```

Each layer is independently useful. Layer 1 alone makes entity identity
machine-discoverable. Layers 1–3 give a queryable graph with plain SQL. Layer 4
is ergonomics over a query the table model already supports.

## Layer 1 — Entity identity declaration (extends the semantic layer)

A table declares the entities it describes and the columns that identify each,
using new keys under the existing `greptime.semantic.` namespace. The values
are column names; **`id` columns must be tag/primary-key columns** (so identity
stays indexable and joinable — identity buried in a JSON attribute bag must be
projected to a column first via a pipeline).

```
greptime.semantic.entity.<entity_type>.id          = comma-separated column names
greptime.semantic.entity.<entity_type>.descriptive = comma-separated column names   (optional)
greptime.semantic.entity.<entity_type>.scope       = comma-separated column names   (optional)
```

**The column name is the identifying attribute key.** OTel identity is a set of
`attribute key → value` pairs; tables ingested from OTLP carry those attributes
as flattened columns whose names are the attribute keys, so the declaration
lists columns and the names double as the semantic keys. Two tables share an
entity's identity by naming the identifying columns consistently. An explicit
`semantic_key=column` mapping form is reserved as a backwards-compatible
extension for tables whose column names diverge from the attribute keys.

**`scope` is not part of identity.** An OTel entity id is an attribute map;
there is no generic scope dimension. If a namespace-like attribute
disambiguates identity for your tables, declare its column in `id` — identity
uniqueness then holds by construction. The `scope` role only surfaces a
namespace/environment value as a convenient filter/display column.

Example — a latency metric table that describes both a `service` and the
`host` it runs on:

```sql
CREATE TABLE app_request_latency (
  ts            TIMESTAMP(3) TIME INDEX,
  service_name  STRING,
  host_id       STRING,
  le            STRING,
  value         DOUBLE,
  PRIMARY KEY (service_name, host_id, le)
) WITH (
  'greptime.semantic.signal_type'           = 'metric',
  'greptime.semantic.entity.service.id'     = 'service_name',
  'greptime.semantic.entity.host.id'        = 'host_id'
);
```

This is the relational image of OTel's `EntityRef`: `entity.service.id` names
the identifying attributes of the `service` entity as columns. A table carrying
several entity types mirrors a resource referencing several entities. The same
mechanism declares agent-telemetry entities (`entity.agent.id`,
`entity.session.id`, ...), so agent events join the graph with no new surface.

Declarations surface in `information_schema.table_semantics`.

### Zero-configuration declarations (well-known conventions)

Explicit declarations are the mechanism; built-in conventions make the graph
light up without them. An explicit declaration always overrides an implicit
one, so non-standard setups (custom relabeling, renamed columns) are covered
by declaring explicitly.

**OTLP traces.** For `greptime_trace_v1` tables the identity column is fixed:
the ingestion path stamps `greptime.semantic.entity.service.id = service_name`
at table creation, and the derivation synthesizes the same declaration for
trace-v1 tables that predate stamping.

**Prometheus metrics on Kubernetes.** A metric table
(`signal_type = metric`, `source = prometheus`) whose tag columns match the
well-known label sets receives implicit declarations:

| Tag columns | Entity | Basis |
| --- | --- | --- |
| `job` | `service` | The OTel↔Prometheus compatibility spec: `service.namespace`/`service.name` MUST be combined into the `job` label |
| `job`, `instance` | `service.instance` | Same spec: `service.instance.id` MUST be converted to the `instance` label |
| `namespace`, `pod` | `k8s.pod` | Kubernetes service-discovery / kube-prometheus-stack relabeling convention |
| `node` | `k8s.node` | same |
| `namespace` | `k8s.namespace` | same |

This is what connects the graph to metrics — with one caveat the
compatibility spec itself creates: `job` is `<service.namespace>/<service.name>`
when a namespace is set, while a trace table's default identity is the bare
`service_name`. The metric- and trace-derived `service` nodes therefore unify
automatically only when `service.namespace` is empty (so `job` equals
`service.name`) and `job` was not relabeled; otherwise alignment needs
pipeline normalization or explicit declarations naming identity columns that
carry the same values. When the ids do align, the same entity appears from
both signals (each row keeping its own `source_tables` lineage), and "the
neighbours' telemetry" reaches metric tables without any user declaration.

`*_info` metrics are entity descriptors, and their rows witness edges under
the same-row co-declaration rule (3b): `kube_pod_info` carries `pod`,
`namespace`, and `node` in one row — deriving `k8s.pod runs_on k8s.node` plus
descriptive attributes (`host_ip`, `pod_ip`, `created_by_*`); `kube_pod_owner`
carries the pod and its `owner_kind`/`owner_name` — `part_of` edges to
workloads. `target_info`, the OTLP-over-Prometheus resource metric (labelled
by `job` and `instance` plus the remaining resource attributes, per the same
compatibility spec), enriches the `service` entity's descriptive attributes.

**Prometheus Remote Write 2.0 metadata.** RW 2.0 carries metric type, help,
and unit inline with each series (the metadata sub-fields SHOULD be provided),
where 1.x had no reliable in-protocol metadata (RW 2.0 support is tracked in
[#4765](https://github.com/GreptimeTeam/greptimedb/issues/4765)). Feeding them
into the semantic vocabulary — `metric.type`, `metric.unit`,
`metadata_quality = declared` — makes the graph's metric-side descriptive
metadata trustworthy instead of suffix-guessed; this intake is an M1 work
item, alongside the implicit declarations above.

**Vocabulary mechanism.** The semantic vocabulary is a closed whitelist, but
entity types are open-ended (`service`, `host`, `k8s.pod`, custom), so the
`entity.*` sub-namespace is validated by **prefix + shape**
(`greptime.semantic.entity.<type>.{id|descriptive|scope}`, `<type>` in the OTel
entity-type charset) instead of membership. Everything else keeps the
"unknown key is rejected" guarantee. Column existence and the id-must-be-tag
rule are enforced against the table schema at DDL time.

## Layer 2 — The computed graph tables

The graph is exposed as two **computed, read-only tables** under
`greptime_private` (not `information_schema`: scanning them triggers real
derivation over telemetry tables, which would break the cheap-metadata
expectation of `information_schema`). All DDL/DML against them is rejected on
every write path.

`semantic_entities` — the node set. One row per **distinct projected entity
observation from a contributing table**: an entity declared by three tables
yields at least three rows per window, more when its scope or descriptive
snapshot varies within the window. `source_tables` is that row's lineage,
rendered as the schema-qualified `"schema.table"` string. This keeps the
derivation free of cross-source merge policies (descriptive conflicts, JSON
merging); consumers deduplicate with `SELECT DISTINCT entity_type, entity_id`,
and a unique node set is the job of the snapshot relation (Layer 4).

| Column | Type | Meaning |
| --- | --- | --- |
| `observed_at` | TIMESTAMP(3) | time bucket (TIME INDEX) |
| `window_start` / `window_end` | TIMESTAMP(3) | observation window |
| `fresh_until` | TIMESTAMP(3) | presence horizon (= `window_end` for derived rows) |
| `entity_type` | STRING | `service`, `host`, `k8s.pod`, ... |
| `entity_id` | STRING | canonical id (see encoding below) |
| `entity_id_attrs` | JSON | identifying attributes; source of truth for composite ids; NULL for single-attribute ids |
| `scope` | STRING | namespace/environment; `''` if none |
| `descriptive` | JSON | snapshot of descriptive attributes |
| `source_tables` | JSON | lineage: contributing tables |

`semantic_relationships` — the edge set, one row per **(window, edge)**: the
span pairs of all trace tables are unioned before aggregation, so an edge
observed across several trace tables still yields a single row with merged RED
metrics.

| Column | Type | Meaning |
| --- | --- | --- |
| `observed_at`, `window_start`, `window_end`, `fresh_until` | TIMESTAMP(3) | as above |
| `src_type`, `src_id`, `dst_type`, `dst_id` | STRING | endpoints |
| `rel_type` | STRING | vocabulary below |
| `provenance` | STRING | `trace` \| `attribute` \| `declared` \| `agent` |
| `confidence` | DOUBLE | derivation certainty: 1.0 = paired/declared; < 1.0 for virtual-node / agent-inferred |
| `generation_id` | STRING | idempotency key of the producing (window, run); empty for read-time rows |
| `request_count`, `error_count`, `duration_sum`, `duration_count` | BIGINT/DOUBLE | RED metrics for `calls` edges |
| `attributes` | JSON | `connection_type`, `db.system`, `peer.service`, ... |

Design notes:

- **Edges are time-ranged facts.** A row asserts an edge *existed in a
  window*, with RED metrics for that window — the TSDB-native answer to "edges
  expire". "The topology now" is `WHERE fresh_until >= now() - INTERVAL '5m'`.
- **Endpoint encoding (v1 storage-level identity key).** For a
  single-attribute identity, `entity_id` is the value verbatim; for a composite
  identity it is the attributes sorted by key, rendered `k1=v1,k2=v2` — a
  human-readable traversal key, not a collision-free canonical encoding.
  `entity_id_attrs` (JSON) is the structured form of a composite identity on
  nodes, but edges carry only the string key. Known v1 limitation: composite
  components containing unescaped `,` or `=` are not guaranteed to be
  distinguished.
- **`provenance` is part of edge identity**, so a declared edge and a derived
  edge between the same pair coexist, and a user-declared edge survives when
  derivation is off.
- **`confidence` expresses derivation certainty, not statistical
  completeness**: `1.0` for a successfully paired or declared edge, `< 1.0` for
  virtual-node or agent-inferred edges. It does not correct for trace sampling.
- **RED metrics describe the observed span-pair population** — the
  servicegraph-connector semantics (`traces_service_graph_request_total`).
  Under sampling, counts understate true traffic, and ratios (error rate, mean
  duration) are representative only when sampling is unbiased with respect to
  status and latency — tail sampling that keeps errors and slow traces skews
  both.
- **Hand-declared edges** (`provenance = 'declared'`) are stored in a physical
  table of the same shape (plus a business validity window) in
  `greptime_private` and unioned into the computed `semantic_relationships`.

**Relationship vocabulary.** Small, typed, inverse-paired; stored direction is
`src -> dst`, the inverse is a query concern:

| `rel_type` | Meaning (src → dst) | Primary provenance | Inverse (query) |
| --- | --- | --- | --- |
| `calls` | `service` calls `service` | trace | `called_by` |
| `runs_on` | `service.instance`/`process` runs on `host`/`node` | attribute | `hosts` |
| `contains` | `node`→`pod`, `pod`→`container` | attribute/declared | `part_of` |
| `part_of` | `service.instance` is part of `service` | attribute/declared | `has_instance` |
| `depends_on` | logical/declared dependency | declared | `dependency_of` |
| `owns` | team/service owns dst | declared | `owned_by` |

`calls` lives at the *logical* `service` layer; `runs_on`/`contains` live at
the *runtime* `service.instance`/`process`/`k8s.pod` layer, with `part_of`
linking the two — this avoids "one logical service runs_on five hosts".
A custom `rel_type` is just a string; only derivation rules and inverse names
are built-in. The trace-derived `calls` edge is qualified by `attributes`
(`connection_type` ∈ {`database`, `messaging`, `virtual_node`}, ...) rather
than exploded into many edge types.

## Layer 3 — Read-time derivation

Scanning a computed table enumerates the entity declarations (from table
options), builds a derivation plan per source, and executes it. The plans are
**typed DataFusion `Expr`/`DataFrame` plans, not SQL text** — user-controlled
identifiers are plain values (no quoting or injection surface), and the window
predicates push down into the source table scans. A PoC over 90k–1.8M rows of
real telemetry validated that all of this runs comfortably as read-time
queries (see *PoC Validation*).

### Derivation contract

Normative for the read-time graph:

- **Authorization.** Derivation enumerates, plans, and executes as the caller,
  not as an internal superuser. Every source table is authorized per table; a
  source the caller cannot read is silently excluded from the derivation (its
  entities, descriptive attributes, and `source_tables` entries never appear),
  and a join-derived edge requires read access to all of its input tables.
  Querying the computed tables must never widen access to the underlying
  telemetry.
- **Time window.** The derived source-scan window is **never narrower than
  the query's `observed_at` range** — the outer filter can only discard extra
  buckets, never recover unscanned ones. Rules: no `observed_at` predicate →
  the product default `[now - 1h, now)`; a lower bound only → `[lower, now)`;
  both bounds → the full requested range (half-open `[start, end)`); an upper
  bound only, or `observed_at` inside a predicate the planner cannot safely
  extract (e.g. under `OR`) → an error asking for an explicit range, never a
  silent fallback. Disjuncts not involving `observed_at` do not affect the
  extracted range. Bucket mapping expands outward (`floor` the lower bound,
  `ceil` the upper to bucket boundaries); `now()` is evaluated once per plan.
- **Bounded work and cancellation.** The window bound is the resource bound: a
  bare `SELECT * FROM semantic_entities` scans at most the last hour of the
  declaring tables. Derivation inherits the caller's cancellation and deadline.
  No quota or spill machinery is introduced; deployments where read-time
  derivation over huge trace tables is too expensive are the materialisation
  case (Future Work).

### Upgrades and schema evolution

- Trace-v1 tables created before the ingest-side auto-stamp carry no
  `entity.service.id` option; the derivation synthesizes the implicit `service`
  declaration for them, so their `calls` edges have endpoint entities without
  touching historical catalog metadata.
- A declaration referencing a column the table no longer has (dropped after
  declaring) is skipped with a warning instead of failing every graph scan.
- The computed table names are reserved on every path that could take them:
  CREATE, and ALTER ... RENAME targets.

### 3a. Trace → `calls` (the service graph)

A self-join of each `greptime_trace_v1` table (recognised by its
`table_data_model` option): pair each client span with its child server span,
project to the service identity, aggregate RED metrics per 60s window — the
SQL form of the Tempo servicegraph connector. The defining query, shown in its
simplified form for the default single-column `service_name` declaration (in
general every `service_name` reference below stands for the
declaration-derived id expression):

```sql
SELECT
  date_bin('60s', client."timestamp")               AS observed_at,
  'service'                                          AS src_type,
  client.service_name                                AS src_id,
  'service'                                          AS dst_type,
  server.service_name                                AS dst_id,
  'calls'                                            AS rel_type,
  'trace'                                            AS provenance,
  count(*)                                           AS request_count,
  count(*) FILTER (WHERE server.span_status_code = 'STATUS_CODE_ERROR')
                                                     AS error_count,
  sum(server.duration_nano) / 1e9                    AS duration_sum,
  count(*)                                           AS duration_count
FROM   otel_traces AS client
JOIN   otel_traces AS server
  ON   client.trace_id = server.trace_id
  AND  server.parent_span_id = client.span_id
WHERE  client.span_kind = 'SPAN_KIND_CLIENT'
  AND  server.span_kind = 'SPAN_KIND_SERVER'
  AND  client.service_name <> server.service_name
GROUP BY 1, src_id, dst_id;
```

Edge endpoints are built from each trace table's **`service` entity
declaration** — the same id expression the registry uses — so edges land on
exactly the entity ids the registry emits, including composite service
identities; span pairs with a NULL identity component are filtered, and
self-calls are excluded by comparing the full endpoint ids (two services with
equal names but different composite identities are not a self-call). A trace
table whose explicit service declaration is unusable contributes no calls
edges rather than silently falling back to another identity.

The implementation additionally bounds the join with a time-proximity window
(a child server span starts within `[-5min, +1h]` of its client span) and
applies the equivalent static bounds to both scans so file/partition pruning
works. The span pairs of **all** trace tables are unioned before the
aggregation, so one edge gets one row per window regardless of how many tables
observed it. Trace-locality is free: the whole trace table is in the database,
so pairing is a self-join, never a cross-instance buffer.

**Virtual nodes** (uninstrumented peers — a client span with no matching
server span becomes an edge to a synthetic node named from
`peer.service`/`db.name`/`server.address`) are a second, anti-join branch,
unioned in.

### 3b. Attribute → `runs_on` / `contains`

**Shared attributes provide join keys; co-declaration or a relationship
template provides the relationship semantics.** A shared value alone does not
determine direction, edge type, or cardinality, so the derivation is
rule-based, not join-everything:

- **Same-row co-declaration**: a table declaring both a `service.instance` and
  a `host` identity on the same rows derives `runs_on` between them — the row
  itself witnesses the relationship, and the built-in vocabulary fixes the
  direction.
- **Cross-table edges** require a declared relationship template (endpoint
  mappings, `rel_type`, direction) — a derivation *rule* declared on schema,
  which is still schema-level configuration, not per-instance edge data.

### 3c. Entity registry

Per declaring table, a filter(window) → project(bin, casts, JSON assembly) →
`DISTINCT` plan; the per-table results are unioned without a cross-source
merge, giving the per-observation row contract above. Rows whose identity columns are NULL identify nothing and are filtered
out. Single-column ids are used verbatim; composite ids render the sorted
`k=v` form plus the `entity_id_attrs` JSON.

### 3d. Agent edges

Agent-telemetry tables that declare `agent`/`session`/`model`/`tool` entities
feed the registry like any other table; span structure derives
`agent uses model` / `agent invoked tool` / `parent_agent calls agent` edges.
Agents may also insert `provenance = 'agent'` edges with `confidence < 1.0`
through the declared-edge table; provenance-in-identity keeps them from ever
clobbering observed edges.

## Layer 4 — Query

The computed tables are ordinary relations: plain SQL joins back to telemetry
tables, and `WITH RECURSIVE` serves multi-hop traversal (validated to depth 10
in the PoC). Bounded-hop traversal over a relationship set that is small
relative to raw telemetry is the performance contract.

The ISO SQL/PGQ surface is ergonomics over the same data. SQL/PGQ requires the
vertex key to uniquely identify a vertex, and the fact tables have one row
per window and, for entities, per source observation — so the property graph is
defined over a **snapshot relation**, not the raw fact tables:
`semantic_graph_snapshot(start, end)` aggregates the queried range into one
row per entity (vertex key `(entity_type, entity_id)`, `source_tables`
merged) and one row per edge (RED metrics merged), and guarantees every edge
endpoint exists in the vertex set — an endpoint with no telemetry-derived
entity row (a virtual node, a declared or agent edge) is synthesized as an
endpoint-only vertex with NULL descriptive and empty `source_tables`.

```sql
CREATE PROPERTY GRAPH observability
  VERTEX TABLES (
    snapshot_entities
      KEY (entity_type, entity_id)
      LABEL entity PROPERTIES (entity_type, entity_id, scope, descriptive)
  )
  EDGE TABLES (
    snapshot_relationships
      KEY (src_type, src_id, rel_type, dst_type, dst_id, provenance)
      SOURCE      KEY (src_type, src_id) REFERENCES snapshot_entities (entity_type, entity_id)
      DESTINATION KEY (dst_type, dst_id) REFERENCES snapshot_entities (entity_type, entity_id)
      LABEL related PROPERTIES (rel_type, provenance, confidence, request_count, error_count, attributes)
  );

SELECT other_type, other_id, rel_type
FROM GRAPH_TABLE (observability
  MATCH (e IS entity WHERE e.entity_id = 'users')
        -[r IS related]-
        (o IS entity)
  COLUMNS (o.entity_type AS other_type, o.entity_id AS other_id, r.rel_type)
);
```

`MATCH` shares the GPML pattern core with Cypher and GQL, so users get
Cypher-like ergonomics inside standard SQL. Execution: fixed-length patterns
rewrite to joins (no new physical operator); bounded variable-length becomes a
bounded union of join chains; unbounded/shortest-path runs over a transient
in-memory CSR built from the relationship rows at query time (the DuckPGQ
approach).

The physical declared-edge table's layout serves the two adjacency lookups
directly: a primary key starting `(src_type, src_id, ...)` makes out-edge
lookup a key-prefix scan; an inverted index on `(dst_type, dst_id)` serves
in-edge ("who depends on this") lookup; the time index prunes to the topology
window.

# Worked Example: diagnosing a slow-query alert

A GreptimeDB self-monitoring scenario: a slow-query alert fires, the agent
reads the alert entity (a `frontend` instance from the alert labels), then:

1. **Frontend → its dependencies** (datanode, metasrv) via the graph — the
   step that was impossible before:

   ```sql
   SELECT dst_type, dst_id, request_count, error_count
   FROM greptime_private.semantic_relationships
   WHERE src_type = 'service' AND src_id = 'frontend'
     AND rel_type = 'calls'
     AND observed_at >= now() - INTERVAL '15m';
   ```

   These `calls` edges are derived from GreptimeDB's own internal traces; the
   topology is not hand-maintained.
2. **Dependencies → their telemetry**: join the neighbour set back to the
   datanode/metasrv metric tables.
3. **Descend to evidence**: from the suspect edge, fetch the slowest witnessing
   server spans in the window, then the logs emitted inside those spans by
   `(trace_id, span_id)` — exact, request-level correlation in one query.

The agent hands the LLM not a topology hint but the specific slow requests and
the log lines inside them — the difference from a root cause.

# PoC Validation

Validated against live GreptimeDB instances holding agent events (~90k rows),
Prometheus self-monitoring metrics (451 tables), and real OTel traces (1.8M
spans of coding-agent telemetry):

- Entity discovery is a read-time `DISTINCT`/`GROUP BY` (edge aggregate over
  ~54k rows: ~31 ms); span pairing produced a real call graph, and
  `WITH RECURSIVE` walked a real trace tree to depth 10 (~300 nodes).
- Cross-signal correlation is a join on shared keys (`trace_id`,
  `gen_ai.request.model`); agent events land as ordinary trace-signal tables —
  no new signal type needed.
- GreptimeDB already auto-derives `opentelemetry_traces_services` /
  `_operations` — a deduplicated distinct-entity table, the precedent
  `semantic_entities` generalises.

Not yet validated: the `calls` self-join cost on very large trace tables under
tight memory, and the `GRAPH_TABLE` surface (deferred).

# Implementation Plan

**M0 — Declaration.** The `entity.*` option sub-namespace with prefix+shape
validation and the id-columns-must-be-tags DDL check; auto-stamp of
`entity.service.id` on the OTLP trace path; declarations visible in
`information_schema.table_semantics`.

**M1 — The read-time graph.** The computed tables in `greptime_private`,
read-only, derived via typed DataFusion plans with JSON-typed output columns:
the entity registry and the trace-derived `calls` edge first, then the
Prometheus/Kubernetes implicit declarations and Remote Write 2.0 metadata
intake, virtual nodes, attribute-derived edges, agent edges, the declared-edge
table and its union, and the derivation contract (caller authorization,
explicit time windows). Queryable with plain SQL and `WITH RECURSIVE`
throughout.

**M2 — Query ergonomics.** The snapshot relation, `CREATE PROPERTY GRAPH` +
`GRAPH_TABLE`/`MATCH` (single-hop, then bounded variable-length) compiled to
join/recursive rewrites, and an MCP tool surface so agents consume the graph
directly.

# Alternatives

- **A real graph database / Apache-AGE-style native store.** Doubles the
  storage path and duplicates data that already lives in the telemetry tables.
  Rejected: the goal is a graph *query*, not a graph database.
- **Cypher as a bolt-on language.** Heavier than SQL/PGQ and pulls toward the
  graph-store model; SQL/PGQ delivers the same `MATCH` pattern core inside the
  SQL surface and planner GreptimeDB already has.
- **Edges only derived, never declared.** Loses static topology that has no
  trace (ownership, declared dependencies) and the user's ability to correct a
  wrong derived edge. Provenance + declared edges fix both.
- **A static (non-time-indexed) relationship model.** Cannot express "the
  topology at 14:23 during the incident", cannot carry edge RED metrics, and
  needs an external expiry mechanism.
- **Storing entity-instance edges in table options.** Options can carry
  derivation *rules* (declarations, relationship templates) but not the edges
  themselves (`api-server-1 → users-db`) — those are data, not schema.

# Open Questions

1. **Endpoint encoding hardening.** The canonical `k=v` rendering (with
   `entity_id_attrs` JSON as the source of truth) is readable but collides
   when values contain `,`/`=`. Open: a fixed-width `entity_id_hash` join key
   (New-Relic-style) for large graphs; typed value encoding; the criteria for
   introducing the `semantic_key=column` mapping form of the declaration;
   `schema_url`-gated entity merge.
2. **Identity reconciliation across signals.** Trace edges key on the trace
   table's service declaration; host edges key on `host.id`/`k8s.pod.uid`.
   Linking them
   relies on tables carrying both columns; how aggressively to enrich (a
   `k8sattributes`-equivalent) gates how connected the graph is.
3. **`GRAPH_TABLE` surface scope.** How much of SQL/PGQ to implement vs. a
   minimal single-hop subset. Start minimal and grow with demand.
4. **Visibility beyond table grants.** The derivation contract filters by
   table-read permission; whether topology needs finer policies (row-level,
   tenant-scoped visibility of cross-schema edges) is open.
5. **Vertical correlation without re-scanning large trace tables.** An
   aggregate edge holds RED metrics but not witnessing trace ids; descending
   to the slow traces re-scans the trace table by `(src, dst, window)`. An
   optional witnesses companion (top-N trace ids per edge per window) would
   make the descent O(N).

# Future Work

- **Scheduled materialisation of the derivations** into stored tables
  (idempotent upserts keyed by `generation_id`) for deployments where
  read-time derivation over very large trace tables is too expensive; a
  streaming, trace-id-keyed Flow operator is the faithful long-term form and a
  separate Flow-engine discussion.
- **eBPF-derived edges** from network-flow data, for environments without
  trace propagation.
- **Change/deploy events as graph context** — every serious RCA treats "what
  changed" as a primary candidate.
- **Durable exemplars**: persist OTLP metric exemplars as a `metric → span`
  link table, a cross-signal edge only an all-in-one store can keep.
- **MCP binding**: "neighbours of entity X with their telemetry" as one tool
  call.
- **In-engine graph compute**: shortest-path / blast-radius operators over
  the transient CSR, for analytics beyond bounded-hop.
- **Upstream proposal**: carry the entity-relationship + read-side-catalog
  shape into an OTel discussion, informed by real usage.

# References

GreptimeDB:
- [Table Semantic Layer RFC](2026-05-28-table-semantic-layer.md)
- [Dataflow framework RFC](2024-01-17-dataflow-framework.md)
- [Inverted index RFC](2023-11-03-inverted-index.md)

OpenTelemetry / Prometheus:
- [Entity data model](https://opentelemetry.io/docs/specs/otel/entities/data-model/)
- [Prometheus and OpenMetrics compatibility](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/)
- [Prometheus Remote Write 2.0](https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/)
- [Tempo service graphs](https://grafana.com/docs/tempo/latest/metrics-from-traces/service_graphs/)
- [servicegraph connector](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/connector/servicegraphconnector/README.md)

Vendor entity/relationship models:
- [Backstage well-known relations](https://backstage.io/docs/features/software-catalog/well-known-relations/)
- [New Relic entity GUID spec](https://github.com/newrelic/entity-definitions/blob/main/docs/entities/guid_spec.md)
- [New Relic relationship synthesis](https://github.com/newrelic/entity-definitions/blob/main/docs/relationships/relationship_synthesis.md)
- [Datadog Software Catalog v3 schema](https://github.com/DataDog/schema/tree/main/service-catalog/v3)
- [ServiceNow IRE](https://www.servicenow.com/docs/r/servicenow-platform/configuration-management-database-cmdb/ire.html)
- [Grafana Asserts entity model](https://www.asserts.ai/blog/an-entity-model-for-prometheus-metrics-2/)

Graph query in SQL:
- [DuckPGQ — SQL/PGQ in an analytical RDBMS (VLDB'23)](https://www.vldb.org/pvldb/vol16/p4034-wolde.pdf)
- [Spanner Graph schema statements](https://docs.cloud.google.com/spanner/docs/reference/standard-sql/graph-schema-statements)
