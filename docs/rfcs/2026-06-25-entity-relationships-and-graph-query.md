---
Feature Name: Entity Relationships and Graph Query
Tracking Issue: TBD
Date: 2026-06-25
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Give GreptimeDB a way to model, derive, and query the **relationships between the entities** that telemetry describes — the service that calls another service, the process that runs on a host, the pod that belongs to a node — and to traverse that graph to pull back the metrics, logs, and traces attached to each entity.

This is the follow-up RFC promised in the Future Work of [`2026-05-28-table-semantic-layer.md`](2026-05-28-table-semantic-layer.md). The semantic layer gave every table a *per-table identity*; this RFC adds the *edges between tables and the entities inside them*.

Two load-bearing decisions:

1. **GreptimeDB does not become a graph database.** Entities and relationships are exposed as tables; the graph is a *logical view* over data already stored, queried with SQL (later with the ISO SQL/PGQ `GRAPH_TABLE`/`MATCH` operator). No separate graph store, no second copy of the data.
2. **The graph is derived at read time.** `greptime_private.semantic_entities` and `greptime_private.semantic_relationships` are computed tables: scanning them derives rows from the declaring telemetry tables on the fly. Nothing has to be materialised for the graph to work, and a newly-appeared entity is visible the instant its first row lands.

# Motivation

The driving use case is **incident localisation**. An alert fires; an LLM-assisted agent can write a genuinely useful root-cause analysis only if it is handed the right context: the alert entity and its telemetry, the upstream/downstream entities it depends on (in microservice architectures the call chain is empirically the highest-signal context) and *their* telemetry, and correlated events — deploys, config changes, and increasingly the agent events of AI agents acting on the system.

GreptimeDB already stores all of this in one database, queried by one engine. What it lacks is the link layer: the metadata is scattered per signal, and an agent must guess the topology from column names and join by hand. All-in-one is the structural lever — in split stacks an entity catalog sits *above* the stores and stitches results in application code; here, correlation is a join inside one query engine and the graph is a view over the same tables. The audience is broader than one product: alert routing, dashboards, impact analysis, and MCP servers all need *"given this entity, what is related to it, and how do I get its telemetry?"* — and OpenTelemetry settles entity identity but explicitly defers entity relationships and the read-side catalog to backends: unclaimed territory.

One scoping insight keeps the graph small. The same observations form one graph at two resolutions: a coarse **entity graph** (services, hosts, pods and their dependencies — slow-moving and small) and a fine **trace causality graph** (spans, the logs inside them, the exemplars that sampled them — one graph per request, 100–1000× the volume of entity edges). Only the coarse level becomes graph rows; the fine links stay a query pattern over already-indexed join keys (`trace_id`/`span_id`/time) within a bounded window.

# Goals

1. Declare, per table, **which entities it describes** and **which columns form each entity's identity**, reusing the existing `greptime.semantic.*` surface — no new DDL keyword.
2. Represent **relationships between entities** as first-class, time-ranged facts with a small typed vocabulary and an explicit **provenance** (declared vs. derived-from-trace vs. derived-from-attribute vs. agent-inferred).
3. **Derive** the high-value edges automatically at read time: the service call graph from trace tables, host/containment edges from identities co-declared on the same rows.
4. **Query** the graph with bounded-hop traversal and a standard SQL surface — discover neighbours and their source tables, then reach their telemetry, all inside one query engine.
5. Correlate **metrics, logs, traces, and agent events** across the shared join keys (entity, trace context, business key, time) as a single-engine join.
6. Keep every layer **optional and additive**: tables without entity declarations keep working, and the computed tables stream empty until something declares an entity.

# Non-Goals

- **Becoming a graph database.** No native graph storage, no graph-tuned on-disk format.
- **A general-purpose property-graph OLTP engine.** We optimise for observability topology: bounded-hop traversal over a relationship set that is small relative to the raw telemetry.
- **Reconciling a canonical CMDB.** Identity is the OTel "minimally sufficient id" derived from labels; conflicts follow the semantic layer's best-effort rule.
- **A new relationship wire protocol.** Relationships are derived or declared locally; upstream standardisation is a future direction.
- **The diagnosis agent / RCA logic itself.** This RFC makes the substrate *queryable*; agents live in the layer above. Agent events are *stored and correlated* here, not *produced* here.

# Architecture

```
                          ┌─────────────────────────────────────────────┐
  Query (Layer 4)         │  SQL joins / WITH RECURSIVE today;           │
                          │  GRAPH_TABLE / MATCH later                   │
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

**Layer 1 — Declaration.** Telemetry tables declare the entities they describe and the columns that identify each, through new keys in the existing `greptime.semantic.` option namespace. Built-in conventions stamp the common cases (OTLP traces, Prometheus-on-Kubernetes metrics) automatically.

**Layer 2 — Exposure.** Two computed, read-only tables present the node set and the edge set as ordinary relations, so everything downstream — SQL, recursive traversal, SQL/PGQ — is just querying tables.

**Layer 3 — Derivation.** Scanning a computed table enumerates the declarations and executes a typed query plan per source table: `DISTINCT` projections for entities, a trace-table self-join for call edges. Nothing is stored; the caller's permissions and time window bound the work.

**Layer 4 — Query.** Plain SQL and `WITH RECURSIVE` work from day one; the SQL/PGQ `GRAPH_TABLE`/`MATCH` operator arrives later as ergonomics over the same data.

Each layer is independently useful. Layer 1 alone makes entity identity machine-discoverable; Layers 1–3 give a queryable graph with plain SQL; Layer 4 is ergonomics over a query the table model already supports.

# Design

The design distils a survey of OpenTelemetry and the entity/topology models of Datadog, New Relic, Backstage, ServiceNow CSDM, Grafana Asserts/Tempo, eBPF tools (Hubble/Pixie), and SQL/PGQ implementations (DuckPGQ, Spanner Graph). Four facts shaped it. OTel settles entity identity (the *Minimally Sufficient Id* rule) but explicitly defers relationships; the de-facto mechanism today is join by shared identifying attribute, which maps cleanly onto a relational model. No vendor treats derived topology as permanent — edges TTL out (New Relic ~75 min) — so for a TSDB the natural form is edges as time-ranged facts. The service graph is span pairing (`client`/`server` on `trace_id`+`parent_span_id`, RED aggregation — the OTel Collector `servicegraph` connector), and a database holding the whole trace table gets the required trace-locality for free as a self-join. And SQL/PGQ adds graph query without a graph store: property graphs are view-like objects over existing tables and `MATCH` compiles to joins, as DuckPGQ proves inside an analytical columnar engine — Apache AGE (own per-label storage) is the model this RFC rejects.

## Declaring entity identity

A table declares the entities it describes with three option keys under the existing `greptime.semantic.` namespace:

```
greptime.semantic.entity.<entity_type>.id          = comma-separated column names
greptime.semantic.entity.<entity_type>.descriptive = comma-separated column names   (optional)
greptime.semantic.entity.<entity_type>.scope       = comma-separated column names   (optional)
```

Values are column names, and **`id` columns must be tag/primary-key columns** so identity stays indexable and joinable — identity buried in a JSON attribute bag must be projected to a column first via a pipeline. A latency metric table that describes both a `service` and the `host` it runs on:

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

This is the relational image of OTel's `EntityRef`, and **the column name is the identifying attribute key**: OTLP-ingested tables carry attributes as flattened columns named after the keys, so the declaration lists columns and the names double as the semantic keys. Two tables share an entity's identity by naming the identifying columns consistently; an explicit `semantic_key=column` mapping form is reserved as a backwards-compatible extension for tables whose column names diverge. **`scope` is not part of identity**: an OTel entity id is an attribute map with no generic scope dimension, so a namespace-like attribute that disambiguates identity belongs in `id` (uniqueness then holds by construction); the `scope` role only surfaces a namespace/environment value as a filter/display column.

The same mechanism declares agent-telemetry entities (`entity.agent.id`, `entity.session.id`, ...), so agent events — which ride the existing `trace`/`event` signals with GenAI semantic conventions — join the graph with no new surface. Declarations appear in `information_schema.table_semantics`.

The semantic vocabulary stays a closed whitelist, but entity types are open-ended, so the `entity.*` sub-namespace is validated by prefix + shape (`greptime.semantic.entity.<type>.{id|descriptive|scope}`, `<type>` in the OTel entity-type charset) instead of membership; column existence and the id-must-be-tag rule are enforced against the table schema at DDL time.

### Zero-configuration conventions

Explicit declarations are the mechanism; built-in conventions make the graph light up without them. An explicit declaration always overrides an implicit one, so non-standard setups (custom relabeling, renamed columns) are covered by declaring explicitly.

**OTLP traces.** The ingestion path stamps `greptime.semantic.entity.service.id = service_name` on `greptime_trace_v1` tables at creation; the derivation synthesizes the same declaration for trace-v1 tables that predate stamping.

**Prometheus metrics on Kubernetes.** A metric table (`signal_type = metric`, `source = prometheus`) whose tag columns match the well-known label sets receives implicit declarations:

| Tag columns | Entity | Basis |
| --- | --- | --- |
| `job` | `service` | The OTel↔Prometheus compatibility spec: `service.namespace`/`service.name` MUST be combined into the `job` label |
| `job`, `instance` | `service.instance` | Same spec: `service.instance.id` MUST be converted to the `instance` label |
| `namespace`, `pod` | `k8s.pod` | Kubernetes service-discovery / kube-prometheus-stack relabeling convention |
| `node` | `k8s.node` | same |
| `namespace` | `k8s.namespace` | same |

This is what connects the graph to metrics — with one caveat the compatibility spec itself creates: `job` is `<service.namespace>/<service.name>` when a namespace is set, while a trace table's default identity is the bare `service_name`. The metric- and trace-derived `service` nodes therefore unify automatically only when `service.namespace` is empty (so `job` equals `service.name`) and `job` was not relabeled; otherwise alignment needs pipeline normalization or explicit declarations naming identity columns that carry the same values. When the ids do align, the same entity appears from both signals, and "the neighbours' telemetry" reaches metric tables without any user declaration.

`*_info` metrics are entity descriptors, and their rows witness edges under the same-row co-declaration rule (see *Read-time derivation*):

- `kube_pod_info` carries `pod`, `namespace`, and `node` in one row, deriving `k8s.pod runs_on k8s.node` plus descriptive attributes (`host_ip`, `pod_ip`, `created_by_*`).
- `kube_pod_owner` implicitly declares a `k8s.workload` entity with id `(namespace, owner_kind, owner_name)` — the kind is part of the identity, so no dynamic entity typing is needed — and its rows derive `k8s.pod part_of k8s.workload`.
- For `target_info` (the OTLP-over-Prometheus resource metric), the labels other than `job` and `instance` are implicit *descriptive* columns of the `service` entity.

The pack's metadata quality depends on Remote Write 2.0's inline per-series metadata feeding `metric.type`/`metric.unit` with `metadata_quality = declared` — an M1 dependency tracked in [#4765](https://github.com/GreptimeTeam/greptimedb/issues/4765); the details belong to the semantic layer.

## The graph as two computed tables

The graph is exposed as two **computed, read-only tables** under `greptime_private` — not `information_schema`, because scanning them triggers real derivation over telemetry tables, which would break the cheap-metadata expectation of `information_schema`. All DDL/DML against them is rejected on every write path.

`semantic_entities` is the node set: one row per **distinct projected entity observation from a contributing table**, carrying the observation window, the entity's type and id (plus the structured `entity_id_attrs` for composite identities), a descriptive snapshot, and the `source_tables` lineage. An entity declared by three tables yields at least three rows per window.

`semantic_relationships` is the edge set: one row per **(window, edge)**, carrying the endpoints, a `rel_type` from the vocabulary below, `provenance` (`trace` | `attribute` | `declared` | `agent`), `confidence`, RED metrics for `calls` edges, and a JSON `attributes` column.

The decisions behind the shape:

- **Edges are time-ranged facts.** A row asserts an edge *existed in a window*, with RED metrics for that window — the TSDB-native answer to "edges expire". "The topology now" is `WHERE fresh_until >= now() - INTERVAL '5m'`.
- **`provenance` is part of edge identity**, so a declared edge and a derived edge between the same pair coexist, and a user-declared edge survives when derivation is off.
- **`confidence` expresses derivation certainty, not statistical completeness**: `1.0` for a successfully paired or declared edge, `< 1.0` for virtual-node or agent-inferred edges. It does not correct for trace sampling.
- **RED metrics describe the observed span-pair population** — the servicegraph-connector semantics. Under sampling, counts understate true traffic, and ratios (error rate, mean duration) are representative only when sampling is unbiased with respect to status and latency — tail sampling that keeps errors and slow traces skews both.
- **Endpoint encoding (v1).** A single-attribute identity is the value verbatim; a composite identity is the attributes sorted by key, rendered `k1=v1,k2=v2` — a human-readable traversal key, not a collision-free canonical encoding. Nodes carry the structured `entity_id_attrs`; edges carry only the string key. Known v1 limitation: composite components containing unescaped `,` or `=` are not guaranteed to be distinguished.
- **Hand-declared edges** (`provenance = 'declared'`) live in a physical table of the same shape (plus a business validity window) in `greptime_private` and are unioned into the computed `semantic_relationships`.

The relationship vocabulary is small, typed, and inverse-paired; stored direction is `src -> dst`, the inverse is a query concern:

| `rel_type` | Meaning (src → dst) | Primary provenance | Inverse (query) |
| --- | --- | --- | --- |
| `calls` | `service` calls `service` | trace | `called_by` |
| `runs_on` | `service.instance`/`process`/`k8s.pod` runs on `host`/`node` | attribute | `hosts` |
| `contains` | `pod`→`container` | attribute/declared | `part_of` |
| `part_of` | `service.instance`→`service`, `k8s.pod`→`k8s.workload` | attribute/declared | `contains` |
| `depends_on` | logical/declared dependency | declared | `dependency_of` |
| `owns` | team/service owns dst | declared | `owned_by` |

`calls` lives at the *logical* `service` layer; `runs_on`/`contains` live at the *runtime* `service.instance`/`process`/`k8s.pod` layer, with `part_of` linking the two — this avoids "one logical service runs_on five hosts". A custom `rel_type` is just a string; only derivation rules and inverse names are built-in. The trace-derived `calls` edge is qualified by `attributes` (`connection_type` ∈ {`database`, `messaging`, `virtual_node`}, ...) rather than exploded into many edge types.

## Read-time derivation

Scanning a computed table enumerates the entity declarations from table options, builds a derivation plan per source table, and executes it. The plans are **typed DataFusion `Expr`/`DataFrame` plans, not SQL text**: user-controlled identifiers are plain values with no quoting or injection surface, and the window predicates push down into the source table scans, where file/partition pruning applies. A PoC over 90k–1.8M rows of real telemetry confirmed this runs comfortably at read time — an edge aggregate over ~54k rows takes ~31 ms.

**The service call graph.** The flagship derivation is a self-join of each `greptime_trace_v1` table (recognised by its `table_data_model` option): pair each client span with its child server span, project to the service identity, aggregate RED metrics per 60s window — the SQL form of the Tempo servicegraph connector. The defining query, shown in its simplified form for the default single-column `service_name` declaration (in general every `service_name` reference stands for the declaration-derived id expression):

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

Edge endpoints are built from each trace table's `service` entity declaration — the same id expression the registry uses — so edges land on exactly the entity ids the registry emits, self-calls are judged on the full endpoint id, and a table whose declaration is unusable contributes no edges rather than silently switching identity. The join is bounded by a time-proximity window, the span pairs of **all** trace tables are unioned before aggregation (one edge, one row, merged RED), and trace-locality is free because the whole trace table is in the database. Uninstrumented peers become **virtual nodes**: a client span with no matching server span yields an edge to a synthetic node named from `peer.service`/`db.name`/`server.address`.

**The entity registry.** Per declaring table: filter to the window, project (time bin, casts, JSON assembly), `DISTINCT`. The per-table results are unioned without any cross-source merge — no descriptive-conflict or JSON-merging policy lives in the derivation. Rows whose identity columns are NULL identify nothing and are filtered out; single-column ids are used verbatim, composite ids render the sorted `k=v` form plus the `entity_id_attrs` JSON. Consumers deduplicate with `SELECT DISTINCT entity_type, entity_id`; a unique node set is the snapshot relation's job (see *Querying the graph*).

**Attribute edges.** Shared attributes provide join keys; co-declaration or a relationship template provides the relationship semantics — a shared value alone determines neither direction nor edge type, so the derivation is rule-based, not join-everything. A table declaring both a `service.instance` and a `host` identity on the same rows derives `runs_on` between them: the row itself witnesses the relationship, and the built-in vocabulary fixes the direction. Cross-table edges require a declared relationship template (endpoint mappings, `rel_type`, direction) — a derivation *rule* declared on schema, still schema-level configuration, not per-instance edge data.

**Agent edges.** Agent-telemetry tables that declare `agent`/`session`/`model`/`tool` entities feed the registry like any other table, and span structure derives `agent uses model` / `agent invoked tool` / `parent_agent calls agent` edges. Agents may also insert `provenance = 'agent'` edges with `confidence < 1.0` through the declared-edge table; provenance-in-identity keeps an LLM-inferred edge visibly distinct from observed structure and unable to clobber it.

**The contract.** Three principles are normative for the read-time graph:

1. **Derivation runs as the caller**, never as an internal superuser. Every source table is authorized per table: a source the caller cannot read is silently excluded (its entities, descriptive attributes, and `source_tables` entries never appear), and a join-derived edge requires read access to all of its input tables. Querying the computed tables never widens access to the underlying telemetry.
2. **The source-scan window is never narrower than the query's `observed_at` range.** No time predicate defaults to the last hour; a predicate the planner cannot safely extract is an error asking for an explicit range — never a silent fallback into incomplete results.
3. **The window bound is the resource bound.** A bare `SELECT * FROM semantic_entities` scans at most the last hour of the declaring tables; derivation inherits the caller's cancellation and deadline, and no quota or spill machinery is introduced. Deployments where read-time derivation over huge trace tables is too expensive are the materialisation case (Future Work).

Schema drift never poisons a scan: trace-v1 tables that predate the ingest-side auto-stamp get a synthesized `service` declaration, and a declaration referencing a since-dropped column is skipped with a warning instead of failing every graph query.

## Querying the graph

The computed tables are ordinary relations: plain SQL joins back to telemetry tables, and `WITH RECURSIVE` serves multi-hop traversal — the PoC walked a real trace tree to depth 10 (~300 nodes). Bounded-hop traversal over a relationship set that is small relative to raw telemetry is the performance contract.

The ISO SQL/PGQ surface is ergonomics over the same data. SQL/PGQ requires the vertex key to uniquely identify a vertex, and the fact tables have one row per window (and, for entities, per source observation) — so the property graph is defined over a **snapshot relation**, not the raw fact tables: `semantic_graph_snapshot(start, end)` aggregates the queried range into one row per entity (vertex key `(entity_type, entity_id)`, `source_tables` merged) and one row per edge (RED metrics merged), and guarantees every edge endpoint exists in the vertex set — an endpoint with no telemetry-derived entity row (a virtual node, a declared or agent edge) is synthesized as an endpoint-only vertex with NULL descriptive and empty `source_tables`.

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

`MATCH` shares the GPML pattern core with Cypher and GQL, so users get Cypher-like ergonomics inside standard SQL. Execution phases in with the patterns: fixed-length patterns rewrite to joins and bounded variable-length to a bounded union of join chains (no new physical operator); unbounded/shortest-path runs over a transient in-memory CSR built from the relationship rows at query time — the DuckPGQ approach. The physical declared-edge table needs no special machinery either: a primary key starting `(src_type, src_id, ...)` serves out-edge lookup, an inverted index on `(dst_type, dst_id)` serves in-edge ("who depends on this") lookup, and the time index prunes to the topology window.

# Worked Example: diagnosing a slow-query alert

A GreptimeDB self-monitoring scenario: a slow-query alert fires and the agent reads the alert entity (a `frontend` instance) from the alert labels. No setup preceded this incident — GreptimeDB's own trace tables carry the auto-stamped `service` declaration, so the graph already exists. Everything below runs inside one query engine; each query is issued from what the previous one returned.

1. **Who is involved?** The registry names the entities and where they come from:

   ```sql
   SELECT entity_type, entity_id, source_tables
   FROM greptime_private.semantic_entities
   WHERE entity_type = 'service' AND observed_at >= now() - INTERVAL '15m';
   ```

2. **Frontend → its dependencies** (datanode, metasrv) — the step that was impossible before:

   ```sql
   SELECT dst_type, dst_id, request_count, error_count
   FROM greptime_private.semantic_relationships
   WHERE src_type = 'service' AND src_id = 'frontend'
     AND rel_type = 'calls'
     AND observed_at >= now() - INTERVAL '15m';
   ```

   These `calls` edges are derived from the internal traces; the topology is not hand-maintained, and the RED columns already say which dependency is erroring.
3. **Dependencies → their telemetry.** The edge rows name the neighbours; the entity rows name their contributing tables in `source_tables`. The agent issues the follow-up query against those tables — p99 latency and error counts for `datanode` over the same window. Two statements, one engine, no cross-store stitching; `source_tables` is a name for the agent to query next, not something SQL dereferences dynamically.
4. **Descend to evidence**: from the suspect edge, fetch the slowest witnessing server spans in the window, then the logs emitted inside those spans by `(trace_id, span_id)` — that correlation *is* a single join, because spans and logs share the trace context.

The agent hands the LLM not a topology hint but the specific slow requests and the log lines inside them — the difference from a root cause.

# Benefits and Drawbacks

Benefits:

- **Zero configuration for the common cases.** The graph lights up from data already stored — OTLP traces and standard Prometheus/Kubernetes metrics — without a single declaration.
- **No second store, no second engine, no staleness.** The graph is a view over the telemetry tables; an entity appears the instant its first row lands.
- **Standards-aligned.** OTel entity model for identity, servicegraph-connector semantics for call edges, SQL/PGQ for the query direction — nothing proprietary to unlearn.
- **Every layer optional and additive.** Undeclared tables are untouched.

Drawbacks:

- Read-time derivation costs a self-join over trace tables on every scan of the computed tables. The default one-hour window bounds it, but very large deployments will want materialisation (Future Work).
- `semantic_entities` exposes per-observation rows; deduplication falls on the consumer (`DISTINCT`) until the snapshot relation lands in M2.
- The v1 `k=v` id encoding can collide on hostile values (unescaped `,`/`=`); hardening is an open question.
- The graph is only as connected as the identity columns tables actually share — the `job`/`service_name` misalignment above is the canonical example, and enrichment discipline gates connectivity.

# Implementation Plan

**M0 — Declaration.** The `entity.*` option sub-namespace with prefix+shape validation and the id-columns-must-be-tags DDL check; auto-stamp of `entity.service.id` on the OTLP trace path; declarations visible in `information_schema.table_semantics`.

**M1 — The read-time graph.** The computed tables in `greptime_private`, read-only, derived via typed DataFusion plans: the entity registry and the trace-derived `calls` edge first; then the Prometheus/Kubernetes implicit declarations and Remote Write 2.0 metadata intake, virtual nodes, attribute-derived edges, agent edges, the declared-edge table and its union, and the derivation contract (caller authorization, explicit time windows). Queryable with plain SQL and `WITH RECURSIVE` throughout.

**M2 — Query ergonomics.** The snapshot relation, `CREATE PROPERTY GRAPH` + `GRAPH_TABLE`/`MATCH` (single-hop, then bounded variable-length) compiled to join/recursive rewrites, and an MCP tool surface so agents consume the graph directly.

# Alternatives

- **Materialising the graph by default.** Cheaper reads, but it couples ingestion, adds backfill and lifecycle management, introduces staleness, and creates a second persisted representation of the same facts. Read-time derivation wins on immediacy and operational simplicity; materialisation remains the escape hatch for large deployments (Future Work).
- **A real graph database / Apache-AGE-style native store.** Doubles the storage path and duplicates data that already lives in the telemetry tables. The goal is a graph *query*, not a graph database.
- **Cypher as a bolt-on language.** Heavier than SQL/PGQ and pulls toward the graph-store model; SQL/PGQ delivers the same `MATCH` pattern core inside the SQL surface and planner GreptimeDB already has.
- **Edges only derived, never declared.** Loses static topology that has no trace (ownership, declared dependencies) and the user's ability to correct a wrong derived edge. Provenance + declared edges fix both.
- **A static (non-time-indexed) relationship model.** Cannot express "the topology at 14:23 during the incident", cannot carry edge RED metrics, and needs an external expiry mechanism.
- **Storing entity-instance edges in table options.** Options can carry derivation *rules* (declarations, relationship templates) but not the edges themselves (`api-server-1 → users-db`) — those are data, not schema.

# Open Questions

1. **Endpoint encoding hardening.** The v1 `k=v` rendering (with `entity_id_attrs` JSON as the source of truth) is readable but collides when values contain `,`/`=`. Open: a fixed-width `entity_id_hash` join key (New-Relic-style); typed value encoding; the criteria for introducing the `semantic_key=column` mapping form; `schema_url`-gated entity merge.
2. **Identity reconciliation across signals.** Trace edges key on the trace table's service declaration; host edges key on `host.id`/`k8s.pod.uid`. Linking them relies on tables carrying both columns; how aggressively to enrich (a `k8sattributes`-equivalent) gates how connected the graph is.
3. **`GRAPH_TABLE` surface scope.** How much of SQL/PGQ to implement vs. a minimal single-hop subset. Start minimal and grow with demand.
4. **Visibility beyond table grants.** The derivation contract filters by table-read permission; whether topology needs finer policies (row-level, tenant-scoped visibility of cross-schema edges) is open.
5. **Vertical correlation without re-scanning large trace tables.** An aggregate edge holds RED metrics but not witnessing trace ids; descending to the slow traces re-scans the trace table by `(src, dst, window)`. An optional witnesses companion (top-N trace ids per edge per window) would make the descent O(N).

# Future Work

- **Scheduled materialisation of the derivations** into stored tables (idempotent upserts keyed by `generation_id`) for deployments where read-time derivation over very large trace tables is too expensive; a streaming, trace-id-keyed Flow operator is the faithful long-term form and a separate Flow-engine discussion.
- **eBPF-derived edges** from network-flow data, for environments without trace propagation.
- **Change/deploy events as graph context** — every serious RCA treats "what changed" as a primary candidate.
- **Durable exemplars**: persist OTLP metric exemplars as a `metric → span` link table, a cross-signal edge only an all-in-one store can keep.
- **MCP binding**: "neighbours of entity X with their telemetry" as one tool call.
- **In-engine graph compute**: shortest-path / blast-radius operators over the transient CSR, for analytics beyond bounded-hop.
- **Upstream proposal**: carry the entity-relationship + read-side-catalog shape into an OTel discussion, informed by real usage.

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
