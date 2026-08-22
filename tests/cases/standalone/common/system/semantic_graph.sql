-- The computed entity-graph tables under greptime_private are read-only
-- virtual tables: readable, but rejecting every DDL/DML path.
select observed_at, entity_type, entity_id, scope from greptime_private.semantic_entities;

select observed_at, src_id, dst_id, rel_type from greptime_private.semantic_relationships;

insert into greptime_private.semantic_entities (observed_at, entity_type, entity_id) values (now(), 'service', 'svc-a');

-- Plain-literal VALUES takes the direct region-insert path, not the query
-- engine; the table-level guard must hold there too.
insert into greptime_private.semantic_entities (observed_at, entity_type, entity_id) values (0, 'service', 'svc-b');

create table greptime_private.semantic_entities (ts timestamp time index);

create table greptime_private.semantic_relationships (ts timestamp time index);

alter table greptime_private.semantic_entities add column extra string;

truncate table greptime_private.semantic_entities;

drop table greptime_private.semantic_entities;

drop table greptime_private.semantic_relationships;

-- Renaming a physical table INTO a reserved name would let the overlay shadow
-- it (or squat the declared table's canonical schema), orphaning its data.
create table greptime_private.graph_rename_probe (ts timestamp time index);

alter table greptime_private.graph_rename_probe rename semantic_entities;

alter table greptime_private.graph_rename_probe rename semantic_relationships_declared;

drop table greptime_private.graph_rename_probe;

-- Read-time derivation from one declaring metric table: entity identities in
-- the computed registry (single-column and composite ids, scope and
-- descriptive columns, JSON output columns), and the same rows witnessing the
-- co-declared vocabulary edges (runs_on / part_of, direction built in).
create table graph_app_metrics (
  ts timestamp time index,
  service_name string,
  instance string,
  host string,
  env string,
  latency double,
  primary key (service_name, instance, host, env)
) with (
  'greptime.semantic.entity.service.id' = 'service_name',
  'greptime.semantic.entity.service.scope' = 'env',
  'greptime.semantic.entity.service.instance.id' = 'instance',
  'greptime.semantic.entity.host.id' = 'host',
  'greptime.semantic.entity.process.id' = 'service_name,host',
  'greptime.semantic.entity.process.descriptive' = 'env'
);

insert into graph_app_metrics values (now(), 'cart', 'cart-0', 'h1', 'us-east', 3.5);

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, entity_id_attrs, scope, descriptive, source_tables
from greptime_private.semantic_entities
order by entity_type, entity_id;

select src_type, src_id, dst_type, dst_id, rel_type, provenance, confidence
from greptime_private.semantic_relationships
order by rel_type, src_id;

drop table graph_app_metrics;

-- Declarations can be added after the fact: a table created without semantic
-- options joins the graph once ALTER TABLE declares its entities, and leaves
-- it again on UNSET.
create table graph_late_metrics (
  ts timestamp time index,
  svc string,
  env string,
  latency double
);

insert into graph_late_metrics values (now(), 'checkout', 'eu-1', 1.5);

select entity_type, entity_id from greptime_private.semantic_entities order by entity_type, entity_id;

alter table graph_late_metrics set 'greptime.semantic.entity.service.id' = 'svc', 'greptime.semantic.entity.service.scope' = 'env';

select entity_type, entity_id, scope from greptime_private.semantic_entities order by entity_type, entity_id;

-- A column referenced by a declaration must keep a string-renderable type.
alter table graph_late_metrics modify column svc binary;

alter table graph_late_metrics unset 'greptime.semantic.entity.service.id';

alter table graph_late_metrics unset 'greptime.semantic.entity.service.scope';

select entity_type, entity_id from greptime_private.semantic_entities order by entity_type, entity_id;

drop table graph_late_metrics;

-- Calls derivation over trace-v1 tables, all branches in one scan: the client
-- span and its child server span land in different tables and still pair into
-- one edge with RED metrics; the epoch-timestamped pair falls outside the
-- default one-hour window; unmatched clients become virtual-node edges named
-- from span attributes (confidence < 1.0, connection type); a trace-model
-- table that lost the fixed span columns is skipped instead of failing the
-- scan. Trace tables need no entity declaration: service entities are
-- implicit.
create table graph_traces_a (
  "timestamp" timestamp(9) time index,
  trace_id string,
  span_id string,
  parent_span_id string,
  span_kind string,
  span_status_code string,
  service_name string,
  duration_nano bigint unsigned,
  "span_attributes.peer.service" string,
  "span_attributes.db.name" string,
  primary key (service_name)
) with ('table_data_model' = 'greptime_trace_v1', 'append_mode' = 'true');

create table graph_traces_b (
  "timestamp" timestamp(9) time index,
  trace_id string,
  span_id string,
  parent_span_id string,
  span_kind string,
  span_status_code string,
  service_name string,
  duration_nano bigint unsigned,
  primary key (service_name)
) with ('table_data_model' = 'greptime_trace_v1', 'append_mode' = 'true');

create table graph_traces_malformed (
  ts timestamp time index,
  note string
) with ('table_data_model' = 'greptime_trace_v1');

insert into graph_traces_a values
  (now(), 't1', 'c1', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 0, NULL, NULL),
  (now(), 't2', 'c2', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 0, NULL, NULL),
  (0, 't0', 'c0', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'stale-src', 0, NULL, NULL),
  (0, 't0', 's0', 'c0', 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 'stale-dst', 100, NULL, NULL),
  (now(), 't3', 'c3', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 250000000, 'redis', NULL),
  (now(), 't4', 'c4', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 100000000, NULL, 'orders-db');

insert into graph_traces_b values
  (now(), 't1', 's1', 'c1', 'SPAN_KIND_SERVER', 'STATUS_CODE_ERROR', 'cart', 1500000000),
  (now(), 't2', 's2', 'c2', 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 'cart', 500000000);

insert into graph_traces_malformed values (now(), 'not a trace');

-- SQLNESS PROTOCOL MYSQL
select src_id, dst_id, rel_type, provenance, confidence,
  request_count, error_count, duration_sum, duration_count, attributes
from greptime_private.semantic_relationships
order by dst_id;

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, entity_id_attrs, scope, source_tables
from greptime_private.semantic_entities
order by entity_id;

drop table graph_traces_a;

drop table graph_traces_b;

drop table graph_traces_malformed;

-- Declared edges: the physical table is created by the system on the first
-- INSERT with its canonical schema. Re-asserting an edge stores a new revision;
-- reads keep only the latest one per edge key.
insert into greptime_private.semantic_relationships_declared
  (observed_at, src_type, src_id, rel_type, dst_type, dst_id, provenance, scope, generation_id, confidence)
values (now() - interval '10 minute', 'service', 'frontend', 'depends_on', 'service', 'users-db', 'declared', '', '', 0.5);

insert into greptime_private.semantic_relationships_declared
  (observed_at, src_type, src_id, rel_type, dst_type, dst_id, provenance, scope, generation_id, confidence)
values (now() - interval '5 minute', 'service', 'frontend', 'depends_on', 'service', 'users-db', 'declared', '', '', 1.0);

-- An old open-ended declaration stays valid until its row expires; an edge
-- retired in the past must not appear.
insert into greptime_private.semantic_relationships_declared
  (observed_at, src_type, src_id, rel_type, dst_type, dst_id, provenance, scope, generation_id, valid_until)
values
  ('1970-01-01 00:00:01', 'service', 'legacy', 'depends_on', 'service', 'mainframe', 'declared', '', '', NULL),
  ('1970-01-01 00:00:01', 'service', 'retired', 'depends_on', 'service', 'oldsys', 'declared', '', '', '2000-01-01 00:00:00');

select src_id, dst_id, rel_type, provenance, confidence
from greptime_private.semantic_relationships
order by src_id;

-- An explicit observed_at window replaces the default last hour; the emitted
-- timestamps of declared edges are synthesized inside the queried window.
select observed_at, window_start, fresh_until, src_id, dst_id
from greptime_private.semantic_relationships
where observed_at >= '2001-01-01 00:00:00' and observed_at < '2001-01-02 00:00:00'
order by src_id;

-- A lower bound alone is fine (the upper bound defaults to now)...
select src_id, dst_id, provenance
from greptime_private.semantic_relationships
where observed_at >= now() - interval '30 minute'
order by src_id;

-- ...but an upper bound alone would scan unbounded history: explicit error.
select src_id from greptime_private.semantic_relationships
where observed_at < '2001-01-02 00:00:00';

-- The declared-edge table's definition is system-owned: user CREATE/ALTER are
-- rejected, while plain DML stays allowed.
create table greptime_private.semantic_relationships_declared (ts timestamp time index);

alter table greptime_private.semantic_relationships_declared add column extra string;

delete from greptime_private.semantic_relationships_declared;

select src_id from greptime_private.semantic_relationships order by src_id;

-- DROP is allowed (nothing structural is lost: the next INSERT recreates the
-- canonical table) and cleans up after this test.
drop table greptime_private.semantic_relationships_declared;

insert into greptime_private.semantic_relationships_declared
  (observed_at, src_type, src_id, rel_type, dst_type, dst_id, provenance, scope, generation_id)
values (now(), 'service', 'reborn', 'depends_on', 'service', 'db', 'declared', '', '');

select src_id from greptime_private.semantic_relationships order by src_id;

drop table greptime_private.semantic_relationships_declared;

-- Agent edges: span structure derives parent_agent calls agent, and span rows
-- co-declaring gen_ai.agent+gen_ai.model / gen_ai.agent+gen_ai.tool witness
-- uses / invokes. The identity columns are fields, not tags.
create table graph_agent_traces (
  "timestamp" timestamp(9) time index,
  trace_id string,
  span_id string,
  parent_span_id string,
  span_kind string,
  span_status_code string,
  service_name string,
  duration_nano bigint unsigned,
  agent_id string,
  model_name string,
  tool_name string,
  primary key (service_name)
) with (
  'table_data_model' = 'greptime_trace_v1',
  'append_mode' = 'true',
  'greptime.semantic.entity.gen_ai.agent.id' = 'agent_id',
  'greptime.semantic.entity.gen_ai.model.id' = 'model_name',
  'greptime.semantic.entity.gen_ai.tool.id' = 'tool_name'
);

insert into graph_agent_traces values
  (now(), 't1', 'p1', NULL, 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'app', 0, 'orchestrator', NULL, NULL),
  (now(), 't1', 'a1', 'p1', 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'app', 2000000000, 'researcher', 'gpt-5', NULL),
  (now(), 't1', 'a2', 'a1', 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'app', 500000000, 'researcher', NULL, 'web_search');

select src_type, src_id, dst_type, dst_id, rel_type, provenance
from greptime_private.semantic_relationships
order by rel_type, dst_id;

drop table graph_agent_traces;

-- Prometheus conventions: whitelisted entity-descriptor metrics (stamped
-- signal_type=metric + source=prometheus by the remote-write path) get
-- implicit declarations, and the co-declared rules derive runs_on / part_of /
-- contains from their rows. Pod identity is the UID, so every KSM descriptor
-- lands on one entity; the empty labels kube-state-metrics emits (unscheduled
-- pod's node, ownerless pod's owner_*) identify nothing. A non-whitelisted
-- metric contributes nothing.
create table kube_pod_info (
  greptime_timestamp timestamp(3) time index,
  uid string,
  namespace string,
  pod string,
  node string,
  greptime_value double,
  primary key (uid, namespace, pod, node)
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

create table kube_pod_owner (
  greptime_timestamp timestamp(3) time index,
  uid string,
  namespace string,
  pod string,
  owner_kind string,
  owner_name string,
  greptime_value double,
  primary key (uid, namespace, pod, owner_kind, owner_name)
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

create table kube_pod_container_info (
  greptime_timestamp timestamp(3) time index,
  uid string,
  container string,
  image string,
  greptime_value double,
  primary key (uid, container, image)
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

create table kube_service_info (
  greptime_timestamp timestamp(3) time index,
  uid string,
  namespace string,
  "service" string,
  cluster_ip string,
  greptime_value double,
  primary key (uid, namespace, "service", cluster_ip)
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

create table target_info (
  greptime_timestamp timestamp(3) time index,
  job string,
  instance string,
  k8s_cluster_name string,
  greptime_value double,
  primary key (job, instance, k8s_cluster_name)
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

create table http_requests_total (
  greptime_timestamp timestamp(3) time index,
  job string,
  instance string,
  greptime_value double,
  primary key (job, instance)
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

insert into kube_pod_info values
  (now(), 'uid-1', 'default', 'api-1', 'node-a', 1),
  (now(), 'uid-2', 'default', 'api-2', 'node-a', 1),
  (now(), 'uid-3', 'default', 'pending-1', '', 1),
  (now(), '', 'default', 'ghost', 'node-a', 1);

insert into kube_pod_owner values
  (now(), 'uid-1', 'default', 'api-1', 'ReplicaSet', 'api-rs', 1),
  (now(), 'uid-2', 'default', 'api-2', 'ReplicaSet', 'api-rs', 1);

insert into kube_pod_container_info values
  (now(), 'uid-1', 'main', 'nginx:1.25', 1);

insert into kube_service_info values
  (now(), 'svc-uid-1', 'default', 'api-svc', '10.0.0.1', 1);

insert into target_info values
  (now(), 'shop/api', 'inst-1', 'prod', 1);

insert into http_requests_total values
  (now(), 'shop/api', 'inst-1', 42);

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, source_tables
from greptime_private.semantic_entities
order by entity_type, entity_id, source_tables;

-- SQLNESS PROTOCOL MYSQL
select src_type, src_id, dst_type, dst_id, rel_type, provenance
from greptime_private.semantic_relationships
order by rel_type, src_id, dst_id;

drop table kube_pod_info;

drop table kube_pod_owner;

drop table kube_pod_container_info;

drop table kube_service_info;

drop table target_info;

drop table http_requests_total;

-- OTel conventions: the ingestion-synthesized greptime_otel_resource_info descriptor
-- (stamped signal_type=metric + source=opentelemetry + metric.type=info) gets implicit
-- declarations under the raw OTel column names. host/container identities are
-- the stable ids only, so rows with an empty host.id / container.id link no
-- infrastructure, and a row without an instance value declares no
-- service.instance. The same table name stamped source=prometheus is not
-- whitelisted. The pod row shows the row-level suppression: a container inside
-- a pod is the k8s.container entity, so the generic container (and every edge
-- it would carry) yields on that row while the bare-runtime row keeps its own.
create table greptime_otel_resource_info (
  greptime_timestamp timestamp(3) time index,
  job string,
  instance string,
  "service.name" string,
  "service.namespace" string,
  "host.id" string,
  "host.name" string,
  "container.id" string,
  "container.name" string,
  "k8s.pod.uid" string,
  "k8s.pod.name" string,
  greptime_value double,
  primary key (job, instance, "service.name", "service.namespace",
    "host.id", "host.name", "container.id", "container.name",
    "k8s.pod.uid", "k8s.pod.name")
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.metric.type' = 'info'
);

insert into greptime_otel_resource_info values
  (now(), 'shop/api', 'inst-1', 'api', 'shop', 'h-1', 'node-a', 'c-1', 'api-ctr', '', '', 1),
  (now(), 'shop/api', 'inst-2', 'api', 'shop', '', 'laptop', '', '', '', '', 1),
  (now(), 'worker', '', 'worker', '', 'h-1', 'node-a', '', '', '', '', 1),
  (now(), 'shop/api', 'inst-3', 'api', 'shop', 'h-2', 'node-b', 'c-2', 'api-ctr', 'uid-1', 'api-pod', 1);

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, source_tables
from greptime_private.semantic_entities
order by entity_type, entity_id, source_tables;

-- SQLNESS PROTOCOL MYSQL
select src_type, src_id, dst_type, dst_id, rel_type, provenance
from greptime_private.semantic_relationships
order by rel_type, src_id, dst_id;

drop table greptime_otel_resource_info;

-- The descriptor whitelist is gated on source=opentelemetry: the same table
-- shape stamped as a prometheus source contributes nothing.
create table greptime_otel_resource_info (
  greptime_timestamp timestamp(3) time index,
  job string,
  instance string,
  "host.id" string,
  greptime_value double,
  primary key (job, instance, "host.id")
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'prometheus'
);

insert into greptime_otel_resource_info values
  (now(), 'shop/api', 'inst-1', 'h-1', 1);

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id
from greptime_private.semantic_entities
order by entity_type, entity_id;

drop table greptime_otel_resource_info;

-- Cross-signal identity: the same service reaches the graph as trace spans and
-- as an ingestion-synthesized descriptor. The two sources name their identity
-- columns differently and only the metric side pre-composes the namespace into
-- job, so this asserts one entity per service and per instance, sourced from
-- both tables.
create table graph_xsignal_traces (
  "timestamp" timestamp(9) time index,
  trace_id string,
  span_id string,
  parent_span_id string,
  span_kind string,
  span_status_code string,
  service_name string,
  duration_nano bigint unsigned,
  "resource_attributes.service.namespace" string,
  "resource_attributes.service.instance.id" string,
  primary key (service_name, "resource_attributes.service.namespace",
    "resource_attributes.service.instance.id")
) with ('table_data_model' = 'greptime_trace_v1', 'append_mode' = 'true');

create table greptime_otel_resource_info (
  greptime_timestamp timestamp(3) time index,
  job string,
  instance string,
  "service.name" string,
  "service.namespace" string,
  greptime_value double,
  primary key (job, instance, "service.name", "service.namespace")
) with (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.metric.type' = 'info'
);

insert into graph_xsignal_traces values
  (now(), 't1', 's1', NULL, 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 'api', 100, 'shop', 'inst-1'),
  (now(), 't2', 's2', NULL, 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 'worker', 100, '', 'inst-2');

insert into greptime_otel_resource_info values
  (now(), 'shop/api', 'inst-1', 'api', 'shop', 1),
  (now(), 'worker', 'inst-2', 'worker', '', 1);

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, source_tables
from greptime_private.semantic_entities
where entity_type in ('service', 'service.instance')
order by entity_type, entity_id, source_tables;

drop table graph_xsignal_traces;

drop table greptime_otel_resource_info;
