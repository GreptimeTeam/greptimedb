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

-- Read-time derivation: declare entity identities on a user table and observe
-- them in the computed registry (single-column and composite ids, scope and
-- descriptive columns, JSON output columns).
create table graph_app_latency (
  ts timestamp time index,
  service_name string,
  host string,
  env string,
  latency double,
  primary key (service_name, host, env)
) with (
  'greptime.semantic.entity.service.id' = 'service_name',
  'greptime.semantic.entity.service.scope' = 'env',
  'greptime.semantic.entity.process.id' = 'service_name,host',
  'greptime.semantic.entity.process.descriptive' = 'env'
);

insert into graph_app_latency values (now(), 'cart', 'h1', 'us-east', 3.5);

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, entity_id_attrs, scope, descriptive, source_tables
from greptime_private.semantic_entities
order by entity_type, entity_id;

drop table graph_app_latency;

-- Renaming a physical table INTO a reserved computed-table name would let the
-- overlay shadow it, orphaning its data.
create table greptime_private.graph_rename_probe (ts timestamp time index);

alter table greptime_private.graph_rename_probe rename semantic_entities;

drop table greptime_private.graph_rename_probe;

-- Calls derivation: a trace-v1-model table with client/server span pairs.
-- Trace tables need no entity declaration: the service entities are implicit.
create table graph_traces (
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

-- The epoch-timestamped pair falls outside the default one-hour derivation
-- window and must not appear in the results.
insert into graph_traces values
  (now(), 't1', 'c1', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 0),
  (now(), 't1', 's1', 'c1', 'SPAN_KIND_SERVER', 'STATUS_CODE_ERROR', 'cart', 1500000000),
  (now(), 't2', 'c2', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 0),
  (now(), 't2', 's2', 'c2', 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 'cart', 500000000),
  (0, 't0', 'c0', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'stale-src', 0),
  (0, 't0', 's0', 'c0', 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 'stale-dst', 100);

-- SQLNESS PROTOCOL MYSQL
select src_type, src_id, dst_type, dst_id, rel_type, provenance, confidence,
  request_count, error_count, duration_sum, duration_count, attributes
from greptime_private.semantic_relationships
order by src_id;

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, entity_id_attrs, scope, source_tables
from greptime_private.semantic_entities
order by entity_id;

drop table graph_traces;

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
-- rejected and a physical table cannot be renamed into the name, while plain
-- DML stays allowed.
create table greptime_private.semantic_relationships_declared (ts timestamp time index);

alter table greptime_private.semantic_relationships_declared add column extra string;

create table greptime_private.declared_rename_probe (ts timestamp time index);

alter table greptime_private.declared_rename_probe rename semantic_relationships_declared;

drop table greptime_private.declared_rename_probe;

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

-- Same-row co-declaration: a table declaring both entity types of a built-in
-- vocabulary pair witnesses the edge on every row carrying both identities;
-- the vocabulary fixes the direction.
create table graph_pod_metrics (
  ts timestamp time index,
  instance string,
  host string,
  "service" string,
  cpu double,
  primary key (instance, host, "service")
) with (
  'greptime.semantic.entity.service.instance.id' = 'instance',
  'greptime.semantic.entity.host.id' = 'host',
  'greptime.semantic.entity.service.id' = 'service'
);

insert into graph_pod_metrics values (now(), 'cart-0', 'node-1', 'cart', 0.5);

select src_type, src_id, dst_type, dst_id, rel_type, provenance, confidence
from greptime_private.semantic_relationships
order by rel_type, src_id;

drop table graph_pod_metrics;

-- Cross-table calls pairing: the client span and its child server span land in
-- different trace tables and must still pair into one edge. A trace-model
-- table that lost the fixed span columns is skipped instead of failing the
-- scan.
create table graph_traces_a (
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
  (now(), 't1', 'c1', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 0);

insert into graph_traces_b values
  (now(), 't1', 's1', 'c1', 'SPAN_KIND_SERVER', 'STATUS_CODE_ERROR', 'cart', 1500000000);

insert into graph_traces_malformed values (now(), 'not a trace');

select src_id, dst_id, rel_type, provenance, confidence, request_count, error_count
from greptime_private.semantic_relationships
order by src_id;

drop table graph_traces_a;

drop table graph_traces_b;

drop table graph_traces_malformed;

-- Virtual nodes: a client span with no matching server span names its peer
-- through span attributes; the edge carries confidence < 1.0 and the
-- connection type.
create table graph_traces_virtual (
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

insert into graph_traces_virtual values
  (now(), 't1', 'c1', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 250000000, 'redis', NULL),
  (now(), 't2', 'c2', NULL, 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET', 'frontend', 100000000, NULL, 'orders-db');

-- SQLNESS PROTOCOL MYSQL
select src_id, dst_id, rel_type, confidence, request_count, attributes
from greptime_private.semantic_relationships
order by dst_id;

drop table graph_traces_virtual;

-- Agent edges: span structure derives parent_agent calls agent, and span rows
-- co-declaring agent+model / agent+tool witness uses / invokes.
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
  primary key (service_name, agent_id, model_name, tool_name)
) with (
  'table_data_model' = 'greptime_trace_v1',
  'append_mode' = 'true',
  'greptime.semantic.entity.agent.id' = 'agent_id',
  'greptime.semantic.entity.model.id' = 'model_name',
  'greptime.semantic.entity.tool.id' = 'tool_name'
);

insert into graph_agent_traces values
  (now(), 't1', 'p1', NULL, 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'app', 0, 'orchestrator', NULL, NULL),
  (now(), 't1', 'a1', 'p1', 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'app', 2000000000, 'researcher', 'gpt-5', NULL),
  (now(), 't1', 'a2', 'a1', 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'app', 500000000, 'researcher', NULL, 'web_search');

select src_type, src_id, dst_type, dst_id, rel_type, provenance
from greptime_private.semantic_relationships
order by rel_type, dst_id;

drop table graph_agent_traces;
