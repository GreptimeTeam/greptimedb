-- Trace V2 uses the same flattened attribute references as V1.
-- Everything after the root column is one literal key, including dots.
create table graph_trace_v2 (
  "timestamp" timestamp(9) time index,
  timestamp_end timestamp(9),
  duration_nano bigint,
  parent_span_id string,
  trace_id string,
  span_id string,
  span_kind string,
  span_name string,
  span_status_code string,
  span_status_message string,
  trace_state string,
  scope_name string,
  scope_version string,
  service_name string,
  span_attributes json2,
  scope_attributes json2,
  resource_attributes json2,
  span_events json,
  span_links json,
  primary key (service_name)
) with (
  'table_data_model' = 'greptime_trace_v2',
  'append_mode' = 'true',
  'greptime.semantic.entity.gen_ai.agent.id' = 'span_attributes.gen_ai.agent.id',
  'greptime.semantic.entity.gen_ai.agent.scope' = 'scope_attributes.environment',
  'greptime.semantic.entity.gen_ai.model.id' = 'span_attributes.gen_ai.request.model',
  'greptime.semantic.entity.literal_peer.id' = 'span_attributes.db.namespace',
  'greptime.semantic.entity.literal_resource.id' = 'resource_attributes.host.name',
  'greptime.semantic.entity.literal_span.id' = 'span_attributes.gen_ai.agent.id',
  'greptime.semantic.entity.literal_scope.id' = 'scope_attributes.deployment.region.name'
);

insert into graph_trace_v2
  ("timestamp", duration_nano, trace_id, span_id, span_kind, span_status_code,
   service_name, span_attributes, scope_attributes, resource_attributes)
values
  ('2026-01-01 00:00:01', 100000000, 'paired', 'client', 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET',
   'frontend', '{"gen_ai.agent.id":"agent-a","gen_ai.request.model":"model-a","gen_ai":{"agent":{"id":"nested-agent"}}}',
   '{"environment":"prod","deployment.region.name":"literal-region","deployment":{"region.name":"nested-region"},"a\"b\\c.d":7}', '{"service.namespace":"shop","service.instance.id":"front-1","host.id":"host-1","host.name":"node-1","host":{"name":"nested-node"},"service":{"namespace":"wrong-namespace"}}'),
  ('2026-01-01 00:00:02', 200000000, 'virtual', 'client', 'SPAN_KIND_CLIENT', 'STATUS_CODE_ERROR',
   'frontend', '{"service.peer.name":"","db.namespace":"orders","db":{"namespace":"wrong-orders"}}', '{}',
   '{"service.namespace":"shop","service.instance.id":"front-1","host.id":"host-1","host.name":"node-1"}'),
  ('2026-01-01 00:00:03', 100, 'missing', 'internal', 'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET',
   'no-resource', '{"gen_ai.agent.id":null}', '{}', '{}');

-- Agent parent/child references are independent of span kind.
insert into graph_trace_v2
  ("timestamp", duration_nano, trace_id, span_id, parent_span_id, span_kind,
   span_status_code, service_name, span_attributes, scope_attributes, resource_attributes)
values ('2026-01-01 00:00:01', 30000000, 'paired', 'agent-child', 'client',
        'SPAN_KIND_INTERNAL', 'STATUS_CODE_UNSET', 'frontend',
        '{"gen_ai.agent.id":"agent-b"}', '{"environment":"prod"}', '{"service.namespace":"shop"}');

-- Calls can pair a V2 client with a V1 server. The literal_resource declaration
-- has exactly the same spelling in both models, including its output JSON keys.
create table graph_trace_v1_peer (
  "timestamp" timestamp(9) time index,
  trace_id string, span_id string, parent_span_id string,
  span_kind string, span_status_code string, duration_nano bigint,
  service_name string,
  "resource_attributes.service.namespace" string,
  "resource_attributes.host.name" string,
  primary key (service_name)
) with ('table_data_model' = 'greptime_trace_v1', 'append_mode' = 'true',
        'greptime.semantic.entity.literal_resource.id' = 'resource_attributes.host.name');

insert into graph_trace_v1_peer values
  ('2026-01-01 00:00:01', 'paired', 'server', 'client', 'SPAN_KIND_SERVER', 'STATUS_CODE_UNSET', 50000000, 'backend', 'shop', 'node-v1');

select entity_type, entity_id, scope
from greptime_private.semantic_entities
where observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00'
order by entity_type, entity_id;

select src_type, src_id, dst_type, dst_id, rel_type, request_count, error_count
from greptime_private.semantic_relationships
where observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00'
order by rel_type, src_type, src_id, dst_id;

-- Empty components in the V1-compatible comma-separated list remain invalid.
alter table graph_trace_v2 set 'greptime.semantic.entity.invalid.id' = 'resource_attributes.host.id,';
alter table graph_trace_v2 set 'greptime.semantic.entity.invalid.id' = 'resource_attributes.host.id,,service_name';

-- ALTER accepts JSON2 references; a missing key simply yields no entity.
alter table graph_trace_v2 set 'greptime.semantic.entity.custom.id' = 'scope_attributes.missing';
alter table graph_trace_v2 set 'greptime.semantic.entity.host.descriptive' = 'resource_attributes.host.name';
-- Preserve implicit host identity by explicitly declaring it as well.
alter table graph_trace_v2 set 'greptime.semantic.entity.host.id' = 'resource_attributes.host.id';

admin flush_table('graph_trace_v2');
admin flush_table('graph_trace_v1_peer');

-- SQLNESS PROTOCOL MYSQL
select entity_type, entity_id, scope, entity_id_attrs, descriptive
from greptime_private.semantic_entities
where observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00'
order by entity_type, entity_id;

select src_type, src_id, dst_type, dst_id, rel_type, request_count, error_count
from greptime_private.semantic_relationships
where observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00'
order by rel_type, src_type, src_id, dst_id;

-- Quotes, backslashes and dots remain a literal key; numbers render as strings.
alter table graph_trace_v2 set 'greptime.semantic.entity.custom.id' = 'scope_attributes.a"b\c.d';
select entity_type, entity_id
from greptime_private.semantic_entities
where entity_type = 'custom'
  and observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00';

-- Path-looking suffixes are literal keys, not nested traversal or array access.
-- These missing keys are valid declarations and produce no entities.
alter table graph_trace_v2 set 'greptime.semantic.entity.literal_missing.id' = 'resource_attributes.host..name';
alter table graph_trace_v2 set 'greptime.semantic.entity.literal_index.id' = 'resource_attributes.host[0]';

-- The root must exist and be one of the three JSON2 attribute columns.
alter table graph_trace_v2 set 'greptime.semantic.entity.invalid.id' = 'span_events.name';
alter table graph_trace_v2 set 'greptime.semantic.entity.invalid.id' = 'resource_attributes.';
alter table graph_trace_v2 set 'greptime.semantic.entity.invalid.id' = 'missing.key';
alter table graph_trace_v2 set 'greptime.semantic.entity.invalid.id' = 'resource_attributes';

create table graph_bad_v2 (
  ts timestamp time index, resource_attributes string
) with ('table_data_model' = 'greptime_trace_v2',
        'greptime.semantic.entity.host.id' = 'resource_attributes.host.id');

-- Ordinary JSON2 tables do not acquire Trace V2 attribute-reference semantics.
create table graph_not_trace (
  ts timestamp time index, resource_attributes json2
) with ('append_mode' = 'true',
        'greptime.semantic.entity.host.id' = 'resource_attributes.host.id');

-- Physical convention columns win over conflicting JSON2 keys.
alter table graph_trace_v2 add column "resource_attributes.host.id" string;
alter table graph_trace_v2 add column "resource_attributes.service.namespace" string;
alter table graph_trace_v2 add column "span_attributes.db.namespace" string;
-- Restore the implicit host declaration for the physical-column check.
alter table graph_trace_v2 unset 'greptime.semantic.entity.host.id';
alter table graph_trace_v2 unset 'greptime.semantic.entity.host.descriptive';
insert into graph_trace_v2
  ("timestamp", duration_nano, trace_id, span_id, span_kind, span_status_code,
   service_name, resource_attributes, span_attributes,
   "resource_attributes.host.id", "resource_attributes.service.namespace", "span_attributes.db.namespace")
values ('2026-01-01 00:00:04', 100, 'physical', 'physical-client', 'SPAN_KIND_CLIENT', 'STATUS_CODE_UNSET',
        'physical-service', '{"host.id":"json-host","service.namespace":"json-namespace"}',
        '{"db.namespace":"json-database","db":{"namespace":"nested-database"}}',
        'physical-host', 'physical-namespace', 'physical-database');

select entity_type, entity_id
from greptime_private.semantic_entities
where entity_type in ('host', 'service')
  and observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00'
order by entity_type, entity_id;

select src_id, dst_id, rel_type
from greptime_private.semantic_relationships
where src_id = 'physical-namespace/physical-service'
  and observed_at >= '2026-01-01 00:00:00' and observed_at < '2026-01-01 00:01:00'
order by rel_type, dst_id;

drop table graph_trace_v2;
drop table graph_trace_v1_peer;
