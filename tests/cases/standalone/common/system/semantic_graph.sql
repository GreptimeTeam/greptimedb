-- The computed entity-graph tables under greptime_private are read-only
-- virtual tables: readable, but rejecting every DDL/DML path.
select observed_at, entity_type, entity_id, scope from greptime_private.semantic_entities;

select observed_at, src_id, dst_id, rel_type from greptime_private.semantic_relationships;

insert into greptime_private.semantic_entities (observed_at, entity_type, entity_id) values (now(), 'service', 'svc-a');

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
