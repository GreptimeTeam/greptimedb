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
