-- should not able to create pg_catalog
create database pg_catalog;

select * from pg_catalog.pg_type order by oid;