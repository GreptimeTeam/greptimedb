CREATE TABLE application_logs (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO application_logs VALUES
    (1, '{"service":"frontend","duration":12,"trace_id":"before-1"}'),
    (2, '{"service":"ingester","duration":34,"trace_id":"before-2"}');

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        max_auto_expanded_paths = 2000,
        service STRING,
        duration BIGINT,
        trace_id STRING
    );

SELECT COUNT(*) AS alter_flushed_sst_num
FROM information_schema.ssts_manifest
WHERE visible
  AND table_id IN (
      SELECT table_id FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = 'application_logs'
  );

INSERT INTO application_logs VALUES
    (3, '{"service":"frontend","duration":56,"trace_id":"after-1"}'),
    (4, '{"service":"compactor","trace_id":"after-2"}');

SELECT ts, attrs.service, attrs.duration, attrs.trace_id
FROM application_logs
ORDER BY ts;

ADMIN FLUSH_TABLE('application_logs');

SELECT ts, attrs.service, attrs.duration, attrs.trace_id
FROM application_logs
ORDER BY ts;

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        trace_id STRING,
        user.id STRING,
        user.name STRING,
        request_id STRING
    );

INSERT INTO application_logs VALUES
    (5, '{"trace_id":"after-3","user":{"id":"u1","name":"alice"},"request_id":"r1"}');

SELECT ts, attrs.trace_id, attrs.user.id, attrs.user.name, attrs.request_id
FROM application_logs
ORDER BY ts;

ALTER TABLE application_logs
    MODIFY COLUMN attrs JSON2 (
        max_auto_expanded_paths = 2000,
        trace_id STRING,
        user.id STRING,
        user.name STRING,
        request_id STRING
    );

INSERT INTO application_logs VALUES
    (6, '{"trace_id":"after-4","user":{"id":"u2"},"request_id":"r2"}');

SELECT ts, attrs.trace_id, attrs.user.id, attrs.user.name, attrs.request_id
FROM application_logs
ORDER BY ts;

ADMIN FLUSH_TABLE('application_logs');

DROP TABLE application_logs;

create table json2_alter_add_settings (
    ts timestamp time index
) with (
    'append_mode' = 'true'
);

-- ADD COLUMN preserves JSON2 settings and type hints, including for existing rows.
insert into json2_alter_add_settings values (1);
alter table json2_alter_add_settings add column j json2(max_auto_expanded_paths = 100);

alter table json2_alter_add_settings add column k json2(service string);

alter table json2_alter_add_settings add column m json2(max_auto_expanded_paths = 0, nested.value bigint);

show create table json2_alter_add_settings;

insert into json2_alter_add_settings values
    (2, '{"a":1}', '{"service":"api"}', '{"nested":{"value":42},"extra":true}');

select ts, j, k.service, m.nested.value, m from json2_alter_add_settings order by ts;

admin flush_table('json2_alter_add_settings');

select ts, j, k.service, m.nested.value, m from json2_alter_add_settings order by ts;

drop table json2_alter_add_settings;
