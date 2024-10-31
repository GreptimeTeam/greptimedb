CREATE TABLE IF NOT EXISTS prune_field (
  ts TIMESTAMP TIME INDEX,
  tag UInt16,
  a UInt8,
  b UInt8,
PRIMARY KEY (tag)) ENGINE = mito WITH('merge_mode'='last_non_null');

insert into prune_field(ts, tag, a, b) values(0, 1, 1, null);

admin flush_table('prune_field');

insert into prune_field(ts, tag, a, b) values(0, 1, null, 1);

admin flush_table('prune_field');

select * from prune_field where a = 1;

select * from prune_field where b = 1;

select * from prune_field;

select * from prune_field where a = 1 and b = 1;

drop table prune_field;
