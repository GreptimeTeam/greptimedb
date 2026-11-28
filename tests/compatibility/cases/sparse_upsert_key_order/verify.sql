INSERT INTO positioned_tag (__table_id, __tsid, host, site, ts, val)
VALUES (1, 7, 'host-a', 'east', 1000, 2);
ADMIN FLUSH_TABLE('positioned_tag');
SELECT host, site, val FROM positioned_tag ORDER BY val;

INSERT INTO declared_key (__table_id, __tsid, host, site, ts, val)
VALUES (1, 7, 'host-a', 'east', 1000, 2);
ADMIN FLUSH_TABLE('declared_key');
SELECT host, site, val FROM declared_key ORDER BY val;
