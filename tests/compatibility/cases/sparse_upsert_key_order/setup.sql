CREATE TABLE positioned_tag (
    __table_id INT UNSIGNED NOT NULL,
    __tsid BIGINT UNSIGNED NOT NULL,
    host STRING,
    ts TIMESTAMP TIME INDEX,
    val INT,
    PRIMARY KEY (__table_id, __tsid, host)
) ENGINE=mito WITH ('primary_key_encoding'='sparse', 'append_mode'='false');
ALTER TABLE positioned_tag ADD COLUMN site STRING PRIMARY KEY AFTER __tsid;
INSERT INTO positioned_tag (__table_id, __tsid, host, site, ts, val)
VALUES (1, 7, 'host-a', 'east', 1000, 1);
ADMIN FLUSH_TABLE('positioned_tag');

CREATE TABLE declared_key (
    __table_id INT UNSIGNED NOT NULL,
    __tsid BIGINT UNSIGNED NOT NULL,
    site STRING,
    host STRING,
    ts TIMESTAMP TIME INDEX,
    val INT,
    PRIMARY KEY (__table_id, __tsid, host, site)
) ENGINE=mito WITH ('primary_key_encoding'='sparse', 'append_mode'='false');
ALTER TABLE declared_key ADD COLUMN extra INT;
INSERT INTO declared_key (__table_id, __tsid, host, site, ts, val)
VALUES (1, 7, 'host-a', 'east', 1000, 1);
ADMIN FLUSH_TABLE('declared_key');
