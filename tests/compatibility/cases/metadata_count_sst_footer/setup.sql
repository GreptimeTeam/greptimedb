CREATE TABLE t_metadata_count_footer (
    host STRING PRIMARY KEY,
    ts TIMESTAMP TIME INDEX,
    v BIGINT
) ENGINE=mito WITH (append_mode = 'false');

INSERT INTO t_metadata_count_footer VALUES ('a', 1000, 1), ('a', 2000, 2);
INSERT INTO t_metadata_count_footer VALUES ('a', 1000, 3), ('b', 1000, 4);
ADMIN FLUSH_TABLE('t_metadata_count_footer');
