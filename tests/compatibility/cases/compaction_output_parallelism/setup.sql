CREATE TABLE compaction_parallelism (
    ts TIMESTAMP TIME INDEX,
    val INT
) ENGINE=mito;

INSERT INTO compaction_parallelism VALUES (0, 10), (1000, 20);
ADMIN FLUSH_TABLE('compaction_parallelism');
