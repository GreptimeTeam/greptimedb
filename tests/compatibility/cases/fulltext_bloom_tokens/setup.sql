CREATE TABLE t_fulltext_bloom_tokens (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false')
);

INSERT INTO t_fulltext_bloom_tokens VALUES
    (1, 'hello_world'),
    (2, '错误error日志'),
    (3, '中国农业银行');

ADMIN FLUSH_TABLE('t_fulltext_bloom_tokens');

-- Two SSTs with old bloom blobs, left for compaction to rewrite.
CREATE TABLE t_fulltext_bloom_compact (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false')
);

INSERT INTO t_fulltext_bloom_compact VALUES
    (1, 'hello_world'),
    (2, '错误error日志');

ADMIN FLUSH_TABLE('t_fulltext_bloom_compact');

INSERT INTO t_fulltext_bloom_compact VALUES
    (3, '中国农业银行');

ADMIN FLUSH_TABLE('t_fulltext_bloom_compact');
