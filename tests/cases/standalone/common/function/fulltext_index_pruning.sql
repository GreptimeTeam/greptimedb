-- Fulltext index pruning must never drop rows that `matches_term` or `LIKE` accepts.
-- Each table holds the same rows in a single flushed SST. The unfiltered
-- projections are the full-scan answers; every filtered query below must return
-- exactly the rows marked true there.

CREATE TABLE ft_en_bloom (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false')
);

INSERT INTO ft_en_bloom VALUES
    (1, 'hello_world'),
    (2, 'trace_id=abc'),
    (3, '错误error日志'),
    (4, '登录手机号18888888888的动态key'),
    (5, '中国农业银行'),
    (6, '连接timeout.5次'),
    (7, '用户user-123登录');

ADMIN flush_table('ft_en_bloom');

SELECT ts, msg @@ 'world' AS "world", msg @@ 'id' AS "id", msg @@ 'error' AS "error", msg @@ '手机号' AS "手机号", msg @@ '手机' AS "手机", msg @@ '机号' AS "机号", msg @@ '18888888888' AS "18888888888", msg @@ '农业' AS "农业", msg @@ 'timeout' AS "timeout", msg @@ 'user' AS "user" FROM ft_en_bloom ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ 'id' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ '手机号' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ '手机' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ '机号' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ '18888888888' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ 'timeout' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg @@ 'user' ORDER BY ts;

SELECT ts, msg LIKE '%world' AS "%world", msg LIKE 'hello\_world' AS "hello\_world", msg LIKE 'hello_world' AS "hello_world", msg LIKE '%id=%' AS "%id=%", msg LIKE 'trace%abc' AS "trace%abc", msg LIKE '%error%' AS "%error%", msg LIKE '%手机号1888%' AS "%手机号1888%", msg LIKE '%农业%' AS "%农业%", msg LIKE '连接timeout.%' AS "连接timeout.%", msg LIKE '%user-123%' AS "%user-123%" FROM ft_en_bloom ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '%world' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE 'hello\_world' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE 'hello_world' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '%id=%' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE 'trace%abc' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '%error%' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '%手机号1888%' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '%农业%' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '连接timeout.%' ORDER BY ts;

SELECT ts, msg FROM ft_en_bloom WHERE msg LIKE '%user-123%' ORDER BY ts;

DROP TABLE ft_en_bloom;

CREATE TABLE ft_zh_bloom (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH(analyzer = 'Chinese', backend = 'bloom', case_sensitive = 'false')
);

INSERT INTO ft_zh_bloom VALUES
    (1, 'hello_world'),
    (2, 'trace_id=abc'),
    (3, '错误error日志'),
    (4, '登录手机号18888888888的动态key'),
    (5, '中国农业银行'),
    (6, '连接timeout.5次'),
    (7, '用户user-123登录');

ADMIN flush_table('ft_zh_bloom');

SELECT ts, msg @@ 'world' AS "world", msg @@ 'id' AS "id", msg @@ 'error' AS "error", msg @@ '手机号' AS "手机号", msg @@ '手机' AS "手机", msg @@ '机号' AS "机号", msg @@ '18888888888' AS "18888888888", msg @@ '农业' AS "农业", msg @@ 'timeout' AS "timeout", msg @@ 'user' AS "user" FROM ft_zh_bloom ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ 'id' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ '手机号' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ '手机' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ '机号' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ '18888888888' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ 'timeout' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg @@ 'user' ORDER BY ts;

SELECT ts, msg LIKE '%world' AS "%world", msg LIKE 'hello\_world' AS "hello\_world", msg LIKE 'hello_world' AS "hello_world", msg LIKE '%id=%' AS "%id=%", msg LIKE 'trace%abc' AS "trace%abc", msg LIKE '%error%' AS "%error%", msg LIKE '%手机号1888%' AS "%手机号1888%", msg LIKE '%农业%' AS "%农业%", msg LIKE '连接timeout.%' AS "连接timeout.%", msg LIKE '%user-123%' AS "%user-123%" FROM ft_zh_bloom ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '%world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE 'hello\_world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE 'hello_world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '%id=%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE 'trace%abc' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '%error%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '%手机号1888%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '%农业%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '连接timeout.%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_bloom WHERE msg LIKE '%user-123%' ORDER BY ts;

DROP TABLE ft_zh_bloom;

CREATE TABLE ft_en_tantivy (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH(analyzer = 'English', backend = 'tantivy', case_sensitive = 'false')
);

INSERT INTO ft_en_tantivy VALUES
    (1, 'hello_world'),
    (2, 'trace_id=abc'),
    (3, '错误error日志'),
    (4, '登录手机号18888888888的动态key'),
    (5, '中国农业银行'),
    (6, '连接timeout.5次'),
    (7, '用户user-123登录');

ADMIN flush_table('ft_en_tantivy');

SELECT ts, msg @@ 'world' AS "world", msg @@ 'id' AS "id", msg @@ 'error' AS "error", msg @@ '手机号' AS "手机号", msg @@ '手机' AS "手机", msg @@ '机号' AS "机号", msg @@ '18888888888' AS "18888888888", msg @@ '农业' AS "农业", msg @@ 'timeout' AS "timeout", msg @@ 'user' AS "user" FROM ft_en_tantivy ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ 'id' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ '手机号' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ '手机' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ '机号' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ '18888888888' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ 'timeout' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg @@ 'user' ORDER BY ts;

SELECT ts, msg LIKE '%world' AS "%world", msg LIKE 'hello\_world' AS "hello\_world", msg LIKE 'hello_world' AS "hello_world", msg LIKE '%id=%' AS "%id=%", msg LIKE 'trace%abc' AS "trace%abc", msg LIKE '%error%' AS "%error%", msg LIKE '%手机号1888%' AS "%手机号1888%", msg LIKE '%农业%' AS "%农业%", msg LIKE '连接timeout.%' AS "连接timeout.%", msg LIKE '%user-123%' AS "%user-123%" FROM ft_en_tantivy ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '%world' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE 'hello\_world' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE 'hello_world' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '%id=%' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE 'trace%abc' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '%error%' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '%手机号1888%' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '%农业%' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '连接timeout.%' ORDER BY ts;

SELECT ts, msg FROM ft_en_tantivy WHERE msg LIKE '%user-123%' ORDER BY ts;

DROP TABLE ft_en_tantivy;

CREATE TABLE ft_zh_tantivy (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH(analyzer = 'Chinese', backend = 'tantivy', case_sensitive = 'false')
);

INSERT INTO ft_zh_tantivy VALUES
    (1, 'hello_world'),
    (2, 'trace_id=abc'),
    (3, '错误error日志'),
    (4, '登录手机号18888888888的动态key'),
    (5, '中国农业银行'),
    (6, '连接timeout.5次'),
    (7, '用户user-123登录');

ADMIN flush_table('ft_zh_tantivy');

SELECT ts, msg @@ 'world' AS "world", msg @@ 'id' AS "id", msg @@ 'error' AS "error", msg @@ '手机号' AS "手机号", msg @@ '手机' AS "手机", msg @@ '机号' AS "机号", msg @@ '18888888888' AS "18888888888", msg @@ '农业' AS "农业", msg @@ 'timeout' AS "timeout", msg @@ 'user' AS "user" FROM ft_zh_tantivy ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ 'id' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ '手机号' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ '手机' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ '机号' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ '18888888888' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ 'timeout' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg @@ 'user' ORDER BY ts;

SELECT ts, msg LIKE '%world' AS "%world", msg LIKE 'hello\_world' AS "hello\_world", msg LIKE 'hello_world' AS "hello_world", msg LIKE '%id=%' AS "%id=%", msg LIKE 'trace%abc' AS "trace%abc", msg LIKE '%error%' AS "%error%", msg LIKE '%手机号1888%' AS "%手机号1888%", msg LIKE '%农业%' AS "%农业%", msg LIKE '连接timeout.%' AS "连接timeout.%", msg LIKE '%user-123%' AS "%user-123%" FROM ft_zh_tantivy ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '%world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE 'hello\_world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE 'hello_world' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '%id=%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE 'trace%abc' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '%error%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '%手机号1888%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '%农业%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '连接timeout.%' ORDER BY ts;

SELECT ts, msg FROM ft_zh_tantivy WHERE msg LIKE '%user-123%' ORDER BY ts;

DROP TABLE ft_zh_tantivy;
