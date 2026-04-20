-- Test basic term matching
-- Expect: true
SELECT matches_term('cat!', 'cat') as result;

-- Test phrase matching with spaces
-- Expect: true
SELECT matches_term('warning:hello world!', 'hello world') as result;

-- Test numbers in term
SELECT matches_term('v1.0!', 'v1.0') as result;

-- Test case sensitivity
-- Expect: true
SELECT matches_term('Cat', 'Cat') as result;
-- Expect: false
SELECT matches_term('cat', 'Cat') as result;

-- Test empty string handling
-- Expect: true
SELECT matches_term('', '') as result;
-- Expect: false
SELECT matches_term('any', '') as result;
-- Expect: false
SELECT matches_term('', 'any') as result;

-- Test partial matches (should fail)
-- Expect: false
SELECT matches_term('category', 'cat') as result;
-- Expect: false
SELECT matches_term('rebooted', 'boot') as result;

-- Test adjacent alphanumeric characters
SELECT matches_term('cat5', 'cat') as result;
SELECT matches_term('dogcat', 'dog') as result;

-- Test leading non-alphanumeric
-- Expect: true
SELECT matches_term('dog/cat', '/cat') as result;
-- Expect: true
SELECT matches_term('dog/cat', 'dog/') as result;
-- Expect: true
SELECT matches_term('dog/cat', 'dog/cat') as result;

-- Test unicode characters
-- Expect: true
SELECT matches_term('café>', 'café') as result;
-- Expect: true
SELECT matches_term('русский!', 'русский') as result;

-- Phase 1 mixed Chinese and numeric behavior
SELECT matches_term('登录手机号18888888888的动态key', '手机号') as result;
SELECT matches_term('登录手机号18888888888的动态key', '18888888888') as result;
SELECT matches_term('登录手机号18888888888的动态key', '手机') as result;
SELECT matches_term('登录手机号18888888888的动态key', '机号') as result;
SELECT matches_term('登录手机号18888888888的动态key', '机号1888') as result;
SELECT matches_term('中国农业银行', '农业') as result;
SELECT matches_term('中国农业银行账号', '行账号') as result;
SELECT matches_term('错误error日志', 'error') as result;

-- Test complete word matching
CREATE TABLE logs (
    `id` TIMESTAMP TIME INDEX,
    `log_message` STRING
);

INSERT INTO logs VALUES
    (1, 'An error occurred!'),
    (2, 'Critical error: system failure'),
    (3, 'error-prone'),
    (4, 'errors'),
    (5, 'error123'),
    (6, 'errorLogs'),
    (7, 'Version v1.0 released'),
    (8, 'v1.0!'),
    (9, 'v1.0a'),
    (10, 'v1.0beta'),
    (11, 'GET /app/start'),
    (12, 'Command: /start-prosess'),
    (13, 'Command: /start'),
    (14, 'start'),
    (15, 'start/stop'),
    (16, 'Alert: system failure detected'),
    (17, 'system failure!'),
    (18, 'system-failure'),
    (19, 'system failure2023'),
    (20, 'critical error: system failure'),
    (21, 'critical failure detected'),
    (22, 'critical issue'),
    (23, 'failure imminent'),
    (24, 'Warning: high temperature'),
    (25, 'WARNING: system overload'),
    (26, 'warned'),
    (27, 'warnings');

-- Test complete word matching for 'error'
-- Expect:
-- 1|An error occurred!|true
-- 2|Critical error: system failure|true
-- 3|error-prone|true
-- 4|errors|false
-- 5|error123|false
-- 6|errorLogs|false
SELECT `id`, `log_message`, matches_term(`log_message`, 'error') as `matches_error` FROM logs WHERE `id` <= 6 ORDER BY `id`;


-- Test complete word matching for 'v1.0'
-- Expect:
-- 7|Version v1.0 released|true
-- 8|v1.0!|true
-- 9|v1.0a|false
-- 10|v1.0beta|false
SELECT `id`, `log_message`, matches_term(`log_message`, 'v1.0') as `matches_version` FROM logs WHERE `id` BETWEEN 7 AND 10 ORDER BY `id`;

-- Test complete word matching for '/start'
-- Expect:
-- 11|GET /app/start|true
-- 12|Command: /start-prosess|true
-- 13|Command: /start|true
-- 14|start|false
-- 15|start/stop|false
SELECT `id`, `log_message`, matches_term(`log_message`, '/start') as `matches_start` FROM logs WHERE `id` BETWEEN 11 AND 15 ORDER BY `id`;

-- Test phrase matching for 'system failure'
-- Expect:
-- 16|Alert: system failure detected|true
-- 17|system failure!|true
-- 18|system-failure|false
-- 19|system failure2023|false
SELECT `id`, `log_message`, matches_term(`log_message`, 'system failure') as `matches_phrase` FROM logs WHERE `id` BETWEEN 16 AND 19 ORDER BY `id`;


-- Test multi-word matching using AND
-- Expect:
-- 20|critical error: system failure|true|true|true
-- 21|critical failure detected|true|true|true
-- 22|critical issue|true|false|false
-- 23|failure imminent|false|true|false
SELECT `id`, `log_message`, 
       matches_term(`log_message`, 'critical') as `matches_critical`,
       matches_term(`log_message`, 'failure') as `matches_failure`,
       matches_term(`log_message`, 'critical') AND matches_term(`log_message`, 'failure') as `matches_both`
FROM logs WHERE `id` BETWEEN 20 AND 23 ORDER BY `id`;

-- Test case-insensitive matching using lower()
-- Expect:
-- 24|Warning: high temperature|true
-- 25|WARNING: system overload|true
-- 26|warned|false
-- 27|warnings|false
SELECT `id`, `log_message`, matches_term(lower(`log_message`), 'warning') as `matches_warning` FROM logs WHERE `id` >= 24 ORDER BY `id`;

DROP TABLE logs;

CREATE TABLE zh_logs (
    `id` TIMESTAMP TIME INDEX,
    `log_message` STRING FULLTEXT INDEX WITH(analyzer = 'Chinese', backend = 'bloom', case_sensitive = 'true', false_positive_rate = '0.01', granularity = '10240')
);

INSERT INTO zh_logs VALUES
    (1, '[2026/04/09/ 13:56:11.031]2026-04-09 13:56:11.031 - [ trace_id=340a6a44b0bd8e37bb7697ss7da61ff0 span_id=085ff5ttf1e0a23b trace_flags=01] - [http-nio-8081-exec-16] INFO c.h.p.xx.web.service.impl.CCCXForwardKKKServiceImpl.pushout(188) - 登录手机号18888888888的动态key：829889AC8'),
    (2, '哈基米曼波');

ADMIN flush_table('zh_logs');

SELECT * FROM zh_logs where `log_message` @@ 'trace_id';

SELECT * FROM zh_logs where `log_message` @@ '手机';

DROP TABLE zh_logs;
