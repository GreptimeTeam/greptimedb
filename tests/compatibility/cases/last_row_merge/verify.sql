SELECT host, ts, cpu, memory
FROM t_last_row_merge
ORDER BY host, ts;

SELECT host, cpu, memory
FROM t_last_row_merge
WHERE memory IS NULL
ORDER BY host;

SHOW CREATE TABLE t_last_row_merge;
