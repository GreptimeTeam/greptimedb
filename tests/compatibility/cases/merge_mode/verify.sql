SELECT host, ts, cpu, memory
FROM merge_mode
ORDER BY host, ts;

SELECT host, cpu
FROM merge_mode
WHERE memory >= 10
ORDER BY host;

SHOW CREATE TABLE merge_mode;
