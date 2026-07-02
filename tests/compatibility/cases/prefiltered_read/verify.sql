SELECT host, site, val, cpu_load, active
FROM t_prefiltered_read
WHERE val >= 5 AND val <= 13
ORDER BY host;

SELECT host, val
FROM t_prefiltered_read
WHERE site = 'eu-west' AND active = true
ORDER BY host;

SELECT count(*) FROM t_prefiltered_read WHERE cpu_load > 0.40 AND host != 'host_e';

SELECT host
FROM t_prefiltered_read
WHERE site IN ('us-east', 'ap-south') AND val > 10
ORDER BY host;
