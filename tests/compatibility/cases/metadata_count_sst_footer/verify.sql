SELECT count(*) AS total FROM t_metadata_count_footer;

SELECT host, v FROM t_metadata_count_footer ORDER BY host, ts;
