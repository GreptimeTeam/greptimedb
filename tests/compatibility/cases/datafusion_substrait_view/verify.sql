SELECT host_name, metric_value, event_time
FROM persisted_scan_filter
ORDER BY host_name;

SELECT region_name, row_count, value_sum, value_avg
FROM persisted_grouped_aggregate
ORDER BY region_name;

SELECT host_name, region_name
FROM persisted_union
ORDER BY host_name, region_name;

SELECT host_name, metric_value, label_name
FROM persisted_inner_join
ORDER BY host_name;

SELECT region_name, host_name, event_time, row_num
FROM persisted_window
ORDER BY region_name, row_num;

SELECT host_name, event_day
FROM persisted_date_format
ORDER BY host_name;
