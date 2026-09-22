ADMIN BUILD_SERIES_INDEX();
ADMIN BUILD_SERIES_INDEX(1);
ADMIN BUILD_SERIES_INDEX('missing_series_table');

CREATE TABLE ordinary_series_target (ts TIMESTAMP TIME INDEX);
ADMIN BUILD_SERIES_INDEX('ordinary_series_target');
DROP TABLE ordinary_series_target;

CREATE TABLE series_physical_target (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE = metric
WITH (physical_metric_table = 'true');
-- SQLNESS REPLACE (region\s\d+\(\d+\,\s\d+\)) region
ADMIN BUILD_SERIES_INDEX('series_physical_target');
CREATE TABLE series_logical_target (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE = metric
WITH (on_physical_table = 'series_physical_target');
ADMIN BUILD_SERIES_INDEX('series_logical_target');
DROP TABLE series_logical_target;
DROP TABLE series_physical_target;
