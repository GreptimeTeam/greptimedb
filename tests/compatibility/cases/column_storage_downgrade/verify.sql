-- SQLNESS ARG expect_error=true
-- SQLNESS REPLACE \S+\.parquet <sst>
SELECT ts, length(payload) FROM external_blob ORDER BY ts;
