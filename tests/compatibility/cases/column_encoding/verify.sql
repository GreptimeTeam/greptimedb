SELECT * FROM legacy_spans ORDER BY ts;

CREATE TABLE encoded_spans (
    ts TIMESTAMP TIME INDEX,
    span_id STRING COMPRESSION WITH (encoding = 'plain', page_size = '64KiB'),
    ts_end TIMESTAMP(9) COMPRESSION WITH (encoding = 'delta_binary_packed')
);

INSERT INTO encoded_spans SELECT * FROM legacy_spans;

ADMIN FLUSH_TABLE('encoded_spans');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE encoded_spans;

INSERT INTO encoded_spans VALUES (3, 'b3', 3000);

ADMIN FLUSH_TABLE('encoded_spans');

SELECT * FROM encoded_spans ORDER BY ts;
