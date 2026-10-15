CREATE TABLE encoded_spans_downgrade (
    ts TIMESTAMP TIME INDEX,
    span_id STRING COMPRESSION WITH (encoding = 'plain', page_size = '64KiB'),
    ts_end TIMESTAMP(9) COMPRESSION WITH (encoding = 'delta_binary_packed')
);

INSERT INTO encoded_spans_downgrade VALUES (1, 'a1', 1000), (2, 'a2', 2000), (3, NULL, NULL);

ADMIN FLUSH_TABLE('encoded_spans_downgrade');
