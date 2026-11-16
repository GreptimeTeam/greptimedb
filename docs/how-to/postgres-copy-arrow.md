# Arrow bulk ingestion through PostgreSQL COPY

`COPY ... FROM STDIN WITH (FORMAT arrow)` accepts a standard **Arrow IPC
stream**, using the same decoding and bulk-ingestion handler as Arrow Flight
`DoPut`. It does not convert batches into protobuf rows or invoke pipelines.

```sql
CREATE TABLE readings (
    host STRING PRIMARY KEY,
    value DOUBLE,
    ts TIMESTAMP TIME INDEX
);

COPY readings FROM STDIN WITH (FORMAT arrow);
```

The COPY payload is the output of an Arrow IPC stream writer, such as Python's
`pyarrow.ipc.new_stream`. It is not an Arrow IPC file, serialized Flight protobuf
messages, or PostgreSQL's native binary COPY representation. The COPY handshake
advertises binary data; clients must send the IPC bytes unchanged rather than
using a PostgreSQL binary-row encoder.

For example, with Psycopg 3 and PyArrow:

```python
import psycopg
import pyarrow as pa

batch = pa.record_batch(
    [
        pa.array(["host-a", "host-b"]),
        pa.array([1.5, 2.5], type=pa.float64()),
        pa.array([1000, 2000], type=pa.timestamp("ms")),
    ],
    names=["host", "value", "ts"],
)

# This small example buffers the IPC stream locally. Larger producers can
# forward their stream writer's output incrementally to copy.write().
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, batch.schema) as writer:
    writer.write_batch(batch)

with psycopg.connect(
    "host=localhost port=4003 dbname=public user=greptime", autocommit=True
) as conn:
    with conn.cursor() as cursor:
        with cursor.copy("COPY readings FROM STDIN WITH (FORMAT arrow)") as copy:
            copy.write(sink.getvalue().to_pybytes())
```

## Target and schema

- The target table must already exist, as with Flight bulk ingestion.
- Qualified table names use the normal COPY catalog/schema resolution and
  permission checks.
- IPC field names identify columns; field order need not match table order.
  A COPY column list is rejected rather than silently ignored.
- One stream contains one schema followed by record batches and optional
  dictionary messages. Type, dictionary, default-value, and storage-engine
  restrictions remain those of the existing Flight bulk path. Accepting an
  Arrow IPC type does not imply the storage path supports it.
- Missing-column reconciliation runs once before the first batch, through the
  existing bulk schema-on-write implementation.

## Options

| COPY option | Flight equivalent | Behavior |
| --- | --- | --- |
| `auto_create_table TRUE/FALSE` | `auto_create_table` request hint | Controls schema-on-write according to the existing server policy. Bulk ingestion still requires an existing table; this can allow missing columns to be added. |
| `insert_skip_wal TRUE/FALSE` | `insert_skip_wal` request hint | Passes the WAL setting to the bulk writer. Skipping WAL has the same durability implications as Flight. |

A boolean option without a value means `TRUE`. Omitted options retain the
query context/server defaults. Options apply to this COPY only, not subsequent
statements. Unknown options, including pipeline options, are rejected.

LZ4/ZSTD compression is described inside IPC record-batch metadata and decoded
by the shared Flight decoder; no COPY compression option is needed.

## Streaming and completion

COPY messages may split IPC headers, schemas, or batch bodies at any byte.
Completed batches are written and acknowledged internally before subsequent
batches are processed. Encoded headers and bodies remain available to the bulk
inserter's single-region fast path; partition splitting uses the existing bulk
routing implementation.

COPY returns one final `COPY n` count. Flight request IDs, per-batch elapsed
seconds, and acknowledgements are not exposed on the PostgreSQL wire. Flight's
optional timestamp-range metadata is not part of the standard IPC stream and
is not used by the current frontend bulk writer.

This is not an atomic multi-batch transaction: if a later batch or truncated
stream fails, earlier completed writes remain. Failed or aborted COPY releases
its retained decoder/transport state. Clean EOF at a message boundary is
accepted, with or without the optional IPC end-of-stream marker; trailing bytes
after that marker are rejected.

Receive buffers and decompression expansion use the shared server memory
limiter with fail-fast admission, like Flight. Batch reservations are released
after acknowledgement, so total import size is not limited by the per-request
buffer budget. Schema and dictionary reservations remain for the stream's
lifetime; dictionary replacement is accounted conservatively.
