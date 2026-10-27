// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end tests for `COPY ... FROM STDIN` over the PostgreSQL protocol,
//! covering the full ingestion path (parser -> insert -> storage).

use bytes::Bytes;
use common_telemetry::init_default_ut_logging;
use futures::SinkExt;
use tokio_postgres::NoTls;

use crate::test_util::{StorageType, setup_pg_server};

async fn connect(port: u16) -> tokio_postgres::Client {
    let url = format!("host=127.0.0.1 port={port} dbname=public connect_timeout=5");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let _handle = tokio::spawn(conn);
    client
}

async fn feed_copy_in(
    client: &tokio_postgres::Client,
    sql: &str,
    chunks: Vec<Bytes>,
) -> std::result::Result<u64, tokio_postgres::Error> {
    let mut sink = Box::pin(client.copy_in(sql).await?);
    for chunk in chunks {
        sink.as_mut().send(chunk).await?;
    }
    sink.as_mut().finish().await
}

fn host_of(message: &tokio_postgres::SimpleQueryMessage, column: usize) -> String {
    let tokio_postgres::SimpleQueryMessage::Row(row) = message else {
        panic!("expected row, got {message:?}");
    };
    // simple_query renders NULL as `None`, not empty string.
    row.get(column).map(str::to_string).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_end_to_end() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_e2e").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE copy_demo (host STRING, val DOUBLE, note STRING, ts TIMESTAMP, TIME INDEX(ts))",
        )
        .await
        .unwrap();

    // CSV format with quoted field and NULL (unquoted empty) value.
    let rows = feed_copy_in(
        &client,
        "COPY copy_demo FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(
            b"host1,1.5,\"hello, world\",2023-11-14 22:13:20\nhost2,,x,2023-11-14 22:13:21\n",
        )],
    )
    .await
    .unwrap();
    assert_eq!(rows, 2);

    // A column list targeting a subset.
    let rows = feed_copy_in(
        &client,
        "COPY copy_demo (note, ts) FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(
            b"from_column_list,2023-11-14 22:13:24\n",
        )],
    )
    .await
    .unwrap();
    assert_eq!(rows, 1);

    // Verify data written through the real insert path.
    let messages = client
        .simple_query("SELECT host, val, note FROM copy_demo ORDER BY ts")
        .await
        .unwrap();
    let rows: Vec<_> = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(host_of(rows[0], 0), "host1");
    assert_eq!(host_of(rows[0], 2), "hello, world");
    assert_eq!(host_of(rows[1], 0), "host2");
    // NULL val and note round-trip as NULL.
    assert_eq!(host_of(rows[2], 0), "");
    assert_eq!(host_of(rows[2], 2), "from_column_list");

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_error_keeps_table_clean() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_error").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE copy_err (host STRING, val DOUBLE, ts TIMESTAMP, TIME INDEX(ts))",
        )
        .await
        .unwrap();

    // Bad double value aborts the copy.
    let err = match feed_copy_in(
        &client,
        "COPY copy_err FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(
            b"host1,not_a_double,2023-11-14 22:13:20\n",
        )],
    )
    .await
    {
        Ok(_) => panic!("expected COPY to fail"),
        Err(e) => e,
    };
    assert!(
        err.as_db_error()
            .expect("expected db error")
            .message()
            .contains("failed to parse"),
        "{err}"
    );

    // A bare COPY (PostgreSQL's text default) fails with guidance.
    let err = match client.copy_in::<_, Bytes>("COPY copy_err FROM STDIN").await {
        Ok(_) => panic!("expected bare COPY to fail"),
        Err(e) => e,
    };
    let message = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_string();
    assert!(message.contains("requires an explicit format"), "{message}");

    // The connection stays usable and no rows were written.
    let messages = client
        .simple_query("SELECT host FROM copy_err")
        .await
        .unwrap();
    assert!(
        messages
            .iter()
            .all(|m| !matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "expected no rows, got {messages:?}"
    );

    server.shutdown().await.unwrap();
}

/// Exercises the CSV format's options end-to-end through a Postgres
/// client driver: HEADER, custom delimiter, quoted fields with embedded
/// newlines and escaped quotes, CRLF record separators and NULL fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_csv_options() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_csv_options").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE copy_csv_opts (host STRING, val DOUBLE, note STRING, ts TIMESTAMP, TIME INDEX(ts))",
        )
        .await
        .unwrap();

    let rows = feed_copy_in(
        &client,
        // WITH omitted, matching PostgreSQL; HEADER skips the first record.
        "COPY copy_csv_opts FROM STDIN (FORMAT csv, HEADER, DELIMITER ';')",
        vec![Bytes::from_static(
            b"host;val;note;ts\r\n\
              h1;1.5;\"line one\nline two\";2023-11-14 22:13:20\r\n\
              h2;;\"say \"\"hi\"\"\";2023-11-14 22:13:21\r\n",
        )],
    )
    .await
    .unwrap();
    assert_eq!(rows, 2);

    // Verify the parsed values came through the real insert path.
    let messages = client
        .simple_query("SELECT host, val, note FROM copy_csv_opts ORDER BY ts")
        .await
        .unwrap();
    let rows: Vec<_> = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(host_of(rows[0], 0), "h1");
    assert_eq!(host_of(rows[0], 1), "1.5");
    // Quoted field with an embedded newline survives the round trip.
    assert_eq!(host_of(rows[0], 2), "line one\nline two");
    assert_eq!(host_of(rows[1], 0), "h2");
    // Unquoted empty field is NULL; escaped quotes decode to one quote.
    assert_eq!(host_of(rows[1], 1), "");
    assert_eq!(host_of(rows[1], 2), "say \"hi\"");

    server.shutdown().await.unwrap();
}

/// COPY OUT → COPY IN round trip: PostgreSQL's COPY OUT emits `t`/`f`
/// for booleans, which COPY IN must accept back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_bool_round_trip() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_bool_rt").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE bool_src (flag BOOLEAN, ts TIMESTAMP, TIME INDEX(ts));
             CREATE TABLE bool_dst (flag BOOLEAN, ts TIMESTAMP, TIME INDEX(ts));
             INSERT INTO bool_src VALUES (true, '2024-01-01 00:00:00'), (false, '2024-01-01 00:00:01'), (true, '2024-01-01 00:00:02');",
        )
        .await
        .unwrap();

    // COPY OUT: collect the exported bytes.
    let mut exported = Vec::new();
    let mut out = Box::pin(
        client
            .copy_out(
                "COPY (SELECT flag, ts FROM bool_src ORDER BY ts) TO STDOUT WITH (FORMAT csv)",
            )
            .await
            .unwrap(),
    );
    use futures::StreamExt;
    while let Some(chunk) = out.as_mut().next().await {
        exported.extend_from_slice(&chunk.unwrap());
    }
    // The wire encoder emits PostgreSQL boolean spellings.
    let text = String::from_utf8(exported.clone()).unwrap();
    assert!(text.contains("t,"), "COPY OUT should emit t/f, got: {text}");
    assert!(text.contains("f,"), "COPY OUT should emit t/f, got: {text}");

    // COPY IN: the exact bytes must import back.
    let rows = feed_copy_in(
        &client,
        "COPY bool_dst FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from(exported)],
    )
    .await
    .unwrap();
    assert_eq!(rows, 3);

    let messages = client
        .simple_query("SELECT flag FROM bool_dst ORDER BY ts")
        .await
        .unwrap();
    let flags: Vec<&str> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .collect();
    // The simple-query path also renders booleans in the t/f spelling.
    assert_eq!(flags, vec!["t", "f", "t"]);

    server.shutdown().await.unwrap();
}

/// A failed COPY must release its state so a later COPY on the same
/// connection works; this guards the error-path state drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_failed_copy_then_new_copy() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_fail_reuse").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE copy_reuse (host STRING, val DOUBLE, ts TIMESTAMP, TIME INDEX(ts))",
        )
        .await
        .unwrap();

    // A copy that fails at CopyDone (bad double parsed at flush time).
    let err = match feed_copy_in(
        &client,
        "COPY copy_reuse FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(
            b"host1,not_a_double,2024-01-01 00:00:00\n",
        )],
    )
    .await
    {
        Ok(_) => panic!("expected COPY to fail"),
        Err(e) => e,
    };
    assert!(err.as_db_error().is_some(), "{err}");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // The connection is usable and a NEW copy succeeds on it.
    let rows = feed_copy_in(
        &client,
        "COPY copy_reuse FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(b"host1,1.5,2024-01-01 00:00:01\n")],
    )
    .await
    .unwrap();
    assert_eq!(rows, 1);

    let messages = client
        .simple_query("SELECT host, val FROM copy_reuse")
        .await
        .unwrap();
    let found: Vec<_> = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(found.len(), 1);

    server.shutdown().await.unwrap();
}

/// COPY/INSERT parity in a non-UTC session: offset-free timestamp strings
/// must be interpreted in the session timezone by both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_session_timezone_parity() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_tz").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE copy_tz (case_id STRING PRIMARY KEY, ts TIMESTAMP, TIME INDEX(ts))",
        )
        .await
        .unwrap();

    client
        .simple_query("SET TIMEZONE TO 'Asia/Shanghai'")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO copy_tz VALUES ('insert', '2024-01-01 00:00:00')")
        .await
        .unwrap();
    let rows = feed_copy_in(
        &client,
        "COPY copy_tz (case_id, ts) FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(b"copy,2024-01-01 00:00:00\n")],
    )
    .await
    .unwrap();
    assert_eq!(rows, 1);

    let messages = client
        .simple_query("SELECT case_id, CAST(ts AS BIGINT) FROM copy_tz ORDER BY case_id")
        .await
        .unwrap();
    let results: Vec<(String, String)> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.to_string(), row.get(1)?.to_string()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 2, "{results:?}");
    assert_eq!(results[0].0, "copy");
    assert_eq!(results[1].0, "insert");
    // The whole point: both paths store the same instant.
    assert_eq!(
        results[0].1, results[1].1,
        "COPY and INSERT must agree on offset-free timestamps: {results:?}"
    );
    // And it is the Asia/Shanghai interpretation, not UTC.
    assert_eq!(results[0].1, "1704038400000");

    server.shutdown().await.unwrap();
}

/// COPY OUT -> COPY IN must preserve timestamp instants in non-UTC
/// sessions: COPY OUT renders timestamps with an explicit session-tz
/// offset, which the timezone-aware COPY IN decoder honors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_out_in_timestamp_round_trip() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_out_in_tz").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE tz_src (host STRING, ts TIMESTAMP, TIME INDEX(ts));
             CREATE TABLE tz_dst (host STRING, ts TIMESTAMP, TIME INDEX(ts));
             INSERT INTO tz_src VALUES ('a', '2024-01-01 00:00:00'), ('b', '2024-07-01 12:30:00');",
        )
        .await
        .unwrap();

    client
        .simple_query("SET TIMEZONE TO 'Asia/Shanghai'")
        .await
        .unwrap();

    let mut exported = Vec::new();
    let mut out = Box::pin(
        client
            .copy_out("COPY (SELECT host, ts FROM tz_src ORDER BY ts) TO STDOUT WITH (FORMAT csv)")
            .await
            .unwrap(),
    );
    use futures::StreamExt;
    while let Some(chunk) = out.as_mut().next().await {
        exported.extend_from_slice(&chunk.unwrap());
    }
    // The export now carries explicit offsets.
    let text = String::from_utf8(exported.clone()).unwrap();
    assert!(
        text.contains("+08"),
        "COPY OUT should render timestamps with the session-tz offset, got: {text}"
    );

    let rows = feed_copy_in(
        &client,
        "COPY tz_dst (host, ts) FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from(exported)],
    )
    .await
    .unwrap();
    assert_eq!(rows, 2);

    // Epochs must match the source exactly.
    let compare = client
        .simple_query(
            "SELECT (SELECT CAST(ts AS BIGINT) FROM tz_src WHERE host='a') = (SELECT CAST(ts AS BIGINT) FROM tz_dst WHERE host='a') \
             AND (SELECT CAST(ts AS BIGINT) FROM tz_src WHERE host='b') = (SELECT CAST(ts AS BIGINT) FROM tz_dst WHERE host='b')",
        )
        .await
        .unwrap();
    let ok: Vec<_> = compare
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(ok, vec!["t"], "epochs must round-trip exactly");

    server.shutdown().await.unwrap();
}

/// Empty fields — quoted or not — import as NULL, matching the
/// file-based COPY ... FROM 'file.csv' behavior exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_empty_field_is_null() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_empty").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE empty_str (note STRING, ts TIMESTAMP, TIME INDEX(ts))")
        .await
        .unwrap();

    let rows = feed_copy_in(
        &client,
        "COPY empty_str (note, ts) FROM STDIN WITH (FORMAT csv)",
        vec![Bytes::from_static(
            b"plain,2024-01-01 00:00:00\n\"\",2024-01-01 00:00:01\n,2024-01-01 00:00:02\n",
        )],
    )
    .await
    .unwrap();
    assert_eq!(rows, 3);

    let messages = client
        .simple_query("SELECT note IS NULL AS is_null, note FROM empty_str ORDER BY ts")
        .await
        .unwrap();
    let rows: Vec<(bool, Option<String>)> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0) == Some("t"), row.get(1).map(str::to_string)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (false, Some("plain".to_string())));
    // Quoted empty string: NULL, exactly like the file-based COPY route.
    assert_eq!(rows[1], (true, None));
    // Unquoted empty field: NULL.
    assert_eq!(rows[2], (true, None));

    server.shutdown().await.unwrap();
}

/// `COPY FROM STDIN WITH (FORMAT influxdb)` ingests line protocol through
/// the same pipeline as the HTTP influxdb write endpoint: lines route to
/// their own measurements, tables are auto-created, and a batch behaves
/// like an HTTP write of the same bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_influxdb_line_protocol() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_influxdb").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    let payload = b"monitor,host=hosta cpu=66.6,memory=1024 1663840496100023100\n\
                    monitor,host=hostb memory=1027 1663840496400340001\n\
                    weather,location=us-midwest temperature=82 1663840496100023102\n";

    // Feed byte-by-byte-ish: lines and UTF-8 may span CopyData chunks.
    let rows = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT influxdb)",
        payload.chunks(7).map(Bytes::copy_from_slice).collect(),
    )
    .await
    .unwrap();
    assert_eq!(rows, 3);

    let messages = client
        .simple_query("SELECT host, cpu, memory FROM monitor ORDER BY host")
        .await
        .unwrap();
    let rows: Vec<(String, String, String)> = messages
        .iter()
        .filter_map(|m| {
            let tokio_postgres::SimpleQueryMessage::Row(row) = m else {
                return None;
            };
            Some((
                row.get(0).unwrap_or("").to_string(),
                row.get(1).unwrap_or("").to_string(),
                row.get(2).unwrap_or("").to_string(),
            ))
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("hosta".into(), "66.6".into(), "1024.0".into()),
            ("hostb".into(), "".into(), "1027.0".into()),
        ]
    );

    let messages = client
        .simple_query("SELECT temperature FROM weather")
        .await
        .unwrap();
    let temps: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(temps, vec!["82.0".to_string()]);

    server.shutdown().await.unwrap();
}

/// The `precision` option carries the HTTP endpoint's semantics:
/// nanosecond timestamps by default, other units on request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_influxdb_precision() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_influxdb_prec").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    let rows = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT influxdb, precision 'ms')",
        vec![Bytes::from_static(b"prec,host=h v=1 1465839830100\n")],
    )
    .await
    .unwrap();
    assert_eq!(rows, 1);

    let messages = client
        .simple_query("SELECT CAST(greptime_timestamp AS BIGINT) FROM prec")
        .await
        .unwrap();
    let ts: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    // 1465839830100 ms == 1465839830100000000 ns
    assert_eq!(ts.len(), 1);
    let ts_ns: i64 = ts[0].parse().unwrap();
    assert_eq!(ts_ns, 1465839830100000000);

    server.shutdown().await.unwrap();
}

/// Records with newlines INSIDE quoted field values are one line-protocol
/// record: the parser owns record boundaries, so a CopyData chunk boundary
/// inside the quoted string — the case a naive line splitter breaks on —
/// must not split it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_influxdb_multiline_quoted_field() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_influxdb_ml").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    // A record whose string field value contains a physical newline, and
    // one with a backslash right before the newline (the parser skips the
    // escaped newline; also not a record boundary).
    let payload: Vec<u8> = b"mlog,host=a line=\"first\nsecond\" 100\n\
                              mlog,host=b line=\"tail\\\nmore\" 200\n"
        .to_vec();

    // Split exactly on the internal newline of the first record: chunk 1
    // ends after `first`, chunk 2 starts with the newline.
    let split_at = payload.windows(6).position(|w| w == b"first\n").unwrap() + b"first".len();
    let (head, tail) = payload.split_at(split_at);
    let rows = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT influxdb)",
        vec![Bytes::copy_from_slice(head), Bytes::copy_from_slice(tail)],
    )
    .await
    .unwrap();
    assert_eq!(rows, 2);

    let messages = client
        .simple_query("SELECT line FROM mlog ORDER BY host")
        .await
        .unwrap();
    let lines: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    // The quoted values keep their physical newlines: `first`, newline,
    // `second`; and `tail`, backslash, newline, `more` (a backslash only
    // escapes `\` and `"` in line-protocol strings, so it stays literal
    // before the newline).
    assert_eq!(
        lines,
        vec!["first\nsecond".to_string(), "tail\\\nmore".to_string()]
    );

    server.shutdown().await.unwrap();
}

/// The payload is bounded by the HTTP write endpoints' own body limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_influxdb_payload_limit() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_influxdb_cap").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    // The HTTP write endpoints' default body limit (64 MiB) — kept in
    // sync with servers::http::DEFAULT_BODY_LIMIT.
    let limit = 64 * 1024 * 1024;
    // Several chunks over the limit, none containing a record boundary.
    let chunk = vec![b'x'; 1024 * 1024];
    let mut sent = 0usize;
    let mut sink = Box::pin(
        client
            .copy_in("COPY FROM STDIN WITH (FORMAT influxdb)")
            .await
            .unwrap(),
    );
    while sent <= limit {
        sink.as_mut()
            .send(Bytes::from(chunk.clone()))
            .await
            .unwrap();
        sent += chunk.len();
    }
    // The cap fires mid-stream; the error surfaces when the copy
    // completes (the sink buffers sends locally).
    let error = sink
        .as_mut()
        .finish()
        .await
        .expect_err("expected the oversized payload to fail");
    assert!(
        error
            .as_db_error()
            .map(|d| d.message().contains("exceeds"))
            .unwrap_or(false),
        "unexpected error: {error}"
    );

    // The connection stays usable.
    let messages = client.simple_query("SELECT 1").await.unwrap();
    assert!(!messages.is_empty());

    server.shutdown().await.unwrap();
}

/// The regression for the retired line-splitter's failure mode: with
/// 32,767 ordinary records plus a multiline quoted record whose first
/// physical line lands exactly on the old 32,768-line flush threshold —
/// and a CopyData chunk boundary at the quoted value's internal newline —
/// the whole payload must still ingest as one request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_influxdb_flush_threshold_multiline() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_influxdb_thr").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    // 32,767 ordinary records, then a record whose quoted value spans
    // three physical lines.
    let mut head = String::new();
    for i in 0..32_767 {
        head.push_str(&format!(
            "m,host=h{i} v={i} {}
",
            1_000 + i
        ));
    }
    head.push_str("mlog,host=z line=\"first\nsecond\nthird\" 99999");
    let tail = "
"
    .to_string();

    let rows = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT influxdb)",
        vec![
            Bytes::from(head.into_bytes()),
            Bytes::from(tail.into_bytes()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(rows, 32_768);

    let messages = client.simple_query("SELECT line FROM mlog").await.unwrap();
    let lines: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(lines, vec!["first\nsecond\nthird".to_string()]);

    server.shutdown().await.unwrap();
}

/// Invalid UTF-8 fails the copy with a clear error and the connection
/// stays usable; a trailing record without a newline is still ingested.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_influxdb_invalid_utf8_and_trailing_record() {
    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_influxdb_utf8").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    // Trailing record without a newline: accepted.
    let rows = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT influxdb)",
        vec![Bytes::from_static(b"m2,host=a v=7 300")],
    )
    .await
    .unwrap();
    assert_eq!(rows, 1);

    // Invalid UTF-8: the copy fails, the connection stays usable.
    let error = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT influxdb)",
        vec![Bytes::from_static(b"m3 v=1 100\n\xff\xfe")],
    )
    .await
    .unwrap_err();
    assert!(
        error
            .as_db_error()
            .map(|d| d.message().contains("not valid UTF-8"))
            .unwrap_or(false),
        "unexpected error: {error}"
    );
    let messages = client.simple_query("SELECT 1").await.unwrap();
    assert!(!messages.is_empty());

    server.shutdown().await.unwrap();
}

/// `COPY FROM STDIN WITH (FORMAT otlp)` ingests one binary OTLP
/// ExportMetricsServiceRequest through the same pipeline as the OTLP/HTTP
/// endpoint — and validates binary (format code 1) COPY support, including
/// payloads split across CopyData chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_copy_in_otlp_binary() {
    use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_rust::proto::opentelemetry::common::v1::any_value;
    use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, KeyValue};
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::number_data_point::Value;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    };
    use prost::Message;

    init_default_ut_logging();
    let (_guard, server) = setup_pg_server(StorageType::File, "pg_copy_in_otlp").await;
    let port = server.bind_addr().unwrap().port();
    let client = connect(port).await;

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "copy_otlp_gauge".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![
                            NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(Value::AsDouble(1.5)),
                                attributes: vec![KeyValue {
                                    key: "host".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("a".into())),
                                    }),
                                }],
                                ..Default::default()
                            },
                            NumberDataPoint {
                                time_unix_nano: 1_700_000_001_000_000_000,
                                value: Some(Value::AsDouble(2.5)),
                                attributes: vec![KeyValue {
                                    key: "host".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("b".into())),
                                    }),
                                }],
                                ..Default::default()
                            },
                        ],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body = request.encode_to_vec();

    // Feed the binary payload in small chunks: the message spans CopyData
    // boundaries exactly like a large text payload would.
    let rows = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT otlp)",
        body.chunks(9).map(Bytes::copy_from_slice).collect(),
    )
    .await
    .unwrap();
    assert_eq!(rows, 2);

    let messages = client
        .simple_query("SELECT host, greptime_value FROM copy_otlp_gauge ORDER BY host")
        .await
        .unwrap();
    let rows: Vec<(String, String)> = messages
        .iter()
        .filter_map(|m| {
            let tokio_postgres::SimpleQueryMessage::Row(row) = m else {
                return None;
            };
            Some((
                row.get(0).unwrap_or("").to_string(),
                row.get(1).unwrap_or("").to_string(),
            ))
        })
        .collect();
    assert_eq!(
        rows,
        vec![("a".into(), "1.5".into()), ("b".into(), "2.5".into())]
    );

    // Garbage bytes fail the copy and the connection stays usable.
    let error = feed_copy_in(
        &client,
        "COPY FROM STDIN WITH (FORMAT otlp)",
        vec![Bytes::from_static(b"\xff\xff\xff\xff")],
    )
    .await
    .unwrap_err();
    assert!(
        error
            .as_db_error()
            .map(|d| d.message().contains("invalid OTLP payload"))
            .unwrap_or(false),
        "unexpected error: {error}"
    );
    let messages = client.simple_query("SELECT 1").await.unwrap();
    assert!(!messages.is_empty());

    server.shutdown().await.unwrap();
}
