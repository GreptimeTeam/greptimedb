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

    // Default text format with escaped tab and `\N` NULLs, split into
    // several chunks across a row boundary.
    let rows = feed_copy_in(
        &client,
        "COPY copy_demo FROM STDIN",
        vec![
            Bytes::from_static(b"host3\t2"),
            Bytes::from_static(b".5\twith\\ttab\t2023-11-14 22:13:22\n"),
            Bytes::from_static(b"host4\t\\N\t\\N\t2023-11-14 22:13:23\n"),
        ],
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
    assert_eq!(rows.len(), 5);
    assert_eq!(host_of(rows[0], 0), "host1");
    assert_eq!(host_of(rows[0], 2), "hello, world");
    assert_eq!(host_of(rows[1], 0), "host2");
    // NULL val and note round-trip as NULL.
    assert_eq!(host_of(rows[3], 0), "host4");
    assert_eq!(host_of(rows[3], 2), "");
    assert_eq!(host_of(rows[4], 0), "");
    assert_eq!(host_of(rows[4], 2), "from_column_list");

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
            .contains("invalid input syntax"),
        "{err}"
    );

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
