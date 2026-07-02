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

use std::path::Path;

use sqlx::{Connection, Executor, MySqlConnection, Row};
use tests_integration::test_util::{StorageType, setup_mysql_server};

type Json2Rows = Vec<(
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
)>;

#[tokio::test(flavor = "multi_thread")]
async fn test_json2_single_mem_range_flush() {
    common_telemetry::init_default_ut_logging();

    let (mut guard, server) =
        setup_mysql_server(StorageType::File, "test_json2_single_range_flush").await;
    let addr = server.bind_addr().unwrap();
    let mut conn = MySqlConnection::connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    create_json2_table(&mut conn, "json2_single_mem_range", 100)
        .await
        .unwrap();

    // One INSERT statement produces one bulk part, so flush sees exactly one
    // mem range. The rows still carry different JSON2 shapes, exercising
    // alignment inside that range.
    conn.execute(
        r#"
INSERT INTO json2_single_mem_range VALUES
    (1, 'host1', '{"payload":{"id":1},"metric":1}'),
    (2, 'host2', '{"payload":{"name":"n2"},"flag":true}')
"#,
    )
    .await
    .unwrap();
    conn.execute("ADMIN FLUSH_TABLE('json2_single_mem_range')")
        .await
        .unwrap();

    assert_eq!(
        vec![
            (
                "host1".to_string(),
                Some("1".to_string()),
                None,
                Some("1".to_string()),
                None,
            ),
            (
                "host2".to_string(),
                None,
                Some("n2".to_string()),
                None,
                Some("true".to_string())
            ),
        ],
        query_json2_rows(&mut conn, "json2_single_mem_range")
            .await
            .unwrap()
    );

    let _ = server.shutdown().await;
    guard.remove_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_json2_multi_mem_range_flush() {
    common_telemetry::init_default_ut_logging();

    let (mut guard, server) =
        setup_mysql_server(StorageType::File, "test_json2_multi_range_flush").await;
    let addr = server.bind_addr().unwrap();
    let mut conn = MySqlConnection::connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    create_json2_table(&mut conn, "json2_multiple_mem_ranges", 100)
        .await
        .unwrap();

    // Separate INSERT statements produce separate bulk parts. The high merge
    // threshold keeps them unmerged, so flush must align JSON2 schemas across
    // multiple mem ranges.
    conn.execute(
        r#"INSERT INTO json2_multiple_mem_ranges VALUES
    (1, 'host1', '{"payload":{"id":1},"metric":1}')"#,
    )
    .await
    .unwrap();
    conn.execute(
        r#"INSERT INTO json2_multiple_mem_ranges VALUES
    (2, 'host2', '{"payload":{"name":"n2"},"flag":true}')"#,
    )
    .await
    .unwrap();
    conn.execute("ADMIN FLUSH_TABLE('json2_multiple_mem_ranges')")
        .await
        .unwrap();

    assert_eq!(
        vec![
            (
                "host1".to_string(),
                Some("1".to_string()),
                None,
                Some("1".to_string()),
                None,
            ),
            (
                "host2".to_string(),
                None,
                Some("n2".to_string()),
                None,
                Some("true".to_string())
            ),
        ],
        query_json2_rows(&mut conn, "json2_multiple_mem_ranges")
            .await
            .unwrap()
    );

    conn.execute(
        r#"INSERT INTO json2_multiple_mem_ranges VALUES
    (3, 'host3', '{"payload":{"id":3,"name":"n3"},"metric":3,"flag":false}')"#,
    )
    .await
    .unwrap();
    conn.execute(
        r#"INSERT INTO json2_multiple_mem_ranges VALUES
    (4, 'host4', '{"payload":{"extra":"e4"},"metric":4}')"#,
    )
    .await
    .unwrap();
    conn.execute("ADMIN FLUSH_TABLE('json2_multiple_mem_ranges')")
        .await
        .unwrap();
    assert_eq!(2, count_parquet_files(guard.home_guard.temp_dir.path()));

    conn.execute("ADMIN COMPACT_TABLE('json2_multiple_mem_ranges')")
        .await
        .unwrap();

    assert_eq!(
        vec![
            (
                "host1".to_string(),
                Some("1".to_string()),
                None,
                Some("1".to_string()),
                None,
            ),
            (
                "host2".to_string(),
                None,
                Some("n2".to_string()),
                None,
                Some("true".to_string())
            ),
            (
                "host3".to_string(),
                Some("3".to_string()),
                Some("n3".to_string()),
                Some("3".to_string()),
                Some("false".to_string())
            ),
            ("host4".to_string(), None, None, Some("4".to_string()), None,),
        ],
        query_json2_rows(&mut conn, "json2_multiple_mem_ranges")
            .await
            .unwrap()
    );

    let _ = server.shutdown().await;
    guard.remove_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_json2_multi_row_insert() {
    common_telemetry::init_default_ut_logging();

    const NUM_ROWS: usize = 1024;
    const TABLE_NAME: &str = "json2_multi_row_insert";

    let (mut guard, server) =
        setup_mysql_server(StorageType::File, "test_json2_multi_row_insert").await;
    let addr = server.bind_addr().unwrap();
    let mut conn = MySqlConnection::connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    create_json2_compaction_table(&mut conn, TABLE_NAME)
        .await
        .unwrap();

    for i in 0..NUM_ROWS {
        let json = json2_payload(i);
        let sql = format!(
            r#"INSERT INTO {TABLE_NAME} VALUES
    ({}, 'host{}', '{}')"#,
            i + 1,
            i,
            json
        );
        conn.execute(sql.as_str()).await.unwrap();
    }

    assert_eq!(
        NUM_ROWS as i64,
        count_table_rows(&mut conn, TABLE_NAME).await.unwrap()
    );

    let _ = server.shutdown().await;
    guard.remove_all().await;
}

async fn create_json2_table(
    conn: &mut MySqlConnection,
    table_name: &str,
    merge_threshold: usize,
) -> sqlx::Result<()> {
    conn.execute(
        format!(
            r#"
CREATE TABLE {table_name} (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    j JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
    'memtable.type' = 'bulk',
    'memtable.bulk.merge_threshold' = '{merge_threshold}',
    'memtable.bulk.encode_row_threshold' = '1000000'
)
"#
        )
        .as_str(),
    )
    .await?;
    Ok(())
}

async fn create_json2_compaction_table(
    conn: &mut MySqlConnection,
    table_name: &str,
) -> sqlx::Result<()> {
    conn.execute(
        format!(
            r#"
CREATE TABLE {table_name} (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    j JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
    'memtable.type' = 'bulk',
    'memtable.bulk.merge_threshold' = '8',
    'memtable.bulk.encode_row_threshold' = '64',
    'memtable.bulk.encode_bytes_threshold' = '100000000'
)
"#
        )
        .as_str(),
    )
    .await?;
    Ok(())
}

async fn query_json2_rows(conn: &mut MySqlConnection, table_name: &str) -> sqlx::Result<Json2Rows> {
    let rows = sqlx::query(
        format!(
            r#"
SELECT
    host,
    j.payload.id::STRING AS payload_id,
    j.payload.name::STRING AS payload_name,
    j.metric::STRING AS metric,
    j.flag::STRING AS flag
FROM {table_name}
ORDER BY ts
"#
        )
        .as_str(),
    )
    .fetch_all(conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("host"),
                row.get::<Option<String>, _>("payload_id"),
                row.get::<Option<String>, _>("payload_name"),
                row.get::<Option<String>, _>("metric"),
                row.get::<Option<String>, _>("flag"),
            )
        })
        .collect())
}

async fn count_table_rows(conn: &mut MySqlConnection, table_name: &str) -> sqlx::Result<i64> {
    let row = sqlx::query(format!("SELECT COUNT(*) AS count FROM {table_name}").as_str())
        .fetch_one(conn)
        .await?;
    Ok(row.get("count"))
}

fn json2_payload(i: usize) -> String {
    match i % 4 {
        0 => format!(r#"{{"payload":{{"id":{i}}},"metric":{i}}}"#),
        1 => format!(r#"{{"payload":{{"name":"n{i}"}},"flag":true}}"#),
        2 => format!(r#"{{"payload":{{"score":{}.5}},"tags":["a","b"]}}"#, i),
        _ => format!(r#"{{"payload":{{"extra":"e{i}"}},"metric":{i},"flag":false}}"#),
    }
}

fn count_parquet_files(path: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };

    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .map(|path| {
            if path.is_dir() {
                count_parquet_files(&path)
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                1
            } else {
                0
            }
        })
        .sum()
}
