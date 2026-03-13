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

use std::io::BufRead;
use std::sync::Arc;
use std::{fs, io};

use common_test_util::find_workspace_path;
use frontend::instance::Instance;
use http::StatusCode;
use servers::http::test_helpers::TestClient;
use servers::http::{HTTP_SERVER, HttpServer};
use servers::server::ServerHandlers;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::execute_sql_and_expect;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_load_jsonbench_data_by_pipeline() -> io::Result<()> {
    common_telemetry::init_default_ut_logging();

    let instance = GreptimeDbStandaloneBuilder::new("test_load_jsonbench_data_by_pipeline")
        .build()
        .await;
    let frontend = instance.fe_instance();

    let ServerHandlers::Init(handlers) = instance.frontend.server_handlers() else {
        unreachable!()
    };
    let router = {
        let handlers = handlers.lock().unwrap();
        let server = handlers
            .get(HTTP_SERVER)
            .and_then(|x| x.0.as_any().downcast_ref::<HttpServer>())
            .unwrap();
        server.build(server.make_app()).unwrap()
    };
    let client = TestClient::new(router).await;

    create_table(frontend).await;

    desc_table(frontend).await;

    create_pipeline(&client).await;

    insert_data_by_pipeline(&client).await?;

    query_data(frontend).await
}

async fn insert_data_by_pipeline(client: &TestClient) -> io::Result<()> {
    let file = fs::read(find_workspace_path(
        "tests-integration/resources/jsonbench-head-10.ndjson",
    ))?;

    let response = client
        .post("/v1/ingest?table=bluesky&pipeline_name=jsonbench")
        .header("Content-Type", "text/plain")
        .body(file)
        .send()
        .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = response.text().await;
    // Note that this pattern also matches the inserted rows: "10".
    let pattern = r#"{"output":[{"affectedrows":10}]"#;
    assert!(response.starts_with(pattern));
    Ok(())
}

async fn create_pipeline(client: &TestClient) {
    let pipeline = r#"
version: 2

processors:
  - json_parse:
      fields:
        - message, data
      ignore_missing: true
  - simple_extract:
      fields:
        - data, time_us
      key: "time_us"
      ignore_missing: false
  - epoch:
      fields:
        - time_us
      resolution: microsecond
  - select:
      fields:
        - time_us
        - data

transform:
  - fields:
      - time_us
    type: epoch, us
    index: timestamp
"#;

    let response = client
        .post("/v1/pipelines/jsonbench")
        .header("Content-Type", "application/x-yaml")
        .body(pipeline)
        .send()
        .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = response.text().await;
    let pattern = r#"{"pipelines":[{"name":"jsonbench""#;
    assert!(response.starts_with(pattern));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_load_jsonbench_data_by_sql() -> io::Result<()> {
    common_telemetry::init_default_ut_logging();

    let instance = GreptimeDbStandaloneBuilder::new("test_load_jsonbench_data_by_sql")
        .build()
        .await;
    let frontend = instance.fe_instance();

    create_table(frontend).await;

    desc_table(frontend).await;

    insert_data_by_sql(frontend).await?;

    query_data(frontend).await
}

async fn query_data(frontend: &Arc<Instance>) -> io::Result<()> {
    let sql = "SELECT count(*) FROM bluesky";
    let expected = r#"
+----------+
| count(*) |
+----------+
| 10       |
+----------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    let sql = "SELECT * FROM bluesky ORDER BY time_us";
    let expected = fs::read_to_string(find_workspace_path(
        "tests-integration/resources/jsonbench-select-all.txt",
    ))?;
    execute_sql_and_expect(frontend, sql, &expected).await;

    // query 1:
    let sql = "
SELECT
    json_get_string(data, '$.commit.collection') AS event, count() AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC, event ASC";
    let expected = r#"
+-----------------------+-------+
| event                 | count |
+-----------------------+-------+
| app.bsky.feed.like    | 3     |
| app.bsky.feed.post    | 3     |
| app.bsky.graph.follow | 3     |
| app.bsky.feed.repost  | 1     |
+-----------------------+-------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    // query 2:
    let sql = "
SELECT
    json_get_string(data, '$.commit.collection') AS event,
    count() AS count,
    count(DISTINCT json_get_string(data, '$.did')) AS users
FROM bluesky
WHERE
    (json_get_string(data, '$.kind') = 'commit') AND
    (json_get_string(data, '$.commit.operation') = 'create')
GROUP BY event
ORDER BY count DESC, event ASC";
    let expected = r#"
+-----------------------+-------+-------+
| event                 | count | users |
+-----------------------+-------+-------+
| app.bsky.feed.like    | 3     | 3     |
| app.bsky.feed.post    | 3     | 3     |
| app.bsky.graph.follow | 3     | 3     |
| app.bsky.feed.repost  | 1     | 1     |
+-----------------------+-------+-------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    // query 3:
    let sql = "
SELECT
    json_get_string(data, '$.commit.collection') AS event,
    date_part('hour', to_timestamp_micros(json_get_int(data, '$.time_us'))) as hour_of_day,
    count() AS count
FROM bluesky
WHERE
    (json_get_string(data, '$.kind') = 'commit') AND
    (json_get_string(data, '$.commit.operation') = 'create') AND
    json_get_string(data, '$.commit.collection') IN
        ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event";
    let expected = r#"
+----------------------+-------------+-------+
| event                | hour_of_day | count |
+----------------------+-------------+-------+
| app.bsky.feed.like   | 16          | 3     |
| app.bsky.feed.post   | 16          | 3     |
| app.bsky.feed.repost | 16          | 1     |
+----------------------+-------------+-------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    // query 4:
    let sql = "
SELECT
    json_get_string(data, '$.did') as user_id,
    min(to_timestamp_micros(json_get_int(data, '$.time_us'))) AS first_post_ts
FROM bluesky
WHERE
    (json_get_string(data, '$.kind') = 'commit') AND
    (json_get_string(data, '$.commit.operation') = 'create') AND
    (json_get_string(data, '$.commit.collection') = 'app.bsky.feed.post')
GROUP BY user_id
ORDER BY first_post_ts ASC, user_id DESC
LIMIT 3";
    let expected = r#"
+----------------------------------+----------------------------+
| user_id                          | first_post_ts              |
+----------------------------------+----------------------------+
| did:plc:yj3sjq3blzpynh27cumnp5ks | 2024-11-21T16:25:49.000167 |
| did:plc:l5o3qjrmfztir54cpwlv2eme | 2024-11-21T16:25:49.001905 |
| did:plc:s4bwqchfzm6gjqfeb6mexgbu | 2024-11-21T16:25:49.003907 |
+----------------------------------+----------------------------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    // query 5:
    let sql = "
SELECT
    json_get_string(data, '$.did') as user_id,
    date_part(
        'epoch',
        max(to_timestamp_micros(json_get_int(data, '$.time_us'))) -
        min(to_timestamp_micros(json_get_int(data, '$.time_us')))
    ) AS activity_span
FROM bluesky
WHERE
    (json_get_string(data, '$.kind') = 'commit') AND
    (json_get_string(data, '$.commit.operation') = 'create') AND
    (json_get_string(data, '$.commit.collection') = 'app.bsky.feed.post')
GROUP BY user_id
ORDER BY activity_span DESC, user_id DESC
LIMIT 3";
    let expected = r#"
+----------------------------------+---------------+
| user_id                          | activity_span |
+----------------------------------+---------------+
| did:plc:yj3sjq3blzpynh27cumnp5ks | 0.0           |
| did:plc:s4bwqchfzm6gjqfeb6mexgbu | 0.0           |
| did:plc:l5o3qjrmfztir54cpwlv2eme | 0.0           |
+----------------------------------+---------------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    Ok(())
}

async fn insert_data_by_sql(frontend: &Arc<Instance>) -> io::Result<()> {
    let file = fs::File::open(find_workspace_path(
        "tests-integration/resources/jsonbench-head-10.ndjson",
    ))?;
    let reader = io::BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Value = serde_json::from_str(&line)?;
        let time_us = json.pointer("/time_us").and_then(|x| x.as_u64()).unwrap();

        let sql = format!(
            "INSERT INTO bluesky (time_us, data) VALUES ({}, '{}')",
            time_us,
            line.replace("'", "''"), // standard method to escape the single quote
        );
        execute_sql_and_expect(frontend, &sql, "Affected Rows: 1").await;
    }
    Ok(())
}

async fn desc_table(frontend: &Arc<Instance>) {
    let sql = "DESC TABLE bluesky";
    let expected = r#"
+---------+------------------------------------------------------------------------------------------------------------------------------------------------+-----+------+---------+---------------+
| Column  | Type                                                                                                                                           | Key | Null | Default | Semantic Type |
+---------+------------------------------------------------------------------------------------------------------------------------------------------------+-----+------+---------+---------------+
| data    | Json<{"_raw":"<String>","commit.collection":"<String>","commit.operation":"<String>","did":"<String>","kind":"<String>","time_us":"<Number>"}> |     | YES  |         | FIELD         |
| time_us | TimestampMicrosecond                                                                                                                           | PRI | NO   |         | TIMESTAMP     |
+---------+------------------------------------------------------------------------------------------------------------------------------------------------+-----+------+---------+---------------+"#;
    execute_sql_and_expect(frontend, sql, expected).await;
}

async fn create_table(frontend: &Arc<Instance>) {
    let sql = r#"
CREATE TABLE bluesky (
  "data" JSON (
    format = "partial",
    fields = Struct<
      kind String,
      "commit.operation" String,
      "commit.collection" String,
      did String,
      time_us Bigint
    >,
  ),
  time_us TimestampMicrosecond TIME INDEX,
)
"#;
    execute_sql_and_expect(frontend, sql, "Affected Rows: 0").await;
}
