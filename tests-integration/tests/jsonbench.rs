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
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::execute_sql_and_expect;

#[tokio::test]
async fn test_load_jsonbench_data() {
    common_telemetry::init_default_ut_logging();

    let instance = GreptimeDbStandaloneBuilder::new("test_load_jsonbench_data")
        .build()
        .await;
    let frontend = instance.fe_instance();

    create_table(frontend).await;

    desc_table(frontend).await;

    insert_data(frontend).await.unwrap();

    query_data(frontend).await.unwrap();
}

async fn query_data(frontend: &Arc<Instance>) -> io::Result<()> {
    let sql = "SELECT count(*) FROM bluesky";
    let expected = r#"
+----------+
| count(*) |
+----------+
| 10       |
+----------+
"#;
    execute_sql_and_expect(frontend, sql, expected).await;

    let sql = "SELECT * FROM bluesky ORDER BY ts";
    let expected = fs::read_to_string(find_workspace_path(
        "tests-integration/resources/jsonbench-select-all.txt",
    ))?;
    execute_sql_and_expect(frontend, sql, &expected).await;

    // query 1:
    let sql = "\
SELECT \
    json_get_string(data, '$.commit.collection') AS event, count() AS count \
FROM bluesky \
GROUP BY event \
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
    let sql = "\
SELECT \
    json_get_string(data, '$.commit.collection') AS event, \
    count() AS count, \
    count(DISTINCT json_get_string(data, '$.did')) AS users \
FROM bluesky \
WHERE \
    (json_get_string(data, '$.kind') = 'commit') AND \
    (json_get_string(data, '$.commit.operation') = 'create') \
GROUP BY event \
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

    Ok(())
}

async fn insert_data(frontend: &Arc<Instance>) -> io::Result<()> {
    let file = fs::File::open(find_workspace_path(
        "tests-integration/resources/jsonbench-head-10.ndjson",
    ))?;
    let reader = io::BufReader::new(file);
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let sql = format!(
            "INSERT INTO bluesky (ts, data) VALUES ({}, '{}')",
            i + 1,
            line.replace("'", "''"), // standard method to escape the single quote
        );
        execute_sql_and_expect(frontend, &sql, "Affected Rows: 1").await;
    }
    Ok(())
}

async fn desc_table(frontend: &Arc<Instance>) {
    let sql = "DESC TABLE bluesky";
    let expected = r#"
+--------+----------------------------------------------------------------------------------------------------------------------------------------------+-----+------+---------+---------------+
| Column | Type                                                                                                                                         | Key | Null | Default | Semantic Type |
+--------+----------------------------------------------------------------------------------------------------------------------------------------------+-----+------+---------+---------------+
| data   | Json<Object{"_raw": String, "commit.collection": String, "commit.operation": String, "did": String, "kind": String, "time_us": Number(I64)}> |     | YES  |         | FIELD         |
| ts     | TimestampMillisecond                                                                                                                         | PRI | NO   |         | TIMESTAMP     |
+--------+----------------------------------------------------------------------------------------------------------------------------------------------+-----+------+---------+---------------+"#;
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
  ts Timestamp TIME INDEX,
)
"#;
    execute_sql_and_expect(frontend, sql, "Affected Rows: 0").await;
}
