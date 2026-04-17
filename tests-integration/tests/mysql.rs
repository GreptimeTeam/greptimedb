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

use itertools::Itertools;
use sqlx::mysql::{MySqlQueryResult, MySqlRow};
use sqlx::{Connection, MySqlConnection, Row};
use tests_integration::test_util::{StorageType, setup_mysql_server};
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_mysql_multiple_statement_execution_support() -> sqlx::Result<()> {
    let (mut guard, server) = setup_mysql_server(
        StorageType::File,
        "test_mysql_multiple_statement_execution_support",
    )
    .await;
    let addr = server.bind_addr().unwrap();
    let mut conn = MySqlConnection::connect(&format!("mysql://{addr}/public")).await?;

    let query = "create table foo (ts timestamp time index, i int)";
    let result = sqlx::raw_sql(query).execute(&mut conn).await?;
    assert_eq!(result.rows_affected(), 0);

    fn to_string(result: either::Either<MySqlQueryResult, MySqlRow>) -> String {
        match result {
            either::Left(result) => {
                format!("OK packet (rows affected: {})", result.rows_affected())
            }
            either::Right(result) => {
                format!(
                    "Row: [{}]",
                    (0..result.columns().len())
                        .map(|i| {
                            let i: i64 = result.get(i);
                            i.to_string()
                        })
                        .join(", ")
                )
            }
        }
    }

    let query = "insert into foo values (1, 1); select i from foo";
    let results = sqlx::raw_sql(query)
        .fetch_many(&mut conn)
        .collect::<sqlx::Result<Vec<_>>>()
        .await?
        .into_iter()
        .map(to_string)
        .join("\n");
    let expected = r#"
OK packet (rows affected: 1)
Row: [1]
OK packet (rows affected: 0)
"#;
    assert_eq!(results, expected.trim());

    let query = "insert into foo values (2, 2); insert into foo values (3, 3)";
    let results = sqlx::raw_sql(query)
        .fetch_many(&mut conn)
        .collect::<sqlx::Result<Vec<_>>>()
        .await?
        .into_iter()
        .map(to_string)
        .join("\n");
    let expected = r#"
OK packet (rows affected: 1)
OK packet (rows affected: 1)
"#;
    assert_eq!(results, expected.trim());

    let query = "select i from foo order by i; select sum(i) from foo";
    let results = sqlx::raw_sql(query)
        .fetch_many(&mut conn)
        .collect::<sqlx::Result<Vec<_>>>()
        .await?
        .into_iter()
        .map(to_string)
        .join("\n");
    let expected = r#"
Row: [1]
Row: [2]
Row: [3]
OK packet (rows affected: 0)
Row: [6]
OK packet (rows affected: 0)
"#;
    assert_eq!(results, expected.trim());

    let query = "select i from foo; select i from bar";
    let result = sqlx::raw_sql(query)
        .fetch_many(&mut conn)
        .collect::<sqlx::Result<Vec<_>>>()
        .await
        .unwrap_err()
        .to_string();
    let expected = r#"error returned from database: 1146 (42S02): (TableNotFound): Failed to plan SQL: Table not found: greptime.public.bar"#;
    assert_eq!(result, expected);

    let query = "select i from bar; select i from foo";
    let result = sqlx::raw_sql(query)
        .fetch_many(&mut conn)
        .collect::<sqlx::Result<Vec<_>>>()
        .await
        .unwrap_err()
        .to_string();
    let expected = r#"error returned from database: 1146 (42S02): (TableNotFound): Failed to plan SQL: Table not found: greptime.public.bar"#;
    assert_eq!(result, expected);

    let _ = server.shutdown().await;
    guard.remove_all().await;
    Ok(())
}
