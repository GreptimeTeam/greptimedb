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

//! Bounded SHOW CREATE requests shared by both export commands.

use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use servers::http::GreptimeQueryOutput;
use servers::http::result::greptime_result_v1::GreptimedbV1Response;

use crate::data::sql::escape_sql_identifier;
use crate::database::DatabaseClient;
use crate::error::{InvalidArgumentsSnafu, Result, UnexpectedSnafu};

const MAX_STATEMENTS: usize = 128;
const MAX_SQL_BYTES: usize = 128 * 1024;

fn show_create_sql(kind: &str, catalog: &str, schema: &str, table: Option<&str>) -> String {
    let mut sql = format!(
        "SHOW CREATE {kind} \"{}\".\"{}\"",
        escape_sql_identifier(catalog),
        escape_sql_identifier(schema),
    );
    if let Some(table) = table {
        sql.push_str(&format!(".\"{}\"", escape_sql_identifier(table)));
    }
    sql.push_str(";\n");
    sql
}

fn next_batch(
    statements: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<Vec<String>> {
    let mut batch = Vec::new();
    let mut bytes = 0;
    while let Some(sql) = statements.peek() {
        if sql.len() > MAX_SQL_BYTES {
            return InvalidArgumentsSnafu {
                msg: format!(
                    "SHOW CREATE exceeds {MAX_SQL_BYTES} SQL bytes ({}): {sql}",
                    sql.len()
                ),
            }
            .fail();
        }
        if batch.len() == MAX_STATEMENTS || bytes + sql.len() > MAX_SQL_BYTES {
            break;
        }
        bytes += sql.len();
        if let Some(sql) = statements.next() {
            batch.push(sql);
        }
    }
    Ok(batch)
}

/// Appends DDL in dependency order supplied by the caller using bounded requests.
pub(crate) fn append_schema_ddl<'a>(
    client: &'a DatabaseClient,
    catalog: &'a str,
    schema: &'a str,
    objects: impl Iterator<Item = (&'static str, Option<&'a str>)> + Send + 'a,
    ddl: &'a mut String,
) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
    let mut statements = objects
        .map(move |(kind, table)| show_create_sql(kind, catalog, schema, table))
        .peekable();
    async move {
        loop {
            let batch = next_batch(&mut statements)?;
            if batch.is_empty() {
                return Ok(());
            }
            let response = client
                .sql_response(&batch.concat(), DEFAULT_SCHEMA_NAME)
                .await?;
            append_results(&batch, &response, ddl)?;
        }
    }
}

fn append_results(
    batch: &[String],
    response: &GreptimedbV1Response,
    ddl: &mut String,
) -> Result<()> {
    let outputs = response.output();
    if outputs.len() != batch.len() {
        return UnexpectedSnafu {
            msg: format!(
                "SHOW CREATE expected {} results, received {} for {}",
                batch.len(),
                outputs.len(),
                batch.concat()
            ),
        }
        .fail();
    }
    for (sql, output) in batch.iter().zip(outputs) {
        let GreptimeQueryOutput::Records(records) = output else {
            return UnexpectedSnafu {
                msg: format!("expected SHOW CREATE records for {sql}"),
            }
            .fail();
        };
        if records.num_cols() != 2 || records.rows().len() != 1 || records.rows()[0].len() != 2 {
            return UnexpectedSnafu {
                msg: format!("expected one two-column SHOW CREATE row for {sql}"),
            }
            .fail();
        }
        let row = &records.rows()[0];
        let Some(create) = row[1].as_str().filter(|value| !value.is_empty()) else {
            return UnexpectedSnafu {
                msg: format!("expected SHOW CREATE DDL string for {sql}"),
            }
            .fail();
        };
        if !row[0].is_string() {
            return UnexpectedSnafu {
                msg: format!("expected SHOW CREATE object name for {sql}"),
            }
            .fail();
        }
        ddl.push_str(create);
        ddl.push_str(";\n");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    use super::*;

    fn records(name: &str, ddl: &str) -> Value {
        json!({"records": {"schema": {"column_schemas": [
            {"name": "Table", "data_type": "String"},
            {"name": "Create Table", "data_type": "String"}
        ]}, "rows": [[name, ddl]], "total_rows": 1}})
    }

    #[test]
    fn batch_limits_include_quoted_utf8_and_separators() {
        let special = show_create_sql("TABLE", "cat.alog", "s\";库", Some("ta.\";表"));
        assert_eq!(
            special,
            "SHOW CREATE TABLE \"cat.alog\".\"s\"\";库\".\"ta.\"\";表\";\n"
        );
        let mut statements = std::iter::repeat_n(special.clone(), 129).peekable();
        assert_eq!(
            next_batch(&mut statements).unwrap(),
            vec![special.clone(); 128]
        );
        assert_eq!(next_batch(&mut statements).unwrap(), vec![special]);
        assert!(next_batch(&mut statements).unwrap().is_empty());

        let prefix = show_create_sql("TABLE", "c", "s", Some(""));
        // Every quote expands to two bytes; this also exercises a multibyte name.
        let name = format!("表{}", "\"".repeat((MAX_SQL_BYTES - prefix.len() - 3) / 2));
        let mut exact = show_create_sql("TABLE", "c", "s", Some(&name));
        let padding = MAX_SQL_BYTES - exact.len();
        exact = show_create_sql(
            "TABLE",
            "c",
            "s",
            Some(&format!("{name}{}", "a".repeat(padding))),
        );
        assert_eq!(exact.len(), MAX_SQL_BYTES);
        let small = show_create_sql("TABLE", "c", "s", Some("next"));
        let mut statements = [exact.clone(), small.clone()].into_iter().peekable();
        assert_eq!(next_batch(&mut statements).unwrap(), vec![exact.clone()]);
        assert_eq!(next_batch(&mut statements).unwrap(), vec![small]);
        let mut oversized = std::iter::once(format!("{exact} ")).peekable();
        assert!(
            next_batch(&mut oversized)
                .unwrap_err()
                .to_string()
                .contains("exceeds 131072")
        );

        let half = show_create_sql(
            "TABLE",
            "c",
            "s",
            Some(&"a".repeat(MAX_SQL_BYTES / 2 - prefix.len())),
        );
        let mut statements = [half.clone(), half.clone(), prefix.clone()]
            .into_iter()
            .peekable();
        assert_eq!(
            next_batch(&mut statements).unwrap(),
            vec![half.clone(), half]
        );
        assert_eq!(next_batch(&mut statements).unwrap(), vec![prefix]);
    }

    #[test]
    fn show_create_requires_exact_record_shape_and_count() {
        let batch = vec![
            "SHOW CREATE TABLE first".into(),
            "SHOW CREATE TABLE middle".into(),
        ];
        let valid = records("first", "CREATE TABLE first");
        for output in [
            vec![],
            vec![valid.clone()],
            vec![valid.clone(); 3],
            vec![valid.clone(), json!({"affectedrows": 1})],
            vec![valid.clone(), records("middle", "")],
            vec![valid.clone(), {
                let mut r = valid.clone();
                r["records"]["rows"] = json!([]);
                r
            }],
            vec![valid.clone(), {
                let mut r = valid.clone();
                r["records"]["rows"] = json!([["middle", 4]]);
                r
            }],
            vec![valid.clone(), {
                let mut r = valid.clone();
                r["records"]["rows"] = json!([["middle", "ddl", "extra"]]);
                r
            }],
        ] {
            let response =
                serde_json::from_value(json!({"output": output, "execution_time_ms": 0})).unwrap();
            let error = append_results(&batch, &response, &mut String::new()).unwrap_err();
            assert!(error.to_string().contains("middle"), "{error}");
        }
    }

    // A single accepted connection makes loss of pool reuse fail at the next read.
    async fn responses(
        bodies: Vec<(u16, String)>,
    ) -> (DatabaseClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut queries = Vec::new();
            for (status, body) in bodies {
                let mut headers = String::new();
                loop {
                    let mut line = String::new();
                    if socket.read_line(&mut line).await.unwrap() == 0 {
                        assert!(headers.is_empty());
                        return queries;
                    }
                    headers.push_str(&line);
                    if line == "\r\n" {
                        break;
                    }
                }
                let headers = headers.to_lowercase();
                assert!(headers.starts_with("post /v1/sql "));
                assert!(headers.contains("authorization: basic dxnlcjpwyxnzd29yza=="));
                assert!(headers.contains("x-greptime-timeout: 7s"));
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                let mut form = vec![0; length];
                socket.read_exact(&mut form).await.unwrap();
                let params: std::collections::HashMap<_, _> =
                    url::form_urlencoded::parse(&form).into_owned().collect();
                assert_eq!(params["db"], "catalog-public");
                queries.push(params["sql"].clone());
                socket
                    .get_mut()
                    .write_all(
                        format!(
                            "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            queries
        });
        let client = DatabaseClient::new(
            address.to_string(),
            "catalog".into(),
            Some("user:password".into()),
            std::time::Duration::from_secs(7),
            None,
            true,
        );
        (client, server)
    }

    #[tokio::test]
    async fn batches_preserve_order_context_and_reuse_connection() {
        let names: Vec<_> = (0..129).map(|i| format!("表.{i}\";x")).collect();
        let outputs: Vec<_> = names
            .iter()
            .map(|name| records(name, &format!("DDL {name}")))
            .collect();
        let bodies = outputs
            .chunks(128)
            .map(|chunk| {
                (
                    200,
                    json!({"output": chunk, "execution_time_ms": 0}).to_string(),
                )
            })
            .collect();
        let (client, server) = responses(bodies).await;
        let mut ddl = String::new();
        append_schema_ddl(
            &client.clone(),
            "catalog",
            "schema",
            names.iter().map(|name| ("TABLE", Some(name.as_str()))),
            &mut ddl,
        )
        .await
        .unwrap();
        assert_eq!(
            ddl,
            names
                .iter()
                .map(|name| format!("DDL {name};\n"))
                .collect::<String>()
        );
        let queries = server.await.unwrap();
        assert_eq!(queries.len(), 2);
        assert!(
            queries[0].starts_with("SHOW CREATE TABLE \"catalog\".\"schema\".\"表.0\"\";x\";\n")
        );
        assert!(queries[0].ends_with(".\"表.127\"\";x\";\n"));
        assert!(queries[1].ends_with(".\"表.128\"\";x\";\n"));
    }

    #[tokio::test]
    async fn legacy_command_stops_before_data_after_schema_failure() {
        use clap::Parser;

        use crate::data::export::ExportCommand;

        let output = |rows: Value, columns: usize| {
            json!({"records": {"schema": {"column_schemas": (0..columns)
                .map(|i| json!({"name": i.to_string(), "data_type": "String"}))
                .collect::<Vec<_>>()}, "rows": rows, "total_rows": rows.as_array().unwrap().len()}})
        };
        let databases = output(json!([["public"]]), 1);
        let mut malformed = records("middle", "CREATE TABLE middle");
        malformed["records"]["rows"][0][1] = json!(42);
        let bodies = vec![
            vec![databases.clone()],
            vec![records("public", "CREATE DATABASE public")],
            vec![databases.clone()],
            vec![output(json!([]), 3)],
            vec![output(
                json!([
                    ["catalog", "public", "first", "BASE TABLE"],
                    ["catalog", "public", "middle", "BASE TABLE"],
                    ["catalog", "public", "last", "BASE TABLE"]
                ]),
                4,
            )],
            vec![
                records("first", "CREATE TABLE first"),
                malformed,
                records("last", "CREATE TABLE last"),
            ],
            vec![databases],
            vec![json!({"affectedrows": 0})],
        ]
        .into_iter()
        .map(|output| {
            (
                200,
                json!({"output": output, "execution_time_ms": 0}).to_string(),
            )
        })
        .collect::<Vec<_>>();
        for target in ["schema", "all"] {
            let (client, server) = responses(bodies.clone()).await;
            let dir = tempfile::tempdir().unwrap();
            let command = ExportCommand::parse_from([
                "export",
                "--addr",
                client.addr(),
                "--database",
                "catalog-public",
                "--output-dir",
                dir.path().to_str().unwrap(),
                "--target",
                target,
                "--auth-basic",
                "user:password",
                "--timeout",
                "7s",
                "--no-proxy",
            ]);
            let tool = command.build().await.unwrap();
            let result = tool.do_work().await;
            drop(tool);
            let queries = server.await.unwrap();
            assert!(result.unwrap_err().to_string().contains("middle"));
            assert!(
                dir.path()
                    .join("catalog/public/create_database.sql")
                    .is_file()
            );
            assert!(!dir.path().join("catalog/public/create_tables.sql").exists());
            assert_eq!(queries.len(), 6);
            assert!(queries[5].contains("SHOW CREATE TABLE \"catalog\".\"public\".\"middle\""));
            assert!(!queries.iter().any(|sql| sql.starts_with("COPY")));
        }
    }

    #[tokio::test]
    async fn rejects_http_and_statement_errors_without_retry() {
        let valid = records("first", "CREATE TABLE first");
        let bodies = vec![
            (400, json!({"code": 3001, "error": "middle not found", "execution_time_ms": 0}).to_string()),
            (200, json!({"code": 3001, "output": [valid.clone(), valid.clone(), valid.clone()], "execution_time_ms": 0}).to_string()),
            (200, json!({"output": [valid.clone(), {"error": "middle denied"}, valid.clone()], "execution_time_ms": 0}).to_string()),
            (200, json!({"output": [valid.clone(), {"error": "middle denied", "records": valid["records"]}, valid], "execution_time_ms": 0}).to_string()),
            (200, json!({"output": [records("first", "DDL"), {"code": 3001, "records": records("middle", "DDL")["records"]}, records("last", "DDL")], "execution_time_ms": 0}).to_string()),
            (200, r#"{"execution_time_ms":0}"#.into()),
            (200, "not json".into()),
        ];
        let count = bodies.len();
        let (client, server) = responses(bodies).await;
        for _ in 0..count {
            let mut ddl = String::new();
            let error = append_schema_ddl(
                &client,
                "catalog",
                "schema",
                ["first", "middle", "last"]
                    .into_iter()
                    .map(|name| ("TABLE", Some(name))),
                &mut ddl,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("middle"), "{error}");
            assert!(ddl.is_empty());
        }
        assert_eq!(server.await.unwrap().len(), count);
    }
}
