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

//! DDL execution for import.

use std::collections::HashSet;

use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_telemetry::info;
use servers::query_handler::sql::{MAX_LOGICAL_TABLE_DDL_BYTES, MAX_LOGICAL_TABLE_DDL_STATEMENTS};
use snafu::ResultExt;
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};

use crate::data::import_v2::error::{DatabaseSnafu, Result};
use crate::database::DatabaseClient;

/// A DDL statement with an explicit execution schema context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlStatement {
    pub sql: String,
    pub execution_schema: Option<String>,
}

impl DdlStatement {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            execution_schema: None,
        }
    }

    pub fn with_execution_schema(sql: String, schema: String) -> Self {
        Self {
            sql,
            execution_schema: Some(schema),
        }
    }
}

/// Executes DDL statements against the database.
pub struct DdlExecutor<'a> {
    client: &'a DatabaseClient,
}

impl<'a> DdlExecutor<'a> {
    /// Creates a new DDL executor.
    pub fn new(client: &'a DatabaseClient) -> Self {
        Self { client }
    }

    /// Executes DDL in order, stopping at the first failed request.
    pub async fn execute_strict(
        &self,
        statements: &[DdlStatement],
        batch_supported: bool,
    ) -> Result<()> {
        let mut batch = LogicalBatch::default();
        for (i, stmt) in statements.iter().enumerate() {
            info!("Executing DDL ({}/{})", i + 1, statements.len());
            let schema = stmt
                .execution_schema
                .as_deref()
                .unwrap_or(DEFAULT_SCHEMA_NAME);
            let candidate = if batch_supported {
                classify(stmt, self.client.catalog()).context(DatabaseSnafu)?
            } else {
                None
            };
            if let Some(candidate) = candidate {
                if !batch.accepts(&candidate, &stmt.sql) {
                    batch.flush(self.client).await?;
                }
                batch.push(candidate, &stmt.sql);
            } else {
                batch.flush(self.client).await?;
                self.client
                    .sql(&stmt.sql, schema)
                    .await
                    .context(DatabaseSnafu)?;
            }
        }
        batch.flush(self.client).await
    }
}

struct LogicalTable {
    catalog: String,
    execution_schema: String,
    schema: String,
    physical: String,
    name: String,
}

fn classify(stmt: &DdlStatement, catalog: &str) -> crate::error::Result<Option<LogicalTable>> {
    if stmt.sql.len() > MAX_LOGICAL_TABLE_DDL_BYTES {
        return Ok(None);
    }
    let parsed = parse_ddl(&stmt.sql)?;
    let [Statement::CreateTable(create)] = parsed.as_slice() else {
        return Ok(None);
    };
    let Some(physical) = create.options.get(LOGICAL_TABLE_METADATA_KEY) else {
        return Ok(None);
    };
    if create.engine != METRIC_ENGINE_NAME
        || create.options.value(PHYSICAL_TABLE_METADATA_KEY).is_some()
    {
        return Ok(None);
    }
    let parts: Vec<_> = create
        .name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.as_str()))
        .collect();
    let schema = stmt
        .execution_schema
        .as_deref()
        .unwrap_or(DEFAULT_SCHEMA_NAME);
    let (catalog, schema, name) = match parts.as_slice() {
        [name] => (catalog, schema, *name),
        [schema, name] => (catalog, *schema, *name),
        [catalog, schema, name] => (*catalog, *schema, *name),
        _ => return Ok(None),
    };
    Ok(Some(LogicalTable {
        catalog: catalog.into(),
        execution_schema: stmt
            .execution_schema
            .as_deref()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .into(),
        schema: schema.into(),
        physical: physical.into(),
        name: name.into(),
    }))
}

pub(crate) fn parse_ddl(sql: &str) -> crate::error::Result<Vec<Statement>> {
    ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default()).map_err(
        |_| {
            crate::error::InvalidArgumentsSnafu {
                msg: "cannot parse import DDL",
            }
            .build()
        },
    )
}

#[derive(Default)]
struct LogicalBatch {
    group: Option<LogicalTable>,
    names: HashSet<String>,
    sql: String,
}

impl LogicalBatch {
    fn accepts(&self, table: &LogicalTable, sql: &str) -> bool {
        self.group.as_ref().is_none_or(|g| {
            g.catalog == table.catalog
                && g.schema == table.schema
                && g.physical == table.physical
                && g.execution_schema == table.execution_schema
        }) && !self.names.contains(&table.name)
            && self.names.len() < MAX_LOGICAL_TABLE_DDL_STATEMENTS
            && self.sql.len() + (if self.sql.is_empty() { 0 } else { 2 }) + sql.len()
                <= MAX_LOGICAL_TABLE_DDL_BYTES
    }

    fn push(&mut self, table: LogicalTable, sql: &str) {
        if !self.sql.is_empty() {
            self.sql.push_str(";\n");
        }
        self.sql.push_str(sql);
        self.names.insert(table.name.clone());
        self.group = Some(table);
    }

    async fn flush(&mut self, client: &DatabaseClient) -> Result<()> {
        if let Some(group) = self.group.as_ref() {
            client
                .logical_tables(&self.sql, &group.execution_schema, self.names.len())
                .await
                .context(DatabaseSnafu)?;
            self.sql.clear();
            self.names.clear();
            self.group = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logical(name: &str, physical: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {name} (ts TIMESTAMP TIME INDEX) ENGINE=metric WITH(on_physical_table='{physical}')"
        )
    }

    #[test]
    fn classification_and_batch_boundaries() {
        let mut batch = LogicalBatch::default();
        for (name, context, physical, expected) in [
            ("A", "public", "p.q", true),
            ("public.b", "other", "p.q", false),
            ("greptime.public.c", "public", "p.q", true),
            ("a", "public", "p.q", false),
            ("\"A\"", "public", "p.q", true),
            ("d", "public", "p", false),
            ("d", "other", "p.q", false),
            ("other.public.d", "public", "p.q", false),
        ] {
            let sql = logical(name, physical);
            let stmt = DdlStatement::with_execution_schema(sql.clone(), context.into());
            let table = classify(&stmt, "greptime").unwrap().unwrap();
            assert_eq!(batch.accepts(&table, &sql), expected, "{name}");
            if expected {
                batch.push(table, &sql);
            }
        }
        for sql in [
            "CREATE DATABASE d",
            "CREATE TABLE t (ts TIMESTAMP TIME INDEX)",
            "CREATE TABLE p (ts TIMESTAMP TIME INDEX) ENGINE=metric WITH(physical_metric_table='')",
            "CREATE VIEW v AS SELECT * FROM a",
        ] {
            assert!(
                classify(&DdlStatement::new(sql.into()), "greptime")
                    .unwrap()
                    .is_none()
            );
        }
        let mut batch = LogicalBatch::default();
        for i in 0..128 {
            let sql = logical(&format!("t{i}"), "p");
            let table = classify(&DdlStatement::new(sql.clone()), "greptime")
                .unwrap()
                .unwrap();
            assert!(batch.accepts(&table, &sql));
            batch.push(table, &sql);
        }
        let table = classify(&DdlStatement::new(logical("next", "p")), "greptime")
            .unwrap()
            .unwrap();
        assert!(!batch.accepts(&table, &logical("next", "p")));
        batch.names.clear();
        batch.sql = "测".repeat(MAX_LOGICAL_TABLE_DDL_BYTES / 3 - 1);
        assert!(batch.accepts(&table, "ab"));
        assert!(!batch.accepts(&table, "测"));
        let oversized =
            DdlStatement::new(logical("large", &"x".repeat(MAX_LOGICAL_TABLE_DDL_BYTES)));
        assert!(classify(&oversized, "greptime").unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_ddl_uses_ordinary_sql_in_order() {
        let large = "x".repeat(MAX_LOGICAL_TABLE_DDL_BYTES);
        let statements = [
            DdlStatement::new(logical("before", "p")),
            DdlStatement::new(format!(
                "CREATE TABLE ordinary (ts TIMESTAMP TIME INDEX, payload STRING DEFAULT '{large}')"
            )),
            DdlStatement::new(logical("between", "p")),
            DdlStatement::new(format!("CREATE VIEW v AS SELECT '{large}' AS payload")),
            DdlStatement::new(logical("oversized", &large)),
            DdlStatement::new(logical("after", "p")),
        ];
        let (client, requests, server) = crate::database::tests::test_server(
            200,
            r#"{"execution_time_ms":0,"output":[{"affectedrows":0}]}"#,
        )
        .await;
        DdlExecutor::new(&client)
            .execute_strict(&statements, true)
            .await
            .unwrap();
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), statements.len());
            for (i, (request, statement)) in requests.iter().zip(&statements).enumerate() {
                let endpoint = if [0, 2, 5].contains(&i) {
                    "ddl/logical-tables"
                } else {
                    "sql"
                };
                assert!(request.starts_with(&format!("POST /v1/{endpoint} ")));
                let body = request.split_once("\r\n\r\n").unwrap().1;
                let sql = url::form_urlencoded::parse(body.as_bytes())
                    .find(|(key, _)| key == "sql")
                    .unwrap()
                    .1;
                assert_eq!(sql, statement.sql);
            }
        }
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn qualified_targets_preserve_request_context() {
        let (client, requests, server) = crate::database::tests::test_server(
            200,
            r#"{"execution_time_ms":0,"output":[{"affectedrows":0}]}"#,
        )
        .await;
        let statements = [
            DdlStatement::new(logical("other.metrics.a", "p")),
            DdlStatement::with_execution_schema(
                logical("other.metrics.b", "p"),
                "existing_schema".into(),
            ),
        ];
        DdlExecutor::new(&client)
            .execute_strict(&statements, true)
            .await
            .unwrap();
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            for (request, schema) in requests.iter().zip(["public", "existing_schema"]) {
                assert!(request.starts_with("POST /v1/ddl/logical-tables "));
                let body = request.split_once("\r\n\r\n").unwrap().1;
                let db = url::form_urlencoded::parse(body.as_bytes())
                    .find(|(k, _)| k == "db")
                    .unwrap()
                    .1;
                assert_eq!(db, format!("greptime-{schema}"));
            }
        }
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn transport_preserves_order_and_stops_without_fallback() {
        use crate::database::tests::test_server;
        let statements = [
            DdlStatement::new("CREATE DATABASE d".into()),
            DdlStatement::new(logical("a", "p").replace(
                "on_physical_table='p'",
                "on_physical_table='p', access_key_id='original-secret'",
            )),
            DdlStatement::new(logical("a", "p")),
            DdlStatement::new("CREATE VIEW v AS SELECT * FROM a".into()),
        ];
        for supported in [true, false] {
            let (client, requests, server) = test_server(
                200,
                r#"{"execution_time_ms":0,"output":[{"affectedrows":0}]}"#,
            )
            .await;
            DdlExecutor::new(&client)
                .execute_strict(&statements, supported)
                .await
                .unwrap();
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 4);
            for (i, request) in requests.iter().enumerate() {
                let path = if supported && (i == 1 || i == 2) {
                    "ddl/logical-tables"
                } else {
                    "sql"
                };
                assert!(request.starts_with(&format!("POST /v1/{path} ")));
                assert!(
                    request
                        .to_lowercase()
                        .contains("authorization: basic dxnlcjpwyxnzd29yza==")
                );
                let body = request.split_once("\r\n\r\n").unwrap().1;
                let form: std::collections::HashMap<_, _> =
                    url::form_urlencoded::parse(body.as_bytes())
                        .into_owned()
                        .collect();
                assert_eq!(form["sql"], statements[i].sql);
                assert_eq!(form["db"], "greptime-public");
            }
            server.abort();
        }
        let statements = [
            DdlStatement::new(logical("a", "p")),
            DdlStatement::new(logical("b", "p")),
            DdlStatement::new("CREATE VIEW v AS SELECT * FROM a".into()),
        ];
        for (status, body, success) in [
            (
                200,
                r#"{"execution_time_ms":0,"output":[{"affectedrows":0},{"affectedrows":0}]}"#,
                true,
            ),
            (
                200,
                r#"{"execution_time_ms":0,"output":[{"affectedrows":0}]}"#,
                false,
            ),
            (
                200,
                r#"{"execution_time_ms":0,"output":[{"affectedrows":0},{"affectedrows":1}]}"#,
                false,
            ),
            (200, r#"{"execution_time_ms":0,"output":[{},{}]}"#, false),
            (
                200,
                r#"{"execution_time_ms":0,"output":[{"affectedrows":0},{"error":"secret"}]}"#,
                false,
            ),
            (500, r#"{"error":"secret"}"#, false),
            (200, "invalid", false),
        ] {
            let (client, requests, server) = test_server(status, body).await;
            let result = DdlExecutor::new(&client)
                .execute_strict(&statements, true)
                .await;
            assert_eq!(result.is_ok(), success);
            assert_eq!(requests.lock().unwrap().len(), if success { 2 } else { 1 });
            if let Err(error) = result {
                assert!(!format!("{error:?}").contains("secret"));
            }
            server.abort();
            let _ = server.await;
        }
    }
}
