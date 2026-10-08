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

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use humantime::format_duration;
use serde_json::Value;
use servers::http::GreptimeQueryOutput;
use servers::http::header::constants::GREPTIME_DB_HEADER_TIMEOUT;
use servers::http::result::greptime_result_v1::GreptimedbV1Response;
use snafu::ResultExt;

use crate::error::{BuildClientSnafu, HttpQuerySqlSnafu, ParseProxyOptsSnafu, Result};

#[derive(Debug, Clone)]
pub struct DatabaseClient {
    addr: String,
    catalog: String,
    auth_header: Option<String>,
    timeout: Duration,
    proxy: Option<reqwest::Proxy>,
    no_proxy: bool,
    client: Arc<tokio::sync::OnceCell<reqwest::Client>>,
}

pub fn parse_proxy_opts(
    proxy: Option<String>,
    no_proxy: bool,
) -> std::result::Result<Option<reqwest::Proxy>, BoxedError> {
    if no_proxy {
        return Ok(None);
    }
    proxy
        .map(|proxy| {
            reqwest::Proxy::all(proxy)
                .context(ParseProxyOptsSnafu)
                .map_err(BoxedError::new)
        })
        .transpose()
}

impl DatabaseClient {
    pub fn new(
        addr: String,
        catalog: String,
        auth_basic: Option<String>,
        timeout: Duration,
        proxy: Option<reqwest::Proxy>,
        no_proxy: bool,
    ) -> Self {
        let auth_header = if let Some(basic) = auth_basic {
            let encoded = general_purpose::STANDARD.encode(basic);
            Some(format!("basic {}", encoded))
        } else {
            None
        };

        if no_proxy {
            common_telemetry::info!("Proxy disabled");
        } else if let Some(ref proxy) = proxy {
            common_telemetry::info!("Using proxy: {:?}", proxy);
        } else {
            common_telemetry::info!("Using system proxy(if any)");
        }

        Self {
            addr,
            catalog,
            auth_header,
            timeout,
            proxy,
            no_proxy,
            client: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub async fn sql_in_public(&self, sql: &str) -> Result<Option<Vec<Vec<Value>>>> {
        self.sql(sql, DEFAULT_SCHEMA_NAME).await
    }

    pub(crate) fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Requires packed export support before creating snapshot artifacts.
    pub async fn require_packed_export(&self) -> Result<()> {
        self.capabilities().await?.require("metric_packed_export")
    }

    /// Reads target capabilities once; older targets advertise no extensions.
    pub(crate) async fn capabilities(&self) -> Result<Capabilities> {
        let url = format!("http://{}/v1/capabilities", self.addr);
        let mut request = self.http_client().await?.get(&url).timeout(self.timeout);
        if let Some(auth) = &self.auth_header {
            request = request.header("Authorization", auth);
        }
        let response = request.send().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "capability request failed",
        })?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Capabilities(serde_json::json!({})));
        }
        let response = response
            .error_for_status()
            .with_context(|_| HttpQuerySqlSnafu {
                reason: "capability request rejected",
            })?;
        let body = response.text().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "cannot read capability response",
        })?;
        let value: Value = serde_json::from_str(&body).map_err(|_| {
            crate::error::UnexpectedSnafu {
                msg: "invalid capability response",
            }
            .build()
        })?;
        if !value.is_object()
            || value.get("error").is_some()
            || value.get("code").is_some_and(|v| v.as_u64() != Some(0))
        {
            return crate::error::UnexpectedSnafu {
                msg: "invalid capability response: expected JSON object",
            }
            .fail();
        }
        for key in [
            "metric_batch_ddl",
            "metric_packed_import",
            "metric_packed_export",
        ] {
            if value.get(key).is_some_and(|v| v.as_u64().is_none()) {
                return crate::error::UnexpectedSnafu {
                    msg: "invalid capability version",
                }
                .fail();
            }
        }
        Ok(Capabilities(value))
    }

    /// Submits a batch once. An ambiguous failure must not be replayed here.
    pub(crate) async fn logical_tables(&self, sql: &str, schema: &str, count: usize) -> Result<()> {
        let db = format!("{}-{}", self.catalog, schema);
        if 8 + form_encoded_len(&db) + form_encoded_len(sql) > 4 * 1024 * 1024 {
            return crate::error::InvalidArgumentsSnafu {
                msg: "logical-table batch exceeds 4 MiB form limit",
            }
            .fail();
        }
        let response = self.ddl_response(sql, schema, "ddl/logical-tables").await?;
        if response.output().len() != count
            || response
                .output()
                .iter()
                .any(|output| !matches!(output, GreptimeQueryOutput::AffectedRows(0)))
        {
            return crate::error::UnexpectedSnafu {
                msg: "invalid logical-table batch output",
            }
            .fail();
        }
        Ok(())
    }

    /// Execute sql query.
    pub async fn sql(&self, sql: &str, schema: &str) -> Result<Option<Vec<Vec<Value>>>> {
        let body = self.sql_response(sql, schema).await?;
        Ok(body.output().first().and_then(|output| match output {
            GreptimeQueryOutput::Records(records) => Some(records.rows().clone()),
            GreptimeQueryOutput::AffectedRows(_) => None,
        }))
    }

    pub(crate) async fn sql_response(
        &self,
        sql: &str,
        schema: &str,
    ) -> Result<GreptimedbV1Response> {
        self.ddl_response(sql, schema, "sql").await
    }

    async fn ddl_response(
        &self,
        sql: &str,
        schema: &str,
        endpoint: &str,
    ) -> Result<GreptimedbV1Response> {
        let url = format!("http://{}/v1/{endpoint}", self.addr);
        let params = [
            ("db", format!("{}-{}", self.catalog, schema)),
            ("sql", sql.to_string()),
        ];
        let mut request = self
            .http_client()
            .await?
            .post(&url)
            .form(&params)
            .header("Content-Type", "application/x-www-form-urlencoded");
        if endpoint == "ddl/logical-tables" {
            request = request.timeout(self.timeout);
        }
        if let Some(ref auth) = self.auth_header {
            request = request.header("Authorization", auth);
        }

        request = request.header(
            GREPTIME_DB_HEADER_TIMEOUT,
            format_duration(self.timeout).to_string(),
        );

        let response = request.send().await.with_context(|_| HttpQuerySqlSnafu {
            reason: format!("bad url: {}", url),
        })?;
        let status = response.status();
        let text = response.text().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "cannot get response text".to_string(),
        })?;
        let value: Value = serde_json::from_str(&text).map_err(|_| {
            crate::error::UnexpectedSnafu {
                msg: format!("invalid SQL response ({status})"),
            }
            .build()
        })?;
        if !status.is_success()
            || value.get("error").is_some()
            || value
                .get("code")
                .is_some_and(|code| code.as_u64() != Some(0))
        {
            return crate::error::UnexpectedSnafu {
                msg: format!(
                    "SQL request failed ({status}, code {:?})",
                    value.get("code").and_then(Value::as_u64)
                ),
            }
            .fail();
        }
        if let Some(outputs) = value.get("output").and_then(Value::as_array) {
            for (index, output) in outputs.iter().enumerate() {
                if output.get("error").is_some()
                    || output
                        .get("code")
                        .is_some_and(|code| code.as_u64() != Some(0))
                {
                    return crate::error::UnexpectedSnafu {
                        msg: format!(
                            "SQL statement {} failed (code {:?})",
                            index + 1,
                            output.get("code").and_then(Value::as_u64)
                        ),
                    }
                    .fail();
                }
            }
        }
        serde_json::from_value(value).map_err(|_| {
            crate::error::UnexpectedSnafu {
                msg: format!("invalid SQL response ({status})"),
            }
            .build()
        })
    }

    async fn http_client(&self) -> Result<&reqwest::Client> {
        self.client
            .get_or_try_init(|| async {
                let mut builder = reqwest::Client::builder();
                if let Some(proxy) = self.proxy.clone() {
                    builder = builder.proxy(proxy);
                }
                if self.no_proxy {
                    builder = builder.no_proxy();
                }
                builder.build().context(BuildClientSnafu)
            })
            .await
    }
}

fn form_encoded_len(value: &str) -> usize {
    value
        .bytes()
        .map(|b| match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' | b' ' => 1,
            _ => 3,
        })
        .sum()
}

pub(crate) struct Capabilities(Value);

impl Capabilities {
    pub(crate) fn supports(&self, capability: &str) -> bool {
        self.0.get(capability).and_then(Value::as_u64) == Some(1)
    }

    pub(crate) fn require(&self, capability: &str) -> Result<()> {
        if !self.supports(capability) {
            return crate::error::InvalidArgumentsSnafu {
                msg: format!("server does not support {capability} version 1"),
            }
            .fail();
        }
        Ok(())
    }
}

/// Split at `-`.
pub(crate) fn split_database(database: &str) -> Result<(String, Option<String>)> {
    let (catalog, schema) = match database.split_once('-') {
        Some((catalog, schema)) => (catalog, schema),
        None => (DEFAULT_CATALOG_NAME, database),
    };

    if schema == "*" {
        Ok((catalog.to_string(), None))
    } else {
        Ok((catalog.to_string(), Some(schema.to_string())))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    #[tokio::test]
    async fn packed_capability_probe_checks_auth_and_protocol_version() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for export in [false, true] {
            for (status, body, supported) in [
                (200, r#"{"metric_packed_import":1,"future":2}"#, true),
                (200, "{}", false),
                (200, r#"{"metric_packed_import":2}"#, false),
                (404, "{}", false),
                (401, "{}", false),
                (403, "{}", false),
                (200, "[]", false),
                (200, "invalid-json", false),
            ] {
                let body = if export {
                    body.replace("metric_packed_import", "metric_packed_export")
                } else {
                    body.to_string()
                };
                let response_body = body.clone();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let body = response_body;
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = vec![0; 4096];
                    let n = socket.read(&mut request).await.unwrap();
                    let request = String::from_utf8_lossy(&request[..n]).to_lowercase();
                    assert!(request.starts_with("get /v1/capabilities "));
                    assert!(request.contains("authorization: basic dxnlcjpwyxnzd29yza=="));
                    socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                });
                let client = super::DatabaseClient::new(
                    address.to_string(),
                    "greptime".into(),
                    Some("user:password".into()),
                    std::time::Duration::from_secs(5),
                    None,
                    true,
                );
                assert_eq!(
                    if export {
                        client.require_packed_export().await.is_ok()
                    } else {
                        client
                            .capabilities()
                            .await
                            .and_then(|c| c.require("metric_packed_import"))
                            .is_ok()
                    },
                    supported,
                    "{status}: {body}"
                );
                server.await.unwrap();
            }
        }
    }

    use super::*;

    #[test]
    fn form_size_counts_utf8_and_escaping() {
        for (db, sql) in [
            ("greptime-public", "SELECT 1"),
            ("测-试", "+&='秘密';\n"),
            ("a_b.*", "a~b"),
        ] {
            let encoded = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("db", db)
                .append_pair("sql", sql)
                .finish();
            assert_eq!(
                8 + form_encoded_len(db) + form_encoded_len(sql),
                encoded.len()
            );
        }
    }

    #[tokio::test]
    async fn batch_capabilities_distinguish_absence_from_errors() {
        for (status, body, expected) in [
            (200, r#"{"metric_batch_ddl":1}"#, Some(true)),
            (200, r#"{"metric_batch_ddl":2}"#, Some(false)),
            (200, "{}", Some(false)),
            (404, "", Some(false)),
            (401, "{}", None),
            (403, "{}", None),
            (500, "{}", None),
            (200, "[]", None),
            (200, "bad", None),
            (200, r#"{"metric_batch_ddl":"1"}"#, None),
            (200, r#"{"error":"secret"}"#, None),
        ] {
            let (client, requests, server) = test_server(status, body).await;
            let result = client.capabilities().await;
            assert_eq!(
                result.as_ref().ok().map(|c| c.supports("metric_batch_ddl")),
                expected
            );
            assert_eq!(requests.lock().unwrap().len(), 1);
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn batch_disconnect_and_timeout_are_errors() {
        use tokio::io::AsyncReadExt;
        for disconnect in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = DatabaseClient::new(
                listener.local_addr().unwrap().to_string(),
                "greptime".into(),
                None,
                Duration::from_millis(100),
                None,
                true,
            );
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 4096];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                if !disconnect {
                    let _ = stopped.await;
                }
            });
            assert!(
                client
                    .logical_tables("CREATE TABLE t", "public", 1)
                    .await
                    .is_err()
            );
            let _ = stop.send(());
            server.await.unwrap();
            client.catalog = "测".repeat(500_000);
            let error = client
                .logical_tables("CREATE TABLE t", "public", 1)
                .await
                .unwrap_err();
            assert!(format!("{error:?}").contains("4 MiB form limit"));
        }
    }

    pub(crate) async fn test_server(
        status: u16,
        body: &str,
    ) -> (
        DatabaseClient,
        Arc<std::sync::Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DatabaseClient::new(
            listener.local_addr().unwrap().to_string(),
            "greptime".into(),
            Some("user:password".into()),
            Duration::from_secs(5),
            None,
            true,
        );
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = requests.clone();
        let body = body.to_string();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .map(|n| n.parse::<usize>().unwrap())
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(bytes).unwrap());
                socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        (client, requests, server)
    }

    #[test]
    fn test_split_database() {
        let result = split_database("catalog-schema").unwrap();
        assert_eq!(result, ("catalog".to_string(), Some("schema".to_string())));

        let result = split_database("schema").unwrap();
        assert_eq!(result, ("greptime".to_string(), Some("schema".to_string())));

        let result = split_database("catalog-*").unwrap();
        assert_eq!(result, ("catalog".to_string(), None));

        let result = split_database("*").unwrap();
        assert_eq!(result, ("greptime".to_string(), None));
    }
}
