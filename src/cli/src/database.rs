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

use crate::error::{
    BuildClientSnafu, HttpQuerySqlSnafu, ParseProxyOptsSnafu, Result, SerdeJsonSnafu,
};

#[derive(Debug, Clone)]
pub struct DatabaseClient {
    addr: String,
    catalog: String,
    auth_header: Option<String>,
    timeout: Duration,
    proxy: Option<reqwest::Proxy>,
    no_proxy: bool,
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
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub async fn sql_in_public(&self, sql: &str) -> Result<Option<Vec<Vec<Value>>>> {
        self.sql(sql, DEFAULT_SCHEMA_NAME).await
    }

    /// Requires the explicit packed-import protocol before any restore mutation.
    pub async fn require_packed_import(&self) -> Result<()> {
        let url = format!("http://{}/v1/capabilities", self.addr);
        let mut builder = reqwest::Client::builder().timeout(self.timeout);
        if let Some(proxy) = self.proxy.clone() {
            builder = builder.proxy(proxy);
        }
        if self.no_proxy {
            builder = builder.no_proxy();
        }
        let client = builder.build().context(BuildClientSnafu)?;
        let mut request = client.get(&url);
        if let Some(auth) = &self.auth_header {
            request = request.header("Authorization", auth);
        }
        let response = request.send().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "packed import capability request failed",
        })?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return crate::error::InvalidArgumentsSnafu {
                msg: "target does not support packed import",
            }
            .fail();
        }
        let response = response
            .error_for_status()
            .with_context(|_| HttpQuerySqlSnafu {
                reason: "packed import capability request rejected",
            })?;
        let body = response.text().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "cannot read capability response",
        })?;
        let value: Value = serde_json::from_str(&body).context(SerdeJsonSnafu)?;
        if !value.is_object() {
            return crate::error::UnexpectedSnafu {
                msg: "invalid capability response: expected JSON object",
            }
            .fail();
        }
        if value.get("metric_packed_import").and_then(Value::as_u64) != Some(1) {
            return crate::error::InvalidArgumentsSnafu {
                msg: "target does not support packed import version 1",
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
        let url = format!("http://{}/v1/sql", self.addr);
        let params = [
            ("db", format!("{}-{}", self.catalog, schema)),
            ("sql", sql.to_string()),
        ];
        let mut builder = reqwest::Client::builder();
        if let Some(proxy) = self.proxy.clone() {
            builder = builder.proxy(proxy);
        }
        if self.no_proxy {
            builder = builder.no_proxy();
        }
        let client = builder.build().context(BuildClientSnafu)?;
        let mut request = client
            .post(&url)
            .form(&params)
            .header("Content-Type", "application/x-www-form-urlencoded");
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
        let response = response
            .error_for_status()
            .with_context(|_| HttpQuerySqlSnafu {
                reason: format!("query failed: {}", sql),
            })?;

        let text = response.text().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "cannot get response text".to_string(),
        })?;

        serde_json::from_str::<GreptimedbV1Response>(&text).context(SerdeJsonSnafu)
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
mod tests {
    #[tokio::test]
    async fn packed_capability_probe_checks_auth_and_protocol_version() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
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
                client.require_packed_import().await.is_ok(),
                supported,
                "{status}: {body}"
            );
            server.await.unwrap();
        }
    }

    use super::*;

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
