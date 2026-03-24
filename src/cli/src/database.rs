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

    pub async fn sql_in_public(&self, sql: &str) -> Result<Option<Vec<Vec<Value>>>> {
        self.sql(sql, DEFAULT_SCHEMA_NAME).await
    }

    /// Execute sql query.
    pub async fn sql(&self, sql: &str, schema: &str) -> Result<Option<Vec<Vec<Value>>>> {
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

        let body = serde_json::from_str::<GreptimedbV1Response>(&text).context(SerdeJsonSnafu)?;
        Ok(body.output().first().and_then(|output| match output {
            GreptimeQueryOutput::Records(records) => Some(records.rows().clone()),
            GreptimeQueryOutput::AffectedRows(_) => None,
        }))
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
