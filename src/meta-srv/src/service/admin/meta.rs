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

use std::collections::HashMap;

use api::v1::meta::{RangeRequest, RangeResponse};
use catalog::helper::{CATALOG_KEY_PREFIX, SCHEMA_KEY_PREFIX, TABLE_GLOBAL_KEY_PREFIX};
use snafu::ResultExt;
use tonic::codegen::http;

use crate::error::Result;
use crate::service::admin::HttpHandler;
use crate::service::store::ext::KvStoreExt;
use crate::service::store::kv::KvStoreRef;
use crate::{error, util};

pub struct CatalogsHandler {
    pub kv_store: KvStoreRef,
}

pub struct SchemasHandler {
    pub kv_store: KvStoreRef,
}

pub struct TablesHandler {
    pub kv_store: KvStoreRef,
}

pub struct TableHandler {
    pub kv_store: KvStoreRef,
}

#[async_trait::async_trait]
impl HttpHandler for CatalogsHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        get_http_response_by_prefix(String::from(CATALOG_KEY_PREFIX), &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for SchemasHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let catalog = match params.get("catalog_name") {
            Some(catalog) => catalog,
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "catalog_name parameter is required".to_string(),
                }
                .fail();
            }
        };
        let prefix = format!("{SCHEMA_KEY_PREFIX}-{catalog}",);
        get_http_response_by_prefix(prefix, &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TablesHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let catalog = match params.get("catalog_name") {
            Some(catalog) => catalog,
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "catalog_name parameter is required".to_string(),
                }
                .fail();
            }
        };

        let schema = match params.get("schema_name") {
            Some(schema) => schema,
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "schema_name parameter is required".to_string(),
                }
                .fail();
            }
        };
        let prefix = format!("{TABLE_GLOBAL_KEY_PREFIX}-{catalog}-{schema}",);
        get_http_response_by_prefix(prefix, &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TableHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let table_name = match params.get("full_table_name") {
            Some(full_table_name) => full_table_name.replace('.', "-"),
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "full_table_name parameter is required".to_string(),
                }
                .fail();
            }
        };
        let table_key = format!("{TABLE_GLOBAL_KEY_PREFIX}-{table_name}");

        let response = self.kv_store.get(table_key.into_bytes()).await?;
        let mut value: String = "Not found result".to_string();
        if let Some(key_value) = response {
            value = String::from_utf8(key_value.value).context(error::InvalidUtf8ValueSnafu)?;
        }
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(value)
            .context(error::InvalidHttpBodySnafu)
    }
}

/// Get kv_store's key list with http response format by prefix key
async fn get_http_response_by_prefix(
    key_prefix: String,
    kv_store: &KvStoreRef,
) -> Result<http::Response<String>> {
    let keys = get_keys_by_prefix(key_prefix, kv_store).await?;
    let body = serde_json::to_string(&keys).context(error::SerializeToJsonSnafu {
        input: format!("{keys:?}"),
    })?;

    http::Response::builder()
        .status(http::StatusCode::OK)
        .body(body)
        .context(error::InvalidHttpBodySnafu)
}

/// Get kv_store's key list by prefix key
async fn get_keys_by_prefix(key_prefix: String, kv_store: &KvStoreRef) -> Result<Vec<String>> {
    let key_prefix_u8 = key_prefix.clone().into_bytes();
    let range_end = util::get_prefix_end_key(&key_prefix_u8);
    let req = RangeRequest {
        key: key_prefix_u8,
        range_end,
        ..Default::default()
    };

    let response: RangeResponse = kv_store.range(req).await?;

    let kvs = response.kvs;
    let mut values = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let value = String::from_utf8(kv.key).context(error::InvalidUtf8ValueSnafu)?;
        let split_list = value.split(&key_prefix).collect::<Vec<&str>>();
        if let Some(v) = split_list.get(1) {
            values.push(v.to_string());
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::PutRequest;

    use crate::service::admin::meta::get_keys_by_prefix;
    use crate::service::store::kv::KvStoreRef;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_get_list_by_prefix() {
        let in_mem = Arc::new(MemStore::new()) as KvStoreRef;

        in_mem
            .put(PutRequest {
                key: "test_key1".as_bytes().to_vec(),
                value: "test_val1".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        in_mem
            .put(PutRequest {
                key: "test_key2".as_bytes().to_vec(),
                value: "test_val2".as_bytes().to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let keys = get_keys_by_prefix(String::from("test_key"), &in_mem)
            .await
            .unwrap();

        assert_eq!("1", keys[0]);
        assert_eq!("2", keys[1]);
    }
}
