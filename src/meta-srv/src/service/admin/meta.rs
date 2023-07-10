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

use common_meta::helper::{build_catalog_prefix, build_schema_prefix};
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_meta::rpc::store::{RangeRequest, RangeResponse};
use common_meta::util;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::error;
use crate::error::{Result, TableMetadataManagerSnafu};
use crate::service::admin::HttpHandler;
use crate::service::store::kv::KvStoreRef;

pub struct CatalogsHandler {
    pub kv_store: KvStoreRef,
}

pub struct SchemasHandler {
    pub kv_store: KvStoreRef,
}

pub struct TablesHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

pub struct TableHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

#[async_trait::async_trait]
impl HttpHandler for CatalogsHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        get_http_response_by_prefix(build_catalog_prefix(), &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for SchemasHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let catalog = params
            .get("catalog_name")
            .context(error::MissingRequiredParameterSnafu {
                param: "catalog_name",
            })?;
        get_http_response_by_prefix(build_schema_prefix(catalog), &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TablesHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let catalog = params
            .get("catalog_name")
            .context(error::MissingRequiredParameterSnafu {
                param: "catalog_name",
            })?;

        let schema = params
            .get("schema_name")
            .context(error::MissingRequiredParameterSnafu {
                param: "schema_name",
            })?;

        let tables = self
            .table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .await
            .context(TableMetadataManagerSnafu)?;

        to_http_response(tables)
    }
}

#[async_trait::async_trait]
impl HttpHandler for TableHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let table_name =
            params
                .get("full_table_name")
                .context(error::MissingRequiredParameterSnafu {
                    param: "full_table_name",
                })?;

        let key: TableNameKey = table_name
            .as_str()
            .try_into()
            .context(TableMetadataManagerSnafu)?;

        let mut result = HashMap::with_capacity(2);

        let table_id = self
            .table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)?
            .map(|x| x.table_id());

        if let Some(table_id) = table_id {
            let table_info_value = self
                .table_metadata_manager
                .table_info_manager()
                .get(table_id)
                .await
                .context(TableMetadataManagerSnafu)?
                .map(|x| format!("{x:?}"))
                .unwrap_or_else(|| "Not Found".to_string());
            result.insert("table_info_value", table_info_value);
        }

        if let Some(table_id) = table_id {
            let table_region_value = self
                .table_metadata_manager
                .table_region_manager()
                .get(table_id)
                .await
                .context(TableMetadataManagerSnafu)?
                .map(|x| format!("{x:?}"))
                .unwrap_or_else(|| "Not Found".to_string());
            result.insert("table_region_value", table_region_value);
        }

        http::Response::builder()
            .status(http::StatusCode::OK)
            // Safety: HashMap<String, String> is definitely "serde-json"-able.
            .body(serde_json::to_string(&result).unwrap())
            .context(error::InvalidHttpBodySnafu)
    }
}

/// Get kv_store's key list with http response format by prefix key
async fn get_http_response_by_prefix(
    key_prefix: String,
    kv_store: &KvStoreRef,
) -> Result<http::Response<String>> {
    let keys = get_keys_by_prefix(key_prefix, kv_store).await?;
    to_http_response(keys)
}

fn to_http_response(keys: Vec<String>) -> Result<http::Response<String>> {
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
        keys_only: true,
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

    use common_meta::helper::{build_catalog_prefix, build_schema_prefix, CatalogKey, SchemaKey};
    use common_meta::key::table_name::TableNameKey;
    use common_meta::key::TableMetaKey;
    use common_meta::rpc::store::PutRequest;

    use crate::service::admin::meta::get_keys_by_prefix;
    use crate::service::store::kv::KvStoreRef;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_get_list_by_prefix() {
        let in_mem = Arc::new(MemStore::new()) as KvStoreRef;
        let catalog_name = "test_catalog";
        let schema_name = "test_schema";
        let table_name = "test_table";
        let catalog = CatalogKey {
            catalog_name: catalog_name.to_string(),
        };
        assert!(in_mem
            .put(PutRequest {
                key: catalog.to_string().as_bytes().to_vec(),
                value: "".as_bytes().to_vec(),
                prev_kv: false,
            })
            .await
            .is_ok());

        let schema = SchemaKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
        };
        assert!(in_mem
            .put(PutRequest {
                key: schema.to_string().as_bytes().to_vec(),
                value: "".as_bytes().to_vec(),
                prev_kv: false,
            })
            .await
            .is_ok());

        let table1 = TableNameKey::new(catalog_name, schema_name, table_name);
        let table2 = TableNameKey::new(catalog_name, schema_name, "test_table1");
        assert!(in_mem
            .put(PutRequest {
                key: table1.as_raw_key(),
                value: "".as_bytes().to_vec(),
                prev_kv: false,
            })
            .await
            .is_ok());
        assert!(in_mem
            .put(PutRequest {
                key: table2.as_raw_key(),
                value: "".as_bytes().to_vec(),
                prev_kv: false,
            })
            .await
            .is_ok());

        let catalog_key = get_keys_by_prefix(build_catalog_prefix(), &in_mem)
            .await
            .unwrap();
        let schema_key = get_keys_by_prefix(build_schema_prefix(schema.catalog_name), &in_mem)
            .await
            .unwrap();
        let table_key = get_keys_by_prefix(
            format!(
                "{}/",
                TableNameKey::prefix_to_table(table1.catalog, table1.schema)
            ),
            &in_mem,
        )
        .await
        .unwrap();

        assert_eq!(catalog_name, catalog_key[0]);
        assert_eq!(schema_name, schema_key[0]);
        assert_eq!(table_name, table_key[0]);
        assert_eq!("test_table1", table_key[1]);
    }
}
