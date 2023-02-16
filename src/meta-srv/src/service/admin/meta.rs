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
use snafu::ResultExt;
use tonic::codegen::http;

use crate::error::Result;
use crate::error;
use crate::util;
use crate::service::admin::HttpHandler;
use crate::service::store::kv::KvStoreRef;

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

const CATALOG_KEY_PREFIX: &str = "__c-";
const SCHEMA_KEY_PREFIX: &str = "__s-";
const TABLE_KEY_PREFIX: &str = "__tg-";

#[async_trait::async_trait]
impl HttpHandler for CatalogsHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        get_key_list_response_by_prefix_key(&String::from(CATALOG_KEY_PREFIX), &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for SchemasHandler {
    async fn handle(&self, _: &str, map: &HashMap<String, String>) -> Result<http::Response<String>> {
        let mut schema_key_prefix = String::from(SCHEMA_KEY_PREFIX);
        match map.get("catalog_name") {
            Some(catalog_value) => {
                schema_key_prefix = schema_key_prefix + catalog_value + "-"
            }
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "catalog_name parameter is required".to_string(),
                }.fail();
            }
        }
        get_key_list_response_by_prefix_key(&schema_key_prefix, &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TablesHandler {
    async fn handle(&self, _: &str, map: &HashMap<String, String>) -> Result<http::Response<String>> {
        let mut table_key_prefix = String::from(TABLE_KEY_PREFIX);
        match map.get("catalog_name") {
            Some(catalog_value) => {
                table_key_prefix = table_key_prefix + catalog_value + "-"
            }
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "catalog_name parameter is required".to_string(),
                }.fail();
            }
        }

        match map.get("schema_name") {
            Some(schema_value) => {
                table_key_prefix = table_key_prefix + schema_value + "-"
            }
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "schema_name parameter is required".to_string(),
                }.fail();
            }
        }
        get_key_list_response_by_prefix_key(&table_key_prefix, &self.kv_store).await
    }
}

#[async_trait::async_trait]
impl HttpHandler for TableHandler {
    async fn handle(&self, _: &str, map: &HashMap<String, String>) -> Result<http::Response<String>> {
        let mut table_key_prefix = String::from(TABLE_KEY_PREFIX);
        match map.get("full_table_name") {
            Some(full_table_name) => {
                let replace_name = full_table_name.replace('.', "-");
                table_key_prefix = table_key_prefix + &replace_name
            }
            None => {
                return error::MissingRequiredParameterSnafu {
                    msg: "full_table_name parameter is required".to_string(),
                }.fail();
            }
        }

        let req = RangeRequest {
            key: table_key_prefix.into_bytes(),
            ..Default::default()
        };

        let response: RangeResponse = self.kv_store.range(req).await?;

        let kvs = response.kvs;
        let value = String::from_utf8(kvs.get_mut(0)).context(error::InvalidUtf8ValueSnafu)?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(value)
            .unwrap())
    }
}

async fn get_key_list_response_by_prefix_key(key_prefix: &String, kv_store: &KvStoreRef) -> Result<http::Response<String>> {
    let keys = get_key_list_by_prefix_key(key_prefix, kv_store).await?;
    let body = serde_json::to_string(&keys).context(error::SerializeToJsonSnafu {
        input: format!("{keys:?}"),
    })?;

    Ok(http::Response::builder()
        .status(http::StatusCode::OK)
        .body(body)
        .unwrap())
}

async fn get_key_list_by_prefix_key(key_prefix: &String, kv_store: &KvStoreRef) -> Result<Vec<String>> {
    let key_prefix_u8 = key_prefix.clone().into_bytes();
    let range_end = util::get_prefix_end_key(&key_prefix_u8);
    let req = RangeRequest {
        key: key_prefix_u8,
        range_end,
        ..Default::default()
    };

    let response: RangeResponse = kv_store.range(req).await?;

    let kvs = response.kvs;
    let mut values = vec![];
    for kv in kvs {
        let value = String::from_utf8(kv.key).context(error::InvalidUtf8ValueSnafu)?;
        let split_value = value.split(key_prefix).collect::<Vec<&str>>()[1];
        values.push(split_value.to_string());
    }
    Ok(values)
}
