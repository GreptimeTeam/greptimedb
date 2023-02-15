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
use crate::service::admin::HttpHandler;
use crate::service::store::kv::KvStoreRef;
use crate::{error, util};

pub struct CatalogHandler {
    pub kv_store: KvStoreRef,
}

const CATALOG_KEY_PREFIX: &str = "__c-";

#[async_trait::async_trait]
impl HttpHandler for CatalogHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        let key = String::from(CATALOG_KEY_PREFIX).into_bytes();
        let range_end = util::get_prefix_end_key(&key);
        let req = RangeRequest {
            key,
            range_end,
            ..Default::default()
        };

        let response: RangeResponse = self.kv_store.range(req).await?;

        let kvs = response.kvs;
        let mut catalog_list = vec![];
        for kv in kvs {
            let catalog = String::from_utf8(kv.key).context(error::InvalidUtf8ValueSnafu)?;
            let split_catalog = catalog.split(CATALOG_KEY_PREFIX).collect::<Vec<&str>>()[1];
            catalog_list.push(split_catalog.to_string());
        }
        let r = serde_json::to_string(&catalog_list).context(error::SerializeToJsonSnafu {
            input: format!("{catalog_list:?}"),
        })?;

        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(r)
            .unwrap())
    }
}
