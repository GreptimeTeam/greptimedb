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

use api::v1::meta::TableRouteValue;
use common_meta::key::TABLE_ROUTE_PREFIX;
use common_meta::rpc::store::{RangeRequest, RangeResponse};
use common_meta::util;
use prost::Message;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use super::HttpHandler;
use crate::error;
use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

pub struct RouteHandler {
    pub kv_store: KvStoreRef,
}

#[async_trait::async_trait]
impl HttpHandler for RouteHandler {
    async fn handle(
        &self,
        _path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let full_table_name = params
            .get("full_table_name")
            .map(|full_table_name| full_table_name.replace('.', "-"))
            .context(error::MissingRequiredParameterSnafu {
                param: "full_table_name",
            })?;

        let route_key = format!("{}-{}", TABLE_ROUTE_PREFIX, full_table_name).into_bytes();

        let range_end = util::get_prefix_end_key(&route_key);

        let req = RangeRequest {
            key: route_key,
            range_end,
            keys_only: false,
            ..Default::default()
        };

        let resp = self.kv_store.range(req).await?;

        let show = pretty_fmt(resp)?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(show)
            .context(error::InvalidHttpBodySnafu)
    }
}

fn pretty_fmt(response: RangeResponse) -> Result<String> {
    let mut show = "".to_string();

    for kv in response.kvs.into_iter() {
        let route_key = String::from_utf8(kv.key).unwrap();
        let route_val =
            TableRouteValue::decode(&kv.value[..]).context(error::DecodeTableRouteSnafu)?;

        show.push_str("route_key:\n");
        show.push_str(&route_key);
        show.push('\n');

        show.push_str("route_value:\n");
        show.push_str(&format!("{:#?}", route_val));
        show.push('\n');
    }

    Ok(show)
}
