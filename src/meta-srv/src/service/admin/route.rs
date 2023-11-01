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

use common_catalog::format_full_table_name;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use tonic::codegen::http;

use super::{util, HttpHandler};
use crate::error;
use crate::error::{
    ParseNumSnafu, Result, TableMetadataManagerSnafu, TableNotFoundSnafu, TableRouteNotFoundSnafu,
};

#[derive(Clone)]
pub struct RouteHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

#[async_trait::async_trait]
impl HttpHandler for RouteHandler {
    async fn handle(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if path.ends_with("/help") {
            return util::to_text_response(
                r#"
            - GET /table?region_id=123
            - GET /table?table_id=456
            - GET /table?catalog=foo&schema=bar&table=baz
            "#,
            );
        }

        let table_id = self.extract_table_id(params).await?;

        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(TableRouteNotFoundSnafu { table_id })?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(serde_json::to_string(&table_route_value).unwrap())
            .context(error::InvalidHttpBodySnafu)
    }
}

impl RouteHandler {
    async fn extract_table_id(&self, params: &HashMap<String, String>) -> Result<TableId> {
        if let Some(id) = params.get("region_id") {
            let table_id = id
                .parse::<u64>()
                .map(|x| RegionId::from_u64(x).table_id())
                .context(ParseNumSnafu {
                    err_msg: format!("invalid region id: {id}"),
                })?;
            return Ok(table_id);
        }

        if let Some(id) = params.get("table_id") {
            let table_id = id.parse::<u32>().context(ParseNumSnafu {
                err_msg: format!("invalid table id: {id}"),
            })?;

            return Ok(table_id);
        }

        let catalog = util::get_value(params, "catalog")?;
        let schema = util::get_value(params, "schema")?;
        let table = util::get_value(params, "table")?;

        let key = TableNameKey::new(catalog, schema, table);

        let table_id = self
            .table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)?
            .map(|x| x.table_id())
            .context(TableNotFoundSnafu {
                name: format_full_table_name(catalog, schema, table),
            })?;

        Ok(table_id)
    }
}
