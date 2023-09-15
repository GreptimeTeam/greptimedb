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

use common_catalog::parse_full_table_name;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use super::HttpHandler;
use crate::error;
use crate::error::{Result, TableNotFoundSnafu, TableRouteNotFoundSnafu};

pub struct RouteHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

#[async_trait::async_trait]
impl HttpHandler for RouteHandler {
    async fn handle(
        &self,
        _path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let table_name =
            params
                .get("full_table_name")
                .context(error::MissingRequiredParameterSnafu {
                    param: "full_table_name",
                })?;

        let (catalog, schema, table) =
            parse_full_table_name(table_name).context(error::InvalidFullTableNameSnafu)?;

        let key = TableNameKey::new(catalog, schema, table);

        let table_id = self
            .table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .map(|x| x.table_id())
            .context(TableNotFoundSnafu { name: table_name })?;

        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(TableRouteNotFoundSnafu { table_id })?;
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(serde_json::to_string(&table_route_value).unwrap())
            .context(error::InvalidHttpBodySnafu)
    }
}
