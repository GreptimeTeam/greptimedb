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

use common_error::ext::BoxedError;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::error;
use crate::error::{Result, TableMetadataManagerSnafu};
use crate::service::admin::HttpHandler;

pub struct CatalogsHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

pub struct SchemasHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
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
        let stream = self
            .table_metadata_manager
            .catalog_manager()
            .catalog_names()
            .await;

        let keys = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(error::ListCatalogsSnafu)?;

        to_http_response(keys)
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
        let stream = self
            .table_metadata_manager
            .schema_manager()
            .schema_names(catalog)
            .await;

        let keys = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(error::ListSchemasSnafu { catalog })?;

        to_http_response(keys)
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
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();

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
        let catalog = params
            .get("catalog")
            .context(error::MissingRequiredParameterSnafu { param: "catalog" })?;
        let schema = params
            .get("schema")
            .context(error::MissingRequiredParameterSnafu { param: "schema" })?;
        let table = params
            .get("table")
            .context(error::MissingRequiredParameterSnafu { param: "table" })?;

        let key = TableNameKey::new(catalog, schema, table);

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
                .table_route_manager()
                .get(table_id)
                .await
                .context(TableMetadataManagerSnafu)?
                .map(|x| format!("{x:?}"))
                .unwrap_or_else(|| "Not Found".to_string());
            result.insert("table_route_value", table_region_value);
        }

        http::Response::builder()
            .status(http::StatusCode::OK)
            // Safety: HashMap<String, String> is definitely "serde-json"-able.
            .body(serde_json::to_string(&result).unwrap())
            .context(error::InvalidHttpBodySnafu)
    }
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
