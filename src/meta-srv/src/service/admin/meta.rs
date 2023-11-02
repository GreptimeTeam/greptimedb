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
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};
use tonic::codegen::http;

use crate::error;
use crate::error::{ParseNumSnafu, Result, TableMetadataManagerSnafu};
use crate::service::admin::{util, HttpHandler};

#[derive(Clone)]
pub struct CatalogsHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

#[derive(Clone)]
pub struct SchemasHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

#[derive(Clone)]
pub struct TablesHandler {
    pub table_metadata_manager: TableMetadataManagerRef,
}

#[derive(Clone)]
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
        path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if path.ends_with("/help") {
            return util::to_text_response(
                r#"
            - GET /schemas?catalog=foo
            "#,
            );
        }

        let catalog = util::get_value(params, "catalog")?;
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
        path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if path.ends_with("/help") {
            return util::to_text_response(
                r#"
            - GET /tables?catalog=foo&schema=bar
            "#,
            );
        }

        let catalog = util::get_value(params, "catalog")?;
        let schema = util::get_value(params, "schema")?;

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
        path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if path.ends_with("/help") {
            return util::to_text_response(
                r#"
            - GET /table?region_ids=1,2,3,4,5
            - GET /table?table_ids=1,2,3,4,5
            - GET /table?catalog=foo&schema=bar&table=baz
            "#,
            );
        }

        let table_ids = self.extract_table_ids(params).await?;

        let table_info_values = self
            .table_metadata_manager
            .table_info_manager()
            .batch_get(&table_ids)
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .collect::<HashMap<_, _>>();

        http::Response::builder()
            .header("Content-Type", "application/json")
            .status(http::StatusCode::OK)
            // Safety: HashMap<String, String> is definitely "serde-json"-able.
            .body(serde_json::to_string(&table_info_values).unwrap())
            .context(error::InvalidHttpBodySnafu)
    }
}

impl TableHandler {
    async fn extract_table_ids(&self, params: &HashMap<String, String>) -> Result<Vec<TableId>> {
        if let Some(ids) = params.get("region_ids") {
            let table_ids = ids
                .split(',')
                .map(|x| {
                    x.parse::<u64>()
                        .map(|y| RegionId::from_u64(y).table_id())
                        .context(ParseNumSnafu {
                            err_msg: format!("invalid region id: {x}"),
                        })
                })
                .collect::<Result<Vec<_>>>()?;

            return Ok(table_ids);
        }

        if let Some(ids) = params.get("table_ids") {
            let table_ids = ids
                .split(',')
                .map(|x| {
                    x.parse::<u32>().context(ParseNumSnafu {
                        err_msg: format!("invalid table id: {x}"),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            return Ok(table_ids);
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
            .map(|x| x.table_id());

        if let Some(table_id) = table_id {
            Ok(vec![table_id])
        } else {
            Ok(vec![])
        }
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
