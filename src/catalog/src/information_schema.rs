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

mod columns;
mod tables;

use std::any::Any;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use common_recordbatch::{RecordBatchStreamAdaptor, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::error::{SchemaConversionSnafu, TablesRecordBatchSnafu};
use table::{Result as TableResult, Table, TableRef};

use self::columns::InformationSchemaColumns;
use crate::error::Result;
use crate::information_schema::tables::InformationSchemaTables;
use crate::CatalogManager;

const TABLES: &str = "tables";
const COLUMNS: &str = "columns";

pub struct InformationSchemaProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaProvider {
    pub fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            catalog_name,
            catalog_manager,
        }
    }
}

impl InformationSchemaProvider {
    pub fn table(&self, name: &str) -> Result<Option<TableRef>> {
        let stream_builder = match name.to_ascii_lowercase().as_ref() {
            TABLES => Arc::new(InformationSchemaTables::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _,
            COLUMNS => Arc::new(InformationSchemaColumns::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _,
            _ => {
                return Ok(None);
            }
        };

        Ok(Some(Arc::new(InformationTable::new(stream_builder))))
    }
}

// TODO(ruihang): make it a more generic trait:
// https://github.com/GreptimeTeam/greptimedb/pull/1639#discussion_r1205001903
pub trait InformationStreamBuilder: Send + Sync {
    fn to_stream(&self) -> Result<SendableRecordBatchStream>;

    fn schema(&self) -> SchemaRef;
}

pub struct InformationTable {
    stream_builder: Arc<dyn InformationStreamBuilder>,
}

impl InformationTable {
    pub fn new(stream_builder: Arc<dyn InformationStreamBuilder>) -> Self {
        Self { stream_builder }
    }
}

#[async_trait]
impl Table for InformationTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.stream_builder.schema()
    }

    fn table_info(&self) -> table::metadata::TableInfoRef {
        unreachable!("Should not call table_info() of InformationTable directly")
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> TableResult<SendableRecordBatchStream> {
        let projection = request.projection;
        let projected_schema = if let Some(projection) = &projection {
            Arc::new(
                self.schema()
                    .try_project(projection)
                    .context(SchemaConversionSnafu)?,
            )
        } else {
            self.schema()
        };
        let stream = self
            .stream_builder
            .to_stream()
            .map_err(BoxedError::new)
            .context(TablesRecordBatchSnafu)?
            .map(move |batch| {
                batch.and_then(|batch| {
                    if let Some(projection) = &projection {
                        batch.try_project(projection)
                    } else {
                        Ok(batch)
                    }
                })
            });
        let stream = RecordBatchStreamAdaptor {
            schema: projected_schema,
            stream: Box::pin(stream),
            output_ordering: None,
        };
        Ok(Box::pin(stream))
    }
}
