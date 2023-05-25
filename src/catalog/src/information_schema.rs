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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use common_recordbatch::{RecordBatchStreamAdaptor, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::error::SchemaConversionSnafu;
use table::{Result as TableResult, Table, TableRef};

use self::columns::InformationSchemaColumns;
use crate::error::Result;
use crate::information_schema::tables::InformationSchemaTables;
use crate::{CatalogProviderRef, SchemaProvider};

const TABLES: &str = "tables";
const COLUMNS: &str = "columns";

pub(crate) struct InformationSchemaProvider {
    catalog_name: String,
    catalog_provider: CatalogProviderRef,
    tables: Vec<String>,
}

impl InformationSchemaProvider {
    pub(crate) fn new(catalog_name: String, catalog_provider: CatalogProviderRef) -> Self {
        Self {
            catalog_name,
            catalog_provider,
            tables: vec![TABLES.to_string(), COLUMNS.to_string()],
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Ok(self.tables.clone())
    }

    async fn table(&self, name: &str) -> Result<Option<TableRef>> {
        let stream = match name.to_ascii_lowercase().as_ref() {
            TABLES => InformationSchemaTables::new(
                self.catalog_name.clone(),
                self.catalog_provider.clone(),
            )
            .to_stream()?,
            COLUMNS => InformationSchemaColumns::new(
                self.catalog_name.clone(),
                self.catalog_provider.clone(),
            )
            .to_stream()?,
            _ => {
                return Ok(None);
            }
        };

        Ok(Some(Arc::new(InformationTable::new(stream))))
    }

    async fn table_exist(&self, name: &str) -> Result<bool> {
        let normalized_name = name.to_ascii_lowercase();
        Ok(self.tables.contains(&normalized_name))
    }
}

pub struct InformationTable {
    schema: SchemaRef,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

impl InformationTable {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        Self {
            schema,
            stream: Arc::new(Mutex::new(Some(stream))),
        }
    }
}

#[async_trait]
impl Table for InformationTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_info(&self) -> table::metadata::TableInfoRef {
        unreachable!("Should not call table_info() of InformationTable directly")
    }

    /// Scan the table and returns a SendableRecordBatchStream.
    async fn scan(
        &self,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        _limit: Option<usize>,
    ) -> TableResult<PhysicalPlanRef> {
        unimplemented!()
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
            .stream
            .lock()
            .unwrap()
            .take()
            .unwrap()
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
        };
        Ok(Box::pin(stream))
    }
}
