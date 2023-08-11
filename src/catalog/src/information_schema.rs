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
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use common_catalog::consts::{
    INFORMATION_SCHEMA_COLUMNS_TABLE_ID, INFORMATION_SCHEMA_NAME,
    INFORMATION_SCHEMA_TABLES_TABLE_ID,
};
use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatchStreamAdaptor, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};
use table::data_source::DataSource;
use table::error::{SchemaConversionSnafu, TablesRecordBatchSnafu};
use table::metadata::{TableIdent, TableInfoBuilder, TableMetaBuilder, TableType};
use table::{Result as TableResult, Table, TableRef};

use self::columns::InformationSchemaColumns;
use crate::error::Result;
use crate::information_schema::tables::InformationSchemaTables;
use crate::table_factory::TableFactory;
use crate::CatalogManager;

pub const TABLES: &str = "tables";
pub const COLUMNS: &str = "columns";

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

    /// Build a map of [TableRef] in information schema.
    /// Includeing `tables` and `columns`.
    pub fn build(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> HashMap<String, TableRef> {
        let mut schema = HashMap::new();

        schema.insert(
            TABLES.to_string(),
            Arc::new(InformationTable::new(
                catalog_name.clone(),
                INFORMATION_SCHEMA_TABLES_TABLE_ID,
                TABLES.to_string(),
                Arc::new(InformationSchemaTables::new(
                    catalog_name.clone(),
                    catalog_manager.clone(),
                )),
            )) as _,
        );
        schema.insert(
            COLUMNS.to_string(),
            Arc::new(InformationTable::new(
                catalog_name.clone(),
                INFORMATION_SCHEMA_COLUMNS_TABLE_ID,
                COLUMNS.to_string(),
                Arc::new(InformationSchemaColumns::new(catalog_name, catalog_manager)),
            )) as _,
        );

        schema
    }

    pub fn table(&self, name: &str) -> Result<Option<TableRef>> {
        let (stream_builder, table_id) = match name.to_ascii_lowercase().as_ref() {
            TABLES => (
                Arc::new(InformationSchemaTables::new(
                    self.catalog_name.clone(),
                    self.catalog_manager.clone(),
                )) as _,
                INFORMATION_SCHEMA_TABLES_TABLE_ID,
            ),
            COLUMNS => (
                Arc::new(InformationSchemaColumns::new(
                    self.catalog_name.clone(),
                    self.catalog_manager.clone(),
                )) as _,
                INFORMATION_SCHEMA_COLUMNS_TABLE_ID,
            ),
            _ => {
                return Ok(None);
            }
        };

        Ok(Some(Arc::new(InformationTable::new(
            self.catalog_name.clone(),
            table_id,
            name.to_string(),
            stream_builder,
        ))))
    }

    pub fn table_factory(&self, name: &str) -> Result<Option<TableFactory>> {
        let (stream_builder, table_id) = match name.to_ascii_lowercase().as_ref() {
            TABLES => (
                Arc::new(InformationSchemaTables::new(
                    self.catalog_name.clone(),
                    self.catalog_manager.clone(),
                )) as _,
                INFORMATION_SCHEMA_TABLES_TABLE_ID,
            ),
            COLUMNS => (
                Arc::new(InformationSchemaColumns::new(
                    self.catalog_name.clone(),
                    self.catalog_manager.clone(),
                )) as _,
                INFORMATION_SCHEMA_COLUMNS_TABLE_ID,
            ),
            _ => {
                return Ok(None);
            }
        };
        let data_source = Arc::new(InformationTable::new(
            self.catalog_name.clone(),
            table_id,
            name.to_string(),
            stream_builder,
        ));

        Ok(Some(Arc::new(move || data_source.clone())))
    }
}

// TODO(ruihang): make it a more generic trait:
// https://github.com/GreptimeTeam/greptimedb/pull/1639#discussion_r1205001903
pub trait InformationStreamBuilder: Send + Sync {
    fn to_stream(&self) -> Result<SendableRecordBatchStream>;

    fn schema(&self) -> SchemaRef;
}

pub struct InformationTable {
    catalog_name: String,
    table_id: TableId,
    name: String,
    stream_builder: Arc<dyn InformationStreamBuilder>,
}

impl InformationTable {
    pub fn new(
        catalog_name: String,
        table_id: TableId,
        name: String,
        stream_builder: Arc<dyn InformationStreamBuilder>,
    ) -> Self {
        Self {
            catalog_name,
            table_id,
            name,
            stream_builder,
        }
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
        let table_meta = TableMetaBuilder::default()
            .schema(self.stream_builder.schema())
            .primary_key_indices(vec![])
            .next_column_id(0)
            .build()
            .unwrap();
        Arc::new(
            TableInfoBuilder::default()
                .ident(TableIdent {
                    table_id: self.table_id,
                    version: 0,
                })
                .name(self.name.clone())
                .catalog_name(self.catalog_name.clone())
                .schema_name(INFORMATION_SCHEMA_NAME.to_string())
                .meta(table_meta)
                .table_type(TableType::Temporary)
                .build()
                .unwrap(),
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> TableResult<SendableRecordBatchStream> {
        self.get_stream(request)
    }
}

impl DataSource for InformationTable {
    fn get_stream(&self, request: ScanRequest) -> TableResult<SendableRecordBatchStream> {
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
