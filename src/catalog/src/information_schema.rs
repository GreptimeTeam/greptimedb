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
mod empty_table;
mod empty_table_schemas;
mod engines;
mod table_names;
mod tables;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use common_catalog::consts::{self, INFORMATION_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use empty_table_schemas::get_schema;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::data_source::DataSource;
use store_api::storage::{ScanRequest, TableId};
use table::error::{SchemaConversionSnafu, TablesRecordBatchSnafu};
use table::metadata::{
    FilterPushDownType, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType,
};
use table::thin_table::{ThinTable, ThinTableAdapter};
use table::TableRef;
pub use table_names::*;

use self::columns::InformationSchemaColumns;
use crate::error::Result;
use crate::information_schema::empty_table::EmptyTable;
use crate::information_schema::engines::InformationSchemaEngines;
use crate::information_schema::tables::InformationSchemaTables;
use crate::CatalogManager;

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
    /// Including `tables` and `columns`.
    pub fn build(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> HashMap<String, TableRef> {
        let provider = Self::new(catalog_name, catalog_manager);

        let mut schema = HashMap::new();
        schema.insert(TABLES.to_owned(), provider.table(TABLES).unwrap());
        schema.insert(COLUMNS.to_owned(), provider.table(COLUMNS).unwrap());
        schema.insert(ENGINES.to_owned(), provider.table(ENGINES).unwrap());
        // Tables not implemented
        schema.insert(
            COLUMN_PRIVILEGES.to_owned(),
            provider.table(COLUMN_PRIVILEGES).unwrap(),
        );
        schema.insert(
            COLUMN_STATISTICS.to_owned(),
            provider.table(COLUMN_STATISTICS).unwrap(),
        );
        schema
    }

    pub fn table(&self, name: &str) -> Option<TableRef> {
        self.information_table(name).map(|table| {
            let table_info = Self::table_info(self.catalog_name.clone(), &table);
            let filter_pushdown = FilterPushDownType::Unsupported;
            let thin_table = ThinTable::new(table_info, filter_pushdown);

            let data_source = Arc::new(InformationTableDataSource::new(table));
            Arc::new(ThinTableAdapter::new(thin_table, data_source)) as _
        })
    }

    fn information_table(&self, name: &str) -> Option<InformationTableRef> {
        match name.to_ascii_lowercase().as_str() {
            TABLES => Some(Arc::new(InformationSchemaTables::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            COLUMNS => Some(Arc::new(InformationSchemaColumns::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            ENGINES => Some(Arc::new(InformationSchemaEngines::new()) as _),
            // Table not implemented
            COLUMN_PRIVILEGES => Some(Arc::new(EmptyTable::new(
                consts::INFORMATION_SCHEMA_COLUMN_PRIVILEGES_TABLE_ID,
                COLUMN_PRIVILEGES,
                get_schema(COLUMN_PRIVILEGES),
            ))),
            COLUMN_STATISTICS => Some(Arc::new(EmptyTable::new(
                consts::INFORMATION_SCHEMA_COLUMN_STATISTICS_TABLE_ID,
                COLUMN_STATISTICS,
                get_schema(COLUMN_STATISTICS),
            ))),
            _ => None,
        }
    }

    fn table_info(catalog_name: String, table: &InformationTableRef) -> TableInfoRef {
        let table_meta = TableMetaBuilder::default()
            .schema(table.schema())
            .primary_key_indices(vec![])
            .next_column_id(0)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(table.table_id())
            .name(table.table_name().to_owned())
            .catalog_name(catalog_name)
            .schema_name(INFORMATION_SCHEMA_NAME.to_owned())
            .meta(table_meta)
            .table_type(table.table_type())
            .build()
            .unwrap();
        Arc::new(table_info)
    }
}

trait InformationTable {
    fn table_id(&self) -> TableId;

    fn table_name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    fn to_stream(&self) -> Result<SendableRecordBatchStream>;

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}

type InformationTableRef = Arc<dyn InformationTable + Send + Sync>;

struct InformationTableDataSource {
    table: InformationTableRef,
}

impl InformationTableDataSource {
    fn new(table: InformationTableRef) -> Self {
        Self { table }
    }

    fn try_project(&self, projection: &[usize]) -> std::result::Result<SchemaRef, BoxedError> {
        let schema = self
            .table
            .schema()
            .try_project(projection)
            .context(SchemaConversionSnafu)
            .map_err(BoxedError::new)?;
        Ok(Arc::new(schema))
    }
}

impl DataSource for InformationTableDataSource {
    fn get_stream(
        &self,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        let projection = request.projection;
        let projected_schema = match &projection {
            Some(projection) => self.try_project(projection)?,
            None => self.table.schema(),
        };

        let stream = self
            .table
            .to_stream()
            .map_err(BoxedError::new)
            .context(TablesRecordBatchSnafu)
            .map_err(BoxedError::new)?
            .map(move |batch| match &projection {
                Some(p) => batch.and_then(|b| b.try_project(p)),
                None => batch,
            });

        let stream = RecordBatchStreamWrapper {
            schema: projected_schema,
            stream: Box::pin(stream),
            output_ordering: None,
        };
        Ok(Box::pin(stream))
    }
}
