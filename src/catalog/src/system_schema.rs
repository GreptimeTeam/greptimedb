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

pub mod information_schema;
mod memory_table;
pub mod pg_catalog;
mod predicate;
mod utils;

use std::collections::HashMap;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use snafu::ResultExt;
use store_api::data_source::DataSource;
use store_api::storage::ScanRequest;
use table::error::{SchemaConversionSnafu, TablesRecordBatchSnafu};
use table::metadata::{
    FilterPushDownType, TableId, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType,
};
use table::{Table, TableRef};

use crate::error::Result;

pub trait SystemSchemaProvider {
    /// Returns a map of [TableRef] in information schema.
    fn tables(&self) -> &HashMap<String, TableRef>;

    /// Returns the [TableRef] by table name.
    fn table(&self, name: &str) -> Option<TableRef> {
        self.tables().get(name).cloned()
    }

    /// Returns table names in the order of table id.
    fn table_names(&self) -> Vec<String> {
        let mut tables = self.tables().values().clone().collect::<Vec<_>>();

        tables.sort_by(|t1, t2| {
            t1.table_info()
                .table_id()
                .partial_cmp(&t2.table_info().table_id())
                .unwrap()
        });
        tables
            .into_iter()
            .map(|t| t.table_info().name.clone())
            .collect()
    }
}

trait SystemSchemaProviderInner {
    fn catalog_name(&self) -> &str;
    fn schema_name() -> &'static str;
    fn build_table(&self, name: &str) -> Option<TableRef> {
        self.system_table(name).map(|table| {
            let table_info = Self::table_info(self.catalog_name().to_string(), &table);
            let filter_pushdown = FilterPushDownType::Inexact;
            let data_source = Arc::new(SystemTableDataSource::new(table));
            let table = Table::new(table_info, filter_pushdown, data_source);
            Arc::new(table)
        })
    }
    fn system_table(&self, name: &str) -> Option<SystemTableRef>;

    fn table_info(catalog_name: String, table: &SystemTableRef) -> TableInfoRef {
        let table_meta = TableMetaBuilder::default()
            .schema(table.schema())
            .primary_key_indices(vec![])
            .next_column_id(0)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(table.table_id())
            .name(table.table_name().to_string())
            .catalog_name(catalog_name)
            .schema_name(Self::schema_name().to_string())
            .meta(table_meta)
            .table_type(table.table_type())
            .build()
            .unwrap();
        Arc::new(table_info)
    }
}

pub(crate) trait SystemTable {
    fn table_id(&self) -> TableId;

    fn table_name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream>;

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}

pub(crate) type SystemTableRef = Arc<dyn SystemTable + Send + Sync>;

struct SystemTableDataSource {
    table: SystemTableRef,
}

impl SystemTableDataSource {
    fn new(table: SystemTableRef) -> Self {
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

impl DataSource for SystemTableDataSource {
    fn get_stream(
        &self,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        let projection = request.projection.clone();
        let projected_schema = match &projection {
            Some(projection) => self.try_project(projection)?,
            None => self.table.schema(),
        };

        let stream = self
            .table
            .to_stream(request)
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
            metrics: Default::default(),
        };

        Ok(Box::pin(stream))
    }
}
