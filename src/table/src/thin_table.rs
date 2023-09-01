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

use std::any::Any;

use async_trait::async_trait;
use common_query::prelude::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::data_source::DataSourceRef;
use store_api::storage::ScanRequest;

use crate::error::{Result, TablesRecordBatchSnafu};
use crate::metadata::{FilterPushDownType, TableInfoRef, TableType};
use crate::Table;

/// The `ThinTable` struct will replace the `Table` trait.
/// TODO(zhongzc): After completion, perform renaming and documentation work.
pub struct ThinTable {
    table_info: TableInfoRef,
    filter_pushdown: FilterPushDownType,
}

impl ThinTable {
    pub fn new(table_info: TableInfoRef, filter_pushdown: FilterPushDownType) -> Self {
        Self {
            table_info,
            filter_pushdown,
        }
    }
}

pub struct ThinTableAdapter {
    table: ThinTable,
    data_source: DataSourceRef,
}

impl ThinTableAdapter {
    pub fn new(table: ThinTable, data_source: DataSourceRef) -> Self {
        Self { table, data_source }
    }

    pub fn data_source(&self) -> DataSourceRef {
        self.data_source.clone()
    }
}

#[async_trait]
impl Table for ThinTableAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.table_info.meta.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        self.table.table_info.clone()
    }

    fn table_type(&self) -> TableType {
        self.table.table_info.table_type
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        self.data_source
            .get_stream(request)
            .context(TablesRecordBatchSnafu)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<FilterPushDownType>> {
        Ok(vec![self.table.filter_pushdown; filters.len()])
    }
}
