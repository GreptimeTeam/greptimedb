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

use std::sync::Arc;

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::data_source::DataSourceRef;
use store_api::storage::ScanRequest;

use crate::error::{Result, TablesRecordBatchSnafu};
use crate::metadata::{FilterPushDownType, TableInfoRef, TableType};

pub mod adapter;
mod metrics;
pub mod numbers;
pub mod scan;

pub type TableRef = Arc<Table>;

/// Table handle.
pub struct Table {
    table_info: TableInfoRef,
    filter_pushdown: FilterPushDownType,
    data_source: DataSourceRef,
}

impl Table {
    pub fn new(
        table_info: TableInfoRef,
        filter_pushdown: FilterPushDownType,
        data_source: DataSourceRef,
    ) -> Self {
        Self {
            table_info,
            filter_pushdown,
            data_source,
        }
    }

    pub fn data_source(&self) -> DataSourceRef {
        self.data_source.clone()
    }

    /// Get a reference to the schema for this table.
    pub fn schema(&self) -> SchemaRef {
        self.table_info.meta.schema.clone()
    }

    /// Get a reference to the table info.
    pub fn table_info(&self) -> TableInfoRef {
        self.table_info.clone()
    }

    /// Get the type of this table for metadata/catalog purposes.
    pub fn table_type(&self) -> TableType {
        self.table_info.table_type
    }

    pub async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        self.data_source
            .get_stream(request)
            .context(TablesRecordBatchSnafu)
    }

    /// Tests whether the table provider can make use of any or all filter expressions
    /// to optimise data retrieval.
    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<FilterPushDownType>> {
        Ok(vec![self.filter_pushdown; filters.len()])
    }
}
