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

use common_query::prelude::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::data_source::DataSourceRef;
use store_api::storage::ScanRequest;

use crate::error::{Result, TablesRecordBatchSnafu};
use crate::metadata::{FilterPushDownType, TableInfoRef, TableType};

/// The `ThinTable` struct will replace the `Table` trait.
/// TODO(zhongzc): After completion, perform renaming and documentation work.
pub struct ThinTable {
    table_info: TableInfoRef,
    filter_pushdown: FilterPushDownType,
    data_source: DataSourceRef,
}

impl ThinTable {
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

    pub fn schema(&self) -> SchemaRef {
        self.table_info.meta.schema.clone()
    }

    pub fn table_info(&self) -> TableInfoRef {
        self.table_info.clone()
    }

    pub fn table_type(&self) -> TableType {
        self.table_info.table_type
    }

    pub async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        self.data_source
            .get_stream(request)
            .context(TablesRecordBatchSnafu)
    }

    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<FilterPushDownType>> {
        Ok(vec![self.filter_pushdown; filters.len()])
    }
}
