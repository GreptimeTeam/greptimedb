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

use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use store_api::data_source::DataSource;
use store_api::storage::ScanRequest;

use crate::error::UnsupportedSnafu;
use crate::metadata::{FilterPushDownType, TableInfoRef};
use crate::{Table, TableRef};

#[derive(Clone)]
pub struct DistTable;

impl DistTable {
    pub fn table(table_info: TableInfoRef) -> TableRef {
        let data_source = Arc::new(DummyDataSource);
        let table = Table::new(table_info, FilterPushDownType::Inexact, data_source);
        Arc::new(table)
    }
}

pub struct DummyDataSource;

impl DataSource for DummyDataSource {
    fn get_stream(
        &self,
        _request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        UnsupportedSnafu {
            operation: "get stream from a distributed table",
        }
        .fail()
        .map_err(BoxedError::new)
    }
}
