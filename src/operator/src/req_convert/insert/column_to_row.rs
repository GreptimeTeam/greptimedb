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

use api::v1::{InsertRequest, InsertRequests, RowInsertRequest, RowInsertRequests};

use crate::error::Result;
use crate::req_convert::common::columns_to_rows;

pub struct ColumnToRow;

impl ColumnToRow {
    pub fn convert(requests: InsertRequests) -> Result<RowInsertRequests> {
        requests
            .inserts
            .into_iter()
            .map(request_column_to_row)
            .collect::<Result<Vec<_>>>()
            .map(|inserts| RowInsertRequests { inserts })
    }
}

fn request_column_to_row(request: InsertRequest) -> Result<RowInsertRequest> {
    let rows = columns_to_rows(request.columns, request.row_count)?;
    Ok(RowInsertRequest {
        table_name: request.table_name,
        rows: Some(rows),
    })
}
