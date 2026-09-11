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

use arrow::record_batch::RecordBatch;
use datatypes::timestamp::append_timestamps;
use snafu::OptionExt;

use crate::error::{self, Result};

/// Extracts non-null timestamps in the source column's native time unit.
pub fn extract_timestamps(rb: &RecordBatch, timestamp_index_name: &str) -> Result<Vec<i64>> {
    let ts_col = rb
        .column_by_name(timestamp_index_name)
        .context(error::ColumnNotFoundSnafu {
            msg: timestamp_index_name,
        })?;
    if rb.num_rows() == 0 {
        return Ok(vec![]);
    }
    let mut timestamps = Vec::with_capacity(rb.num_rows());
    append_timestamps(ts_col, &mut timestamps).with_context(|| {
        error::InvalidTimeIndexTypeSnafu {
            ty: ts_col.data_type().clone(),
        }
    })?;
    Ok(timestamps)
}
