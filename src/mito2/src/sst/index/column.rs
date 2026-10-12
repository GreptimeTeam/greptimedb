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

use api::v1::SemanticType;
use datatypes::arrow::array::Array;
use datatypes::arrow::record_batch::RecordBatch;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::sst::parquet::format::PrimaryKeyArray;

/// Yields (first row, row count) for indexing a materialized column.
/// Tags are constant within consecutive equal PK dictionary keys. Fields and
/// timestamps must still be visited row by row. Distinct dictionary entries may
/// contain equal PK bytes; keeping those runs separate is correct and avoids decoding.
/// Inputs without a non-null PK dictionary fall back to visiting individual rows.
pub(crate) fn column_index_rows(
    batch: &RecordBatch,
    semantic_type: SemanticType,
) -> impl Iterator<Item = (usize, usize)> + '_ {
    let keys = (semantic_type == SemanticType::Tag)
        .then(|| batch.column_by_name(PRIMARY_KEY_COLUMN_NAME))
        .flatten()
        .and_then(|array| array.as_any().downcast_ref::<PrimaryKeyArray>())
        .filter(|array| array.null_count() == 0 && array.values().null_count() == 0)
        .map(|array| array.keys().values());
    let mut row = 0;
    std::iter::from_fn(move || {
        if row == batch.num_rows() {
            return None;
        }
        let start = row;
        row += 1;
        if let Some(keys) = keys {
            while row < keys.len() && keys[row] == keys[start] {
                row += 1;
            }
        }
        Some((start, row - start))
    })
}
