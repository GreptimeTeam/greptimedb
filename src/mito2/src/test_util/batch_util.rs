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

use common_recordbatch::RecordBatches;
use datatypes::arrow::array::RecordBatch;
use datatypes::arrow::compute;
use datatypes::arrow::compute::SortColumn;
use datatypes::arrow::util::pretty;

/// Sorts `batches` by column `names`.
pub fn sort_batches_and_print(batches: &RecordBatches, names: &[&str]) -> String {
    let schema = batches.schema();
    let record_batches = batches.iter().map(|batch| batch.df_record_batch());
    let record_batch = compute::concat_batches(schema.arrow_schema(), record_batches).unwrap();
    let columns: Vec<_> = names
        .iter()
        .map(|name| {
            let array = record_batch.column_by_name(name).unwrap();
            SortColumn {
                values: array.clone(),
                options: None,
            }
        })
        .collect();
    let indices = compute::lexsort_to_indices(&columns, None).unwrap();
    let columns = record_batch
        .columns()
        .iter()
        .map(|array| compute::take(&array, &indices, None).unwrap())
        .collect();
    let record_batch = RecordBatch::try_new(record_batch.schema(), columns).unwrap();

    pretty::pretty_format_batches(&[record_batch])
        .unwrap()
        .to_string()
}
