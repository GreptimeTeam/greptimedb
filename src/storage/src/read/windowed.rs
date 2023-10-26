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

use arrow::compute::SortOptions;
use arrow::row::{RowConverter, SortField};
use arrow_array::{Array, ArrayRef};
use common_recordbatch::OrderOption;
use common_telemetry::timer;
use datatypes::data_type::DataType;
use datatypes::vectors::Helper;
use metrics::histogram;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::read::{Batch, BatchReader};
use crate::schema::{ProjectedSchemaRef, StoreSchema};

/// [WindowedReader] provides a windowed record batch reader that scans all rows within a window
/// at a time and sort these rows ordered in `[<timestamp>, <PK>]` order.
pub struct WindowedReader<R> {
    /// Schema to read
    pub schema: ProjectedSchemaRef,
    /// Each reader reads a slice of time window
    pub readers: Vec<R>,
    /// `order_options` defines how records within windows are sorted.
    pub order_options: Vec<OrderOption>,
}

impl<R> WindowedReader<R> {
    /// Creates a new [WindowedReader] from given schema and a set of boxed readers.
    ///
    /// ### Note
    /// [WindowedReader] always reads the readers in a reverse order. The last reader in `readers`
    /// gets polled first.
    pub fn new(
        schema: ProjectedSchemaRef,
        readers: Vec<R>,
        order_options: Vec<OrderOption>,
    ) -> Self {
        Self {
            schema,
            readers,
            order_options,
        }
    }
}

#[async_trait::async_trait]
impl<R> BatchReader for WindowedReader<R>
where
    R: BatchReader,
{
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let _window_scan_elapsed = timer!(crate::metrics::WINDOW_SCAN_ELAPSED);
        let Some(mut reader) = self.readers.pop() else {
            return Ok(None);
        };

        let store_schema = self.schema.schema_to_read();
        let mut batches = vec![];
        while let Some(batch) = reader.next_batch().await? {
            batches.push(
                batch
                    .columns
                    .into_iter()
                    .map(|v| v.to_arrow_array())
                    .collect::<Vec<_>>(),
            );
        }

        let Some(num_columns) = batches.get(0).map(|b| b.len()) else {
            // the reader does not yield data, a batch of empty vectors must be returned instead of
            // an empty batch without any column.
            let empty_columns = store_schema
                .columns()
                .iter()
                .map(|s| s.desc.data_type.create_mutable_vector(0).to_vector())
                .collect();
            return Ok(Some(Batch::new(empty_columns)));
        };
        let mut vectors_in_batch = Vec::with_capacity(num_columns);

        for idx in 0..num_columns {
            let columns: Vec<&dyn Array> =
                batches.iter().map(|b| b[idx].as_ref()).collect::<Vec<_>>();
            vectors_in_batch
                .push(arrow::compute::concat(&columns).context(error::ConvertColumnsToRowsSnafu)?);
        }
        if let Some(v) = vectors_in_batch.get(0) {
            histogram!(crate::metrics::WINDOW_SCAN_ROWS_PER_WINDOW, v.len() as f64);
        }
        let sorted = sort_by_rows(&self.schema, vectors_in_batch, &self.order_options)?;
        let vectors = sorted
            .iter()
            .zip(store_schema.columns().iter().map(|c| &c.desc.name))
            .map(|(arr, name)| {
                Helper::try_into_vector(arr).context(error::ConvertChunkSnafu { name })
            })
            .collect::<Result<_>>()?;
        Ok(Some(Batch::new(vectors)))
    }
}

fn sort_by_rows(
    schema: &ProjectedSchemaRef,
    arrays: Vec<ArrayRef>,
    order_options: &[OrderOption],
) -> Result<Vec<ArrayRef>> {
    let store_schema = schema.schema_to_read();
    let sort_columns = build_sorted_columns(store_schema, order_options);
    // Convert columns to rows to speed lexicographic sort
    // TODO(hl): maybe optimize to lexsort_to_index when only timestamp column is involved.
    let row_converter = RowConverter::new(
        sort_columns
            .iter()
            .map(|(idx, descending)| {
                SortField::new_with_options(
                    store_schema.columns()[*idx].desc.data_type.as_arrow_type(),
                    SortOptions {
                        descending: *descending,
                        nulls_first: true,
                    },
                )
            })
            .collect(),
    )
    .context(error::ConvertColumnsToRowsSnafu)?;

    let columns_to_sort = sort_columns
        .into_iter()
        .map(|(idx, _)| arrays[idx].clone())
        .collect::<Vec<_>>();

    let rows_to_sort = row_converter
        .convert_columns(&columns_to_sort)
        .context(error::ConvertColumnsToRowsSnafu)?;

    let mut sort_pairs = rows_to_sort.iter().enumerate().collect::<Vec<_>>();
    sort_pairs.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

    let idx =
        arrow::array::UInt32Array::from_iter_values(sort_pairs.iter().map(|(i, _)| *i as u32));

    let sorted = arrays
        .iter()
        .map(|arr| arrow::compute::take(arr, &idx, None))
        .collect::<arrow::error::Result<Vec<_>>>()
        .context(error::SortArraysSnafu)?;

    debug_assert_eq!(sorted.len(), store_schema.num_columns());

    Ok(sorted)
}

/// Builds sorted columns from `order_options`.
/// Returns a vector of columns indices to sort and sort orders (true means descending order).
fn build_sorted_columns(schema: &StoreSchema, order_options: &[OrderOption]) -> Vec<(usize, bool)> {
    order_options
        .iter()
        .map(|o| (schema.column_index(&o.name), o.options.descending))
        .collect()
}
