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
use datatypes::data_type::DataType;
use datatypes::prelude::VectorRef;
use datatypes::vectors::{Helper, MutableVector};
use snafu::ResultExt;

use crate::error;
use crate::read::{Batch, BatchReader};
use crate::schema::ProjectedSchemaRef;

/// [WindowedReader] provides a windowed record batch reader that scans all rows within a window
/// at a time and sort these rows ordered in `[<timestamp>, <PK>]` order.
pub struct WindowedReader<R> {
    /// Schema to read
    pub schema: ProjectedSchemaRef,
    /// Each reader reads a slice of time window
    pub readers: Vec<R>,
}

impl<R> WindowedReader<R> {
    pub fn new(schema: ProjectedSchemaRef, readers: Vec<R>) -> Self {
        Self { schema, readers }
    }
}

#[async_trait::async_trait]
impl<R> BatchReader for WindowedReader<R>
where
    R: BatchReader,
{
    async fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        let Some(mut reader) = self.readers.pop() else { return Ok(None); };
        let mut existing = self
            .schema
            .schema_to_read()
            .columns()
            .iter()
            .map(|c| c.desc.data_type.create_mutable_vector(32))
            .collect::<Vec<Box<dyn MutableVector>>>();

        while let Some(batch) = reader.next_batch().await? {
            for (builder, arr) in existing.iter_mut().zip(batch.columns.iter()) {
                builder
                    .extend_slice_of(&**arr, 0, arr.len())
                    .context(error::PushBatchSnafu)?;
            }
        }

        let vectors_in_batch = existing.into_iter().map(|mut b| b.to_vector()).collect();
        let res = sort_by_rows(&self.schema, vectors_in_batch)?;
        Ok(Some(Batch::new(res)))
    }
}

fn sort_by_rows(
    schema: &ProjectedSchemaRef,
    vectors: Vec<VectorRef>,
) -> crate::error::Result<Vec<VectorRef>> {
    let sort_columns = build_sort_columns(schema);

    let store_schema = schema.schema_to_read();
    // Convert columns to rows to speed lexicographic sort
    let mut row_converter = RowConverter::new(
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
    .context(crate::error::ConvertColumnsToRowsSnafu)?;

    let columns_to_sort = sort_columns
        .iter()
        .map(|(idx, _)| vectors[*idx].to_arrow_array())
        .collect::<Vec<_>>();

    let rows_to_sort = row_converter.convert_columns(&columns_to_sort).unwrap();

    let mut sort_pairs = rows_to_sort.iter().enumerate().collect::<Vec<_>>();
    sort_pairs.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

    let idx =
        arrow::array::UInt32Array::from_iter_values(sort_pairs.iter().map(|(i, _)| *i as u32));

    let sorted = vectors
        .iter()
        .map(|arr| arrow::compute::take(&arr.to_arrow_array(), &idx, None))
        .collect::<arrow::error::Result<Vec<_>>>()
        .unwrap();

    debug_assert_eq!(sorted.len(), store_schema.num_columns());

    let vectors = sorted
        .iter()
        .zip(store_schema.columns().iter().map(|c| &c.desc.name))
        .map(|(arr, name)| Helper::try_into_vector(arr).context(error::ConvertChunkSnafu { name }))
        .collect::<crate::error::Result<_>>()?;

    Ok(vectors)
}

/// [<PK_1>, <PK_2>, TS] to [TS, <PK_1>, <PK_2>]
fn build_sort_columns(schema: &ProjectedSchemaRef) -> Vec<(usize, bool)> {
    let row_key_end = schema.schema_to_read().row_key_end();
    let ts_col_index = row_key_end - 1;
    let mut res = (0..(row_key_end - 1))
        .map(|idx| (idx, false))
        .collect::<Vec<_>>();
    res.insert(0, (ts_col_index, true));
    res
}
