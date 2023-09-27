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

//! Cache size of different cache value.

use std::mem;

use parquet::file::metadata::{
    FileMetaData, ParquetColumnIndex, ParquetMetaData, ParquetOffsetIndex, RowGroupMetaData,
};
use parquet::file::page_index::index::Index;
use parquet::format::{ColumnOrder, KeyValue, PageLocation};
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor, Type};

/// Returns estimated size of [ParquetMetaData].
pub fn parquet_meta_size(meta: &ParquetMetaData) -> usize {
    // struct size
    let mut size = mem::size_of::<ParquetMetaData>();
    // file_metadata
    size += file_meta_heap_size(meta.file_metadata());
    // row_groups
    size += meta
        .row_groups()
        .iter()
        .map(row_group_meta_heap_size)
        .sum::<usize>();
    // column_index
    size += meta
        .column_index()
        .map(parquet_column_index_heap_size)
        .unwrap_or(0);
    // offset_index
    size += meta
        .offset_index()
        .map(parquet_offset_index_heap_size)
        .unwrap_or(0);

    size
}

/// Returns estimated size of [FileMetaData] allocated from heap.
fn file_meta_heap_size(meta: &FileMetaData) -> usize {
    // created_by
    let mut size = meta.created_by().map(|s| s.len()).unwrap_or(0);
    // key_value_metadata
    size += meta
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| {
                    kv.key.len()
                        + kv.value.as_ref().map(|v| v.len()).unwrap_or(0)
                        + mem::size_of::<KeyValue>()
                })
                .sum()
        })
        .unwrap_or(0);
    // schema_descr (It's a ptr so we also add size of SchemaDescriptor).
    size += mem::size_of::<SchemaDescriptor>();
    size += schema_descr_heap_size(meta.schema_descr());
    // column_orders
    size += meta
        .column_orders()
        .map(|orders| orders.len() * mem::size_of::<ColumnOrder>())
        .unwrap_or(0);

    size
}

/// Returns estimated size of [SchemaDescriptor] allocated from heap.
fn schema_descr_heap_size(descr: &SchemaDescriptor) -> usize {
    // schema
    let mut size = mem::size_of::<Type>();
    // leaves
    size += descr
        .columns()
        .iter()
        .map(|descr| mem::size_of::<ColumnDescriptor>() + column_descr_heap_size(descr))
        .sum::<usize>();
    // leaf_to_base
    size += descr.num_columns() * mem::size_of::<usize>();

    size
}

/// Returns estimated size of [ColumnDescriptor] allocated from heap.
fn column_descr_heap_size(descr: &ColumnDescriptor) -> usize {
    descr.path().parts().iter().map(|s| s.len()).sum()
}

/// Returns estimated size of [ColumnDescriptor] allocated from heap.
fn row_group_meta_heap_size(meta: &RowGroupMetaData) -> usize {
    mem::size_of_val(meta.columns())
}

/// Returns estimated size of [ParquetColumnIndex] allocated from heap.
fn parquet_column_index_heap_size(column_index: &ParquetColumnIndex) -> usize {
    column_index
        .iter()
        .map(|row_group| row_group.len() * mem::size_of::<Index>() + mem::size_of_val(row_group))
        .sum()
}

/// Returns estimated size of [ParquetOffsetIndex] allocated from heap.
fn parquet_offset_index_heap_size(offset_index: &ParquetOffsetIndex) -> usize {
    offset_index
        .iter()
        .map(|row_group| {
            row_group
                .iter()
                .map(|column| {
                    column.len() * mem::size_of::<PageLocation>() + mem::size_of_val(column)
                })
                .sum::<usize>()
                + mem::size_of_val(row_group)
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::test_util::parquet_meta;

    #[test]
    fn test_parquet_meta_size() {
        let metadata = parquet_meta();

        assert_eq!(948, parquet_meta_size(&metadata));
    }
}
