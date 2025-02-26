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

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use parquet::basic::ColumnOrder;
use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::format;
use parquet::schema::types::{from_thrift, SchemaDescriptor};
use snafu::ResultExt;

use crate::error;
use crate::error::Result;

// Refer to https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/file/footer.rs#L74-L90
/// Convert [format::FileMetaData] to [ParquetMetaData]
pub fn parse_parquet_metadata(t_file_metadata: format::FileMetaData) -> Result<ParquetMetaData> {
    let schema = from_thrift(&t_file_metadata.schema).context(error::ConvertMetaDataSnafu)?;
    let schema_desc_ptr = Arc::new(SchemaDescriptor::new(schema));

    let mut row_groups = Vec::with_capacity(t_file_metadata.row_groups.len());
    for rg in t_file_metadata.row_groups {
        row_groups.push(
            RowGroupMetaData::from_thrift(schema_desc_ptr.clone(), rg)
                .context(error::ConvertMetaDataSnafu)?,
        );
    }
    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_desc_ptr);

    let file_metadata = FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        t_file_metadata.key_value_metadata,
        schema_desc_ptr,
        column_orders,
    );
    // There may be a problem owing to lacking of column_index and offset_index,
    // if we open page index in the future.
    Ok(ParquetMetaData::new(file_metadata, row_groups))
}

// Port from https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/file/footer.rs#L106-L137
/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
fn parse_column_orders(
    t_column_orders: Option<Vec<format::ColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> Option<Vec<ColumnOrder>> {
    match t_column_orders {
        Some(orders) => {
            // Should always be the case
            assert_eq!(
                orders.len(),
                schema_descr.num_columns(),
                "Column order length mismatch"
            );
            let mut res = Vec::with_capacity(schema_descr.num_columns());
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match orders[i] {
                    format::ColumnOrder::TYPEORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Some(res)
        }
        None => None,
    }
}

const FETCH_PARALLELISM: usize = 8;
const MERGE_GAP: usize = 512 * 1024;

/// Asynchronously fetches byte ranges from an object store.
///
/// * `FETCH_PARALLELISM` - The number of concurrent fetch operations.
/// * `MERGE_GAP` - The maximum gap size (in bytes) to merge small byte ranges for optimized fetching.
pub async fn fetch_byte_ranges(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    Ok(object_store
        .reader_with(file_path)
        .concurrent(FETCH_PARALLELISM)
        .gap(MERGE_GAP)
        .await?
        .fetch(ranges.to_vec())
        .await?
        .into_iter()
        .map(|buf| buf.to_bytes())
        .collect::<Vec<_>>())
}
