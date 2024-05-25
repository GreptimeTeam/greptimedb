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
use object_store::{ErrorKind, ObjectStore};
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

/// Fetches data from object store.
/// If the object store supports blocking, use sequence blocking read.
/// Otherwise, use concurrent read.
pub async fn fetch_byte_ranges(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    if object_store.info().full_capability().blocking {
        fetch_ranges_seq(file_path, object_store, ranges).await
    } else {
        fetch_ranges_concurrent(file_path, object_store, ranges).await
    }
}

/// Fetches data from object store sequentially
async fn fetch_ranges_seq(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    let block_object_store = object_store.blocking();
    let file_path = file_path.to_string();
    let ranges = ranges.to_vec();

    let f = move || -> object_store::Result<Vec<Bytes>> {
        ranges
            .into_iter()
            .map(|range| {
                let data = block_object_store
                    .read_with(&file_path)
                    .range(range.start..range.end)
                    .call()?;
                Ok::<_, object_store::Error>(data.to_bytes())
            })
            .collect::<object_store::Result<Vec<_>>>()
    };

    maybe_spawn_blocking(f).await
}

/// Fetches data from object store concurrently.
async fn fetch_ranges_concurrent(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    // TODO(QuenKar): may merge small ranges to a bigger range to optimize.
    let mut handles = Vec::with_capacity(ranges.len());
    for range in ranges {
        let future_read = object_store.read_with(file_path);
        handles.push(async move {
            let data = future_read.range(range.start..range.end).await?;
            Ok::<_, object_store::Error>(data.to_bytes())
        });
    }
    let results = futures::future::try_join_all(handles).await?;
    Ok(results)
}

//  Port from https://github.com/apache/arrow-rs/blob/802ed428f87051fdca31180430ddb0ecb2f60e8b/object_store/src/util.rs#L74-L83
/// Takes a function and spawns it to a tokio blocking pool if available
async fn maybe_spawn_blocking<F, T>(f: F) -> object_store::Result<T>
where
    F: FnOnce() -> object_store::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime
            .spawn_blocking(f)
            .await
            .map_err(new_task_join_error)?,
        Err(_) => f(),
    }
}

//  https://github.com/apache/opendal/blob/v0.46.0/core/src/raw/tokio_util.rs#L21-L24
/// Parse tokio error into opendal::Error.
fn new_task_join_error(e: tokio::task::JoinError) -> object_store::Error {
    object_store::Error::new(ErrorKind::Unexpected, "tokio task join failed").set_source(e)
}
