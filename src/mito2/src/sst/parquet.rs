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

//! SST in parquet format.

mod format;
pub mod reader;
mod stats;
pub mod writer;

use common_base::readable_size::ReadableSize;

use crate::sst::file::FileTimeRange;

/// Key of metadata in parquet SST.
pub const PARQUET_METADATA_KEY: &str = "greptime:metadata";
const DEFAULT_WRITE_BUFFER_SIZE: ReadableSize = ReadableSize::mb(8);
/// Default batch size to read parquet files.
pub(crate) const DEFAULT_READ_BATCH_SIZE: usize = 1024;
/// Default row group size for parquet files.
const DEFAULT_ROW_GROUP_SIZE: usize = 100 * DEFAULT_READ_BATCH_SIZE;

/// Parquet write options.
#[derive(Debug)]
pub struct WriteOptions {
    /// Buffer size for async writer.
    pub write_buffer_size: ReadableSize,
    /// Row group size.
    pub row_group_size: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
        }
    }
}

/// Parquet SST info returned by the writer.
pub struct SstInfo {
    /// Time range of the SST.
    pub time_range: FileTimeRange,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of rows.
    pub num_rows: usize,
}
