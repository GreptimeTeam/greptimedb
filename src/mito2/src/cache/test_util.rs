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

//! Utilities for testing cache.

use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::array::{ArrayRef, Int64Array};
use datatypes::arrow::record_batch::RecordBatch;
use object_store::services::Fs;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaData;

/// Returns a parquet meta data.
pub(crate) fn parquet_meta() -> Arc<ParquetMetaData> {
    let file_data = parquet_file_data();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file_data)).unwrap();
    builder.metadata().clone()
}

/// Write a test parquet file to a buffer
fn parquet_file_data() -> Vec<u8> {
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, to_write.schema(), None).unwrap();
    writer.write(&to_write).unwrap();
    writer.close().unwrap();

    buffer
}

pub(crate) fn new_fs_store(path: &str) -> ObjectStore {
    let builder = Fs::default();
    ObjectStore::new(builder.root(path)).unwrap().finish()
}
