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

//! Request-owned, bounded backing buffers for independent Parquet streams.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use tokio::sync::Mutex;

pub const WINDOW_SIZE: usize = 8 * 1024 * 1024;
const WINDOW_BUDGET: usize = 2 * WINDOW_SIZE;

struct Window {
    path: String,
    offset: u64,
    bytes: Bytes,
}

/// Shared only by table decoders belonging to one authorized COPY request.
/// The lock also coalesces concurrent fetches for the same bytes.
pub struct PackReadWindows {
    store: ObjectStore,
    windows: Mutex<Vec<Window>>,
}

impl PackReadWindows {
    pub fn new(store: ObjectStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            windows: Mutex::new(Vec::new()),
        })
    }

    async fn read(&self, path: &str, object_length: u64, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > object_length {
            return Err(ParquetError::General("packed read outside object".into()));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let mut windows = self.windows.lock().await;
        for window in windows.iter() {
            if window.path == path
                && range.start >= window.offset
                && range.end <= window.offset + window.bytes.len() as u64
            {
                return Ok(window.bytes.slice(
                    (range.start - window.offset) as usize..(range.end - window.offset) as usize,
                ));
            }
        }
        let required = usize::try_from(range.end - range.start)
            .map_err(|_| ParquetError::General("packed read length overflow".into()))?;
        if required > WINDOW_SIZE {
            return Err(ParquetError::General(
                "packed cache range exceeds window".into(),
            ));
        }
        let fetch_len = (object_length - range.start).min(WINDOW_SIZE as u64) as usize;
        // Evict only unpinned allocations. Bytes slices held by a decoder still
        // count against the request budget, including their entire backing window.
        let mut retained: usize = windows.iter().map(|w| w.bytes.len()).sum();
        while retained.saturating_add(fetch_len) > WINDOW_BUDGET || windows.len() >= 2 {
            let Some(i) = windows.iter().position(|w| w.bytes.is_unique()) else {
                return Err(ParquetError::General(
                    "packed read backing-buffer budget exhausted".into(),
                ));
            };
            retained -= windows.remove(i).bytes.len();
        }
        let bytes = self
            .store
            .read_with(path)
            .range(range.start..range.start + fetch_len as u64)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?
            .to_bytes();
        if bytes.len() != fetch_len {
            return Err(ParquetError::General("short packed object read".into()));
        }
        // Backend buffers can be slices of larger allocations. Retain an exact
        // window allocation; transport staging is separate from cached backing.
        let bytes = Bytes::copy_from_slice(&bytes);
        common_telemetry::debug!(
            path,
            offset = range.start,
            length = fetch_len,
            "Fetched packed read window"
        );
        let result = bytes.slice(..required);
        windows.push(Window {
            path: path.into(),
            offset: range.start,
            bytes,
        });
        Ok(result)
    }
}

/// Pins one complete small stream; larger streams use the streaming reader.
pub struct PackedParquetReader {
    windows: Arc<PackReadWindows>,
    path: String,
    object_length: u64,
    offset: u64,
    length: u64,
    bytes: Option<Bytes>,
}

impl PackedParquetReader {
    pub fn new(
        windows: Arc<PackReadWindows>,
        path: String,
        object_length: u64,
        offset: u64,
        length: u64,
    ) -> Result<Self> {
        if length < 12
            || length > WINDOW_SIZE as u64
            || offset
                .checked_add(length)
                .is_none_or(|end| end > object_length)
        {
            return Err(ParquetError::General("invalid packed Parquet range".into()));
        }
        Ok(Self {
            windows,
            path,
            object_length,
            offset,
            length,
            bytes: None,
        })
    }
}

impl AsyncFileReader for PackedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            if range.start > range.end || range.end > self.length {
                return Err(ParquetError::General(
                    "read outside indexed Parquet stream".into(),
                ));
            }
            if self.bytes.is_none() {
                self.bytes = Some(
                    self.windows
                        .read(
                            &self.path,
                            self.object_length,
                            self.offset..self.offset + self.length,
                        )
                        .await?,
                );
            }
            Ok(self
                .bytes
                .as_ref()
                .unwrap()
                .slice(range.start as usize..range.end as usize))
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let length = self.length;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(options.map(|o| o.metadata_options().clone()))
                .load_and_finish(self, length)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{Field, Schema};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};

    use super::*;

    fn parquet(array: ArrayRef) -> (Vec<u8>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        (writer.into_inner().unwrap(), batch)
    }

    #[tokio::test]
    async fn independent_schemas_empty_streams_and_shared_ranges() {
        let dir = common_test_util::temp_dir::create_temp_dir("packed-reader");
        let store = crate::test_util::test_store(dir.path().to_str().unwrap());
        let fixtures = [
            parquet(Arc::new(Int64Array::from(vec![42]))),
            parquet(Arc::new(StringArray::from(vec!["different type"]))),
            parquet(Arc::new(Int64Array::from(Vec::<i64>::new()))),
        ];
        let packed: Vec<u8> = fixtures
            .iter()
            .flat_map(|(b, _)| b.iter().copied())
            .collect();
        store.write("pack-0.bin", packed.clone()).await.unwrap();
        let windows = PackReadWindows::new(store.clone());
        // A pinned shared window serves all the independent stream readers.
        let pinned = windows
            .read("pack-0.bin", packed.len() as u64, 0..packed.len() as u64)
            .await
            .unwrap();
        store.delete("pack-0.bin").await.unwrap();
        let mut offset = 0;
        for (bytes, expected) in fixtures {
            let reader = PackedParquetReader::new(
                windows.clone(),
                "pack-0.bin".into(),
                packed.len() as u64,
                offset,
                bytes.len() as u64,
            )
            .unwrap();
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
            assert_eq!(builder.schema(), &expected.schema());
            let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                expected.num_rows()
            );
            if expected.num_rows() > 0 {
                assert_eq!(batches[0], expected);
            }
            offset += bytes.len() as u64;
        }
        assert_eq!(windows.windows.lock().await.len(), 1);
        assert_eq!(pinned.len(), packed.len());
        let isolated = PackReadWindows::new(store);
        assert!(
            isolated
                .read("pack-0.bin", packed.len() as u64, 0..1)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn small_stream_pins_one_window_for_multiple_columns() {
        use parquet::file::properties::WriterProperties;
        let schema = Arc::new(Schema::new(
            (0..3)
                .map(|i| Field::new(format!("v{i}"), arrow_schema::DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let value = "x".repeat(1024 * 1024);
        let arrays = (0..3)
            .map(|_| Arc::new(StringArray::from(vec![value.as_str()])) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut writer = ArrowWriter::try_new(
            Vec::new(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        assert!(bytes.len() < WINDOW_SIZE);
        let dir = common_test_util::temp_dir::create_temp_dir("packed-columns");
        let store = crate::test_util::test_store(dir.path().to_str().unwrap());
        let mut object = vec![0; WINDOW_SIZE - 1024];
        let offset = object.len() as u64;
        object.extend_from_slice(&bytes);
        let length = object.len() as u64;
        store.write("pack-0.bin", object).await.unwrap();
        let windows = PackReadWindows::new(store);
        let pinned = windows.read("pack-0.bin", length, 0..1).await.unwrap();
        let mut reader = PackedParquetReader::new(
            windows.clone(),
            "pack-0.bin".into(),
            length,
            offset,
            bytes.len() as u64,
        )
        .unwrap();
        let metadata = reader.get_metadata(None).await.unwrap();
        let mut columns = Vec::new();
        for column in metadata.row_group(0).columns() {
            let (start, len) = column.byte_range();
            columns.push(reader.get_bytes(start..start + len).await.unwrap());
        }
        let batches: Vec<_> = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .build()
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches, vec![batch]);
        let retained = windows.windows.lock().await;
        assert_eq!(retained.len(), 2);
        assert!(retained.iter().all(|w| w.bytes.len() <= WINDOW_SIZE));
        assert!(retained.iter().map(|w| w.bytes.len()).sum::<usize>() <= WINDOW_BUDGET);
        assert_eq!(columns.len(), 3);
        assert_eq!(pinned[0], 0);
    }

    #[tokio::test]
    async fn corrupt_metadata_short_reads_and_relative_bounds() {
        let dir = common_test_util::temp_dir::create_temp_dir("packed-corrupt");
        let store = crate::test_util::test_store(dir.path().to_str().unwrap());
        store.write("pack-0.bin", vec![0u8; 12]).await.unwrap();
        let windows = PackReadWindows::new(store);
        assert!(windows.read("pack-0.bin", 20, 0..20).await.is_err());
        let mut reader = PackedParquetReader::new(windows, "pack-0.bin".into(), 12, 0, 12).unwrap();
        assert!(reader.get_bytes(0..13).await.is_err());
        assert!(reader.get_metadata(None).await.is_err());
    }
}
