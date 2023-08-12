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

//! Parquet writer.

use common_telemetry::debug;
use object_store::ObjectStore;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::error::Result;
use crate::read::Source;
use crate::sst::parquet::{SstInfo, WriteOptions, PARQUET_METADATA_KEY};
use crate::sst::stream_writer::BufferedWriter;

/// Parquet SST writer.
pub struct ParquetWriter<'a> {
    /// SST output file path.
    file_path: &'a str,
    /// Input data source.
    source: Source,
    object_store: ObjectStore,
}

impl<'a> ParquetWriter<'a> {
    /// Creates a new parquet SST writer.
    pub fn new(file_path: &'a str, source: Source, object_store: ObjectStore) -> ParquetWriter {
        ParquetWriter {
            file_path,
            source,
            object_store,
        }
    }

    /// Iterates source and writes all rows to Parquet file.
    ///
    /// Returns the [SstInfo] if the SST is written.
    pub async fn write_all(&mut self, opts: &WriteOptions) -> Result<Option<SstInfo>> {
        let metadata = self.source.metadata();

        let json = metadata.to_json()?;
        let key_value_meta = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

        let props_builder = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value_meta]))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(opts.row_group_size);
        // TODO(yingwen): Set column encoding for internal columns and timestamp.
        // e.g. Use DELTA_BINARY_PACKED and disable dictionary for sequence.

        let writer_props = props_builder.build();

        let mut buffered_writer = BufferedWriter::try_new(
            self.file_path.to_string(),
            self.object_store.clone(),
            &metadata.schema,
            Some(writer_props),
            opts.write_buffer_size.as_bytes() as usize,
        )
        .await?;

        while let Some(batch) = self.source.next_batch().await? {
            buffered_writer.write(&batch).await?;
        }
        // Get stats from the source.
        let stats = self.source.stats();

        if stats.num_rows == 0 {
            debug!(
                "No data written, try to stop the writer: {}",
                self.file_path
            );

            buffered_writer.close().await?;
            return Ok(None);
        }

        let (_file_meta, file_size) = buffered_writer.close().await?;
        let time_range = (stats.min_timestamp, stats.max_timestamp);

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(Some(SstInfo {
            time_range,
            file_size,
            num_rows: stats.num_rows,
        }))
    }
}

// TODO(yingwen): Port tests.
