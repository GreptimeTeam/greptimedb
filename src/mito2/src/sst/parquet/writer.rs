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

use std::num::NonZeroUsize;

use common_datasource::file_format::parquet::BufferedWriter;
use common_telemetry::{debug, warn};
use common_time::Timestamp;
use futures::TryFutureExt;
use object_store::ObjectStore;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;
use store_api::storage::RegionId;

use super::helper::parse_parquet_metadata;
use crate::error::{InvalidMetadataSnafu, Result, WriteBufferSnafu};
use crate::read::{Batch, Source};
use crate::sst::file::FileId;
use crate::sst::index::creator::SstIndexCreator;
use crate::sst::location;
use crate::sst::parquet::format::WriteFormat;
use crate::sst::parquet::{SstInfo, WriteOptions, PARQUET_METADATA_KEY};

/// Parquet SST writer.
pub struct ParquetWriter {
    /// Directory of the region.
    region_dir: String,
    /// SST file id.
    file_id: FileId,
    /// Region metadata of the source and the target SST.
    metadata: RegionMetadataRef,
    object_store: ObjectStore,
}

impl ParquetWriter {
    /// Creates a new parquet SST writer.
    pub fn new(
        region_dir: String,
        file_id: FileId,
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
    ) -> ParquetWriter {
        ParquetWriter {
            region_dir,
            file_id,
            metadata,
            object_store,
        }
    }

    /// Iterates source and writes all rows to Parquet file.
    ///
    /// Returns the [SstInfo] if the SST is written.
    pub async fn write_all(
        &mut self,
        mut source: Source,
        opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let json = self.metadata.to_json().context(InvalidMetadataSnafu)?;
        let key_value_meta = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

        // TODO(yingwen): Find and set proper column encoding for internal columns: op type and tsid.
        let props_builder = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value_meta]))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(opts.row_group_size);

        let props_builder = Self::customize_column_config(props_builder, &self.metadata);
        let writer_props = props_builder.build();

        let file_path = location::sst_file_path(&self.region_dir, self.file_id);
        let write_format = WriteFormat::new(self.metadata.clone());
        let mut buffered_writer = BufferedWriter::try_new(
            file_path.clone(),
            self.object_store.clone(),
            write_format.arrow_schema(),
            Some(writer_props),
            opts.write_buffer_size.as_bytes() as usize,
        )
        .await
        .context(WriteBufferSnafu)?;

        let mut stats = SourceStats::default();
        let mut index = Indexer::new(
            self.file_id,
            self.region_dir.clone(),
            &self.metadata,
            self.object_store.clone(),
            opts,
        );

        while let Some(batch) = write_next_batch(&mut source, &write_format, &mut buffered_writer)
            .or_else(|err| async {
                // abort index creation if error occurs.
                index.abort().await;
                Err(err)
            })
            .await?
        {
            stats.update(&batch);
            index.update(&batch).await;
        }

        let inverted_index_available = index.finish().await;

        if stats.num_rows == 0 {
            debug!("No data written, try to stop the writer: {file_path}");

            buffered_writer.close().await.context(WriteBufferSnafu)?;
            return Ok(None);
        }

        let (file_meta, file_size) = buffered_writer.close().await.context(WriteBufferSnafu)?;

        // Safety: num rows > 0 so we must have min/max.
        let time_range = stats.time_range.unwrap();

        // convert FileMetaData to ParquetMetaData
        let parquet_metadata = parse_parquet_metadata(file_meta)?;

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(Some(SstInfo {
            time_range,
            file_size,
            num_rows: stats.num_rows,
            file_metadata: Some(parquet_metadata),
            inverted_index_available,
        }))
    }

    /// Customizes per-column config according to schema and maybe column cardinality.
    fn customize_column_config(
        builder: WriterPropertiesBuilder,
        region_metadata: &RegionMetadataRef,
    ) -> WriterPropertiesBuilder {
        let ts_col = ColumnPath::new(vec![region_metadata
            .time_index_column()
            .column_schema
            .name
            .clone()]);
        let seq_col = ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]);

        builder
            .set_column_encoding(seq_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(seq_col, false)
            .set_column_encoding(ts_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(ts_col, false)
    }
}

async fn write_next_batch(
    source: &mut Source,
    write_format: &WriteFormat,
    buffered_writer: &mut BufferedWriter,
) -> Result<Option<Batch>> {
    let Some(batch) = source.next_batch().await? else {
        return Ok(None);
    };

    let arrow_batch = write_format.convert_batch(&batch)?;
    buffered_writer
        .write(&arrow_batch)
        .await
        .context(WriteBufferSnafu)?;

    Ok(Some(batch))
}

#[derive(Default)]
struct SourceStats {
    /// Number of rows fetched.
    num_rows: usize,
    /// Time range of fetched batches.
    time_range: Option<(Timestamp, Timestamp)>,
}

impl SourceStats {
    fn update(&mut self, batch: &Batch) {
        if batch.is_empty() {
            return;
        }

        self.num_rows += batch.num_rows();
        // Safety: batch is not empty.
        let (min_in_batch, max_in_batch) = (
            batch.first_timestamp().unwrap(),
            batch.last_timestamp().unwrap(),
        );
        if let Some(time_range) = &mut self.time_range {
            time_range.0 = time_range.0.min(min_in_batch);
            time_range.1 = time_range.1.max(max_in_batch);
        } else {
            self.time_range = Some((min_in_batch, max_in_batch));
        }
    }
}

#[derive(Default)]
struct Indexer {
    file_id: FileId,
    region_id: RegionId,
    inner: Option<SstIndexCreator>,
}

impl Indexer {
    fn new(
        file_id: FileId,
        region_dir: String,
        metadata: &RegionMetadataRef,
        object_store: ObjectStore,
        opts: &WriteOptions,
    ) -> Self {
        let Some(option) = &opts.inverted_index else {
            debug!(
                "Skip creating index due to config, region_id: {}, file_id: {}",
                metadata.region_id, file_id,
            );
            return Self::default();
        };

        if metadata.primary_key.is_empty() {
            debug!(
                "No tag columns, skip creating index, region_id: {}, file_id: {}",
                metadata.region_id, file_id,
            );
            return Self::default();
        }

        let Some(row_group_size) = NonZeroUsize::new(opts.row_group_size) else {
            warn!(
                "Row group size is 0, skip creating index, region_id: {}, file_id: {}",
                metadata.region_id, file_id,
            );
            return Self::default();
        };

        let creator = SstIndexCreator::new(
            region_dir.clone(),
            file_id,
            metadata,
            object_store,
            option.creation_memory_usage_threshold,
            row_group_size,
        );

        Self {
            file_id,
            region_id: metadata.region_id,
            inner: Some(creator),
        }
    }

    async fn update(&mut self, batch: &Batch) {
        if let Some(creator) = self.inner.as_mut() {
            if let Err(err) = creator.update(batch).await {
                warn!(
                    err; "Failed to update index, skip creating index, region_id: {}, file_id: {}",
                    self.region_id, self.file_id,
                );

                // Skip index creation if error occurs.
                self.inner = None;
            }
        }
    }

    async fn finish(mut self) -> bool {
        if let Some(creator) = self.inner.as_mut() {
            match creator.finish().await {
                Ok((row_count, byte_count)) => {
                    debug!(
                        "Create index successfully, region_id: {}, file_id: {}, bytes: {}, rows: {}",
                        self.region_id, self.file_id, byte_count, row_count
                    );
                    return true;
                }
                Err(err) => {
                    warn!(
                        err; "Failed to create index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }
            }
        }

        false
    }

    async fn abort(&mut self) {
        if let Some(creator) = self.inner.as_mut() {
            if let Err(err) = creator.abort().await {
                warn!(
                    err; "Failed to abort index, region_id: {}, file_id: {}",
                    self.region_id, self.file_id,
                );
            }

            self.inner = None;
        }
    }
}
