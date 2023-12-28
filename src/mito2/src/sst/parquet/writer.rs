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
use object_store::ObjectStore;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;

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
    /// Input data source.
    source: Source,
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
        source: Source,
        object_store: ObjectStore,
    ) -> ParquetWriter {
        ParquetWriter {
            region_dir,
            file_id,
            source,
            metadata,
            object_store,
        }
    }

    /// Iterates source and writes all rows to Parquet file.
    ///
    /// Returns the [SstInfo] if the SST is written.
    pub async fn write_all(&mut self, opts: &WriteOptions) -> Result<Option<SstInfo>> {
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

        let file_path = location::sst_file_path(&self.region_dir, &self.file_id);
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
        let mut index_creator = (!self.metadata.primary_key.is_empty()).then(|| {
            SstIndexCreator::new(
                self.region_dir.clone(),
                self.file_id,
                &self.metadata,
                self.object_store.clone(),
                Some(4 * 1024 * 1024),
                NonZeroUsize::new(opts.row_group_size).unwrap(),
            )
        });

        while let Some(batch) = self.source.next_batch().await? {
            stats.update(&batch);
            let arrow_batch = write_format.convert_batch(&batch)?;

            buffered_writer
                .write(&arrow_batch)
                .await
                .context(WriteBufferSnafu)?;

            if let Some(creator) = index_creator.as_mut() {
                if let Err(err) = creator.update(&batch).await {
                    let region_id = &self.metadata.region_id;
                    let file_id = &self.file_id;
                    warn!("Failed to update index, error: {err}, region_id: {region_id}, file_id: {file_id}");

                    // Skip index creation if error occurs.
                    index_creator = None;
                }
            }
        }

        if let Some(mut creator) = index_creator {
            let region_id = &self.metadata.region_id;
            let file_id = &self.file_id;
            match creator.finish().await {
                Ok((row_count, byte_count)) => {
                    debug!("Create index successfully, region_id: {region_id}, file_id: {file_id}, bytes: {byte_count}, rows: {row_count}");
                }
                Err(err) => {
                    warn!("Failed to create index, error: {err}, region_id: {region_id}, file_id: {file_id}");
                    return Ok(None);
                }
            }
        }

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
