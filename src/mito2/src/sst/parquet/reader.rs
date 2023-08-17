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

//! Parquet reader.

use std::sync::Arc;

use async_compat::CompatExt;
use async_trait::async_trait;
use common_time::range::TimestampRange;
use datatypes::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::ParquetError;
use parquet::format::KeyValue;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;
use table::predicate::Predicate;
use tokio::io::BufReader;

use crate::error::{
    InvalidMetadataSnafu, InvalidParquetSnafu, OpenDalSnafu, ReadParquetSnafu, Result,
};
use crate::read::{Batch, BatchReader};
use crate::sst::file::FileHandle;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::PARQUET_METADATA_KEY;

/// Parquet SST reader builder.
pub struct ParquetReaderBuilder {
    file_dir: String,
    file_handle: FileHandle,
    object_store: ObjectStore,
    predicate: Option<Predicate>,
    time_range: Option<TimestampRange>,
    projection: Option<Vec<ColumnId>>,
}

impl ParquetReaderBuilder {
    /// Returns a new [ParquetReaderBuilder] to read specific SST.
    pub fn new(
        file_dir: String,
        file_handle: FileHandle,
        object_store: ObjectStore,
    ) -> ParquetReaderBuilder {
        ParquetReaderBuilder {
            file_dir,
            file_handle,
            object_store,
            predicate: None,
            time_range: None,
            projection: None,
        }
    }

    /// Attaches the predicate to the builder.
    pub fn predicate(mut self, predicate: Predicate) -> ParquetReaderBuilder {
        self.predicate = Some(predicate);
        self
    }

    /// Attaches the time range to the builder.
    pub fn time_range(mut self, time_range: TimestampRange) -> ParquetReaderBuilder {
        self.time_range = Some(time_range);
        self
    }

    /// Attaches the projection to the builder.
    ///
    /// The reader only applies the projection to fields.
    pub fn projection(mut self, projection: Vec<ColumnId>) -> ParquetReaderBuilder {
        self.projection = Some(projection);
        self
    }

    /// Builds a [ParquetReader].
    pub fn build(self) -> ParquetReader {
        let file_path = self.file_handle.file_path(&self.file_dir);
        ParquetReader {
            file_path,
            file_handle: self.file_handle,
            object_store: self.object_store,
            predicate: self.predicate,
            time_range: self.time_range,
            projection: self.projection,
            stream: None,
            read_format: None,
            batches: Vec::new(),
        }
    }
}

type BoxedRecordBatchStream = BoxStream<'static, std::result::Result<RecordBatch, ParquetError>>;

/// Parquet batch reader.
pub struct ParquetReader {
    /// Path of the file.
    file_path: String,
    /// SST file to read.
    ///
    /// Holds the file handle to avoid the file purge purge it.
    file_handle: FileHandle,
    object_store: ObjectStore,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Time range to filter.
    time_range: Option<TimestampRange>,
    /// Metadata of columns to read.
    ///
    /// `None` reads all columns. Due to schema change, the projection
    /// can contain columns not in the parquet file.
    projection: Option<Vec<ColumnId>>,

    /// Inner parquet record batch stream.
    stream: Option<BoxedRecordBatchStream>,
    /// Helper to read record batches.
    ///
    /// Not `None` if [ParquetReader::stream] is not `None`.
    read_format: Option<ReadFormat>,
    /// Buffered batches to return.
    batches: Vec<Batch>,
}

impl ParquetReader {
    // TODO(yingwen): Init reader in builder so we can get the schema of the reader.
    /// Initializes the reader and the parquet stream.
    async fn maybe_init(&mut self) -> Result<()> {
        if self.stream.is_some() {
            // Already initialized.
            return Ok(());
        }

        // Creates parquet stream builder.
        let reader = self
            .object_store
            .reader(&self.file_path)
            .await
            .context(OpenDalSnafu)?
            .compat();
        let buf_reader = BufReader::new(reader);
        let mut builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
            .await
            .context(ReadParquetSnafu {
                path: &self.file_path,
            })?;

        // Decode region metadata.
        let key_value_meta = builder.metadata().file_metadata().key_value_metadata();
        let region_meta = self.get_region_metadata(key_value_meta)?;

        // Prune row groups by metadata.
        if let Some(predicate) = &self.predicate {
            // TODO(yingwen): Now we encode tags into the full primary key so we need some approach
            // to implement pruning.
            let pruned_row_groups = predicate
                .prune_row_groups(builder.metadata().row_groups())
                .into_iter()
                .enumerate()
                .filter_map(|(idx, valid)| if valid { Some(idx) } else { None })
                .collect::<Vec<_>>();
            builder = builder.with_row_groups(pruned_row_groups);
        }

        let read_format = ReadFormat::new(Arc::new(region_meta));
        // The arrow schema converted from the region meta should be the same as parquet's.
        // We only compare fields to avoid schema's metadata breaks the comparision.
        ensure!(
            read_format.arrow_schema().fields() == builder.schema().fields(),
            InvalidParquetSnafu {
                file: &self.file_path,
                reason: format!(
                    "schema mismatch, expect: {:?}, given: {:?}",
                    read_format.arrow_schema().fields(),
                    builder.schema().fields()
                )
            }
        );

        let parquet_schema_desc = builder.metadata().file_metadata().schema_descr();
        if let Some(column_ids) = self.projection.as_ref() {
            let indices = read_format.projection_indices(column_ids.iter().copied());
            let projection_mask = ProjectionMask::roots(parquet_schema_desc, indices);
            builder = builder.with_projection(projection_mask);
        }

        let stream = builder.build().context(ReadParquetSnafu {
            path: &self.file_path,
        })?;
        self.stream = Some(Box::pin(stream));
        self.read_format = Some(read_format);

        Ok(())
    }

    /// Decode region metadata from key value.
    fn get_region_metadata(
        &self,
        key_value_meta: Option<&Vec<KeyValue>>,
    ) -> Result<RegionMetadata> {
        let key_values = key_value_meta.context(InvalidParquetSnafu {
            file: &self.file_path,
            reason: "missing key value meta",
        })?;
        let meta_value = key_values
            .iter()
            .find(|kv| kv.key == PARQUET_METADATA_KEY)
            .with_context(|| InvalidParquetSnafu {
                file: &self.file_path,
                reason: format!("key {} not found", PARQUET_METADATA_KEY),
            })?;
        let json = meta_value
            .value
            .as_ref()
            .with_context(|| InvalidParquetSnafu {
                file: &self.file_path,
                reason: format!("No value for key {}", PARQUET_METADATA_KEY),
            })?;

        RegionMetadata::from_json(json).context(InvalidMetadataSnafu)
    }
}

#[async_trait]
impl BatchReader for ParquetReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.maybe_init().await?;

        if let Some(batch) = self.batches.pop() {
            return Ok(Some(batch));
        }

        // We need to fetch next record batch and convert it to batches.
        let Some(record_batch) =
            self.stream
                .as_mut()
                .unwrap()
                .try_next()
                .await
                .context(ReadParquetSnafu {
                    path: &self.file_path,
                })?
        else {
            return Ok(None);
        };

        // Safety: the reader is initialized.
        let read_format = self.read_format.as_ref().unwrap();
        read_format.convert_record_batch(&record_batch, &mut self.batches)?;
        // Reverse batches so we could pop it.
        self.batches.reverse();

        Ok(self.batches.pop())
    }
}
