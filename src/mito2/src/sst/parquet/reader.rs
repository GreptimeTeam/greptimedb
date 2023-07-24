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

use async_compat::CompatExt;
use async_trait::async_trait;
use common_time::range::TimestampRange;
use datatypes::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use snafu::ResultExt;
use table::predicate::Predicate;
use tokio::io::BufReader;

use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result};
use crate::read::{Batch, BatchReader};
use crate::sst::file::FileHandle;

/// Parquet SST reader builder.
pub struct ParquetReaderBuilder {
    file_dir: String,
    file_handle: FileHandle,
    object_store: ObjectStore,
    predicate: Option<Predicate>,
    time_range: Option<TimestampRange>,
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

    /// Builds a [ParquetReader].
    pub fn build(self) -> ParquetReader {
        let file_path = self.file_handle.file_path(&self.file_dir);
        ParquetReader {
            file_path,
            file_handle: self.file_handle,
            object_store: self.object_store,
            predicate: self.predicate,
            time_range: self.time_range,
            stream: None,
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

    /// Inner parquet record batch stream.
    stream: Option<BoxedRecordBatchStream>,
}

impl ParquetReader {
    /// Initializes the reader and the parquet stream.
    async fn maybe_init(&mut self) -> Result<()> {
        if self.stream.is_some() {
            // Already initialized.
            return Ok(());
        }

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

        // TODO(yingwen): Decode region metadata, create read adapter.

        // Prune row groups by metadata.
        if let Some(predicate) = &self.predicate {
            let pruned_row_groups = predicate
                .prune_row_groups(builder.metadata().row_groups())
                .into_iter()
                .enumerate()
                .filter_map(|(idx, valid)| if valid { Some(idx) } else { None })
                .collect::<Vec<_>>();
            builder = builder.with_row_groups(pruned_row_groups);
        }

        // TODO(yingwen): Projection.

        let stream = builder.build().context(ReadParquetSnafu {
            path: &self.file_path,
        })?;
        self.stream = Some(Box::pin(stream));

        Ok(())
    }

    /// Converts our [Batch] from arrow's [RecordBatch].
    fn convert_arrow_record_batch(&self, _record_batch: RecordBatch) -> Result<Batch> {
        unimplemented!()
    }
}

#[async_trait]
impl BatchReader for ParquetReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.maybe_init().await?;

        self.stream
            .as_mut()
            .unwrap()
            .try_next()
            .await
            .context(ReadParquetSnafu {
                path: &self.file_path,
            })?
            .map(|rb| self.convert_arrow_record_batch(rb))
            .transpose()
    }
}
