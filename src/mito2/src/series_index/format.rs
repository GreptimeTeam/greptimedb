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

//! Storage-format boundaries. Encodings stay outside reconciliation and source scheduling.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_time::range::TimestampRange;
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::read::BoxedRecordBatchStream;
use crate::read::series_scan::prepared::PreparedSeriesSourceFactory;
use crate::series_index::artifact::ArtifactMetadata;
use crate::series_index::catalog::{SeriesIndexEntry, series_index_path, series_metadata};
use crate::series_index::{
    SeriesIndexFileHandle, SeriesIndexSearcher, SeriesIndexWriter, SeriesIndexWriterOptions,
};

/// Writers own partial outputs until finish succeeds; abort removes every partial object.
/// A failed finish must clean up its output before returning; implementations also own
/// cleanup on cancellation while a writer is live.
#[async_trait]
pub(crate) trait FormatWriter: Send {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Index implementations return sorted, complete primary keys in one Binary column.
#[async_trait]
pub(crate) trait SeriesIndexFormat: Debug + Send + Sync {
    fn name(&self) -> &str;
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>>;
    async fn search(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        handle: SeriesIndexFileHandle,
        predicate: Option<Predicate>,
        time_range: Option<TimestampRange>,
    ) -> Result<BoxedRecordBatchStream>;
}

/// Builds samples artifacts; reading belongs to the prepared-source factory.
#[async_trait]
pub(crate) trait SamplesFormat: Debug + Send + Sync {
    fn name(&self) -> &str;
    /// The caller supplies a unique prefix. Finish reports all objects owned below it.
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        generation: FileId,
    ) -> Result<Box<dyn FormatWriter>>;
}

/// Independent backend selection, captured by readers with their catalog snapshot.
#[derive(Debug, Clone)]
pub(crate) struct SeriesFormats {
    /// Optional complete read path; independent of the artifact writers and batch adapters.
    pub(crate) prepared_source: Option<Arc<dyn PreparedSeriesSourceFactory>>,
    pub(crate) index: Arc<dyn SeriesIndexFormat>,
    pub(crate) samples: Option<Arc<dyn SamplesFormat>>,
}

impl Default for SeriesFormats {
    fn default() -> Self {
        Self {
            prepared_source: None,
            index: Arc::new(ParquetSeriesIndex),
            samples: None,
        }
    }
}

#[derive(Debug)]
struct ParquetSeriesIndex;

#[async_trait]
impl SeriesIndexFormat for ParquetSeriesIndex {
    fn name(&self) -> &str {
        "parquet"
    }

    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>> {
        let path = series_index_path(metadata.region_id, entry.index_uuid);
        let writer = SeriesIndexWriter::try_new(
            metadata,
            store,
            &path,
            SeriesIndexWriterOptions::default(),
            Some(series_metadata(entry)?),
        )
        .await?;
        Ok(Box::new(ParquetWriter {
            writer,
            path,
            generation: entry.index_uuid,
        }))
    }

    async fn search(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        handle: SeriesIndexFileHandle,
        predicate: Option<Predicate>,
        time_range: Option<TimestampRange>,
    ) -> Result<BoxedRecordBatchStream> {
        SeriesIndexSearcher::try_new(metadata, store, handle, predicate.as_ref(), time_range)
            .await?
            .search_primary_keys()
    }
}

struct ParquetWriter {
    writer: SeriesIndexWriter,
    path: String,
    generation: FileId,
}

#[async_trait]
impl FormatWriter for ParquetWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).await
    }
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata> {
        let metrics = self.writer.finish().await?;
        Ok(ArtifactMetadata {
            format: "parquet".to_string(),
            generation: self.generation,
            objects: vec![self.path],
            file_size: metrics.output_bytes,
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.writer.abort().await.map(|_| ())
    }
}
