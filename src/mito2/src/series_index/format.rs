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

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::error::Result;
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, SamplesFileHandle};
use crate::series_index::catalog::SeriesIndexEntry;
use crate::series_index::{MetricSeriesId, SeriesIndexFileHandle};

/// Writers own partial outputs until finish succeeds; abort removes every partial object.
/// A failed finish must clean up its output before returning; implementations also own
/// cleanup on cancellation while a writer is live.
#[async_trait]
pub(crate) trait FormatWriter: Send {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// A read has independent traversal state and pins its immutable artifact.
#[allow(
    dead_code,
    reason = "Query adapter will consume the format reader separately."
)]
#[derive(Debug, Clone, Default)]
pub(crate) struct SeriesReadRequest {
    pub(crate) projection: Vec<String>,
    pub(crate) table_id: Option<u32>,
    pub(crate) ids: Option<BTreeSet<MetricSeriesId>>,
    pub(crate) filters: Vec<SimpleFilterEvaluator>,
}

#[allow(
    dead_code,
    reason = "Query adapter will consume the format reader separately."
)]
pub(crate) trait FormatReader: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn read(&self, request: SeriesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// Builds and opens immutable series-index artifacts.
#[async_trait]
pub(crate) trait SeriesIndexFormat: Debug + Send + Sync {
    fn format(&self) -> ArtifactFormat;
    #[allow(dead_code, reason = "Query adapter is deferred.")]
    async fn reader(
        &self,
        _store: ObjectStore,
        _handle: SeriesIndexFileHandle,
    ) -> Result<Box<dyn FormatReader>> {
        crate::error::UnsupportedOperationSnafu {
            err_msg: "This series format has no reader",
        }
        .fail()
    }
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>>;
}

/// Selection in the stored timestamp unit. No visibility filtering is implicit.
#[allow(dead_code, reason = "Query adapter is deferred.")]
#[derive(Debug, Clone, Default)]
pub(crate) struct SamplesReadRequest {
    pub(crate) projection: Vec<String>,
    pub(crate) table_id: Option<u32>,
    pub(crate) ids: Option<BTreeSet<MetricSeriesId>>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
}

#[allow(dead_code, reason = "Query adapter is deferred.")]
pub(crate) trait SamplesFormatReader: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn read(&self, request: SamplesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// Builds and opens immutable per-SST samples artifacts.
#[async_trait]
pub(crate) trait SamplesFormat: Debug + Send + Sync {
    fn format(&self) -> ArtifactFormat;
    #[allow(dead_code, reason = "Query adapter is deferred.")]
    async fn reader(
        &self,
        _store: ObjectStore,
        _handle: SamplesFileHandle,
    ) -> Result<Box<dyn SamplesFormatReader>> {
        crate::error::UnsupportedOperationSnafu {
            err_msg: "This samples format has no reader",
        }
        .fail()
    }
    /// The caller supplies a unique prefix. Finish reports all objects owned below it.
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        source_file_id: FileId,
        generation: FileId,
    ) -> Result<Box<dyn FormatWriter>>;
}

/// Optional writers, captured once per reconciliation pass.
/// Without an index backend, builds use the existing Parquet writer.
#[derive(Debug, Clone, Default)]
pub(crate) struct SeriesFormats {
    pub(crate) index: Option<Arc<dyn SeriesIndexFormat>>,
    pub(crate) samples: Option<Arc<dyn SamplesFormat>>,
}
