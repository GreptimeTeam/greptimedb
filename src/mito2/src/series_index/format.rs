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
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::error::Result;
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata};
use crate::series_index::catalog::SeriesIndexEntry;

/// Writers own partial outputs until finish succeeds; abort removes every partial object.
/// A failed finish must clean up its output before returning; implementations also own
/// cleanup on cancellation while a writer is live.
#[async_trait]
pub(crate) trait FormatWriter: Send {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Builds series-index artifacts; read interfaces are deferred.
#[async_trait]
pub(crate) trait SeriesIndexFormat: Debug + Send + Sync {
    fn format(&self) -> ArtifactFormat;
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>>;
}

/// Builds samples artifacts; read interfaces are deferred.
#[async_trait]
pub(crate) trait SamplesFormat: Debug + Send + Sync {
    fn format(&self) -> ArtifactFormat;
    /// The caller supplies a unique prefix. Finish reports all objects owned below it.
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
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
