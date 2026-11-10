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

//! Planning and creation contracts for built-in indexes.

pub(crate) mod builtin;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use datatypes::arrow::record_batch::RecordBatch;
use index::target::IndexTarget;
use store_api::metadata::RegionMetadataRef;

use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::sst::file::RegionIndexId;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// Schema and options used to prepare work without reading input files.
pub(crate) struct PlanContext<'a> {
    /// Shared region schema.
    pub metadata: &'a RegionMetadataRef,
    /// Region index options.
    pub index_options: &'a IndexOptions,
    /// Lifecycle event requesting the build.
    pub build_type: IndexBuildType,
}

/// Inputs for constructing all of one handler's indexes in a single output file.
pub(crate) struct BuildContext<'a> {
    /// Shared output schema.
    pub metadata: &'a RegionMetadataRef,
    /// Physical output index identity.
    pub output: RegionIndexId,
    /// Existing intermediate-file facility shared by built-in creators.
    pub intermediate_manager: &'a IntermediateManager,
    /// Actual output row-group size used to align inverted-index segments.
    pub row_group_size: Option<usize>,
}

/// Prepares one built-in index implementation.
pub(crate) trait IndexKindHandler: Send + Sync + Debug {
    /// Built-in implementation owned by this handler.
    fn kind(&self) -> IndexKind;

    /// Prepares typed parameters. `None` means no work.
    fn plan(&self, context: &PlanContext<'_>) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError>;
}

/// An immutable typed plan shared by output files of an operation.
#[async_trait]
pub(crate) trait PlannedIndex: Send + Sync + Debug {
    /// Built-in implementation owned by this plan.
    fn kind(&self) -> IndexKind;

    /// Creates a per-file index creator before any output batch is consumed.
    async fn build(
        &self,
        context: &BuildContext<'_>,
    ) -> Result<Option<Box<dyn IndexCreator>>, BoxedError>;
}

/// Accumulates rows and writes blobs into the engine-owned Puffin lifecycle.
/// Implementations release retained state on drop, including cancellation.
#[async_trait]
pub(crate) trait IndexCreator: Send {
    /// Borrows a batch and its reusable primary-key decode cache.
    async fn update(&mut self, batch: &mut Batch) -> Result<(), BoxedError>;

    /// Borrows array data; cloning shared array handles is allowed.
    async fn update_flat(&mut self, batch: &RecordBatch) -> Result<(), BoxedError>;

    /// Writes completed entries. Publication waits for the engine to close Puffin.
    /// The creator is dropped afterward, including on error, without calling its
    /// `abort` method; implementations must finalize their own temporary resources.
    async fn finish(
        &mut self,
        writer: &mut SstPuffinWriter,
    ) -> Result<Vec<CompletedEntry>, BoxedError>;

    /// Releases temporary resources; the engine owns final-file cleanup.
    async fn abort(&mut self) -> Result<(), BoxedError>;

    /// Estimated retained memory, not exact allocator or process RSS accounting.
    fn memory_usage(&self) -> usize;

    /// Targets needed when preparing the shared dense primary-key decoder.
    fn indexed_targets(&self) -> Vec<IndexTarget> {
        Vec::new()
    }

    /// Whether this creator participates in existing dense-key predecoding.
    fn needs_dense_pk(&self) -> bool {
        false
    }
}
