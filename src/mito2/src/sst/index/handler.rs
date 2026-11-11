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

//! Planning and creation contracts for built-in and injected indexes.

pub(crate) mod builtin;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use datatypes::arrow::record_batch::RecordBatch;
use index::target::IndexTarget;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::sst::file::{ColumnIndexMetadata, FileHandle, RegionIndexId};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::{SstPuffinManager, SstPuffinWriter};
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// A requested column format; typed implementation parameters stay in its plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEntry {
    /// Column to index.
    pub column_id: ColumnId,
    /// Implementation owning this entry.
    pub kind: IndexKind,
    /// Requested format version. The manifest records extension types, not versions.
    /// A format upgrade requires a forced rebuild; the provider validates Puffin
    /// properties and payload compatibility before reusing input state.
    pub format_version: u32,
}

/// Schema and options used to prepare work without reading input files.
pub struct PlanContext<'a> {
    /// Shared region schema.
    pub metadata: &'a RegionMetadataRef,
    /// Region index options.
    pub index_options: &'a IndexOptions,
    /// Lifecycle event requesting the build.
    pub build_type: IndexBuildType,
    /// Existing column descriptors of the target SST during rebuild.
    /// Empty for new outputs, including compaction; input descriptors are available
    /// through [`BuildContext::inputs`] when constructing a creator.
    pub existing_indexes: &'a [ColumnIndexMetadata],
}

/// Inputs for constructing all of one handler's indexes in a single output file.
pub struct BuildContext<'a> {
    /// Shared output schema.
    pub metadata: &'a RegionMetadataRef,
    /// Physical output index identity.
    pub output: RegionIndexId,
    /// Output manager, possibly backed by a local write cache.
    pub puffin_manager: &'a SstPuffinManager,
    /// Existing intermediate-file facility shared by built-in creators.
    pub intermediate_manager: &'a IntermediateManager,
    /// Pinned physical input files.
    pub inputs: &'a [FileHandle],
    /// Manager for reading persisted input indexes. With no inputs this falls back
    /// to the output manager; determine input availability from `inputs.is_empty()`.
    pub input_puffin_manager: &'a SstPuffinManager,
    /// Permits coarse input-summary reuse only for a complete single output.
    /// The caller that owns the output plan verifies this scope before construction.
    pub can_merge_inputs: bool,
    /// Retained-memory allowance for an injected creator, capped by the remaining
    /// operation budget and its plan ceiling. `None` applies no limit at this layer.
    /// Built-in creators use spill budgets.
    pub memory_limit: Option<usize>,
    /// Actual output row-group size used to align inverted-index segments.
    pub row_group_size: Option<usize>,
    /// Exact row count when rebuilding an existing SST; unknown for compaction outputs.
    pub output_rows: Option<u64>,
}

/// Shared plugin installed by the embedding application.
pub type IndexKindProviderRef = Arc<dyn IndexKindProvider>;

/// Supplies extension handlers while the engine retains the complete lifecycle.
pub trait IndexKindProvider: Send + Sync + Debug {
    /// Related algorithms may share one handler to reuse row traversal.
    fn handlers(&self) -> Vec<Arc<dyn IndexKindHandler>>;
}

/// Owns one or more related kinds that can share preparation and row traversal.
pub trait IndexKindHandler: Send + Sync + Debug {
    /// Nonempty, unique kinds exclusively owned by this handler. A group must be
    /// entirely built-in or entirely extension kinds.
    fn kinds(&self) -> &[IndexKind];

    /// Prepares typed parameters. `None` means no work; an empty entry list may
    /// still request removal of obsolete descriptors during maintenance.
    fn plan(&self, context: &PlanContext<'_>) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError>;
}

/// An immutable typed plan shared by output files of an operation.
#[async_trait]
pub trait PlannedIndex: Send + Sync + Debug {
    /// Nonempty, unique subset of the handler's kinds, including removal plans.
    fn kinds(&self) -> &[IndexKind];

    /// Requested column formats. Every entry kind must belong to this plan.
    fn entries(&self) -> &[PlanEntry];

    /// Prepares input reuse before any output batch is consumed.
    async fn build(
        &self,
        context: &BuildContext<'_>,
    ) -> Result<Option<Box<dyn IndexCreator>>, BoxedError>;

    /// Whether building this plan can reuse persisted input summaries.
    /// Row-only plans do not require checking input object existence.
    fn needs_input_reuse(&self) -> bool {
        false
    }

    /// Optional hard ceiling; built-in creators use their existing spill budgets.
    fn memory_limit(&self) -> Option<usize> {
        None
    }
}

/// Accumulates rows and writes blobs into the engine-owned Puffin lifecycle.
/// Implementations release retained state on drop, including cancellation.
#[async_trait]
pub trait IndexCreator: Send {
    /// Whether rebuilding this index requires scanning SST rows after preparation.
    /// A creator reusing persisted state must account for output rows without updates.
    fn requires_row_scan(&self) -> bool {
        true
    }

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
