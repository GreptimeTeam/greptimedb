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

//! Shared index plans and physical inputs carried into SST writers.

use std::sync::Arc;

use crate::sst::file::FileHandle;
use crate::sst::index::puffin_manager::SstPuffinManager;
use crate::sst::index::registry::IndexBuildPlan;

/// An immutable plan with operation-scoped input files and output constraints.
/// Currently compaction and explicit rebuild supply these inputs; flush does not
/// inject extension handlers and builds only the existing built-in indexes.
#[derive(Clone)]
pub struct IndexBuildInputs {
    /// Typed plans shared by output files of this operation.
    pub plan: Arc<IndexBuildPlan>,
    /// Pinned physical input files; flushes have no inputs.
    pub inputs: Arc<[FileHandle]>,
    /// Reads persisted inputs even when output uses a local write cache.
    /// Input availability is determined by `inputs.is_empty()`, not this manager.
    pub input_puffin_manager: SstPuffinManager,
    /// Allows coarse input reuse only for the complete input scope without slicing.
    /// The caller that owns the output plan validates this scope.
    /// This is not a promise that input rows equal live output rows: deduplication
    /// or deletion may remove rows. Handlers must record input-derived coverage
    /// separately from actual output counts when reusing approximate summaries.
    pub can_merge_inputs: bool,
    /// Existing allocation available to optional index creators; `None` sets no limit here.
    /// Single-region compaction and rebuild currently provide no shared total limit.
    pub memory_limit: Option<usize>,
    /// Exact row count of an unchanged SST during rebuild, otherwise unknown.
    pub output_rows: Option<u64>,
}
