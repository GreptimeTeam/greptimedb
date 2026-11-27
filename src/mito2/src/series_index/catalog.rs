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

//! Index catalog persistence, coverage metadata, and file paths.

use std::collections::BTreeMap;

use common_time::Timestamp;
use serde::{Deserialize, Serialize};
use store_api::storage::FileId;

pub(crate) use crate::sst::file::WindowSequence;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesIndexEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) artifact: Option<crate::series_index::artifact::ArtifactMetadata>,
    /// Completed size in the catalog; zero in the footer written before completion.
    pub(crate) file_size: u64,
    pub(crate) index_uuid: FileId,
    /// Inclusive bucket start.
    pub(crate) bucket_start: Timestamp,
    /// Exclusive bucket end.
    pub(crate) bucket_end: Timestamp,
    /// Source SST IDs retained for diagnostics and exact coverage of unknown-sequence files. Compaction can replace these
    /// files without changing indexed data, so known-sequence reuse does not depend on IDs.
    /// Native indexes omit this field and use region-manifest coverage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_file_ids: Option<Vec<FileId>>,
    pub(crate) min_file_sequence: u64,
    pub(crate) max_file_sequence: u64,
    /// Width used to align the half-open compaction windows, in seconds.
    pub(crate) compaction_window_secs: i64,
    /// Source SST summaries keyed by aligned start; intervals may overlap.
    /// Each file contributes one summary regardless of its span. Equal starts merge
    /// by taking the maximum end and sequence. See [`WindowSequence`] for the
    /// sequence assumption used to detect new data. Compaction changing summary
    /// boundaries may conservatively trigger a rebuild.
    pub(crate) window_sequences: BTreeMap<i64, WindowSequence>,
}
