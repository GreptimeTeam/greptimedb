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

use common_time::Timestamp;
use serde::{Deserialize, Serialize};

use crate::storage::{RegionGroup, RegionId, RegionNumber, RegionSeq, TableId};

/// An entry describing a SST file known by the engine's manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSstEntry {
    /// Engine label, e.g., "mito", "metric".
    pub engine: String,
    /// The table directory this file belongs to.
    pub table_dir: String,
    /// The region id this file belongs to.
    pub region_id: RegionId,
    /// The table id this file belongs to.
    pub table_id: TableId,
    /// The region number this file belongs to.
    pub region_number: RegionNumber,
    /// The region group this file belongs to.
    pub region_group: RegionGroup,
    /// The region sequence this file belongs to.
    pub region_sequence: RegionSeq,
    /// Engine-specific file identifier (string form).
    pub file_id: String,
    /// SST level.
    pub level: u8,
    /// Full path of the SST file in object store.
    pub file_path: String,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of rows in the SST.
    pub num_rows: u64,
    /// Number of row groups in the SST.
    pub num_row_groups: u64,
    /// Min timestamp.
    pub min_ts: Timestamp,
    /// Max timestamp.
    pub max_ts: Timestamp,
    /// The sequence number associated with this file.
    pub sequence: Option<u64>,
}

/// An entry describing a SST file listed from storage layer directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSstEntry {
    /// Engine label, e.g., "mito", "metric".
    pub engine: String,
    /// Full path of the SST file in object store.
    pub file_path: String,
    /// File size in bytes.
    pub file_size: Option<u64>,
    /// Last modified time in milliseconds since epoch, if available from storage.
    pub last_modified_ms: Option<Timestamp>,
}
