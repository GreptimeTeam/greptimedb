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

//! Specific options for the metric engine to create or open a region.

use std::collections::HashMap;

use itertools::Itertools as _;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::ColumnId;

/// Ignore building index on the column `tsid` which is unfriendly to the inverted index and
/// will occupy excessive space if indexed.
const IGNORE_COLUMN_IDS_FOR_DATA_REGION: [ColumnId; 1] = [ReservedColumnId::tsid()];

/// The empirical value for the seg row count of the metric data region.
/// Compared to the mito engine, the pattern of the metric engine constructs smaller indices.
/// Therefore, compared to the default seg row count of 1024, by adjusting it to a smaller
/// value and appropriately increasing the size of the index, it results in an improved indexing effect.
const SEG_ROW_COUNT_FOR_DATA_REGION: u32 = 256;

/// Sets data region specific options.
pub fn set_data_region_options(options: &mut HashMap<String, String>) {
    // Set the index options for the data region.
    options.insert(
        "index.inverted_index.ignore_column_ids".to_string(),
        IGNORE_COLUMN_IDS_FOR_DATA_REGION.iter().join(","),
    );
    options.insert(
        "index.inverted_index.segment_row_count".to_string(),
        SEG_ROW_COUNT_FOR_DATA_REGION.to_string(),
    );
    // Set memtable options for the data region.
    options.insert("memtable.type".to_string(), "partition_tree".to_string());
}
