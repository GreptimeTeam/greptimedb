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

//! A physical plan for window sort(Which is sorting multiple sorted ranges according to input `PartitionRange`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use common_time::Timestamp;
use datafusion::physical_plan::ExecutionPlan;
use store_api::region_engine::PartitionRange;

#[derive(Debug, Clone)]
pub struct WindowedSortExec {
    ranges: Vec<PartitionRange>,
    /// Overlapping Timestamp Ranges'index given the input ranges
    ///
    /// note the key ranges here should not overlapping with each other
    overlap_counts: BTreeMap<(Timestamp, Timestamp), Vec<usize>>,
    /// record all existing timestamps in the input `ranges`
    all_exist_timestamps: BTreeSet<Timestamp>,
    input: Arc<dyn ExecutionPlan>,
}

impl WindowedSortExec {}

/// Find all exist timestamps from given ranges
pub fn find_all_exist_timestamps(ranges: &[PartitionRange]) -> BTreeSet<Timestamp> {
    ranges
        .iter()
        .flat_map(|p| [p.start, p.end].into_iter())
        .collect()
}

/// Check if the input ranges's lower bound is monotonic.
pub fn check_lower_bound_monotonicity(ranges: &[PartitionRange]) -> bool {
    if ranges.is_empty() {
        return true;
    }
    ranges.windows(2).all(|w| w[0].start <= w[1].start)
}
