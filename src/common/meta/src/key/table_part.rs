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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::key::TABLE_PART_PREFIX;

/// The key stores table repartition relation information.
/// i.e. the src/dst region after repartition, which means dst region may still hold
/// some files from src region, this should be updated after repartition is done.
/// And gc scheduler will use this information to clean up those files(and this mapping if all files from src region are cleaned).
///
/// The layout: `__table_part/{table_id}`.
pub struct TablePartKey {
    pub table_id: TableId,
}

impl TablePartKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }

    /// Returns the range prefix of the table partition key.
    pub fn range_prefix() -> Vec<u8> {
        format!("{}/", TABLE_PART_PREFIX).into_bytes()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TablePartValue {
    /// Mapping from src region to dst regions after repartition.
    pub src_to_dst: BTreeMap<RegionId, BTreeSet<RegionId>>,
}

impl TablePartValue {
    pub fn update_mapping(&mut self, src: RegionId, dst: &[RegionId]) {
        self.src_to_dst.entry(src).or_default().extend(dst);
    }

    pub fn remove_mapping(&mut self, src: RegionId, dst: RegionId) {
        if let Some(dst_set) = self.src_to_dst.get_mut(&src) {
            dst_set.remove(&dst);
            if dst_set.is_empty() {
                self.src_to_dst.remove(&src);
            }
        }
    }

    pub fn remove_mappings(&mut self, src: RegionId, dsts: &[RegionId]) {
        if let Some(dst_set) = self.src_to_dst.get_mut(&src) {
            for dst in dsts {
                dst_set.remove(dst);
            }
            if dst_set.is_empty() {
                self.src_to_dst.remove(&src);
            }
        }
    }
}
