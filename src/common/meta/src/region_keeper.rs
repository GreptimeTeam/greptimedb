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

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use store_api::storage::RegionId;

use crate::DatanodeId;

/// Tracks the operating(i.e., creating, opening, dropping) regions.
#[derive(Debug, Clone)]
pub struct OperatingRegionGuard {
    datanode_id: DatanodeId,
    region_id: RegionId,
    inner: Arc<RwLock<HashSet<(DatanodeId, RegionId)>>>,
}

impl Drop for OperatingRegionGuard {
    fn drop(&mut self) {
        let mut inner = self.inner.write().unwrap();
        inner.remove(&(self.datanode_id, self.region_id));
    }
}

impl OperatingRegionGuard {
    /// Returns opening region info.
    pub fn info(&self) -> (DatanodeId, RegionId) {
        (self.datanode_id, self.region_id)
    }
}

pub type MemoryRegionKeeperRef = Arc<MemoryRegionKeeper>;

/// Tracks regions in memory.
///
/// It used in following cases:
/// - Tracks the opening regions before the corresponding metadata is created.
/// - Tracks the deleting regions after the corresponding metadata is deleted.
#[derive(Debug, Clone, Default)]
pub struct MemoryRegionKeeper {
    inner: Arc<RwLock<HashSet<(DatanodeId, RegionId)>>>,
}

impl MemoryRegionKeeper {
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns [OpeningRegionGuard] if Region(`region_id`) on Peer(`datanode_id`) does not exist.
    pub fn register(
        &self,
        datanode_id: DatanodeId,
        region_id: RegionId,
    ) -> Option<OperatingRegionGuard> {
        let mut inner = self.inner.write().unwrap();

        if inner.insert((datanode_id, region_id)) {
            Some(OperatingRegionGuard {
                datanode_id,
                region_id,
                inner: self.inner.clone(),
            })
        } else {
            None
        }
    }

    /// Returns true if the keeper contains a (`datanoe_id`, `region_id`) tuple.
    pub fn contains(&self, datanode_id: DatanodeId, region_id: RegionId) -> bool {
        let inner = self.inner.read().unwrap();
        inner.contains(&(datanode_id, region_id))
    }

    /// Returns a set of filtered out regions that are opening.
    pub fn filter_opening_regions(
        &self,
        datanode_id: DatanodeId,
        mut region_ids: HashSet<RegionId>,
    ) -> HashSet<RegionId> {
        let inner = self.inner.read().unwrap();
        region_ids.retain(|region_id| !inner.contains(&(datanode_id, *region_id)));

        region_ids
    }

    /// Returns number of element in tracking set.
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use store_api::storage::RegionId;

    use crate::region_keeper::MemoryRegionKeeper;

    #[test]
    fn test_opening_region_keeper() {
        let keeper = MemoryRegionKeeper::new();

        let guard = keeper.register(1, RegionId::from_u64(1)).unwrap();
        assert!(keeper.register(1, RegionId::from_u64(1)).is_none());
        let guard2 = keeper.register(1, RegionId::from_u64(2)).unwrap();

        let output = keeper.filter_opening_regions(
            1,
            HashSet::from([
                RegionId::from_u64(1),
                RegionId::from_u64(2),
                RegionId::from_u64(3),
            ]),
        );
        assert_eq!(output.len(), 1);
        assert!(output.contains(&RegionId::from_u64(3)));

        assert_eq!(keeper.len(), 2);
        drop(guard);

        assert_eq!(keeper.len(), 1);

        assert!(keeper.contains(1, RegionId::from_u64(2)));
        drop(guard2);

        assert!(keeper.is_empty());
    }
}
