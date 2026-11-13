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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;

use crate::DatanodeId;

/// An operating region on a datanode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatingRegion {
    pub role: RegionRole,
    /// The random non-zero id of a region migration's registration of its target region, carried
    /// by the leases granted because of this registration. `None` for other operations.
    pub candidate_lease_id: Option<u64>,
}

/// Tracks the operating(i.e., creating, opening, dropping) regions.
#[derive(Debug, Clone)]
pub struct OperatingRegionGuard {
    datanode_id: DatanodeId,
    region_id: RegionId,
    candidate_lease_id: Option<u64>,
    inner: Arc<RwLock<HashMap<(DatanodeId, RegionId), OperatingRegion>>>,
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

    /// Returns the candidate lease id of a region migration's target region registration.
    pub fn candidate_lease_id(&self) -> Option<u64> {
        self.candidate_lease_id
    }
}

pub type MemoryRegionKeeperRef = Arc<MemoryRegionKeeper>;

/// Returns the first non-zero value of `random`, since 0 means no candidate lease id on the wire.
fn new_candidate_lease_id(mut random: impl FnMut() -> u64) -> u64 {
    loop {
        let candidate_lease_id = random();
        if candidate_lease_id != 0 {
            return candidate_lease_id;
        }
    }
}

/// Tracks regions in memory.
///
/// It used in following cases:
/// - Tracks the opening regions before the corresponding metadata is created.
/// - Tracks the deleting regions after the corresponding metadata is deleted.
#[derive(Debug, Clone, Default)]
pub struct MemoryRegionKeeper {
    inner: Arc<RwLock<HashMap<(DatanodeId, RegionId), OperatingRegion>>>,
}

impl MemoryRegionKeeper {
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns [OperatingRegionGuard] if Region(`region_id`) on Peer(`datanode_id`) does not exist.
    pub fn register_with_role(
        &self,
        datanode_id: DatanodeId,
        region_id: RegionId,
        role: RegionRole,
    ) -> Option<OperatingRegionGuard> {
        self.register(
            datanode_id,
            region_id,
            OperatingRegion {
                role,
                candidate_lease_id: None,
            },
        )
    }

    /// Registers the target region of a region migration as a follower with a new random
    /// candidate lease id. Returns [OperatingRegionGuard] if Region(`region_id`) on
    /// Peer(`datanode_id`) does not exist.
    pub fn register_candidate_region(
        &self,
        datanode_id: DatanodeId,
        region_id: RegionId,
    ) -> Option<OperatingRegionGuard> {
        self.register(
            datanode_id,
            region_id,
            OperatingRegion {
                role: RegionRole::Follower,
                candidate_lease_id: Some(new_candidate_lease_id(rand::random)),
            },
        )
    }

    fn register(
        &self,
        datanode_id: DatanodeId,
        region_id: RegionId,
        operating_region: OperatingRegion,
    ) -> Option<OperatingRegionGuard> {
        let mut inner = self.inner.write().unwrap();

        match inner.entry((datanode_id, region_id)) {
            Entry::Occupied(_) => None,
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(operating_region);
                Some(OperatingRegionGuard {
                    datanode_id,
                    region_id,
                    candidate_lease_id: operating_region.candidate_lease_id,
                    inner: self.inner.clone(),
                })
            }
        }
    }

    /// Returns true if the keeper contains a (`datanoe_id`, `region_id`) tuple.
    pub fn contains(&self, datanode_id: DatanodeId, region_id: RegionId) -> bool {
        let inner = self.inner.read().unwrap();
        inner.contains_key(&(datanode_id, region_id))
    }

    /// Extracts all operating regions with roles from `region_ids`.
    pub fn extract_operating_region_roles(
        &self,
        datanode_id: DatanodeId,
        region_ids: &HashSet<RegionId>,
    ) -> HashMap<RegionId, RegionRole> {
        let inner = self.inner.read().unwrap();
        region_ids
            .iter()
            .filter_map(|region_id| {
                inner
                    .get(&(datanode_id, *region_id))
                    .map(|region| (*region_id, region.role))
            })
            .collect()
    }

    /// Extracts all operating regions from `region_ids`.
    pub fn extract_operating_regions(
        &self,
        datanode_id: DatanodeId,
        region_ids: &HashSet<RegionId>,
    ) -> HashMap<RegionId, OperatingRegion> {
        let inner = self.inner.read().unwrap();
        region_ids
            .iter()
            .filter_map(|region_id| {
                inner
                    .get(&(datanode_id, *region_id))
                    .map(|region| (*region_id, *region))
            })
            .collect()
    }

    /// Returns number of element in tracking set.
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.len()
    }

    /// Returns true if it's empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use crate::region_keeper::{MemoryRegionKeeper, OperatingRegion, new_candidate_lease_id};

    #[test]
    fn test_opening_region_keeper() {
        let keeper = MemoryRegionKeeper::new();

        let guard = keeper
            .register_with_role(1, RegionId::from_u64(1), RegionRole::Leader)
            .unwrap();
        assert!(
            keeper
                .register_with_role(1, RegionId::from_u64(1), RegionRole::Leader)
                .is_none()
        );
        let guard2 = keeper
            .register_with_role(1, RegionId::from_u64(2), RegionRole::Follower)
            .unwrap();

        let regions = HashSet::from([
            RegionId::from_u64(1),
            RegionId::from_u64(2),
            RegionId::from_u64(3),
        ]);
        let output = keeper.extract_operating_region_roles(1, &regions);
        assert_eq!(output.len(), 2);

        assert!(output.contains_key(&RegionId::from_u64(1)));
        assert!(output.contains_key(&RegionId::from_u64(2)));
        assert_eq!(keeper.len(), 2);

        let regions = HashSet::from([
            RegionId::from_u64(1),
            RegionId::from_u64(2),
            RegionId::from_u64(3),
        ]);
        let output = keeper.extract_operating_region_roles(1, &regions);
        assert_eq!(
            output,
            HashMap::from([
                (RegionId::from_u64(1), RegionRole::Leader),
                (RegionId::from_u64(2), RegionRole::Follower),
            ])
        );
        assert_eq!(keeper.len(), 2);

        drop(guard);
        assert_eq!(keeper.len(), 1);
        assert!(keeper.contains(1, RegionId::from_u64(2)));

        drop(guard2);
        assert!(keeper.is_empty());
    }

    #[test]
    fn test_register_candidate_region_uses_new_candidate_lease_id() {
        let keeper = MemoryRegionKeeper::new();
        let region_id = RegionId::from_u64(1);
        let regions = HashSet::from([region_id]);

        let guard = keeper
            .register_with_role(1, region_id, RegionRole::Follower)
            .unwrap();
        assert_eq!(guard.candidate_lease_id(), None);
        assert_eq!(
            keeper.extract_operating_regions(1, &regions)[&region_id].candidate_lease_id,
            None
        );
        drop(guard);

        let guard = keeper.register_candidate_region(1, region_id).unwrap();
        let first = guard.candidate_lease_id().unwrap();
        assert!(keeper.register_candidate_region(1, region_id).is_none());
        assert_eq!(
            keeper.extract_operating_regions(1, &regions)[&region_id],
            OperatingRegion {
                role: RegionRole::Follower,
                candidate_lease_id: Some(first),
            }
        );
        drop(guard);

        let guard = keeper.register_candidate_region(1, region_id).unwrap();
        assert_ne!(guard.candidate_lease_id().unwrap(), first);
    }

    #[test]
    fn test_new_candidate_lease_id_skips_zero() {
        let mut values = [0, 0, 7].into_iter();
        assert_eq!(new_candidate_lease_id(|| values.next().unwrap()), 7);
    }
}
