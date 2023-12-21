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

//! Internal states of metric engine

use std::collections::{HashMap, HashSet};

use snafu::OptionExt;
use store_api::storage::RegionId;

use crate::error::{PhysicalRegionNotFoundSnafu, Result};
use crate::metrics::LOGICAL_REGION_COUNT;
use crate::utils::to_data_region_id;

/// Internal states of metric engine
#[derive(Default)]
pub(crate) struct MetricEngineState {
    /// Mapping from physical region id to its logical region ids
    /// `logical_regions` records a reverse mapping from logical region id to
    /// physical region id
    physical_regions: HashMap<RegionId, HashSet<RegionId>>,
    /// Mapping from logical region id to physical region id.
    logical_regions: HashMap<RegionId, RegionId>,
    /// Cache for the columns of physical regions.
    /// The region id in key is the data region id.
    physical_columns: HashMap<RegionId, HashSet<String>>,
}

impl MetricEngineState {
    pub fn add_physical_region(
        &mut self,
        physical_region_id: RegionId,
        physical_columns: HashSet<String>,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        self.physical_regions
            .insert(physical_region_id, HashSet::new());
        self.physical_columns
            .insert(physical_region_id, physical_columns);
    }

    /// # Panic
    /// if the physical region does not exist
    pub fn add_physical_columns(
        &mut self,
        physical_region_id: RegionId,
        physical_columns: impl IntoIterator<Item = String>,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        let columns = self.physical_columns.get_mut(&physical_region_id).unwrap();
        for col in physical_columns {
            columns.insert(col);
        }
    }

    /// # Panic
    /// if the physical region does not exist
    pub fn add_logical_region(
        &mut self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        self.physical_regions
            .get_mut(&physical_region_id)
            .unwrap()
            .insert(logical_region_id);
        self.logical_regions
            .insert(logical_region_id, physical_region_id);
    }

    pub fn get_physical_region_id(&self, logical_region_id: RegionId) -> Option<RegionId> {
        self.logical_regions.get(&logical_region_id).copied()
    }

    pub fn physical_columns(&self) -> &HashMap<RegionId, HashSet<String>> {
        &self.physical_columns
    }

    pub fn physical_regions(&self) -> &HashMap<RegionId, HashSet<RegionId>> {
        &self.physical_regions
    }

    pub fn logical_regions(&self) -> &HashMap<RegionId, RegionId> {
        &self.logical_regions
    }

    /// Remove all data that are related to the physical region id.
    pub fn remove_physical_region(&mut self, physical_region_id: RegionId) -> Result<()> {
        let physical_region_id = to_data_region_id(physical_region_id);

        let logical_regions = self.physical_regions.get(&physical_region_id).context(
            PhysicalRegionNotFoundSnafu {
                region_id: physical_region_id,
            },
        )?;

        LOGICAL_REGION_COUNT.sub(logical_regions.len() as i64);

        for logical_region in logical_regions {
            self.logical_regions.remove(logical_region);
        }
        self.physical_regions.remove(&physical_region_id);
        self.physical_columns.remove(&physical_region_id);
        Ok(())
    }
}
