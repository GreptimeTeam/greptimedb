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

use common_time::timestamp::TimeUnit;
use snafu::OptionExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::ColumnMetadata;
use store_api::storage::{ColumnId, RegionId};

use crate::engine::options::PhysicalRegionOptions;
use crate::error::{PhysicalRegionNotFoundSnafu, Result};
use crate::metrics::LOGICAL_REGION_COUNT;
use crate::utils::to_data_region_id;

pub struct PhysicalRegionState {
    logical_regions: HashSet<RegionId>,
    physical_columns: HashMap<String, ColumnId>,
    primary_key_encoding: PrimaryKeyEncoding,
    options: PhysicalRegionOptions,
    time_index_unit: TimeUnit,
}

impl PhysicalRegionState {
    pub fn new(
        physical_columns: HashMap<String, ColumnId>,
        primary_key_encoding: PrimaryKeyEncoding,
        options: PhysicalRegionOptions,
        time_index_unit: TimeUnit,
    ) -> Self {
        Self {
            logical_regions: HashSet::new(),
            physical_columns,
            primary_key_encoding,
            options,
            time_index_unit,
        }
    }

    /// Returns a reference to the logical region ids.
    pub fn logical_regions(&self) -> &HashSet<RegionId> {
        &self.logical_regions
    }

    /// Returns a reference to the physical columns.
    pub fn physical_columns(&self) -> &HashMap<String, ColumnId> {
        &self.physical_columns
    }

    /// Returns a reference to the physical region options.
    pub fn options(&self) -> &PhysicalRegionOptions {
        &self.options
    }

    /// Removes a logical region id from the physical region state.
    /// Returns true if the logical region id was present.
    pub fn remove_logical_region(&mut self, logical_region_id: RegionId) -> bool {
        self.logical_regions.remove(&logical_region_id)
    }
}

/// Internal states of metric engine
#[derive(Default)]
pub(crate) struct MetricEngineState {
    /// Physical regions states.
    physical_regions: HashMap<RegionId, PhysicalRegionState>,
    /// Mapping from logical region id to physical region id.
    logical_regions: HashMap<RegionId, RegionId>,
    /// Cache for the column metadata of logical regions.
    /// The column order is the same with the order in the metadata, which is
    /// alphabetically ordered on column name.
    logical_columns: HashMap<RegionId, Vec<ColumnMetadata>>,
}

impl MetricEngineState {
    pub fn add_physical_region(
        &mut self,
        physical_region_id: RegionId,
        physical_columns: HashMap<String, ColumnId>,
        primary_key_encoding: PrimaryKeyEncoding,
        options: PhysicalRegionOptions,
        time_index_unit: TimeUnit,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        self.physical_regions.insert(
            physical_region_id,
            PhysicalRegionState::new(
                physical_columns,
                primary_key_encoding,
                options,
                time_index_unit,
            ),
        );
    }

    /// # Panic
    /// if the physical region does not exist
    pub fn add_physical_columns(
        &mut self,
        physical_region_id: RegionId,
        physical_columns: impl IntoIterator<Item = (String, ColumnId)>,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        let state = self.physical_regions.get_mut(&physical_region_id).unwrap();
        for (col, id) in physical_columns {
            state.physical_columns.insert(col, id);
        }
    }

    /// # Panic
    /// if the physical region does not exist
    pub fn add_logical_regions(
        &mut self,
        physical_region_id: RegionId,
        logical_region_ids: impl IntoIterator<Item = RegionId>,
    ) {
        let physical_region_id = to_data_region_id(physical_region_id);
        let state = self.physical_regions.get_mut(&physical_region_id).unwrap();
        for logical_region_id in logical_region_ids {
            state.logical_regions.insert(logical_region_id);
            self.logical_regions
                .insert(logical_region_id, physical_region_id);
        }
    }

    pub fn invalid_logical_regions_cache(
        &mut self,
        logical_region_ids: impl IntoIterator<Item = RegionId>,
    ) {
        for logical_region_id in logical_region_ids {
            self.logical_columns.remove(&logical_region_id);
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
            .logical_regions
            .insert(logical_region_id);
        self.logical_regions
            .insert(logical_region_id, physical_region_id);
    }

    /// Replace the logical columns of the logical region with given columns.
    pub fn set_logical_columns(
        &mut self,
        logical_region_id: RegionId,
        columns: Vec<ColumnMetadata>,
    ) {
        self.logical_columns.insert(logical_region_id, columns);
    }

    pub fn get_physical_region_id(&self, logical_region_id: RegionId) -> Option<RegionId> {
        self.logical_regions.get(&logical_region_id).copied()
    }

    pub fn logical_columns(&self) -> &HashMap<RegionId, Vec<ColumnMetadata>> {
        &self.logical_columns
    }

    pub fn physical_region_states(&self) -> &HashMap<RegionId, PhysicalRegionState> {
        &self.physical_regions
    }

    pub fn exist_physical_region(&self, physical_region_id: RegionId) -> bool {
        self.physical_regions.contains_key(&physical_region_id)
    }

    pub fn physical_region_time_index_unit(
        &self,
        physical_region_id: RegionId,
    ) -> Option<TimeUnit> {
        self.physical_regions
            .get(&physical_region_id)
            .map(|state| state.time_index_unit)
    }

    pub fn get_primary_key_encoding(
        &self,
        physical_region_id: RegionId,
    ) -> Option<PrimaryKeyEncoding> {
        self.physical_regions
            .get(&physical_region_id)
            .map(|state| state.primary_key_encoding)
    }

    pub fn logical_regions(&self) -> &HashMap<RegionId, RegionId> {
        &self.logical_regions
    }

    /// Remove all data that are related to the physical region id.
    pub fn remove_physical_region(&mut self, physical_region_id: RegionId) -> Result<()> {
        let physical_region_id = to_data_region_id(physical_region_id);

        let logical_regions = &self
            .physical_regions
            .get(&physical_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: physical_region_id,
            })?
            .logical_regions;

        LOGICAL_REGION_COUNT.sub(logical_regions.len() as i64);

        for logical_region in logical_regions {
            self.logical_regions.remove(logical_region);
        }
        self.physical_regions.remove(&physical_region_id);
        Ok(())
    }

    /// Remove all data that are related to the logical region id.
    pub fn remove_logical_region(&mut self, logical_region_id: RegionId) -> Result<()> {
        let physical_region_id = self.logical_regions.remove(&logical_region_id).context(
            PhysicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            },
        )?;

        self.physical_regions
            .get_mut(&physical_region_id)
            .unwrap() // Safety: physical_region_id is got from physical_regions
            .remove_logical_region(logical_region_id);

        self.logical_columns.remove(&logical_region_id);

        Ok(())
    }

    pub fn is_logical_region_exist(&self, logical_region_id: RegionId) -> bool {
        self.logical_regions().contains_key(&logical_region_id)
    }
}
