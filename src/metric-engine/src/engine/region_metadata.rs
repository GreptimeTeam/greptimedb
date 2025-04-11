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

//! Implementation of retrieving logical region's region metadata.

use std::collections::HashMap;

use store_api::metadata::ColumnMetadata;
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::Result;

impl MetricEngineInner {
    /// Load column metadata of a logical region.
    ///
    /// The return value is ordered on column name.
    pub async fn load_logical_columns(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<ColumnMetadata>> {
        // First try to load from state cache
        if let Some(columns) = self
            .state
            .read()
            .unwrap()
            .logical_columns()
            .get(&logical_region_id)
        {
            return Ok(columns.clone());
        }

        // Else load from metadata region and update the cache.
        let _read_guard = self
            .metadata_region
            .read_lock_logical_region(logical_region_id)
            .await?;
        // Load logical and physical columns, and intersect them to get logical column metadata.
        let logical_column_metadata = self
            .metadata_region
            .logical_columns(physical_region_id, logical_region_id)
            .await?
            .into_iter()
            .map(|(_, column_metadata)| column_metadata)
            .collect::<Vec<_>>();

        // Update cache
        let mut mutable_state = self.state.write().unwrap();
        // Merge with existing cached columns.
        let existing_columns = mutable_state
            .logical_columns()
            .get(&logical_region_id)
            .cloned()
            .unwrap_or_default()
            .into_iter();
        let mut dedup_columns = logical_column_metadata
            .into_iter()
            .chain(existing_columns)
            .map(|c| (c.column_id, c))
            .collect::<HashMap<_, _>>()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        // Sort columns on column name to ensure the order
        dedup_columns.sort_unstable_by(|c1, c2| c1.column_schema.name.cmp(&c2.column_schema.name));
        mutable_state.set_logical_columns(logical_region_id, dedup_columns.clone());

        Ok(dedup_columns)
    }

    /// Load logical column names of a logical region.
    ///
    /// The return value is ordered on column name alphabetically.
    pub async fn load_logical_column_names(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<String>> {
        // First try to load from state cache
        if let Some(columns) = self
            .state
            .read()
            .unwrap()
            .logical_columns()
            .get(&logical_region_id)
        {
            return Ok(columns
                .iter()
                .map(|c| c.column_schema.name.clone())
                .collect());
        }

        // Else load from metadata region
        let columns = self
            .load_logical_columns(physical_region_id, logical_region_id)
            .await?
            .into_iter()
            .map(|c| c.column_schema.name)
            .collect::<Vec<_>>();

        Ok(columns)
    }
}
