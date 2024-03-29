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

use store_api::metadata::ColumnMetadata;
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::Result;

impl MetricEngineInner {
    /// Load column metadata of a logical region.
    ///
    /// The return value is ordered on [ColumnId].
    pub async fn load_logical_columns(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<ColumnMetadata>> {
        // load logical and physical columns, and intersect them to get logical column metadata
        let mut logical_column_metadata = self
            .metadata_region
            .logical_columns(physical_region_id, logical_region_id)
            .await?
            .into_iter()
            .map(|(_, column_metadata)| column_metadata)
            .collect::<Vec<_>>();

        // sort columns on column id to ensure the order
        logical_column_metadata
            .sort_unstable_by(|c1, c2| c1.column_schema.name.cmp(&c2.column_schema.name));

        Ok(logical_column_metadata)
    }
}
