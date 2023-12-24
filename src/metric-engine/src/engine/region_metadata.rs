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

use std::collections::{HashMap, HashSet};

use api::v1::SemanticType;
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::Result;

impl MetricEngineInner {
    /// Load column metadata of a logical region.
    ///
    /// The return value is ordered.
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
        // let physical_columns = self
        //     .data_region
        //     .physical_columns(physical_region_id)
        //     .await?
        //     .into_iter()
        //     .map(|col| (col.column_schema.name.clone(), col))
        //     .collect::<HashMap<String, ColumnMetadata>>();
        // let mut logical_column_metadata = physical_columns
        //     .into_iter()
        //     .filter_map(|mut col| {
        //         // recover the semantic type of logical columns
        //         logical_columns
        //             .get(&col.column_schema.name)
        //             .map(|semantic_type| {
        //                 col.semantic_type = *semantic_type;
        //                 col
        //             })
        //     })
        //     .collect::<Vec<_>>();
        // let logical_column_metadata = logical_columns
        //     .into_iter()
        //     .map(|(name, semantic_type)| {
        //         let mut col = physical_columns.get(&name).unwrap().clone();
        //         col.semantic_type = col.semantic_type;
        //         col
        //     })
        //     .collect::<Vec<_>>();

        // sort columns on column id to ensure the order
        logical_column_metadata.sort_unstable_by_key(|col| col.column_id);

        Ok(logical_column_metadata)
    }
}
