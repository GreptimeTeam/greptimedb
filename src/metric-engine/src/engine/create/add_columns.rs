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

use store_api::metadata::ColumnMetadata;
use store_api::storage::RegionId;

use crate::data_region::DataRegion;
use crate::engine::IndexOptions;
use crate::error::Result;
use crate::metrics::PHYSICAL_COLUMN_COUNT;

/// Add new columns to the physical data region.
pub(crate) async fn add_columns_to_physical_data_region(
    data_region_id: RegionId,
    index_options: IndexOptions,
    new_columns: &mut [ColumnMetadata],
    data_region: &DataRegion,
) -> Result<()> {
    // Return early if no new columns are added.
    if new_columns.is_empty() {
        return Ok(());
    }

    data_region
        .add_columns(data_region_id, new_columns, index_options)
        .await?;

    PHYSICAL_COLUMN_COUNT.add(new_columns.len() as _);

    Ok(())
}
