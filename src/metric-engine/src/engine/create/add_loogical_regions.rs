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

use std::collections::HashMap;

use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::storage::RegionId;

use crate::error::{MitoWriteOperationSnafu, Result};
use crate::metadata_region::MetadataRegion;
use crate::utils;

/// Add logical regions to the metadata region.
pub async fn add_logical_regions_to_meta_region(
    metadata_region: &MetadataRegion,
    physical_region_id: RegionId,
    logical_regions: impl Iterator<Item = (RegionId, HashMap<&str, &ColumnMetadata>)>,
) -> Result<()> {
    let region_id = utils::to_metadata_region_id(physical_region_id);
    let iter = logical_regions
        .into_iter()
        .flat_map(|(logical_region_id, column_metadatas)| {
            Some((
                MetadataRegion::concat_region_key(logical_region_id),
                String::new(),
            ))
            .into_iter()
            .chain(column_metadatas.into_iter().map(
                move |(name, column_metadata)| {
                    (
                        MetadataRegion::concat_column_key(logical_region_id, name),
                        MetadataRegion::serialize_column_metadata(column_metadata),
                    )
                },
            ))
        })
        .collect::<Vec<_>>();

    let put_request = MetadataRegion::build_put_request_from_iter(iter.into_iter());
    metadata_region
        .mito
        .handle_request(
            region_id,
            store_api::region_request::RegionRequest::Put(put_request),
        )
        .await
        .context(MitoWriteOperationSnafu)?;

    Ok(())
}
