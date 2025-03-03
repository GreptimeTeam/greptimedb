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
use store_api::metric_engine_consts::LOGICAL_TABLE_METADATA_KEY;
use store_api::region_request::RegionCreateRequest;
use store_api::storage::RegionId;

use crate::error::{MissingRegionOptionSnafu, ParseRegionIdSnafu, Result};

/// Groups the create logical regions requests by physical region id.
pub fn group_create_logical_regions_requests_by_physical_region_id(
    requests: Vec<(RegionId, RegionCreateRequest)>,
) -> Result<HashMap<RegionId, Vec<(RegionId, RegionCreateRequest)>>> {
    let mut result = HashMap::with_capacity(requests.len());
    for (region_id, request) in requests {
        let physical_region_id = parse_physical_region_id(&request)?;
        result
            .entry(physical_region_id)
            .or_insert_with(Vec::new)
            .push((region_id, request));
    }

    Ok(result)
}

/// Parses the physical region id from the request.
pub fn parse_physical_region_id(request: &RegionCreateRequest) -> Result<RegionId> {
    let physical_region_id_raw = request
        .options
        .get(LOGICAL_TABLE_METADATA_KEY)
        .ok_or(MissingRegionOptionSnafu {}.build())?;

    let physical_region_id: RegionId = physical_region_id_raw
        .parse::<u64>()
        .with_context(|_| ParseRegionIdSnafu {
            raw: physical_region_id_raw,
        })?
        .into();

    Ok(physical_region_id)
}
