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

use snafu::{ensure, ResultExt};
use store_api::metric_engine_consts::LOGICAL_TABLE_METADATA_KEY;
use store_api::region_request::RegionCreateRequest;
use store_api::storage::RegionId;

use crate::error::{
    ConflictRegionOptionSnafu, EmptyRequestSnafu, MissingRegionOptionSnafu, ParseRegionIdSnafu,
    Result,
};

/// Validate the create logical regions request.
///
/// Returns extracted physical region id from the first request.
pub fn validate_create_logical_regions(
    requests: &[(RegionId, RegionCreateRequest)],
) -> Result<RegionId> {
    ensure!(!requests.is_empty(), EmptyRequestSnafu {});

    let (_, request) = requests.first().unwrap();
    let first_physical_region_id_raw = request
        .options
        .get(LOGICAL_TABLE_METADATA_KEY)
        .ok_or(MissingRegionOptionSnafu {}.build())?;

    let physical_region_id: RegionId = first_physical_region_id_raw
        .parse::<u64>()
        .with_context(|_| ParseRegionIdSnafu {
            raw: first_physical_region_id_raw,
        })?
        .into();

    // TODO(weny): Can we remove the check?
    for (_, request) in requests.iter().skip(1) {
        let physical_region_id_raw = request
            .options
            .get(LOGICAL_TABLE_METADATA_KEY)
            .ok_or(MissingRegionOptionSnafu {}.build())?;

        ensure!(
            physical_region_id_raw == first_physical_region_id_raw,
            ConflictRegionOptionSnafu {}
        );
    }

    Ok(physical_region_id)
}
