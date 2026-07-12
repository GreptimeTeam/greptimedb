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

use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use store_api::storage::RegionId;

use crate::error::{Error, Result};

/// Groups the region routes by the leader peer.
///
/// # Panics
///
/// Panics if the leader peer is not set for any of the region routes.
pub(crate) fn group_region_routes_by_peer(
    region_routes: &[RegionRoute],
) -> HashMap<&Peer, Vec<RegionId>> {
    let mut map: HashMap<&Peer, Vec<RegionId>> = HashMap::new();
    for region_route in region_routes {
        map.entry(region_route.leader_peer.as_ref().unwrap())
            .or_default()
            .push(region_route.region.id);
    }
    map
}

/// Returns `true` if all results are successful.
fn all_successful(results: &[Result<()>]) -> bool {
    results.iter().all(Result::is_ok)
}

pub enum HandleMultipleResult<'a> {
    AllSuccessful,
    AllRetryable(Vec<(usize, &'a Error)>),
    PartialRetryable {
        retryable_errors: Vec<(usize, &'a Error)>,
        non_retryable_errors: Vec<(usize, &'a Error)>,
    },
    AllNonRetryable(Vec<(usize, &'a Error)>),
}

/// Evaluates results from multiple operations and categorizes errors by retryability.
///
/// If all operations succeed, returns `AllSuccessful`.
/// If all errors are retryable, returns `AllRetryable`.
/// If all errors are non-retryable, returns `AllNonRetryable`.
/// Otherwise, returns `PartialRetryable` with separate collections for retryable and non-retryable errors.
pub(crate) fn handle_multiple_results<'a>(results: &'a [Result<()>]) -> HandleMultipleResult<'a> {
    if all_successful(results) {
        return HandleMultipleResult::AllSuccessful;
    }

    let mut retryable_errors = Vec::new();
    let mut non_retryable_errors = Vec::new();
    for (index, result) in results.iter().enumerate() {
        if let Err(error) = result {
            if error.is_retryable() {
                retryable_errors.push((index, error));
            } else {
                non_retryable_errors.push((index, error));
            }
        }
    }

    match (retryable_errors.is_empty(), non_retryable_errors.is_empty()) {
        (true, false) => HandleMultipleResult::AllNonRetryable(non_retryable_errors),
        (false, true) => HandleMultipleResult::AllRetryable(retryable_errors),
        (false, false) => HandleMultipleResult::PartialRetryable {
            retryable_errors,
            non_retryable_errors,
        },
        // Should not happen, but include for completeness
        (true, true) => HandleMultipleResult::AllSuccessful,
    }
}
