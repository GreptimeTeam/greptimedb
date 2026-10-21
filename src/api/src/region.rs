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

use common_base::AffectedRows;
use greptime_proto::v1::region::{RegionRequestHeader, RegionResponse as RegionResponseV1};

/// Request extension advertising support for per-entry write failures.
pub const ACCEPT_PARTIAL_RESULT: &str = "accept_partial_result";

/// Advertises support for per-entry write failures in the request header.
pub fn set_accept_partial_result(header: &mut RegionRequestHeader) {
    header
        .query_context
        .get_or_insert_with(Default::default)
        .extensions
        .insert(ACCEPT_PARTIAL_RESULT.to_string(), "true".to_string());
}

/// This result struct is derived from [RegionResponseV1]
#[derive(Debug)]
pub struct RegionResponse {
    pub affected_rows: AffectedRows,
    pub extensions: HashMap<String, Vec<u8>>,
    pub metadata: Vec<u8>,
}

/// Successful entries and failed-entry errors indexed by input order.
#[derive(Debug)]
pub struct BatchRegionResponse<E> {
    pub affected_rows: AffectedRows,
    pub extensions: HashMap<String, Vec<u8>>,
    pub failures: Vec<(usize, E)>,
}

impl<E> BatchRegionResponse<E> {
    /// Returns the failure with the smallest original request index.
    pub fn first_failure(&self) -> Option<&(usize, E)> {
        self.failures.iter().min_by_key(|(index, _)| *index)
    }
}

impl RegionResponse {
    pub fn from_region_response(region_response: RegionResponseV1) -> Self {
        Self {
            affected_rows: region_response.affected_rows as _,
            extensions: region_response.extensions,
            metadata: region_response.metadata,
        }
    }

    /// Creates one response without extension
    pub fn new(affected_rows: AffectedRows) -> Self {
        Self {
            affected_rows,
            extensions: Default::default(),
            metadata: Vec::new(),
        }
    }

    /// Creates one response with metadata.
    pub fn from_metadata(metadata: Vec<u8>) -> Self {
        Self {
            affected_rows: 0,
            extensions: Default::default(),
            metadata,
        }
    }
}
