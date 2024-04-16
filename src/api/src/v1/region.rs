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
pub use greptime_proto::v1::region::*;

/// This result struct is derived from [RegionResponse]
#[derive(Debug)]
pub struct RegionHandleResponse {
    pub affected_rows: AffectedRows,
    pub extension: HashMap<String, Vec<u8>>,
}

impl RegionHandleResponse {
    pub fn from_region_response(region_response: RegionResponse) -> Self {
        Self {
            affected_rows: region_response.affected_rows as _,
            extension: region_response.extension,
        }
    }

    /// Creates one response without extension
    pub fn new(affected_rows: AffectedRows) -> Self {
        Self {
            affected_rows,
            extension: Default::default(),
        }
    }
}
