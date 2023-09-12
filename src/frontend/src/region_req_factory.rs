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

use api::v1::region::region_request::Body;
use api::v1::region::{
    DeleteRequests as RegionDeleteRequests, InsertRequests as RegionInsertRequests, RegionRequest,
    RegionRequestHeader,
};

pub struct RegionRequestFactory {
    header: RegionRequestHeader,
}

impl RegionRequestFactory {
    pub fn new(header: RegionRequestHeader) -> Self {
        Self { header }
    }

    pub fn build_insert(&self, requests: RegionInsertRequests) -> RegionRequest {
        RegionRequest {
            header: Some(self.header.clone()),
            body: Some(Body::Inserts(requests)),
        }
    }

    pub fn build_delete(&self, requests: RegionDeleteRequests) -> RegionRequest {
        RegionRequest {
            header: Some(self.header.clone()),
            body: Some(Body::Deletes(requests)),
        }
    }
}
