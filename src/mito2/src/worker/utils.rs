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

use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::worker::{BufferableRequest, RegionWorkerLoop};

#[macro_export]
macro_rules! admit_or_return {
    ($worker:expr, $region_id:expr, $policy:expr, $request:expr) => {
        match $policy {
            RegionRequestPolicy::Accept => {}
            RegionRequestPolicy::Stall => {
                $worker.buffered_requests.push($region_id, $request);
                return;
            }
            RegionRequestPolicy::Reject(reason) => {
                $request.reject($region_id, reason);
                return;
            }
        }
    };
}

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_buffered_request(
        &mut self,
        region_id: RegionId,
        req: BufferableRequest,
    ) {
        match req {
            BufferableRequest::Truncate((req, sender)) => {
                self.handle_truncate_request(region_id, req, sender).await;
            }
        }
    }
}
