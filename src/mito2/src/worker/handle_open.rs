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

//! Handling open request.

use common_telemetry::logging;

use crate::error::Result;
use crate::worker::request::OpenRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_open_request(&mut self, request: OpenRequest) -> Result<()> {
        if self.regions.is_region_exists(request.region_id) {
            return Ok(());
        }

        logging::info!("Try to open region {}", request.region_id);

        // TODO(yingwen):
        // 1. try to open manifest
        // 2. recover region from wal
        // 3. insert region into wal

        unimplemented!()
    }
}
