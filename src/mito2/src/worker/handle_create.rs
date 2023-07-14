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

//! Handling create request.

use crate::error::Result;
use crate::worker::request::CreateRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_create_request(&mut self, _request: CreateRequest) -> Result<()> {
        // 1. Checks whether the table exists.

        // 2. Convert the request into RegionMetadata

        // 3. Write manifest
        unimplemented!()
    }
}
