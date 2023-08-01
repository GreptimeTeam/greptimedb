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

use std::sync::Arc;

use store_api::storage::RegionId;

use crate::sst::file::FileId;

/// Request to remove a file.
#[derive(Debug)]
pub struct PurgeRequest {
    /// Region id of the file.
    pub region_id: RegionId,
    /// Id of the file.
    pub file_id: FileId,
}

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync {
    /// Send a purge request to the background worker.
    fn send_request(&self, request: PurgeRequest);
}

pub type FilePurgerRef = Arc<dyn FilePurger>;

// TODO(yingwen): Remove this once we implement the real purger.
/// A purger that does nothing.
#[derive(Debug)]
struct NoopPurger {}

impl FilePurger for NoopPurger {
    fn send_request(&self, _request: PurgeRequest) {}
}
