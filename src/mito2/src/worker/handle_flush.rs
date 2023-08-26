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

//! Handling flush related requests.

use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Checks whether the engine reaches flush threshold. If so, finds regions in this
    /// worker to flush.
    pub(crate) fn maybe_flush_worker(&self) {
        if !self.write_buffer_manager.should_flush_engine() {
            // No need to flush worker.
            return;
        }
        // If the engine needs flush, each worker will find some regions to flush. We might
        // flush more memory than expect but it should be acceptable.
        self.find_regions_to_flush();
    }

    pub(crate) fn find_regions_to_flush(&self) {
        unimplemented!()
    }

    pub(crate) fn flush_region(&self) {
        unimplemented!()
    }
}
