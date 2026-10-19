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

//! Scan memory reservations held for reader lifetimes.

use common_memory_manager::MemoryGuard;

use crate::CallbackMemoryMetrics;

/// Reserved scan memory, released automatically on drop.
#[derive(Debug)]
pub struct QueryMemoryReservation {
    pub(crate) inner: MemoryGuard<CallbackMemoryMetrics>,
}

impl QueryMemoryReservation {
    pub(crate) fn try_acquire_additional(&mut self, bytes: u64) -> bool {
        self.inner.try_acquire_additional(bytes)
    }
}
