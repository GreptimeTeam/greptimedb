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

//! Unified metrics adapter for all server protocols.

use common_memory_manager::MemoryMetrics;

use crate::metrics::{REQUEST_MEMORY_IN_USE, REQUEST_MEMORY_LIMIT, REQUEST_MEMORY_REJECTED};

/// Metrics adapter for unified request memory tracking.
///
/// This adapter tracks memory usage for all server protocols (HTTP, gRPC, Arrow Flight)
/// without distinguishing between them. All requests contribute to the same set of metrics.
#[derive(Clone, Copy, Debug, Default)]
pub struct RequestMemoryMetrics;

impl MemoryMetrics for RequestMemoryMetrics {
    fn set_limit(&self, bytes: i64) {
        REQUEST_MEMORY_LIMIT.set(bytes);
    }

    fn set_in_use(&self, bytes: i64) {
        REQUEST_MEMORY_IN_USE.set(bytes);
    }

    fn inc_rejected(&self, reason: &str) {
        REQUEST_MEMORY_REJECTED.with_label_values(&[reason]).inc();
    }
}
