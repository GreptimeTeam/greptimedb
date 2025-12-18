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

//! Memory metrics adapters for HTTP and gRPC servers.

use common_memory_manager::MemoryMetrics;

use crate::metrics::{
    GRPC_MEMORY_IN_USE, GRPC_MEMORY_LIMIT, GRPC_MEMORY_REJECTED, HTTP_MEMORY_IN_USE,
    HTTP_MEMORY_LIMIT, HTTP_MEMORY_REJECTED,
};

/// HTTP memory metrics adapter for MemoryManager.
#[derive(Clone, Copy, Debug, Default)]
pub struct HttpMemoryMetrics;

impl MemoryMetrics for HttpMemoryMetrics {
    fn set_limit(&self, bytes: i64) {
        HTTP_MEMORY_LIMIT.set(bytes);
    }

    fn set_in_use(&self, bytes: i64) {
        HTTP_MEMORY_IN_USE.set(bytes);
    }

    fn inc_rejected(&self, reason: &str) {
        HTTP_MEMORY_REJECTED.with_label_values(&[reason]).inc();
    }
}

/// gRPC memory metrics adapter for MemoryManager.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcMemoryMetrics;

impl MemoryMetrics for GrpcMemoryMetrics {
    fn set_limit(&self, bytes: i64) {
        GRPC_MEMORY_LIMIT.set(bytes);
    }

    fn set_in_use(&self, bytes: i64) {
        GRPC_MEMORY_IN_USE.set(bytes);
    }

    fn inc_rejected(&self, reason: &str) {
        GRPC_MEMORY_REJECTED.with_label_values(&[reason]).inc();
    }
}
