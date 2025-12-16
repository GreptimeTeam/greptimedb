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

use std::fmt;

/// Memory permit granularity for different use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermitGranularity {
    /// 1 KB per permit
    ///
    /// Use for:
    /// - HTTP/gRPC request limiting (small, high-concurrency operations)
    /// - Small batch operations
    /// - Scenarios requiring fine-grained fairness
    Kilobyte,

    /// 1 MB per permit (default)
    ///
    /// Use for:
    /// - Query execution memory management
    /// - Compaction memory control
    /// - Large, long-running operations
    #[default]
    Megabyte,
}

impl PermitGranularity {
    /// Returns the number of bytes per permit.
    #[inline]
    pub const fn bytes(self) -> u64 {
        match self {
            Self::Kilobyte => 1024,
            Self::Megabyte => 1024 * 1024,
        }
    }

    /// Returns a human-readable string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kilobyte => "1KB",
            Self::Megabyte => "1MB",
        }
    }

    /// Converts bytes to permits based on this granularity.
    ///
    /// Rounds up to ensure the requested bytes are fully covered.
    /// Clamped to Semaphore::MAX_PERMITS.
    #[inline]
    pub fn bytes_to_permits(self, bytes: u64) -> u32 {
        use tokio::sync::Semaphore;

        let granularity_bytes = self.bytes();
        bytes
            .saturating_add(granularity_bytes - 1)
            .saturating_div(granularity_bytes)
            .min(Semaphore::MAX_PERMITS as u64)
            .min(u32::MAX as u64) as u32
    }

    /// Converts permits to bytes based on this granularity.
    #[inline]
    pub fn permits_to_bytes(self, permits: u32) -> u64 {
        (permits as u64).saturating_mul(self.bytes())
    }
}

impl fmt::Display for PermitGranularity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
