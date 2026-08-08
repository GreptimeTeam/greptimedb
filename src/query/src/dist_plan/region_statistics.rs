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

//! Query-layer contract for obtaining best-effort row-count estimates of
//! regions during physical planning.
//!
//! [`MergeScanExec`](super::merge_scan::MergeScanExec) aggregates these
//! estimates into its `partition_statistics` so that DataFusion can compare
//! scan sizes (e.g. in [`JoinSelection`]) and prefer the small side as the
//! hash-join build side.
//!
//! The contract is deliberately narrow and synchronous: DataFusion calls
//! `ExecutionPlan::partition_statistics` during physical planning and cannot
//! block on storage or meta round trips. Concrete implementations live in
//! caller crates that own the region statistics source (for example the
//! datanode's in-memory [`RegionServer`] statistics), keeping the query crate
//! free of datanode/Mito dependencies.

use std::sync::Arc;

use store_api::storage::RegionId;

/// Provides best-effort row-count estimates for regions during query planning.
///
/// Implementations must be cheap and must never fail the query: any
/// unavailable, stale, or erroneous estimate is reported as `None`, which
/// makes the corresponding [`MergeScanExec`](super::merge_scan::MergeScanExec)
/// fall back to unknown statistics.
pub trait RegionRowCountProvider: Send + Sync {
    /// Returns the estimated number of rows stored in `region`, or `None` when
    /// the estimate is unavailable (unknown region, stale provider, provider
    /// error).
    fn row_count(&self, region: RegionId) -> Option<u64>;
}

/// Shared reference to a [`RegionRowCountProvider`].
pub type RegionRowCountProviderRef = Arc<dyn RegionRowCountProvider>;
