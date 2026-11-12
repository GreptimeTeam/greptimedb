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

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_query::Output;
use session::context::QueryContextRef;
use table::metadata::TableInfoRef;

use crate::error::Result;

/// Keeps a request's admission slot and conversion memory alive across its submissions.
pub type PendingRowsPermit = Arc<dyn Send + Sync>;

/// Accepts prepared table writes without coupling the inserter to a batcher implementation.
/// Schema creation, alteration and default evaluation remain the caller's responsibility.
#[async_trait]
pub trait PendingRowsBatcher: Send + Sync {
    /// Returns the conversion memory limit, or zero when byte admission is disabled.
    fn memory_limit_bytes(&self) -> u64 {
        0
    }

    /// Admits an original request before conversion, retaining its slot and estimated
    /// Arrow memory across all table submissions. Memory acquisition must not wait
    /// because the caller can already hold transport and decoded-row reservations.
    /// `held_bytes` counts those reservations, which cannot be released during conversion.
    async fn acquire(&self, estimated_bytes: usize, held_bytes: u64) -> Result<PendingRowsPermit>;

    /// Submits rows according to the acknowledgement policy, retaining the slot until
    /// writing completes even when the response acknowledges queue admission only.
    /// Cancelling the response wait does not retract an already enqueued submission.
    /// Early acknowledgements retain actual storage results in the output metadata.
    async fn submit(
        &self,
        table_info: TableInfoRef,
        batch: RecordBatch,
        ctx: QueryContextRef,
        permit: PendingRowsPermit,
    ) -> Result<Output>;
}
