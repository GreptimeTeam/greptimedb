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
use session::context::QueryContextRef;
use table::metadata::TableInfoRef;
use tokio::sync::OwnedSemaphorePermit;

use crate::error::Result;

/// Accepts prepared table writes without coupling the inserter to a batcher implementation.
/// Schema creation, alteration and default evaluation remain the caller's responsibility.
#[async_trait]
pub trait PendingRowsBatcher: Send + Sync {
    /// Acquires one slot per original request, shared by all of its table submissions.
    async fn acquire(&self) -> Result<Arc<OwnedSemaphorePermit>>;

    /// Waits for the submitted rows to be written, retaining the slot through completion.
    /// Cancelling the response wait does not retract an already enqueued submission.
    async fn submit(
        &self,
        table_info: TableInfoRef,
        batch: RecordBatch,
        ctx: QueryContextRef,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<usize>;
}
