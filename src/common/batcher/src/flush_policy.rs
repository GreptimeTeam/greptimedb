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

pub mod timing;

use tokio::time::Instant;

use crate::pending_batch::PendingBatch;

/// The event that caused the worker to reconsider flushing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushTrigger {
    /// A complete submission was appended to the batch.
    Submission,
    /// The flush timer reached its deadline.
    Deadline,
}

/// Decides when a pending batch is ready without owning its payloads or execution.
///
/// A deadline wakeup must either allow flushing or produce a future deadline;
/// otherwise the caller would repeatedly poll an expired timer.
pub trait FlushPolicy {
    /// Returns the next timer deadline, or `None` when no timer is needed.
    fn deadline<T>(&self, batch: &PendingBatch<T>) -> Option<Instant>;

    /// Returns whether the batch should be flushed for this event at `now`.
    ///
    /// Workers never flush empty batches, regardless of this decision.
    fn should_flush<T>(&self, batch: &PendingBatch<T>, now: Instant, trigger: FlushTrigger)
    -> bool;
}
