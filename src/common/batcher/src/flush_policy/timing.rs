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

use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::time::Instant;

use crate::flush_policy::{FlushPolicy, FlushTrigger};
use crate::pending_batch::PendingBatch;

/// Decides whether the first-submission deadline or row threshold requires a flush.
///
/// The row threshold is checked after appending a complete submission. It is not a
/// hard batch-size limit and never splits a submission.
#[derive(Debug, Clone, Copy)]
pub struct TimingFlushPolicy {
    flush_interval: Duration,
    max_batch_rows: NonZeroUsize,
}

impl TimingFlushPolicy {
    /// Creates a policy, rejecting zero intervals and unrepresentable deadlines.
    pub fn try_new(flush_interval: Duration, max_batch_rows: NonZeroUsize) -> Option<Self> {
        if !Self::validate(flush_interval) {
            return None;
        }
        Some(Self {
            flush_interval,
            max_batch_rows,
        })
    }

    /// Checks that the interval is nonzero and its deadline is representable.
    pub fn validate(flush_interval: Duration) -> bool {
        !flush_interval.is_zero() && Instant::now().checked_add(flush_interval).is_some()
    }

    /// Checks only the row trigger when a caller drives deadline events separately.
    pub fn reached_row_threshold<T>(&self, batch: &PendingBatch<T>) -> bool {
        !batch.is_empty() && batch.total_rows() >= self.max_batch_rows.get()
    }
}

impl FlushPolicy for TimingFlushPolicy {
    /// Returns the deadline anchored to the first submission, or `None` for an empty batch.
    ///
    /// An unrepresentable deadline falls back to the first submission time so that
    /// extreme timestamps cannot leave a nonempty batch waiting indefinitely.
    fn deadline<T>(&self, batch: &PendingBatch<T>) -> Option<Instant> {
        batch
            .first_submitted_at()
            .map(|first| first.checked_add(self.flush_interval).unwrap_or(first))
    }

    /// Returns whether a nonempty batch has reached either trigger.
    fn should_flush<T>(
        &self,
        batch: &PendingBatch<T>,
        now: Instant,
        trigger: FlushTrigger,
    ) -> bool {
        !batch.is_empty()
            && (self.reached_row_threshold(batch)
                || (trigger == FlushTrigger::Deadline
                    && self.deadline(batch).is_some_and(|deadline| now >= deadline)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_matches_construction() {
        for interval in [Duration::ZERO, Duration::from_secs(1), Duration::MAX] {
            assert_eq!(
                TimingFlushPolicy::validate(interval),
                TimingFlushPolicy::try_new(interval, NonZeroUsize::MIN).is_some()
            );
        }
    }

    #[test]
    fn test_first_submission_deadline() {
        let policy =
            TimingFlushPolicy::try_new(Duration::from_millis(10), NonZeroUsize::new(100).unwrap())
                .unwrap();
        let mut batch = PendingBatch::new();
        let first = Instant::now();
        assert_eq!(policy.deadline(&batch), None);
        assert!(!policy.should_flush(&batch, first, FlushTrigger::Deadline));
        batch.push(1, 1, first);
        batch.push(2, 1, first + Duration::from_millis(9));
        assert_eq!(
            policy.deadline(&batch),
            Some(first + Duration::from_millis(10))
        );
        assert!(!policy.should_flush(
            &batch,
            first + Duration::from_millis(9),
            FlushTrigger::Deadline
        ));
        assert!(policy.should_flush(
            &batch,
            first + Duration::from_millis(10),
            FlushTrigger::Deadline
        ));
        assert!(!policy.reached_row_threshold(&batch));
        assert!(!policy.should_flush(
            &batch,
            first + Duration::from_millis(10),
            FlushTrigger::Submission,
        ));
        assert_eq!(batch.take(), vec![1, 2]);
        let next = first + Duration::from_millis(20);
        batch.push(3, 0, next);
        assert!(!policy.should_flush(&batch, next, FlushTrigger::Deadline));
        assert!(policy.should_flush(
            &batch,
            next + Duration::from_millis(10),
            FlushTrigger::Deadline
        ));
    }

    #[test]
    fn test_row_threshold_preserves_complete_submissions() {
        let policy =
            TimingFlushPolicy::try_new(Duration::from_secs(1), NonZeroUsize::new(3).unwrap())
                .unwrap();
        let now = Instant::now();
        for rows in [2, 3, 4] {
            let mut batch = PendingBatch::new();
            batch.push(vec![0; rows], rows, now);
            assert_eq!(
                policy.should_flush(&batch, now, FlushTrigger::Submission),
                rows >= 3
            );
            assert_eq!(policy.reached_row_threshold(&batch), rows >= 3);
            assert_eq!(batch.take(), vec![vec![0; rows]]);
        }
    }

    #[test]
    fn test_invalid_interval() {
        let max_batch_rows = NonZeroUsize::new(1).unwrap();
        assert!(TimingFlushPolicy::try_new(Duration::ZERO, max_batch_rows).is_none());
        assert!(TimingFlushPolicy::try_new(Duration::MAX, max_batch_rows).is_none());
    }
}
