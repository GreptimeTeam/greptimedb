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

use tokio::time::Instant;

/// Accumulates complete submissions in arrival order, without interpreting their data.
#[derive(Debug)]
pub struct PendingBatch<T> {
    items: Vec<T>,
    total_rows: usize,
    first_submitted_at: Option<Instant>,
}

impl<T> Default for PendingBatch<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            total_rows: 0,
            first_submitted_at: None,
        }
    }
}

impl<T> PendingBatch<T> {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a complete submission without moving the first submission's timestamp.
    pub fn push(&mut self, item: T, total_rows: usize, now: Instant) {
        self.first_submitted_at.get_or_insert(now);
        self.items.push(item);
        // Saturation preserves threshold decisions even for caller-supplied oversized weights.
        self.total_rows = self.total_rows.saturating_add(total_rows);
    }

    /// Returns the accumulated row count, saturated at `usize::MAX`.
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Returns the number of complete submissions.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns whether there are no submissions, including zero-row submissions.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the time the first submission was appended to the current batch.
    pub fn first_submitted_at(&self) -> Option<Instant> {
        self.first_submitted_at
    }

    /// Takes all submissions in arrival order and resets the batch's accounting.
    pub fn take(&mut self) -> Vec<T> {
        self.total_rows = 0;
        self.first_submitted_at = None;
        std::mem::take(&mut self.items)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_complete_submissions_and_reset() {
        let mut batch = PendingBatch::new();
        let first = Instant::now();
        assert!(batch.is_empty());
        assert_eq!(batch.first_submitted_at(), None);
        batch.push(vec![1, 2], 2, first);
        batch.push(vec![3, 4, 5], 3, first + Duration::from_secs(1));
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.total_rows(), 5);
        assert_eq!(batch.first_submitted_at(), Some(first));
        assert_eq!(batch.take(), vec![vec![1, 2], vec![3, 4, 5]]);
        assert!(batch.is_empty());
        assert_eq!(batch.total_rows(), 0);
        assert_eq!(batch.first_submitted_at(), None);
        let next = first + Duration::from_secs(2);
        batch.push(vec![6], 1, next);
        assert_eq!(batch.first_submitted_at(), Some(next));
    }

    #[test]
    fn test_zero_rows_and_saturating_count() {
        let mut batch = PendingBatch::new();
        let now = Instant::now();
        batch.push(1, 0, now);
        assert!(!batch.is_empty());
        assert_eq!(batch.total_rows(), 0);
        batch.push(2, usize::MAX, now);
        batch.push(3, 1, now);
        assert_eq!(batch.total_rows(), usize::MAX);
        assert_eq!(batch.take(), vec![1, 2, 3]);
    }
}
