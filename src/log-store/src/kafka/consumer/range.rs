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

use std::cmp::min;
use std::ops::Range;

use store_api::logstore::EntryId;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NextBatchHint {
    pub(crate) bytes: usize,
    pub(crate) len: usize,
}

/// Represents a range [next_entry_id, end_entry_id) of WAL entries for a region.
#[derive(Debug)]
pub struct RegionWalRange {
    current_entry_id: EntryId,
    end_entry_id: EntryId,
    max_batch_size: usize,
}

impl RegionWalRange {
    pub fn new(range: Range<EntryId>, max_batch_size: usize) -> Self {
        Self {
            current_entry_id: range.start,
            end_entry_id: range.end,
            max_batch_size,
        }
    }

    fn next_batch_size(&self) -> Option<u64> {
        if self.current_entry_id < self.end_entry_id {
            Some(self.end_entry_id.saturating_sub(self.current_entry_id))
        } else {
            None
        }
    }

    pub(crate) fn next_batch_hint(&self, avg_size: usize) -> Option<NextBatchHint> {
        if let Some(size) = self.next_batch_size() {
            let bytes = min(size as usize * avg_size, self.max_batch_size);
            let len = bytes / avg_size;

            return Some(NextBatchHint { bytes, len });
        }

        None
    }

    pub(crate) fn peek(&self) -> Option<EntryId> {
        if self.current_entry_id < self.end_entry_id {
            Some(self.current_entry_id)
        } else {
            None
        }
    }

    pub(crate) fn next(&mut self) -> Option<EntryId> {
        if self.current_entry_id < self.end_entry_id {
            let next = self.current_entry_id;
            self.current_entry_id += 1;
            Some(next)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_wal_range() {
        let range = RegionWalRange::new(0..1024, 1024);
        assert_eq!(
            range.next_batch_hint(10),
            Some(NextBatchHint {
                bytes: 1024,
                len: 102
            })
        );

        let mut range = RegionWalRange::new(0..1, 1024);

        assert_eq!(range.next_batch_size(), Some(1));
        assert_eq!(range.peek(), Some(0));

        // Advance 1 step
        assert_eq!(range.next(), Some(0));
        assert_eq!(range.next_batch_size(), None);

        // Advance 1 step
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), None);
        // No effect
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), None);

        let mut range = RegionWalRange::new(0..0, 1024);
        assert_eq!(range.next_batch_size(), None);
        // No effect
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), None);
    }
}
