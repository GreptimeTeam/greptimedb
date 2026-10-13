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

//! Lossless in-memory representation of an already validated immutable block.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

/// Owned by one immutable snapshot; keys include the complete block descriptor.
/// The limit covers retained blocks, not in-flight readers or allocator overhead.
#[derive(Debug)]
pub(crate) struct BlockCache {
    entries: BTreeMap<[u64; 6], (u64, Arc<DecodedBlock>)>,
    order: BTreeSet<(u64, [u64; 6])>,
    clock: u64,
    used: usize,
    capacity: usize,
}

impl BlockCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: BTreeSet::new(),
            clock: 0,
            used: 0,
            capacity,
        }
    }

    fn tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            self.entries.clear();
            self.order.clear();
            self.used = 0;
            self.clock = 0;
        }
        self.clock += 1;
        self.clock
    }

    pub(crate) fn get(&mut self, key: [u64; 6]) -> Option<Arc<DecodedBlock>> {
        let now = self.tick();
        let (time, block) = self.entries.get_mut(&key)?;
        self.order.remove(&(*time, key));
        *time = now;
        self.order.insert((now, key));
        Some(Arc::clone(block))
    }

    pub(crate) fn insert(&mut self, key: [u64; 6], block: Arc<DecodedBlock>) {
        // Allow for both tree entries and Arc bookkeeping in the admission charge.
        let size = block.charged_bytes() + 256;
        if size > self.capacity || self.entries.contains_key(&key) {
            return;
        }
        while self.used > self.capacity - size {
            let Some((_, victim)) = self.order.pop_first() else {
                break;
            };
            if let Some((_, removed)) = self.entries.remove(&victim) {
                self.used -= removed.charged_bytes() + 256;
            }
        }
        let time = self.tick();
        self.entries.insert(key, (time, block));
        self.order.insert((time, key));
        self.used += size;
    }
}

#[derive(Debug)]
enum Timestamps {
    Regular { first: i64, step: i64 },
    Explicit(Box<[i64]>),
}

#[derive(Debug)]
pub(crate) struct DecodedBlock {
    timestamps: Timestamps,
    bits: Box<[u64]>,
}

impl DecodedBlock {
    pub(crate) fn new(timestamps: Vec<i64>, bits: Vec<u64>) -> Result<Self, &'static str> {
        if timestamps.is_empty() || timestamps.len() != bits.len() {
            return Err("Invalid decoded block lengths");
        }
        if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("Decoded block timestamps are not strictly increasing");
        }
        let step = if timestamps.len() == 1 {
            Some(0)
        } else {
            timestamps[1].checked_sub(timestamps[0])
        };
        let regular = step.filter(|step| {
            timestamps
                .windows(2)
                .all(|pair| i128::from(pair[1]) - i128::from(pair[0]) == i128::from(*step))
        });
        let timestamps = match regular {
            Some(step) => Timestamps::Regular {
                first: timestamps[0],
                step,
            },
            None => Timestamps::Explicit(timestamps.into_boxed_slice()),
        };
        Ok(Self {
            timestamps,
            bits: bits.into_boxed_slice(),
        })
    }

    pub(crate) fn charged_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.bits.len() * std::mem::size_of::<u64>()
            + match &self.timestamps {
                Timestamps::Regular { .. } => 0,
                Timestamps::Explicit(times) => std::mem::size_of_val(times.as_ref()),
            }
    }

    fn timestamp(&self, index: usize) -> i64 {
        match &self.timestamps {
            // Construction checks every timestamp; the widened arithmetic avoids
            // overflow when the sequence spans negative and positive timestamps.
            Timestamps::Regular { first, step } => {
                (i128::from(*first) + i128::from(*step) * index as i128) as i64
            }
            Timestamps::Explicit(times) => times[index],
        }
    }

    pub(crate) fn range(&self, start: i64, end: i64) -> Range<usize> {
        if start > end {
            return 0..0;
        }
        let lower = |value: i64, inclusive: bool| {
            let mut low = 0;
            let mut high = self.bits.len();
            while low < high {
                let middle = low + (high - low) / 2;
                let timestamp = self.timestamp(middle);
                if timestamp < value || (inclusive && timestamp == value) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            low
        };
        lower(start, false)..lower(end, true)
    }

    pub(crate) fn samples(&self, start: i64, end: i64) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.range(start, end)
            .map(|index| (self.timestamp(index), self.bits[index]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_lru_and_snapshot_isolation() {
        let block = Arc::new(DecodedBlock::new(vec![1, 2], vec![0, 1]).unwrap());
        let charge = block.charged_bytes() + 256;
        let mut cache = BlockCache::new(charge * 2);
        cache.insert([1; 6], Arc::clone(&block));
        cache.insert([2; 6], Arc::clone(&block));
        assert!(cache.get([1; 6]).is_some());
        cache.insert([3; 6], Arc::clone(&block));
        assert!(cache.get([2; 6]).is_none());
        assert!(cache.get([1; 6]).is_some());
        assert_eq!(cache.used, charge * 2);
        cache.insert([1; 6], Arc::clone(&block));
        assert_eq!(cache.used, charge * 2);
        let mut other = BlockCache::new(charge - 1);
        other.insert([1; 6], block);
        assert!(other.get([1; 6]).is_none());
        assert_eq!(other.used, 0);
    }

    #[test]
    fn exact_bits_and_inclusive_selection() {
        let bits = vec![0, 1 << 63, 0x7ff0000000000002, 0x7ff8000000000001];
        let block = DecodedBlock::new(vec![-30, -20, -10, 0], bits.clone()).unwrap();
        assert!(matches!(block.timestamps, Timestamps::Regular { .. }));
        assert_eq!(
            block.samples(-20, -10).collect::<Vec<_>>(),
            vec![(-20, bits[1]), (-10, bits[2])]
        );
        assert_eq!(
            block
                .samples(i64::MIN, i64::MAX)
                .map(|(_, b)| b)
                .collect::<Vec<_>>(),
            bits
        );
        assert_eq!(block.range(1, 10), 4..4);
        assert_eq!(block.range(-100, -31), 0..0);
        assert!(block.range(10, -10).is_empty());
    }

    #[test]
    fn irregular_and_extreme_timestamps_match_scalar_filter() {
        for times in [
            vec![i64::MIN, i64::MAX],
            vec![i64::MIN, -1, i64::MAX - 1],
            vec![-8, -3, 0, 10],
            vec![i64::MAX],
        ] {
            let block = DecodedBlock::new(times.clone(), vec![7; times.len()]).unwrap();
            for start in [i64::MIN, -8, -4, 0, 10, i64::MAX] {
                for end in [i64::MIN, -8, -3, 0, 9, i64::MAX] {
                    let expected: Vec<_> = times
                        .iter()
                        .copied()
                        .filter(|t| *t >= start && *t <= end)
                        .map(|t| (t, 7))
                        .collect();
                    assert_eq!(block.samples(start, end).collect::<Vec<_>>(), expected);
                }
            }
        }
    }

    #[test]
    fn rejects_invalid_order_and_lengths() {
        for times in [vec![], vec![1, 1], vec![2, 1]] {
            assert!(DecodedBlock::new(times.clone(), vec![0; times.len()]).is_err());
        }
        assert!(DecodedBlock::new(vec![1, 2], vec![0]).is_err());
    }

    #[test]
    fn charges_explicit_timestamp_storage() {
        let regular = DecodedBlock::new(vec![0, 10, 20], vec![0; 3]).unwrap();
        let irregular = DecodedBlock::new(vec![0, 10, 21], vec![0; 3]).unwrap();
        assert_eq!(irregular.charged_bytes() - regular.charged_bytes(), 24);
        assert!(regular.charged_bytes() >= 24 + std::mem::size_of::<DecodedBlock>());
    }
}
