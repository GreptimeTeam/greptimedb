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

pub mod applier;
pub mod creator;
pub mod error;
pub mod reader;

use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::LazyLock;

/// The seed used for the Bloom filter.
pub const SEED: u128 = 42;

static ELEMENT_HASHER: LazyLock<fastbloom::DefaultHasher> =
    LazyLock::new(|| fastbloom::DefaultHasher::seeded(&SEED.to_be_bytes()));

/// Returns the hash fastbloom derives for `elem` with the persisted [`SEED`].
///
/// A filter built from these hashes with [`PrehashedBuildHasher`] has exactly the same
/// bits as one built by inserting the elements themselves, so files stay readable by
/// both paths.
pub fn element_hash(elem: &[u8]) -> u64 {
    ELEMENT_HASHER.hash_one(elem)
}

/// Hasher that returns an already computed [`element_hash`] unchanged.
#[derive(Default)]
pub struct PrehashedHasher(u64);

impl Hasher for PrehashedHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("PrehashedHasher only accepts u64 hashes")
    }

    fn write_u64(&mut self, hash: u64) {
        self.0 = hash;
    }
}

pub type PrehashedBuildHasher = BuildHasherDefault<PrehashedHasher>;

/// A persisted bloom filter probed by [`element_hash`] values.
pub type PrehashedBloomFilter = fastbloom::BloomFilter<512, PrehashedBuildHasher>;

#[cfg(test)]
mod tests {
    use fastbloom::BloomFilter;

    use super::*;

    #[test]
    fn test_prehashed_filter_matches_seeded_filter() {
        // Persisted filters are read back with `.seed(&SEED)`, so building them from
        // precomputed hashes must set exactly the same bits.
        for count in [0usize, 1, 7, 100, 5000] {
            let elems = (0..count)
                .map(|i| format!("elem-{i}").into_bytes())
                .chain([Vec::new()])
                .collect::<Vec<_>>();
            let mut seeded = BloomFilter::with_false_pos(0.01)
                .seed(&SEED)
                .expected_items(elems.len());
            let mut prehashed = BloomFilter::with_false_pos(0.01)
                .hasher(PrehashedBuildHasher::default())
                .expected_items(elems.len());
            for elem in &elems {
                seeded.insert(elem);
                prehashed.insert(&element_hash(elem));
            }
            assert_eq!(seeded.as_slice(), prehashed.as_slice());
        }
    }
}
