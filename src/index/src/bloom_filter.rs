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

use serde::{Deserialize, Serialize};

pub mod applier;
pub mod creator;
pub mod error;
pub mod reader;

pub type Bytes = Vec<u8>;
pub type BytesRef<'a> = &'a [u8];

/// The seed used for the Bloom filter.
pub const SEED: u128 = 42;

/// The Meta information of the bloom filter stored in the file.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BloomFilterMeta {
    /// The number of rows per segment.
    pub rows_per_segment: usize,

    /// The number of segments.
    pub seg_count: usize,

    /// The number of total rows.
    pub row_count: usize,

    /// The size of the bloom filter excluding the meta information.
    pub bloom_filter_segments_size: usize,

    /// Offset and size of bloom filters in the file.
    pub bloom_filter_segments: Vec<BloomFilterSegmentLocation>,
}

/// The location of the bloom filter segment in the file.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct BloomFilterSegmentLocation {
    /// The offset of the bloom filter segment in the file.
    pub offset: u64,

    /// The size of the bloom filter segment in the file.
    pub size: u64,

    /// The number of elements in the bloom filter segment.
    pub elem_count: usize,
}
