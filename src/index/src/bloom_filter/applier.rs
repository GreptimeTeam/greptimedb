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

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use fastbloom::BloomFilter;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::RowGroupMetaData;

use super::error::Result;
use super::{BloomFilterMeta, BloomFilterSegmentLocation, Bytes};

// format
#[async_trait]
pub trait BloomFilterReader {
    async fn range_read(&mut self, offset: u64, size: u32) -> Result<Vec<u8>>;

    async fn read_vec(&mut self, ranges: &[Range<u64>]) -> Result<Vec<Vec<u8>>>;

    async fn metadata(&mut self) -> Result<BloomFilterMeta>;

    async fn bloom_filter(&mut self, loc: &BloomFilterSegmentLocation) -> Result<BloomFilter>;
}
// end of format

pub struct BloomFilterApplier {
    reader: Box<dyn BloomFilterReader>,
    meta: BloomFilterMeta,
}

impl BloomFilterApplier {
    pub async fn new(mut reader: Box<dyn BloomFilterReader>) -> Result<Self> {
        let meta = reader.metadata().await?;

        Ok(Self { reader, meta })
    }

    pub async fn search(
        &mut self,
        probes: &[Bytes],
        row_group_metas: &[RowGroupMetaData],
        basement: &BTreeMap<usize, Option<RowSelection>>,
    ) -> Result<Vec<BloomFilterSegmentLocation>> {
        // 0. Fast path - if basement is empty return empty vec
        if basement.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Compute prefix sum for row counts
        let mut prefix_sum = Vec::with_capacity(row_group_metas.len() + 1);
        prefix_sum.push(0);
        for meta in row_group_metas {
            prefix_sum.push(prefix_sum.last().unwrap() + meta.num_rows());
        }

        // 2. Calculate bloom filter segment locations
        let mut segment_locations = Vec::new();
        for (&row_group_idx, _) in basement {
            if row_group_idx >= row_group_metas.len() {
                continue;
            }

            segment_locations.push(BloomFilterSegmentLocation {
                start_row: prefix_sum[row_group_idx],
                end_row: prefix_sum[row_group_idx + 1],
            });
        }

        // 3. Probe each bloom filter segment
        let mut matched_locations = Vec::new();
        let mut unique_segments = HashSet::new();

        for loc in segment_locations {
            // Skip if we've already checked this segment
            if !unique_segments.insert((loc.start_row, loc.end_row)) {
                continue;
            }

            let bloom = self.reader.bloom_filter(&loc).await?;
            let mut matches = true;

            // Check if all probes exist in bloom filter
            for probe in probes {
                if !bloom.contains(probe) {
                    matches = false;
                    break;
                }
            }

            if matches {
                matched_locations.push(loc);
            }
        }

        Ok(matched_locations)
    }
}
