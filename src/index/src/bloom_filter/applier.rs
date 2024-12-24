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

use std::collections::BTreeMap;

use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::RowGroupMetaData;

use crate::bloom_filter::error::Result;
use crate::bloom_filter::reader::BloomFilterReader;
use crate::bloom_filter::{BloomFilterMeta, BloomFilterSegmentLocation, Bytes};

pub struct BloomFilterApplier {
    reader: Box<dyn BloomFilterReader + Send>,
    meta: BloomFilterMeta,
}

impl BloomFilterApplier {
    pub async fn new(mut reader: Box<dyn BloomFilterReader + Send>) -> Result<Self> {
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
        let mut sum = 0usize;
        let mut prefix_sum = Vec::with_capacity(row_group_metas.len() + 1);
        prefix_sum.push(0usize);
        for meta in row_group_metas {
            sum += meta.num_rows() as usize;
            prefix_sum.push(sum);
        }

        // 2. Calculate bloom filter segment locations
        let mut segment_locations = Vec::new();
        for &row_group_idx in basement.keys() {
            // TODO(ruihang): support further filter over row selection

            // todo: dedup & overlap
            let rows_range_start = prefix_sum[row_group_idx] / self.meta.rows_per_segment;
            let rows_range_end = prefix_sum[row_group_idx + 1] / self.meta.rows_per_segment;

            for i in rows_range_start..rows_range_end {
                // 3. Probe each bloom filter segment
                let loc = BloomFilterSegmentLocation {
                    offset: self.meta.bloom_filter_segments[i].offset,
                    size: self.meta.bloom_filter_segments[i].size,
                    elem_count: self.meta.bloom_filter_segments[i].elem_count,
                };
                let bloom = self.reader.bloom_filter(&loc).await?;

                // Check if all probes exist in bloom filter
                let mut matches = true;
                for probe in probes {
                    if !bloom.contains(probe) {
                        matches = false;
                        break;
                    }
                }

                if matches {
                    segment_locations.push(loc);
                }
            }
        }

        Ok(segment_locations)
    }
}
