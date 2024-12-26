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

use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::RowGroupMetaData;

use crate::bloom_filter::error::Result;
use crate::bloom_filter::reader::BloomFilterReader;
use crate::bloom_filter::{BloomFilterMeta, BloomFilterSegmentLocation, Bytes};

/// Enumerates types of predicates for value filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// Predicate for matching values in a list.
    InList(InListPredicate),
}

/// `InListPredicate` contains a list of acceptable values. A value needs to match at least
/// one of the elements (logical OR semantic) for the predicate to be satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InListPredicate {
    /// List of acceptable values.
    pub list: HashSet<Bytes>,
}

pub struct BloomFilterApplier {
    reader: Box<dyn BloomFilterReader + Send>,
    meta: BloomFilterMeta,
}

impl BloomFilterApplier {
    pub async fn new(mut reader: Box<dyn BloomFilterReader + Send>) -> Result<Self> {
        let meta = reader.metadata().await?;

        Ok(Self { reader, meta })
    }

    /// Searches for matching row groups using bloom filters.
    ///
    /// This method applies bloom filter index to eliminate row groups that definitely
    /// don't contain the searched values. It works by:
    ///
    /// 1. Computing prefix sums for row counts
    /// 2. Calculating bloom filter segment locations for each row group
    ///     1. A row group may span multiple bloom filter segments
    /// 3. Probing bloom filter segments
    /// 4. Removing non-matching row groups from the basement
    ///     1. If a row group doesn't match any bloom filter segment with any probe, it is removed
    ///
    /// # Note
    /// The method modifies the `basement` map in-place by removing row groups that
    /// don't match the bloom filter criteria.
    pub async fn search(
        &mut self,
        probes: &HashSet<Bytes>,
        row_group_metas: &[RowGroupMetaData],
        basement: &mut BTreeMap<usize, Option<RowSelection>>,
    ) -> Result<()> {
        // 0. Fast path - if basement is empty return empty vec
        if basement.is_empty() {
            return Ok(());
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
        let mut row_groups_to_remove = HashSet::new();
        for &row_group_idx in basement.keys() {
            // TODO(ruihang): support further filter over row selection

            // todo: dedup & overlap
            let rows_range_start = prefix_sum[row_group_idx] / self.meta.rows_per_segment;
            let rows_range_end = (prefix_sum[row_group_idx + 1] as f64
                / self.meta.rows_per_segment as f64)
                .ceil() as usize;

            let mut is_any_range_hit = false;
            for i in rows_range_start..rows_range_end {
                // 3. Probe each bloom filter segment
                let loc = BloomFilterSegmentLocation {
                    offset: self.meta.bloom_filter_segments[i].offset,
                    size: self.meta.bloom_filter_segments[i].size,
                    elem_count: self.meta.bloom_filter_segments[i].elem_count,
                };
                let bloom = self.reader.bloom_filter(&loc).await?;

                // Check if any probe exists in bloom filter
                let mut matches = false;
                for probe in probes {
                    if bloom.contains(probe) {
                        matches = true;
                        break;
                    }
                }

                is_any_range_hit |= matches;
                if matches {
                    break;
                }
            }
            if !is_any_range_hit {
                row_groups_to_remove.insert(row_group_idx);
            }
        }

        // 4. Remove row groups that do not match any bloom filter segment
        for row_group_idx in row_groups_to_remove {
            basement.remove(&row_group_idx);
        }

        Ok(())
    }
}
