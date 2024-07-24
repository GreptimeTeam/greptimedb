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

//! Utilities to read the last row of each time series.

use std::sync::Arc;

use async_trait::async_trait;
use store_api::storage::TimeSeriesRowSelector;

use crate::cache::{
    selector_result_cache_hit, selector_result_cache_miss, CacheManagerRef, SelectorResultKey,
    SelectorResultValue,
};
use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};
use crate::sst::file::FileId;
use crate::sst::parquet::reader::RowGroupReader;

/// Reader to keep the last row for each time series.
/// It assumes that batches from the input reader are
/// - sorted
/// - all deleted rows has been filtered.
/// - not empty
///
/// This reader is different from the [MergeMode](crate::region::options::MergeMode) as
/// it focus on time series (the same key).
pub(crate) struct LastRowReader {
    /// Inner reader.
    reader: BoxedBatchReader,
    /// The last batch pending to return.
    selector: LastRowSelector,
}

impl LastRowReader {
    /// Creates a new `LastRowReader`.
    pub(crate) fn new(reader: BoxedBatchReader) -> Self {
        Self {
            reader,
            selector: LastRowSelector::default(),
        }
    }

    /// Returns the last row of the next key.
    pub(crate) async fn next_last_row(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(yielded) = self.selector.on_next(batch) {
                return Ok(Some(yielded));
            }
        }
        Ok(self.selector.finish())
    }
}

#[async_trait]
impl BatchReader for LastRowReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.next_last_row().await
    }
}

/// Cached last row reader for specific row group.
/// If the last rows for current row group are already cached, this reader returns the cached value.
/// If cache misses, [RowGroupLastRowReader] reads last rows from row group and updates the cache
/// upon finish.
pub(crate) enum RowGroupLastRowCachedReader {
    /// Cache hit, reads last rows from cached value.
    Hit(LastRowCacheReader),
    /// Cache miss, reads from row group reader and update cache.
    Miss(RowGroupLastRowReader),
}

impl RowGroupLastRowCachedReader {
    pub(crate) fn new(
        file_id: FileId,
        row_group_idx: usize,
        cache_manager: Option<CacheManagerRef>,
        row_group_reader: RowGroupReader,
    ) -> Self {
        let key = SelectorResultKey {
            file_id,
            row_group_idx,
            selector: TimeSeriesRowSelector::LastRow,
        };

        let Some(cache_manager) = cache_manager else {
            return Self::new_miss(key, row_group_reader, None);
        };
        if let Some(value) = cache_manager.get_selector_result(&key) {
            let schema_matches = value.projection
                == row_group_reader
                    .context()
                    .read_format()
                    .projection_indices();
            if schema_matches {
                // Schema matches, use cache batches.
                Self::new_hit(value)
            } else {
                Self::new_miss(key, row_group_reader, Some(cache_manager))
            }
        } else {
            Self::new_miss(key, row_group_reader, Some(cache_manager))
        }
    }

    /// Creates new Hit variant and updates metrics.
    fn new_hit(value: Arc<SelectorResultValue>) -> Self {
        selector_result_cache_hit();
        Self::Hit(LastRowCacheReader { value, idx: 0 })
    }

    /// Creates new Miss variant and updates metrics.
    fn new_miss(
        key: SelectorResultKey,
        row_group_reader: RowGroupReader,
        cache_manager: Option<CacheManagerRef>,
    ) -> Self {
        selector_result_cache_miss();
        Self::Miss(RowGroupLastRowReader::new(
            key,
            row_group_reader,
            cache_manager,
        ))
    }
}

#[async_trait]
impl BatchReader for RowGroupLastRowCachedReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            RowGroupLastRowCachedReader::Hit(r) => r.next_batch().await,
            RowGroupLastRowCachedReader::Miss(r) => r.next_batch().await,
        }
    }
}

/// Last row reader that returns the cached last rows for row group.
pub(crate) struct LastRowCacheReader {
    value: Arc<SelectorResultValue>,
    idx: usize,
}

impl LastRowCacheReader {
    /// Iterates cached last rows.
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        if self.idx < self.value.result.len() {
            let res = Ok(Some(self.value.result[self.idx].clone()));
            self.idx += 1;
            res
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct RowGroupLastRowReader {
    key: SelectorResultKey,
    reader: RowGroupReader,
    selector: LastRowSelector,
    yielded_batches: Vec<Batch>,
    cache_manager: Option<CacheManagerRef>,
}

impl RowGroupLastRowReader {
    fn new(
        key: SelectorResultKey,
        reader: RowGroupReader,
        cache_manager: Option<CacheManagerRef>,
    ) -> Self {
        Self {
            key,
            reader,
            selector: LastRowSelector::default(),
            yielded_batches: vec![],
            cache_manager,
        }
    }

    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(yielded) = self.selector.on_next(batch) {
                if self.cache_manager.is_some() {
                    self.yielded_batches.push(yielded.clone());
                }
                return Ok(Some(yielded));
            }
        }
        let last_batch = if let Some(last_batch) = self.selector.finish() {
            if self.cache_manager.is_some() {
                self.yielded_batches.push(last_batch.clone());
            }
            Some(last_batch)
        } else {
            None
        };

        // All last rows in row group are yielded, update cache.
        self.maybe_update_cache();
        Ok(last_batch)
    }

    /// Updates row group's last row cache if cache manager is present.
    fn maybe_update_cache(&mut self) {
        if let Some(cache) = &self.cache_manager {
            let value = Arc::new(SelectorResultValue {
                result: std::mem::take(&mut self.yielded_batches),
                projection: self
                    .reader
                    .context()
                    .read_format()
                    .projection_indices()
                    .to_vec(),
            });
            cache.put_selector_result(self.key, value)
        }
    }
}

/// Common struct that selects only the last row of each time series.
#[derive(Default)]
pub struct LastRowSelector {
    last_batch: Option<Batch>,
}

impl LastRowSelector {
    /// Handles next batch. Return the yielding batch if present.
    pub fn on_next(&mut self, batch: Batch) -> Option<Batch> {
        if let Some(last) = &self.last_batch {
            if last.primary_key() == batch.primary_key() {
                // Same key, update last batch.
                self.last_batch = Some(batch);
                None
            } else {
                // Different key, return the last row in `last` and update `last_batch` by
                // current batch.
                debug_assert!(!last.is_empty());
                let last_row = last.slice(last.num_rows() - 1, 1);
                self.last_batch = Some(batch);
                Some(last_row)
            }
        } else {
            self.last_batch = Some(batch);
            None
        }
    }

    /// Finishes the selector and returns the pending batch if any.
    pub fn finish(&mut self) -> Option<Batch> {
        if let Some(last) = self.last_batch.take() {
            // This is the last key.
            let last_row = last.slice(last.num_rows() - 1, 1);
            return Some(last_row);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;

    use super::*;
    use crate::test_util::{check_reader_result, new_batch, VecBatchReader};

    #[tokio::test]
    async fn test_last_row_one_batch() {
        let input = [new_batch(
            b"k1",
            &[1, 2],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[21, 22],
        )];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[new_batch(b"k1", &[2], &[11], &[OpType::Put], &[22])],
        )
        .await;

        // Only one row.
        let input = [new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21])];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21])],
        )
        .await;
    }

    #[tokio::test]
    async fn test_last_row_multi_batch() {
        let input = [
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_batch(
                b"k1",
                &[3, 4],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[23, 24],
            ),
            new_batch(
                b"k2",
                &[1, 2],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
        ];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[
                new_batch(b"k1", &[4], &[11], &[OpType::Put], &[24]),
                new_batch(b"k2", &[2], &[11], &[OpType::Put], &[32]),
            ],
        )
        .await;
    }
}
