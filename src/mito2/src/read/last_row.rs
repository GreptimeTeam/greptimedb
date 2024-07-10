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
use async_trait::async_trait;

use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};

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
    last_batch: Option<Batch>,
}

impl LastRowReader {
    /// Creates a new `LastRowReader`.
    pub(crate) fn new(reader: BoxedBatchReader) -> Self {
        Self {
            reader,
            last_batch: None,
        }
    }

    /// Returns the last row of the next key.
    pub(crate) async fn next_last_row(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(last) = &self.last_batch {
                if last.primary_key() == batch.primary_key() {
                    // Same key, update last batch.
                    self.last_batch = Some(batch);
                } else {
                    // Different key, return the last row in `last` and update `last_batch` by
                    // current batch.
                    debug_assert!(!last.is_empty());
                    let last_row = last.slice(last.num_rows() - 1, 1);
                    self.last_batch = Some(batch);
                    return Ok(Some(last_row));
                }
            } else {
                self.last_batch = Some(batch);
            }
        }

        if let Some(last) = self.last_batch.take() {
            // This is the last key.
            let last_row = last.slice(last.num_rows() - 1, 1);
            return Ok(Some(last_row));
        }

        Ok(None)
    }
}

#[async_trait]
impl BatchReader for LastRowReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.next_last_row().await
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
