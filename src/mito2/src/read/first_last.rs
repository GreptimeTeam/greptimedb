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

//! Utilities to keep the first/last row for each time series.

use async_trait::async_trait;

use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};

/// Reader to keep the last row for each time series.
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
