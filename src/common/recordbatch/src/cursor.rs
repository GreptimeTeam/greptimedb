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

use futures::StreamExt;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::recordbatch::merge_record_batches;
use crate::{RecordBatch, SendableRecordBatchStream};

struct Inner {
    stream: SendableRecordBatchStream,
    current_row_index: usize,
    current_batch: Option<RecordBatch>,
    total_rows_in_current_batch: usize,
}

/// A cursor on RecordBatchStream that fetches data batch by batch
pub struct RecordBatchStreamCursor {
    inner: Mutex<Inner>,
}

impl RecordBatchStreamCursor {
    pub fn new(stream: SendableRecordBatchStream) -> RecordBatchStreamCursor {
        Self {
            inner: Mutex::new(Inner {
                stream,
                current_row_index: 0,
                current_batch: None,
                total_rows_in_current_batch: 0,
            }),
        }
    }

    /// Take `size` of row from the `RecordBatchStream` and create a new
    /// `RecordBatch` for these rows.
    pub async fn take(&self, size: usize) -> Result<RecordBatch> {
        let mut remaining_rows_to_take = size;
        let mut accumulated_rows = Vec::new();

        let mut inner = self.inner.lock().await;

        while remaining_rows_to_take > 0 {
            // Ensure we have a current batch or fetch the next one
            if inner.current_batch.is_none()
                || inner.current_row_index >= inner.total_rows_in_current_batch
            {
                match inner.stream.next().await {
                    Some(Ok(batch)) => {
                        inner.total_rows_in_current_batch = batch.num_rows();
                        inner.current_batch = Some(batch);
                        inner.current_row_index = 0;
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        // Stream is exhausted
                        break;
                    }
                }
            }

            // If we still have no batch after attempting to fetch
            let current_batch = match &inner.current_batch {
                Some(batch) => batch,
                None => break,
            };

            // Calculate how many rows we can take from this batch
            let rows_to_take_from_batch = remaining_rows_to_take
                .min(inner.total_rows_in_current_batch - inner.current_row_index);

            // Slice the current batch to get the desired rows
            let taken_batch =
                current_batch.slice(inner.current_row_index, rows_to_take_from_batch)?;

            // Add the taken batch to accumulated rows
            accumulated_rows.push(taken_batch);

            // Update cursor and remaining rows
            inner.current_row_index += rows_to_take_from_batch;
            remaining_rows_to_take -= rows_to_take_from_batch;
        }

        // If no rows were accumulated, return empty
        if accumulated_rows.is_empty() {
            return Ok(RecordBatch::new_empty(inner.stream.schema()));
        }

        // If only one batch was accumulated, return it directly
        if accumulated_rows.len() == 1 {
            return Ok(accumulated_rows.remove(0));
        }

        // Merge multiple batches
        merge_record_batches(inner.stream.schema(), &accumulated_rows)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::StringVector;

    use super::*;
    use crate::RecordBatches;

    #[tokio::test]
    async fn test_cursor() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::string_datatype(),
            false,
        )]));

        let rbs = RecordBatches::try_from_columns(
            schema.clone(),
            vec![Arc::new(StringVector::from(vec!["hello", "world"])) as _],
        )
        .unwrap();

        let cursor = RecordBatchStreamCursor::new(rbs.as_stream());
        let result_rb = cursor.take(1).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 1);

        let result_rb = cursor.take(1).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 1);

        let result_rb = cursor.take(1).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 0);

        let rb = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(StringVector::from(vec!["hello", "world"])) as _],
        )
        .unwrap();
        let rbs2 =
            RecordBatches::try_new(schema.clone(), vec![rb.clone(), rb.clone(), rb]).unwrap();
        let cursor = RecordBatchStreamCursor::new(rbs2.as_stream());
        let result_rb = cursor.take(3).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 3);
        let result_rb = cursor.take(2).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 2);
        let result_rb = cursor.take(2).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 1);
        let result_rb = cursor.take(2).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 0);

        let rb = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(StringVector::from(vec!["hello", "world"])) as _],
        )
        .unwrap();
        let rbs3 =
            RecordBatches::try_new(schema.clone(), vec![rb.clone(), rb.clone(), rb]).unwrap();
        let cursor = RecordBatchStreamCursor::new(rbs3.as_stream());
        let result_rb = cursor.take(10).await.expect("take from cursor failed");
        assert_eq!(result_rb.num_rows(), 6);
    }
}
