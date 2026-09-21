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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use operator::error::Error;
use session::context::QueryContextRef;
use table::metadata::TableInfoRef;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

/// One complete input and its completion/admission ownership.
pub(in crate::batcher::table) struct PendingBatch {
    pub table_info: TableInfoRef,
    pub batch: RecordBatch,
    pub ctx: QueryContextRef,
    pub response_tx: oneshot::Sender<Result<(), Arc<Error>>>,
    pub _permit: Arc<OwnedSemaphorePermit>,
}

pub(in crate::batcher::table) fn notify_batches(
    batches: Vec<PendingBatch>,
    result: Result<(), Arc<Error>>,
) {
    for batch in batches {
        let _ = batch.response_tx.send(result.clone());
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use operator::error::UnexpectedSnafu;
    use operator::test_util::new_test_table_info;
    use session::context::QueryContext;
    use tokio::sync::Semaphore;

    use crate::batcher::table::pending_batch::*;

    #[tokio::test]
    async fn test_completion_fans_out_and_releases_admission() {
        for fail in [false, true] {
            let semaphore = Arc::new(Semaphore::new(3));
            let mut batches = Vec::new();
            let mut receivers = Vec::new();
            for value in 0..3 {
                let (response_tx, response_rx) = oneshot::channel();
                let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
                batches.push(PendingBatch {
                    table_info: Arc::new(new_test_table_info(1, "t", [0].into_iter())),
                    batch: RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int32Array::from(vec![value]))],
                    )
                    .unwrap(),
                    ctx: QueryContext::arc(),
                    response_tx,
                    _permit: Arc::new(semaphore.clone().acquire_owned().await.unwrap()),
                });
                receivers.push(response_rx);
            }
            assert_eq!(0, semaphore.available_permits());
            // A cancelled waiter must not prevent delivery to the others.
            drop(receivers.pop());
            let error = Arc::new(
                UnexpectedSnafu {
                    violated: "test flush failure".to_string(),
                }
                .build(),
            );
            notify_batches(batches, if fail { Err(error.clone()) } else { Ok(()) });
            assert_eq!(3, semaphore.available_permits());
            for receiver in receivers {
                match receiver.await.unwrap() {
                    Ok(()) => assert!(!fail),
                    Err(actual) => {
                        assert!(fail);
                        assert!(Arc::ptr_eq(&error, &actual));
                    }
                }
            }
        }
    }
}
