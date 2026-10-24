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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{DfRecordBatch, RecordBatch};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use snafu::ResultExt;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::AbortHandle;

use crate::cache::CacheStrategy;
use crate::error::{JoinSnafu, Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_scan::SeriesBatch;

/// All kinds of [`Batch`]es to produce in scanner.
pub enum ScanBatch {
    Series(SeriesBatch),
    RecordBatch(DfRecordBatch),
}

pub type ScanBatchStream = BoxStream<'static, Result<ScanBatch>>;

/// A stream that takes [`ScanBatch`]es and produces (converts them to) [`RecordBatch`]es.
pub(crate) struct ConvertBatchStream {
    inner: ScanBatchStream,
    projection_mapper: Arc<FlatProjectionMapper>,
    cache_strategy: CacheStrategy,
    partition_metrics: PartitionMetrics,
    pending: VecDeque<RecordBatch>,
}

impl ConvertBatchStream {
    pub(crate) fn new(
        inner: ScanBatchStream,
        projection_mapper: Arc<FlatProjectionMapper>,
        cache_strategy: CacheStrategy,
        partition_metrics: PartitionMetrics,
    ) -> Self {
        Self {
            inner,
            projection_mapper,
            cache_strategy,
            partition_metrics,
            pending: VecDeque::new(),
        }
    }

    fn convert(&mut self, batch: ScanBatch) -> common_recordbatch::error::Result<RecordBatch> {
        match batch {
            ScanBatch::Series(series) => {
                debug_assert!(
                    self.pending.is_empty(),
                    "ConvertBatchStream should not convert a new SeriesBatch when pending batches exist"
                );

                let SeriesBatch::Flat(flat_batch) = series;
                // Safety: Only flat format returns this batch.
                for batch in flat_batch.batches {
                    self.pending.push_back(
                        self.projection_mapper
                            .convert(&batch, &self.cache_strategy)?,
                    );
                }

                let output_schema = self.projection_mapper.output_schema();
                Ok(self
                    .pending
                    .pop_front()
                    .unwrap_or_else(|| RecordBatch::new_empty(output_schema)))
            }
            ScanBatch::RecordBatch(df_record_batch) => {
                // Safety: Only flat format returns this batch.
                self.projection_mapper
                    .convert(&df_record_batch, &self.cache_strategy)
            }
        }
    }
}

impl Stream for ConvertBatchStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.pending.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }

        let batch = futures::ready!(self.inner.poll_next_unpin(cx));
        let Some(batch) = batch else {
            return Poll::Ready(None);
        };

        let record_batch = match batch {
            Ok(batch) => {
                let start = Instant::now();
                let record_batch = self.convert(batch);
                self.partition_metrics
                    .inc_convert_batch_cost(start.elapsed());
                record_batch
            }
            Err(e) => Err(BoxedError::new(e)).context(ExternalSnafu),
        };
        Poll::Ready(Some(record_batch))
    }
}

/// Polls `stream` on its own compaction runtime task, buffering up to
/// `capacity` batches ahead of the consumer. While polling, the task holds a
/// permit of `semaphore` if one is given.
///
/// The channel also closes if the task panics, so the returned stream checks
/// how the task ended before reporting the end of the input. Dropping the
/// returned stream aborts the task.
pub(crate) fn spawn_compact_stream(
    mut stream: BoxedRecordBatchStream,
    capacity: usize,
    semaphore: Option<Arc<Semaphore>>,
) -> BoxedRecordBatchStream {
    let (sender, mut receiver) = mpsc::channel(capacity);
    let task = common_runtime::spawn_compact(async move {
        loop {
            // Releases the permit before sending, so a full channel never holds it.
            let permit = match &semaphore {
                Some(semaphore) => match semaphore.acquire().await {
                    Ok(permit) => Some(permit),
                    Err(e) => {
                        let reason = format!("Failed to acquire a stream permit: {e}");
                        let _ = sender.send(UnexpectedSnafu { reason }.fail()).await;
                        break;
                    }
                },
                None => None,
            };
            let next = stream.next().await;
            drop(permit);
            let Some(result) = next else {
                break;
            };
            // Stops after forwarding the first error or once the consumer is gone.
            let failed = result.is_err();
            if sender.send(result).await.is_err() || failed {
                break;
            }
        }
    });
    // Own the guard before the lazy stream can be dropped without being polled.
    let abort = AbortOnDrop(task.abort_handle());
    Box::pin(try_stream! {
        let _abort = abort;
        while let Some(batch) = receiver.recv().await {
            yield batch?;
        }
        task.await.context(JoinSnafu)?;
    })
}

/// Aborts a task when its consumer is dropped before the end.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use datatypes::arrow::array::{ArrayRef, AsArray, Int64Array};
    use datatypes::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;
    use crate::error::{Error, InvalidRecordBatchSnafu};
    use crate::read::flat_merge::FlatMergeReader;

    /// Keeps the producer pending and exposes when its source is polled and dropped.
    struct PendingStream {
        polled: Option<oneshot::Sender<()>>,
        dropped: Option<oneshot::Sender<()>>,
    }

    impl PendingStream {
        fn new() -> (Self, oneshot::Receiver<()>, oneshot::Receiver<()>) {
            let (polled_tx, polled_rx) = oneshot::channel();
            let (dropped_tx, dropped_rx) = oneshot::channel();
            (
                Self {
                    polled: Some(polled_tx),
                    dropped: Some(dropped_tx),
                },
                polled_rx,
                dropped_rx,
            )
        }
    }

    impl Stream for PendingStream {
        type Item = Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(polled) = self.polled.take() {
                let _ = polled.send(());
            }
            Poll::Pending
        }
    }

    impl Drop for PendingStream {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    async fn wait_for_signal(signal: oneshot::Receiver<()>) {
        // Notifications establish ordering; the timeout only bounds a broken test.
        timeout(Duration::from_secs(10), signal)
            .await
            .expect("timed out waiting for the source")
            .expect("source dropped without sending the signal");
    }

    fn batch(value: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let column = Arc::new(Int64Array::from(vec![value])) as ArrayRef;
        RecordBatch::try_new(schema, vec![column]).unwrap()
    }

    fn values_stream(values: std::ops::Range<i64>) -> BoxedRecordBatchStream {
        Box::pin(futures::stream::iter(values.map(|value| Ok(batch(value)))))
    }

    /// Drains `stream`, returning the values read before it ended or failed.
    async fn drain(mut stream: BoxedRecordBatchStream) -> (Vec<i64>, Option<Error>) {
        let mut values = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(batch) => values.push(batch.column(0).as_primitive::<Int64Type>().value(0)),
                Err(e) => return (values, Some(e)),
            }
        }
        (values, None)
    }

    #[tokio::test]
    async fn test_spawn_compact_stream_forwards_batches_in_order() {
        let (values, error) = drain(spawn_compact_stream(values_stream(0..5), 2, None)).await;

        assert_eq!(vec![0, 1, 2, 3, 4], values);
        assert!(error.is_none());
    }

    #[tokio::test]
    async fn test_spawn_compact_stream_stops_at_first_error() {
        let stream = futures::stream::iter(vec![
            Ok(batch(0)),
            InvalidRecordBatchSnafu { reason: "boom" }.fail(),
            Ok(batch(2)),
        ]);

        let (values, error) = drain(spawn_compact_stream(Box::pin(stream), 2, None)).await;

        assert_eq!(vec![0], values);
        assert!(
            matches!(error, Some(Error::InvalidRecordBatch { .. })),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_spawn_compact_stream_reports_panic_instead_of_end() {
        let mut polled = false;
        let stream = futures::stream::poll_fn(move |_| {
            if std::mem::replace(&mut polled, true) {
                panic!("source panicked");
            }
            Poll::Ready(Some(Ok(batch(0))))
        });

        let (values, error) = drain(spawn_compact_stream(Box::pin(stream), 2, None)).await;

        // A truncated input must never look like a complete one.
        assert_eq!(vec![0], values);
        assert!(matches!(error, Some(Error::Join { .. })), "{error:?}");
    }

    #[rstest::rstest]
    #[case::before_first_poll(false)]
    #[case::after_first_poll(true)]
    #[tokio::test]
    async fn test_dropped_consumer_cancels_pending_source(#[case] poll_consumer: bool) {
        let (source, polled, dropped) = PendingStream::new();
        let semaphore = Arc::new(Semaphore::new(1));
        let mut consumer = spawn_compact_stream(Box::pin(source), 1, Some(semaphore.clone()));
        wait_for_signal(polled).await;
        assert_eq!(0, semaphore.available_permits());
        if poll_consumer {
            assert!(futures::poll!(consumer.next()).is_pending());
        }

        // A guard created inside the lazy stream misses the before-first-poll case.
        drop(consumer);
        wait_for_signal(dropped).await;
        let _permit = timeout(Duration::from_secs(10), semaphore.acquire())
            .await
            .expect("producer retained its permit")
            .unwrap();
    }

    #[tokio::test]
    async fn test_merge_initialization_error_cancels_unpolled_source() {
        let (source, polled, dropped) = PendingStream::new();
        let consumer = spawn_compact_stream(Box::pin(source), 1, None);
        wait_for_signal(polled).await;
        let error_stream = Box::pin(futures::stream::iter(vec![
            InvalidRecordBatchSnafu { reason: "boom" }.fail(),
        ]));
        let sources = vec![spawn_compact_stream(error_stream, 1, None), consumer];

        let result = FlatMergeReader::new(batch(0).schema(), sources, 1024, None).await;

        assert!(matches!(result, Err(Error::InvalidRecordBatch { .. })));
        wait_for_signal(dropped).await;
    }

    #[tokio::test]
    async fn test_cancelled_merge_initialization_drops_all_sources() {
        let (first, first_polled, first_dropped) = PendingStream::new();
        let (second, second_polled, second_dropped) = PendingStream::new();
        let sources = vec![
            spawn_compact_stream(Box::pin(first), 1, None),
            spawn_compact_stream(Box::pin(second), 1, None),
        ];
        wait_for_signal(first_polled).await;
        wait_for_signal(second_polled).await;
        let mut initialization =
            Box::pin(FlatMergeReader::new(batch(0).schema(), sources, 1024, None));
        assert!(futures::poll!(&mut initialization).is_pending());

        // Only the first consumer has been polled by the sequential initialization.
        drop(initialization);

        wait_for_signal(first_dropped).await;
        wait_for_signal(second_dropped).await;
    }

    #[tokio::test]
    async fn test_spawn_compact_streams_share_semaphore() {
        let semaphore = Arc::new(Semaphore::new(1));
        let streams: Vec<_> = (0..3)
            .map(|i| {
                spawn_compact_stream(
                    values_stream(i * 10..i * 10 + 3),
                    1,
                    Some(semaphore.clone()),
                )
            })
            .collect();

        for (i, stream) in streams.into_iter().enumerate() {
            let start = i as i64 * 10;
            let (values, error) = drain(stream).await;
            assert_eq!((start..start + 3).collect::<Vec<_>>(), values);
            assert!(error.is_none());
        }
    }
}
