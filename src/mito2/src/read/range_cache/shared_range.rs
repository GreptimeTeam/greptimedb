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

//! A single range scan with independently advancing readers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use datatypes::arrow::record_batch::RecordBatch;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryStreamExt};
use tokio::sync::{Mutex, OwnedSemaphorePermit};

use crate::cache::RangeResultMemoryLimiter;
use crate::error::Error;
use crate::read::BoxedRecordBatchStream;
use crate::read::range_cache::{self, CachedBatchSlice};

type SharedBatch = Result<Option<RecordBatch>, Arc<Error>>;
type NextBatch = Shared<BoxFuture<'static, SharedBatch>>;

/// The registry must hold only a weak reference: readers own the scan lifetime.
/// The source is polled on demand, so an unused range does not run ahead.
/// Cache admission stops when retention reservations run out. Unread batches
/// remain owned by active readers; this is not a hard limit on their backlog.
pub(crate) struct SharedRange {
    source: Arc<Mutex<Option<BoxedRecordBatchStream>>>,
    buffer: Mutex<RangeBuffer>,
    limiter: Arc<RangeResultMemoryLimiter>,
}

/// A reader's cursor refers to original batch boundaries, including empty batches.
pub(crate) struct RangeCursor {
    batch: Arc<AtomicUsize>,
    reused: bool,
}

pub(crate) enum RangeRead {
    Batch(RecordBatch),
    Finished,
    Failed(Arc<Error>),
}

impl SharedRange {
    /// Wraps one source stream and its full-result retention budget.
    pub(crate) fn new(
        source: BoxedRecordBatchStream,
        limiter: Arc<RangeResultMemoryLimiter>,
    ) -> Arc<Self> {
        Arc::new(Self {
            source: Arc::new(Mutex::new(Some(source))),
            buffer: Mutex::new(RangeBuffer::default()),
            limiter,
        })
    }

    /// Register before consuming any data. Once full-result retention stops,
    /// new queries use their own ordinary scan rather than joining mid-range.
    pub(crate) async fn subscribe(&self) -> Option<RangeCursor> {
        let mut buffer = self.buffer.lock().await;
        if buffer.streaming_only || matches!(buffer.finished, Some(Err(_))) {
            return None;
        }
        let batch = Arc::new(AtomicUsize::new(0));
        let reused = !buffer.readers.is_empty();
        buffer.readers.push(Arc::downgrade(&batch));
        Some(RangeCursor { batch, reused })
    }

    /// Exactly one reader hands the complete result to the existing compactor.
    pub(crate) async fn take_complete_batches(&self) -> Option<Vec<CachedBatchSlice>> {
        let mut buffer = self.buffer.lock().await;
        if buffer.streaming_only || buffer.published || !matches!(buffer.finished, Some(Ok(()))) {
            return None;
        }
        buffer.published = true;
        let mut batches: Vec<CachedBatchSlice> = Vec::new();
        let mut previous = None;
        for entry in &buffer.batches {
            let allocation = Arc::as_ptr(&entry.allocation);
            if previous == Some(allocation) {
                if let Some(last) = batches.last_mut() {
                    last.slice_lengths.push(entry.batch.num_rows());
                }
            } else {
                batches.push(CachedBatchSlice {
                    batch: entry.allocation.batch.clone(),
                    slice_lengths: vec![entry.batch.num_rows()],
                });
                previous = Some(allocation);
            }
        }
        Some(batches)
    }

    /// Returns the next batch for this reader, polling the shared source as needed.
    pub(crate) async fn read(&self, cursor: &mut RangeCursor) -> RangeRead {
        loop {
            let (index, next) = {
                let mut buffer = self.buffer.lock().await;
                buffer.release_consumed();
                let position = cursor.batch.load(Ordering::Relaxed);
                if let Some(batch) = buffer.batches.get(position - buffer.first_batch) {
                    let batch = batch.batch.clone();
                    cursor.batch.store(position + 1, Ordering::Relaxed);
                    if cursor.reused {
                        crate::metrics::RANGE_CACHE_EVENTS
                            .with_label_values(&["reused_rows"])
                            .inc_by(batch.num_rows() as u64);
                        crate::metrics::RANGE_CACHE_EVENTS
                            .with_label_values(&["reused_batches"])
                            .inc();
                    }
                    buffer.release_consumed();
                    return RangeRead::Batch(batch);
                }
                if let Some(result) = &buffer.finished {
                    return match result {
                        Ok(()) => RangeRead::Finished,
                        Err(error) => RangeRead::Failed(error.clone()),
                    };
                }
                let index = buffer.first_batch + buffer.batches.len();
                let next = buffer
                    .next
                    .get_or_insert_with(|| {
                        let source = self.source.clone();
                        async move {
                            let mut source = source.lock().await;
                            let result = match source.as_mut() {
                                Some(stream) => stream.try_next().await.map_err(Arc::new),
                                None => Ok(None),
                            };
                            if !matches!(&result, Ok(Some(_))) {
                                // Release file readers as soon as the source terminates.
                                *source = None;
                            }
                            result
                        }
                        .boxed()
                        .shared()
                    })
                    .clone();
                (index, next)
            };

            // No buffer lock is held across source I/O. A slower reader can still
            // consume buffered batches while another reader awaits the next one.
            let result = next.await;
            let mut buffer = self.buffer.lock().await;
            if index != buffer.first_batch + buffer.batches.len() || buffer.finished.is_some() {
                continue;
            }
            buffer.next = None;
            match result {
                Ok(Some(batch)) => {
                    let bytes = batch.get_array_memory_size();
                    let permit = if buffer.streaming_only {
                        None
                    } else {
                        self.limiter.try_acquire(bytes)
                    };
                    for (event, count) in [
                        ("built_rows", batch.num_rows()),
                        ("built_batches", 1),
                        ("built_reference_bytes", bytes),
                    ] {
                        crate::metrics::RANGE_CACHE_EVENTS
                            .with_label_values(&[event])
                            .inc_by(count as u64);
                    }
                    buffer.retained_bytes += bytes;
                    buffer.peak_retained_bytes =
                        buffer.peak_retained_bytes.max(buffer.retained_bytes);
                    crate::metrics::RANGE_CACHE_STAGING_BYTES.add(bytes as i64);
                    if buffer.streaming_only {
                        crate::metrics::RANGE_CACHE_STREAMING_BYTES.add(bytes as i64);
                    }
                    let missing_permit = permit.is_none();
                    let allocation = Arc::new(BufferedAllocation {
                        batch: batch.clone(),
                        _permit: permit,
                    });
                    buffer
                        .batches
                        .push_back(BufferedBatch { batch, allocation });
                    if !buffer.streaming_only {
                        buffer.pending_batches += 1;
                        buffer.pending_rows +=
                            buffer.batches.back().map_or(0, |b| b.batch.num_rows());
                        buffer.pending_bytes += bytes;
                        let compact = missing_permit
                            || range_cache::should_compact_batches(
                                buffer.pending_batches,
                                buffer.pending_rows,
                                buffer.pending_bytes,
                            );
                        let retained = !compact || buffer.compact_pending(&self.limiter);
                        if !retained {
                            crate::metrics::RANGE_CACHE_EVENTS
                                .with_label_values(&["budget_fallback"])
                                .inc();
                            buffer.streaming_only = true;
                            crate::metrics::RANGE_CACHE_STREAMING_BYTES
                                .add(buffer.retained_bytes as i64);
                        }
                    }
                    buffer.release_consumed();
                }
                Ok(None) => buffer.finished = Some(Ok(())),
                Err(error) => buffer.finished = Some(Err(error)),
            }
        }
    }
}

#[derive(Default)]
struct RangeBuffer {
    batches: VecDeque<BufferedBatch>,
    first_batch: usize,
    pending_batches: usize,
    pending_rows: usize,
    pending_bytes: usize,
    retained_bytes: usize,
    peak_retained_bytes: usize,
    peak_streaming_batches: usize,
    next: Option<NextBatch>,
    finished: Option<Result<(), Arc<Error>>>,
    streaming_only: bool,
    published: bool,
    readers: Vec<Weak<AtomicUsize>>,
}

impl RangeBuffer {
    /// Detaches the pending slices before deciding whether to stop cache admission.
    /// Each compacted allocation owns one reservation, shared by its batch views.
    fn compact_pending(&mut self, limiter: &RangeResultMemoryLimiter) -> bool {
        let start = self.batches.len() - self.pending_batches;
        let input = self
            .batches
            .iter()
            .skip(start)
            .map(|entry| entry.batch.clone())
            .collect();
        let compacted = match range_cache::compact_record_batches(input) {
            Ok(compacted) => compacted,
            Err(error) => {
                common_telemetry::warn!(error; "Failed to compact shared range buffer");
                return false;
            }
        };
        let old_bytes = self.pending_bytes;
        let bytes = compacted.batch.get_array_memory_size();
        // Release the old reservations before the nonblocking replacement attempt.
        // Reader-owned batches retain their own Arrow references independently.
        self.batches.truncate(start);
        let permit = limiter.try_acquire(bytes);
        let retained = permit.is_some();
        let allocation = Arc::new(BufferedAllocation {
            batch: compacted.batch,
            _permit: permit,
        });
        let mut offset = 0;
        for len in compacted.slice_lengths {
            self.batches.push_back(BufferedBatch {
                batch: allocation.batch.slice(offset, len),
                allocation: allocation.clone(),
            });
            offset += len;
        }
        self.retained_bytes = self.retained_bytes - old_bytes + bytes;
        crate::metrics::RANGE_CACHE_STAGING_BYTES.add(bytes as i64 - old_bytes as i64);
        self.pending_batches = 0;
        self.pending_rows = 0;
        self.pending_bytes = 0;
        retained
    }

    fn release_consumed(&mut self) {
        if !self.streaming_only {
            return;
        }
        let mut consumed = self.first_batch + self.batches.len();
        self.readers.retain(|reader| {
            if let Some(position) = reader.upgrade() {
                consumed = consumed.min(position.load(Ordering::Relaxed));
                true
            } else {
                false
            }
        });
        while self.first_batch < consumed {
            if let Some(batch) = self.batches.pop_front()
                && Arc::strong_count(&batch.allocation) == 1
            {
                let bytes = batch.allocation.batch.get_array_memory_size();
                self.retained_bytes -= bytes;
                crate::metrics::RANGE_CACHE_STAGING_BYTES.sub(bytes as i64);
                crate::metrics::RANGE_CACHE_STREAMING_BYTES.sub(bytes as i64);
            }
            self.first_batch += 1;
        }
        self.peak_streaming_batches = self.peak_streaming_batches.max(self.batches.len());
    }
}

impl Drop for RangeBuffer {
    fn drop(&mut self) {
        crate::metrics::RANGE_CACHE_STAGING_BYTES.sub(self.retained_bytes as i64);
        if self.streaming_only {
            crate::metrics::RANGE_CACHE_STREAMING_BYTES.sub(self.retained_bytes as i64);
        }
        crate::metrics::RANGE_CACHE_BUFFER_PEAK_BYTES.observe(self.peak_retained_bytes as f64);
        crate::metrics::RANGE_CACHE_STREAMING_PEAK_BATCHES
            .observe(self.peak_streaming_batches as f64);
    }
}

struct BufferedBatch {
    batch: RecordBatch,
    allocation: Arc<BufferedAllocation>,
}

struct BufferedAllocation {
    batch: RecordBatch,
    _permit: Option<OwnedSemaphorePermit>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datatypes::arrow::array::{ArrayRef, Int64Array};
    use futures::stream;
    use tokio::sync::Notify;

    use super::*;

    fn batch(value: i64) -> RecordBatch {
        let values = Arc::new(Int64Array::from(vec![value])) as ArrayRef;
        RecordBatch::try_from_iter([("value", values)]).unwrap()
    }

    fn value(read: RangeRead) -> i64 {
        match read {
            RangeRead::Batch(batch) => batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            _ => panic!("expected a batch"),
        }
    }

    #[tokio::test]
    async fn pending_source_does_not_block_buffered_readers() {
        let produced = Arc::new(AtomicUsize::new(0));
        let counter = produced.clone();
        let gate = Arc::new(Notify::new());
        let ready = gate.clone();
        let source = async_stream::try_stream! {
            counter.fetch_add(1, Ordering::Relaxed);
            yield batch(1);
            ready.notified().await;
            counter.fetch_add(1, Ordering::Relaxed);
            yield batch(2);
        };
        let limiter = Arc::new(RangeResultMemoryLimiter::new(4096, 1));
        let shared = SharedRange::new(Box::pin(source), limiter.clone());
        let mut first = shared.subscribe().await.unwrap();
        let mut second = shared.subscribe().await.unwrap();
        assert_eq!(value(shared.read(&mut first).await), 1);
        {
            let next = shared.read(&mut first);
            tokio::pin!(next);
            assert!(futures::poll!(&mut next).is_pending());
            assert_eq!(value(shared.read(&mut second).await), 1);
        }
        drop(first);
        gate.notify_one();
        assert_eq!(value(shared.read(&mut second).await), 2);
        assert!(matches!(
            shared.read(&mut second).await,
            RangeRead::Finished
        ));
        assert_eq!(produced.load(Ordering::Relaxed), 2);
        drop(shared);
        assert_eq!(limiter.available_permits(), 4096);
    }

    #[tokio::test]
    async fn slow_reader_backlog_grows_and_is_reclaimed() {
        let batches = (0..256).map(batch).collect::<Vec<_>>();
        let expected_bytes = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let shared = SharedRange::new(
            Box::pin(stream::iter(batches.into_iter().map(Ok))),
            Arc::new(RangeResultMemoryLimiter::new(1, 1)),
        );
        let mut fast = shared.subscribe().await.unwrap();
        let mut slow = shared.subscribe().await.unwrap();
        // A paused subscriber must not stop the other reader or discard its data.
        for expected in 0..256 {
            assert_eq!(value(shared.read(&mut fast).await), expected);
        }
        assert!(matches!(shared.read(&mut fast).await, RangeRead::Finished));
        {
            let buffer = shared.buffer.lock().await;
            assert!(buffer.streaming_only);
            assert_eq!(buffer.retained_bytes, expected_bytes);
            assert_eq!(buffer.peak_retained_bytes, expected_bytes);
            assert_eq!(buffer.peak_streaming_batches, 256);
        }
        assert!(shared.subscribe().await.is_none());
        for expected in 0..256 {
            assert_eq!(value(shared.read(&mut slow).await), expected);
        }
        assert!(matches!(shared.read(&mut slow).await, RangeRead::Finished));
        let buffer = shared.buffer.lock().await;
        assert!(buffer.batches.is_empty());
        assert_eq!(buffer.retained_bytes, 0);
    }
}
