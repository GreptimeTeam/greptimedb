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

use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{DfRecordBatch, RecordBatch};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use snafu::ResultExt;

use crate::cache::CacheStrategy;
use crate::error::Result;
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
    #[allow(dead_code)]
    cache_strategy: CacheStrategy,
    partition_metrics: PartitionMetrics,
    pending: VecDeque<RecordBatch>,
    /// Flat format batches of the current [`SeriesBatch`] that are not converted yet.
    ///
    /// Conversion is bounded: a [`SeriesBatch`] may contain many batches and we
    /// convert only one at a time to avoid buffering all of them in `pending`.
    pending_flat_batches: VecDeque<DfRecordBatch>,
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
            pending_flat_batches: VecDeque::new(),
        }
    }

    fn convert(&mut self, batch: ScanBatch) -> common_recordbatch::error::Result<RecordBatch> {
        match batch {
            ScanBatch::Series(series) => {
                debug_assert!(
                    self.pending.is_empty() && self.pending_flat_batches.is_empty(),
                    "ConvertBatchStream should not convert a new SeriesBatch when pending batches exist"
                );

                let SeriesBatch::Flat(flat_batch) = series;
                // Safety: Only flat format returns this batch.
                // Keep the pending queue bounded: enqueue the flat batches and
                // convert only one of them per call.
                self.pending_flat_batches = flat_batch.batches.into_iter().collect();
                self.convert_next_pending()
            }
            ScanBatch::RecordBatch(df_record_batch) => {
                // Safety: Only flat format returns this batch.
                self.projection_mapper
                    .convert(&df_record_batch, &self.cache_strategy)
            }
        }
    }

    /// Converts and returns the next unconverted flat batch, or an empty batch if
    /// there are no more flat batches to convert.
    fn convert_next_pending(&mut self) -> common_recordbatch::error::Result<RecordBatch> {
        let Some(batch) = self.pending_flat_batches.pop_front() else {
            let output_schema = self.projection_mapper.output_schema();
            return Ok(RecordBatch::new_empty(output_schema));
        };

        self.projection_mapper.convert(&batch, &self.cache_strategy)
    }
}

impl Stream for ConvertBatchStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.pending.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }

        // Convert the remaining flat batches of the current SeriesBatch (if any)
        // before pulling the next batch from the inner stream. This keeps the
        // conversion bounded: only one flat batch is converted per poll.
        if !self.pending_flat_batches.is_empty() {
            let start = Instant::now();
            let record_batch = self.convert_next_pending();
            self.partition_metrics
                .inc_convert_batch_cost(start.elapsed());
            return Poll::Ready(Some(record_batch));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datatypes::arrow::array::UInt64Array;
    use futures::StreamExt;
    use store_api::storage::RegionId;

    use super::*;
    use crate::read::series_scan::FlatSeriesBatch;
    use crate::test_util::sst_util::{new_record_batch_by_range, sst_region_metadata};

    /// Creates a `ScanBatch::Series` in flat format containing `num_batches` record
    /// batches. The `i`-th batch has `field_0` values `[i * 10, i * 10 + 1]`.
    fn new_flat_series_scan_batch(num_batches: usize) -> ScanBatch {
        let batches = (0..num_batches)
            .map(|i| new_record_batch_by_range(&["series", "series"], i * 10, i * 10 + 2))
            .collect::<Vec<_>>();
        ScanBatch::Series(SeriesBatch::Flat(FlatSeriesBatch {
            batches: batches.into(),
        }))
    }

    /// Creates a `ConvertBatchStream` with the given inner stream and a mapper
    /// that projects all columns. The mapper's input schema matches the flat
    /// batches built by `new_record_batch_by_range`:
    /// `[tag_0, tag_1, field_0, ts, __primary_key, __sequence, __op_type]`.
    fn new_convert_batch_stream(inner: ScanBatchStream) -> ConvertBatchStream {
        let metadata = Arc::new(sst_region_metadata());
        let mapper = Arc::new(FlatProjectionMapper::all(&metadata).unwrap());
        let metrics = PartitionMetrics::new(
            RegionId::new(1024, 0),
            0,
            "test",
            std::time::Instant::now(),
            false,
            &ExecutionPlanMetricsSet::new(),
        );
        ConvertBatchStream::new(inner, mapper, CacheStrategy::Disabled, metrics)
    }

    /// Polls the stream once with a no-op waker.
    ///
    /// The stream never yields `Pending` because its inner stream is a plain
    /// iterator-based stream.
    fn poll_next_batch(
        stream: &mut ConvertBatchStream,
    ) -> Option<common_recordbatch::error::Result<RecordBatch>> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match stream.poll_next_unpin(&mut cx) {
            Poll::Ready(item) => item,
            Poll::Pending => panic!("ConvertBatchStream should never return Pending"),
        }
    }

    /// Extracts the `field_0` values of a converted record batch.
    ///
    /// The mapper projects all metadata columns, so the output schema is
    /// `[tag_0, tag_1, field_0, ts]` and `field_0` is column 2.
    fn field_0_values(batch: &RecordBatch) -> Vec<u64> {
        let array = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        array.values().to_vec()
    }

    #[test]
    fn convert_stream_bounds_pending_batches() {
        let mut stream = new_convert_batch_stream(
            futures::stream::iter(Vec::<crate::error::Result<ScanBatch>>::new()).boxed(),
        );

        // Converts a single FlatSeriesBatch that contains 4 batches.
        let first = stream.convert(new_flat_series_scan_batch(4)).unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(field_0_values(&first), vec![0, 1]);

        // Only the first batch is converted and returned: the converted pending
        // queue stays bounded. With the eager behavior it would hold 3 converted
        // batches here.
        assert!(stream.pending.len() <= 1);
        // The remaining batches are kept unconverted and converted lazily.
        assert_eq!(stream.pending_flat_batches.len(), 3);

        // Subsequent polls convert and yield one batch at a time, in order.
        for values in [vec![10, 11], vec![20, 21], vec![30, 31]] {
            let batch = poll_next_batch(&mut stream)
                .expect("expected a batch")
                .unwrap();
            assert_eq!(field_0_values(&batch), values);
        }

        // All batches are drained and the inner stream is exhausted.
        assert!(poll_next_batch(&mut stream).is_none());
    }

    #[test]
    fn convert_stream_drains_all_batches_in_order() {
        // The inner stream yields one Series batch with 3 flat batches followed by
        // a plain RecordBatch.
        let series = new_flat_series_scan_batch(3);
        let record_batch = new_record_batch_by_range(&["series", "series"], 100, 102);
        let inner: ScanBatchStream =
            futures::stream::iter(vec![Ok(series), Ok(ScanBatch::RecordBatch(record_batch))])
                .boxed();
        let mut stream = new_convert_batch_stream(inner);

        let mut collected = Vec::new();
        while let Some(batch) = poll_next_batch(&mut stream) {
            collected.push(field_0_values(&batch.unwrap()));
        }

        assert_eq!(
            collected,
            vec![vec![0, 1], vec![10, 11], vec![20, 21], vec![100, 101],]
        );
    }
}
