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
