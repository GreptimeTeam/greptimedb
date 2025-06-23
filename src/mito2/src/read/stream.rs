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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use common_error::ext::BoxedError;
use common_recordbatch::error::{ArrowComputeSnafu, ExternalSnafu};
use common_recordbatch::{DfRecordBatch, RecordBatch};
use datatypes::compute;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use snafu::ResultExt;

use crate::cache::CacheStrategy;
use crate::error::Result;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_scan::SeriesBatch;
use crate::read::Batch;

/// All kinds of [`Batch`]es to produce in scanner.
pub enum ScanBatch {
    Normal(Batch),
    Series(SeriesBatch),
}

pub type ScanBatchStream = BoxStream<'static, Result<ScanBatch>>;

/// A stream that takes [`ScanBatch`]es and produces (converts them to) [`RecordBatch`]es.
pub(crate) struct ConvertBatchStream {
    inner: ScanBatchStream,
    projection_mapper: Arc<ProjectionMapper>,
    cache_strategy: CacheStrategy,
    partition_metrics: PartitionMetrics,
    buffer: Vec<DfRecordBatch>,
}

impl ConvertBatchStream {
    pub(crate) fn new(
        inner: ScanBatchStream,
        projection_mapper: Arc<ProjectionMapper>,
        cache_strategy: CacheStrategy,
        partition_metrics: PartitionMetrics,
    ) -> Self {
        Self {
            inner,
            projection_mapper,
            cache_strategy,
            partition_metrics,
            buffer: Vec::new(),
        }
    }

    fn convert(&mut self, batch: ScanBatch) -> common_recordbatch::error::Result<RecordBatch> {
        match batch {
            ScanBatch::Normal(batch) => {
                if batch.is_empty() {
                    Ok(self.projection_mapper.empty_record_batch())
                } else {
                    self.projection_mapper.convert(&batch, &self.cache_strategy)
                }
            }
            ScanBatch::Series(series) => {
                self.buffer.clear();
                self.buffer.reserve(series.batches.len());

                for batch in series.batches {
                    let record_batch = self
                        .projection_mapper
                        .convert(&batch, &self.cache_strategy)?;
                    self.buffer.push(record_batch.into_df_record_batch());
                }

                let output_schema = self.projection_mapper.output_schema();
                let record_batch =
                    compute::concat_batches(output_schema.arrow_schema(), &self.buffer)
                        .context(ArrowComputeSnafu)?;

                RecordBatch::try_from_df_record_batch(output_schema, record_batch)
            }
        }
    }
}

impl Stream for ConvertBatchStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
