use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use common_error::ext::BoxedError;
use common_recordbatch::error::{ArrowComputeSnafu, ExternalSnafu};
use common_recordbatch::RecordBatch;
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

pub enum ScanBatch {
    Normal(Batch),
    Series(SeriesBatch),
}

pub type ScanBatchStream = BoxStream<'static, Result<ScanBatch>>;

pub(crate) struct ConvertBatchStream {
    inner: ScanBatchStream,
    projection_mapper: Arc<ProjectionMapper>,
    cache_strategy: CacheStrategy,
    partition_metrics: PartitionMetrics,
    convert_cost: Duration,
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
            convert_cost: Duration::from_secs(0),
        }
    }

    fn convert(&self, batch: ScanBatch) -> common_recordbatch::error::Result<RecordBatch> {
        match batch {
            ScanBatch::Normal(batch) => {
                self.projection_mapper.convert(&batch, &self.cache_strategy)
            }
            ScanBatch::Series(series) => {
                let mut record_batches = Vec::with_capacity(series.batches.len());
                for batch in series.batches {
                    let record_batch = self
                        .projection_mapper
                        .convert(&batch, &self.cache_strategy)?;
                    record_batches.push(record_batch.into_df_record_batch());
                }

                let output_schema = self.projection_mapper.output_schema();
                let record_batch =
                    compute::concat_batches(output_schema.arrow_schema(), &record_batches)
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
            self.partition_metrics
                .inc_convert_batch_cost(self.convert_cost);
            self.convert_cost = Duration::from_secs(0);

            return Poll::Ready(None);
        };

        let record_batch = match batch {
            Ok(batch) => {
                let start = Instant::now();
                let record_batch = self.convert(batch);
                self.convert_cost += start.elapsed();
                record_batch
            }
            Err(e) => Err(BoxedError::new(e)).context(ExternalSnafu),
        };
        Poll::Ready(Some(record_batch))
    }
}
