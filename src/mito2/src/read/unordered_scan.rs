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

//! Unordered scanner.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_stream::{stream, try_stream};
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::debug;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::{Stream, StreamExt};
use snafu::ResultExt;
use store_api::region_engine::{PartitionRange, RegionScanner, ScannerProperties};

use crate::cache::CacheManager;
use crate::error::Result;
use crate::read::compat::CompatBatch;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{scan_file_ranges, scan_mem_ranges, PartitionMetrics};
use crate::read::{Batch, ScannerMetrics, Source};
use crate::sst::parquet::reader::ReaderMetrics;

/// Scans a region without providing any output ordering guarantee.
///
/// Only an append only table should use this scanner.
pub struct UnorderedScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
}

impl UnorderedScan {
    /// Creates a new [UnorderedScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        let mut properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        let stream_ctx = Arc::new(StreamContext::unordered_scan_ctx(input));
        properties.partitions = vec![stream_ctx.partition_ranges()];

        Self {
            properties,
            stream_ctx,
        }
    }

    /// Scans the region and returns a stream.
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let part_num = self.properties.num_partitions();
        let streams = (0..part_num)
            .map(|i| self.scan_partition(i))
            .collect::<Result<Vec<_>, BoxedError>>()?;
        let stream = stream! {
            for mut stream in streams {
                while let Some(rb) = stream.next().await {
                    yield rb;
                }
            }
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.schema(),
            Box::pin(stream),
        ));
        Ok(stream)
    }

    // /// Fetch a batch from the source and convert it into a record batch.
    // async fn fetch_from_source(
    //     source: &mut Source,
    //     mapper: &ProjectionMapper,
    //     cache: Option<&CacheManager>,
    //     compat_batch: Option<&CompatBatch>,
    //     metrics: &mut ScannerMetrics,
    // ) -> common_recordbatch::error::Result<Option<RecordBatch>> {
    //     let start = Instant::now();

    //     let Some(mut batch) = source
    //         .next_batch()
    //         .await
    //         .map_err(BoxedError::new)
    //         .context(ExternalSnafu)?
    //     else {
    //         metrics.scan_cost += start.elapsed();

    //         return Ok(None);
    //     };

    //     if let Some(compat) = compat_batch {
    //         batch = compat
    //             .compat_batch(batch)
    //             .map_err(BoxedError::new)
    //             .context(ExternalSnafu)?;
    //     }
    //     metrics.scan_cost += start.elapsed();

    //     let convert_start = Instant::now();
    //     let record_batch = mapper.convert(&batch, cache)?;
    //     metrics.convert_cost += convert_start.elapsed();

    //     Ok(Some(record_batch))
    // }

    /// Scans a [PartitionRange] by its `identifier` and returns a stream.
    fn scan_partition_range(
        stream_ctx: Arc<StreamContext>,
        part_range_id: usize,
        part_metrics: PartitionMetrics,
    ) -> impl Stream<Item = Result<Batch>> {
        stream! {
            // Gets range meta.
            let range_meta = &stream_ctx.ranges[part_range_id];
            for index in &range_meta.row_group_indices {
                if stream_ctx.is_mem_range_index(*index) {
                    let stream = scan_mem_ranges(stream_ctx.clone(), part_metrics.clone(), *index);
                    for await batch in stream {
                        yield batch;
                    }
                } else {
                    let stream = scan_file_ranges(stream_ctx.clone(), part_metrics.clone(), *index, "unordered_scan_files");
                    for await batch in stream {
                        yield batch;
                    }
                }
            }
        }
    }

    // // TODO(yingwen): Move them or refactor them to collect metrics later.
    // /// Scans memtable ranges at `index`.
    // pub(crate) fn scan_mem_ranges<'a>(
    //     stream_ctx: &'a StreamContext,
    //     index: RowGroupIndex,
    //     metrics: &'a mut ScannerMetrics,
    // ) -> impl Stream<Item = common_recordbatch::error::Result<RecordBatch>> + 'a {
    //     try_stream! {
    //         let mapper = &stream_ctx.input.mapper;
    //         let cache = stream_ctx.input.cache_manager.as_deref();
    //         let ranges = stream_ctx.build_mem_ranges(index);
    //         metrics.num_mem_ranges += ranges.len();
    //         for range in ranges {
    //             let build_reader_start = Instant::now();
    //             let iter = range.build_iter().map_err(BoxedError::new).context(ExternalSnafu)?;
    //             metrics.build_reader_cost += build_reader_start.elapsed();

    //             let mut source = Source::Iter(iter);
    //             while let Some(batch) =
    //                 Self::fetch_from_source(&mut source, mapper, cache, None, metrics).await?
    //             {
    //                 metrics.num_batches += 1;
    //                 metrics.num_rows += batch.num_rows();
    //                 let yield_start = Instant::now();
    //                 yield batch;
    //                 metrics.yield_cost += yield_start.elapsed();
    //             }
    //         }
    //     }
    // }

    // /// Scans file ranges at `index`.
    // pub(crate) fn scan_file_ranges<'a>(
    //     stream_ctx: &'a StreamContext,
    //     index: RowGroupIndex,
    //     reader_metrics: &'a mut ReaderMetrics,
    //     metrics: &'a mut ScannerMetrics,
    // ) -> impl Stream<Item = common_recordbatch::error::Result<RecordBatch>> + 'a {
    //     try_stream! {
    //         let mapper = &stream_ctx.input.mapper;
    //         let cache = stream_ctx.input.cache_manager.as_deref();
    //         let ranges= stream_ctx
    //             .build_file_ranges(index, reader_metrics)
    //             .await
    //             .map_err(BoxedError::new)
    //             .context(ExternalSnafu)?;
    //         metrics.num_file_ranges += ranges.len();
    //         for range in ranges {
    //             let build_reader_start = Instant::now();
    //             let reader = range
    //                 .reader(None)
    //                 .await
    //                 .map_err(BoxedError::new)
    //                 .context(ExternalSnafu)?;
    //             metrics.build_reader_cost += build_reader_start.elapsed();
    //             let compat_batch = range.compat_batch();
    //             let mut source = Source::PruneReader(reader);
    //             while let Some(batch) =
    //                 Self::fetch_from_source(&mut source, mapper, cache, compat_batch, metrics)
    //                     .await?
    //             {
    //                 metrics.num_batches += 1;
    //                 metrics.num_rows += batch.num_rows();
    //                 let yield_start = Instant::now();
    //                 yield batch;
    //                 metrics.yield_cost += yield_start.elapsed();
    //             }
    //             if let Source::PruneReader(mut reader) = source {
    //                 reader_metrics.merge_from(reader.metrics());
    //             }
    //         }
    //     }
    // }

    fn scan_partition_impl(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        let part_metrics = PartitionMetrics::from_metrics(ScannerMetrics {
            prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
            ..Default::default()
        });
        let stream_ctx = self.stream_ctx.clone();
        let ranges_opt = self.properties.partitions.get(partition).cloned();

        let stream = try_stream! {
            let first_poll = stream_ctx.query_start.elapsed();
            let Some(part_ranges) = ranges_opt else {
                return;
            };

            let mut metrics = ScannerMetrics::default();
            let reader_metrics = ReaderMetrics::default();
            let mut fetch_start = Instant::now();
            let cache = stream_ctx.input.cache_manager.as_deref();
            // Scans each part.
            for part_range in part_ranges {
                let stream = Self::scan_partition_range(
                    stream_ctx.clone(),
                    part_range.identifier,
                    part_metrics.clone(),
                );
                // TODO(yingwen): metrics and convert.
                for await batch in stream {
                    let batch = batch.map_err(BoxedError::new).context(ExternalSnafu)?;
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    let convert_start = Instant::now();
                    let record_batch = stream_ctx.input.mapper.convert(&batch, cache)?;
                    metrics.convert_cost += convert_start.elapsed();
                    let yield_start = Instant::now();
                    yield record_batch;
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }
            }

            // TODO(yingwen): remove this.
            // reader_metrics.observe_rows("unordered_scan_files");
            metrics.total_cost = stream_ctx.query_start.elapsed();
            metrics.observe_metrics_on_finish();
            let mapper = &stream_ctx.input.mapper;
            debug!(
                "Unordered scan partition {} finished, region_id: {}, metrics: {:?}, reader_metrics: {:?}, first_poll: {:?}",
                partition, mapper.metadata().region_id, metrics, reader_metrics, first_poll,
            );
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.stream_ctx.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }
}

impl RegionScanner for UnorderedScan {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.stream_ctx.input.mapper.output_schema()
    }

    fn prepare(&mut self, ranges: Vec<Vec<PartitionRange>>) -> Result<(), BoxedError> {
        self.properties.partitions = ranges;
        Ok(())
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scan_partition_impl(partition)
    }

    fn has_predicate(&self) -> bool {
        let predicate = self.stream_ctx.input.predicate();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }
}

impl DisplayAs for UnorderedScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnorderedScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        self.stream_ctx.format_for_explain(t, f)
    }
}

impl fmt::Debug for UnorderedScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnorderedScan")
            .field("parts", &self.stream_ctx.parts)
            .finish()
    }
}

#[cfg(test)]
impl UnorderedScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.stream_ctx.input
    }
}
