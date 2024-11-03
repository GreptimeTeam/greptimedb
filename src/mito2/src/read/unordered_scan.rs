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
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::{Stream, StreamExt};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{PartitionRange, RegionScanner, ScannerProperties};

use crate::error::{PartitionOutOfRangeSnafu, Result};
use crate::read::scan_region::{ScanInput, StreamContext};
use crate::read::scan_util::{scan_file_ranges, scan_mem_ranges, PartitionMetrics};
use crate::read::{Batch, ScannerMetrics};

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
                    let stream = scan_mem_ranges(stream_ctx.clone(), part_metrics.clone(), *index, range_meta.time_range);
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

    fn scan_partition_impl(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        if partition >= self.properties.partitions.len() {
            return Err(BoxedError::new(
                PartitionOutOfRangeSnafu {
                    given: partition,
                    all: self.properties.partitions.len(),
                }
                .build(),
            ));
        }

        let part_metrics = PartitionMetrics::new(
            self.stream_ctx.input.mapper.metadata().region_id,
            partition,
            "UnorderedScan",
            self.stream_ctx.query_start,
            ScannerMetrics {
                prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
                ..Default::default()
            },
        );
        let stream_ctx = self.stream_ctx.clone();
        let part_ranges = self.properties.partitions[partition].clone();
        let distinguish_range = self.properties.distinguish_partition_range();

        let stream = try_stream! {
            part_metrics.on_first_poll();

            let cache = stream_ctx.input.cache_manager.as_deref();
            // Scans each part.
            for part_range in part_ranges {
                let mut metrics = ScannerMetrics::default();
                let mut fetch_start = Instant::now();
                #[cfg(debug_assertions)]
                let mut checker = crate::read::BatchChecker::default()
                    .with_start(Some(part_range.start))
                    .with_end(Some(part_range.end));

                let stream = Self::scan_partition_range(
                    stream_ctx.clone(),
                    part_range.identifier,
                    part_metrics.clone(),
                );
                for await batch in stream {
                    let batch = batch.map_err(BoxedError::new).context(ExternalSnafu)?;
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    debug_assert!(!batch.is_empty());
                    if batch.is_empty() {
                        continue;
                    }

                    #[cfg(debug_assertions)]
                    checker.ensure_part_range_batch(
                        "UnorderedScan",
                        stream_ctx.input.mapper.metadata().region_id,
                        partition,
                        part_range,
                        &batch,
                    );

                    let convert_start = Instant::now();
                    let record_batch = stream_ctx.input.mapper.convert(&batch, cache)?;
                    metrics.convert_cost += convert_start.elapsed();
                    let yield_start = Instant::now();
                    yield record_batch;
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }

                // Yields an empty part to indicate this range is terminated.
                // The query engine can use this to optimize some queries.
                if distinguish_range {
                    let yield_start = Instant::now();
                    yield stream_ctx.input.mapper.empty_record_batch();
                    metrics.yield_cost += yield_start.elapsed();
                }

                metrics.scan_cost += fetch_start.elapsed();
                part_metrics.merge_metrics(&metrics);
            }

            part_metrics.on_finish();
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

    fn prepare(
        &mut self,
        ranges: Vec<Vec<PartitionRange>>,
        distinguish_partition_range: bool,
    ) -> Result<(), BoxedError> {
        self.properties.partitions = ranges;
        self.properties.distinguish_partition_range = distinguish_partition_range;
        Ok(())
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scan_partition_impl(partition)
    }

    fn has_predicate(&self) -> bool {
        let predicate = self.stream_ctx.input.predicate();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.stream_ctx.input.mapper.metadata().clone()
    }
}

impl DisplayAs for UnorderedScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnorderedScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        self.stream_ctx.format_for_explain(f)
    }
}

impl fmt::Debug for UnorderedScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnorderedScan")
            .field("num_ranges", &self.stream_ctx.ranges.len())
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
