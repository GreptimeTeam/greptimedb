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

//! Snapshot-bound series discovery and independently repeatable ordinal reads.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use common_error::ext::BoxedError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::BinaryArray;
use datatypes::arrow::buffer::Buffer;
use datatypes::prelude::ConcreteDataType;
use datatypes::series::samples::SeriesSchema;
use futures::TryStreamExt;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::{
    PartitionRange, PreparedSeriesSource, RawLabelSource, SendableRawSeriesStream, SeriesInfo,
};
use tokio::sync::Semaphore;

use crate::error::{
    DecodeSnafu, MergeCandidateSeriesSnafu, PartitionOutOfRangeSnafu, Result, UnexpectedSnafu,
};
use crate::read::pruner::{PartitionPruner, Pruner, PrunerOptions};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{PartitionMetrics, PartitionMetricsList};
use crate::read::series_candidate::SeriesCandidateScanner;
use crate::read::series_reader::{AssignedSeriesBatch, SeriesRange, SeriesReader};
use crate::read::series_scan::{new_partition_metrics, raw};
use crate::read::stream::ScanBatch;
use crate::series_index::MetricSeriesId;

#[derive(Clone)]
pub(crate) struct RawOptions {
    pub(crate) schema: Arc<SeriesSchema>,
    pub(crate) fields: Vec<(usize, ConcreteDataType)>,
    pub(crate) predicates: Vec<Arc<dyn PhysicalExpr>>,
}

struct PreparedSource {
    reservation: Arc<MemoryReservation>,
    identities: Vec<(SeriesInfo, MetricSeriesId)>,
    stream_ctx: Arc<StreamContext>,
    ranges: Vec<PartitionRange>,
    range: SeriesRange,
    pruner: Arc<PartitionPruner>,
    semaphore: Arc<Semaphore>,
    metrics: PartitionMetrics,
    labels: RawLabelSource,
    options: RawOptions,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare(
    stream_ctx: Arc<StreamContext>,
    partitions: Vec<Vec<PartitionRange>>,
    partition: usize,
    labels: RawLabelSource,
    options: RawOptions,
    metrics_set: ExecutionPlanMetricsSet,
    metrics_list: Arc<PartitionMetricsList>,
    explain_verbose: bool,
) -> Result<Arc<dyn PreparedSeriesSource>> {
    ensure!(
        partition < partitions.len(),
        PartitionOutOfRangeSnafu {
            given: partition,
            all: partitions.len()
        }
    );
    let metrics = new_partition_metrics(
        &stream_ctx,
        explain_verbose,
        &metrics_set,
        partition,
        &metrics_list,
    );
    let workers = common_stat::get_total_cpu_cores().max(1);
    let semaphore = Arc::new(Semaphore::new(workers));
    let pruner = Arc::new(Pruner::new_with_options(
        stream_ctx.clone(),
        workers,
        PrunerOptions {
            retain_builders: true,
            enable_predicate_prefilter: false,
        },
    ));
    let candidates = SeriesCandidateScanner::try_new(
        stream_ctx.clone(),
        partitions.clone(),
        pruner,
        semaphore.clone(),
        stream_ctx.input.scan_memory_pool.clone(),
        metrics_set,
        metrics.clone(),
    )?;
    let codec = SparsePrimaryKeyCodec::new(stream_ctx.input.region_metadata());
    let mut keys = candidates.build_primary_key_stream().await?;
    let reservation = MemoryConsumer::new("PreparedSeriesSource::identities")
        .register(&stream_ctx.input.scan_memory_pool);
    let mut identities = Vec::new();
    let mut last = Vec::new();
    while let Some(batch) = keys.try_next().await? {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "index identities are not Binary",
            })?;
        for key in keys.iter().flatten() {
            if key == last {
                continue;
            }
            last = key.to_vec();
            let (table_id, tsid) = codec.decode_ids(key).context(DecodeSnafu)?;
            if SeriesRange::partition_for(tsid, partitions.len()) == partition {
                reservation
                    .try_grow(key.len() + std::mem::size_of::<(SeriesInfo, MetricSeriesId)>())
                    .context(MergeCandidateSeriesSnafu)?;
                identities.push((
                    SeriesInfo {
                        primary_key: Buffer::from(key),
                        metric: None,
                    },
                    MetricSeriesId { table_id, tsid },
                ));
            }
        }
    }
    let range = SeriesRange::new(partition, partitions.len()).context(UnexpectedSnafu {
        reason: "invalid series partition",
    })?;
    Ok(Arc::new(PreparedSource {
        reservation: Arc::new(reservation),
        identities,
        stream_ctx,
        ranges: partitions.into_iter().flatten().collect(),
        range,
        pruner: candidates.partition_pruner(),
        semaphore,
        metrics,
        labels,
        options,
    }))
}

impl PreparedSeriesSource for PreparedSource {
    fn num_series(&self) -> usize {
        self.identities.len()
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.labels
    }
    fn series_info(&self, ordinal: usize) -> std::result::Result<SeriesInfo, BoxedError> {
        self.identities
            .get(ordinal)
            .map(|(info, _)| info.clone())
            .context(UnexpectedSnafu {
                reason: "series ordinal out of bounds",
            })
            .map_err(BoxedError::new)
    }
    fn read_series(
        &self,
        range: Range<usize>,
    ) -> std::result::Result<SendableRawSeriesStream, BoxedError> {
        let selected = self
            .identities
            .get(range)
            .context(UnexpectedSnafu {
                reason: "series range out of bounds",
            })
            .map_err(BoxedError::new)?;
        let identities: HashSet<Vec<u8>> = selected
            .iter()
            .map(|(info, _)| info.primary_key.to_vec())
            .collect();
        let assigned = AssignedSeriesBatch::new(
            self.range,
            selected.iter().map(|(_, id)| *id).collect(),
            false,
        );
        let reader = SeriesReader::try_new(
            self.stream_ctx.clone(),
            self.ranges.clone(),
            assigned,
            self.pruner.clone(),
            self.semaphore.clone(),
            self.metrics.clone(),
        )
        .map_err(BoxedError::new)?
        .with_samples();
        let reservation = self.reservation.clone();
        let batches = Box::pin(async_stream::try_stream! {
            let mut stream = reader.build_stream().await?;
            while let Some(batch) = stream.try_next().await? { yield ScanBatch::RecordBatch(batch); }
            drop(reservation);
        });
        let raw = raw::make_stream(
            batches,
            self.options.schema.clone(),
            self.labels.clone(),
            self.options.fields.clone(),
            self.metrics.clone(),
            self.options.predicates.clone(),
        );
        // IDs locate data; complete keys define the prepared ordinal identity.
        Ok(raw::filter_identities(raw, identities))
    }
}
