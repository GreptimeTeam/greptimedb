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

use std::collections::{BinaryHeap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_query::series_frame::{pack_raw_series, raw_series_frame_schema_with_ordering};
use common_recordbatch::adapter::{
    RecordBatchStreamAdapter, RegionQueryStatCounters, SERIES_SCAN_EXEC_NAME,
};
use common_recordbatch::{
    DfRecordBatch, DfSendableRecordBatchStream, MemoryTrackedStream, QueryMemoryTracker,
    RecordBatch, SendableRecordBatchStream,
};
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::warn;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream as DfRecordBatchStream, StatisticsArgs, apply_expression_roots,
};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{ColumnStatistics, DataFusionError, Statistics};
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, is_null,
};
use datafusion_physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalSortExpr, conjunction,
};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::compute::SortOptions;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use futures::{Stream, StreamExt};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
use store_api::region_engine::{
    BoundTag, PartitionRange, PrepareRequest, QueryScanContext, RawLabelSource, RegionScannerRef,
};
use store_api::storage::{ScanRequest, TimeSeriesDistribution};

use crate::table::metrics::StreamMetrics;

/// The name of [`RegionScanExec`] plan node.
pub const REGION_SCAN_EXEC_NAME: &str = "RegionScanExec";

/// A plan to read multiple partitions from a region of a table.
#[derive(Clone)]
pub struct RegionScanExec {
    scanner: Arc<Mutex<RegionScannerRef>>,
    arrow_schema: ArrowSchemaRef,
    /// The expected output ordering for the plan.
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    metric: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
    append_mode: bool,
    total_rows: usize,
    is_partition_set: bool,
    // TODO(ruihang): handle TimeWindowed dist via this parameter
    distribution: Option<TimeSeriesDistribution>,
    explain_verbose: bool,
    query_memory_tracker: Option<QueryMemoryTracker>,
    /// Dynamic filters accepted by the scanner and applied to decoded output batches.
    /// The wrappers themselves remain live and are updated by their producers.
    pushed_dyn_filters: Arc<Mutex<Vec<Arc<dyn PhysicalExpr>>>>,
}

impl std::fmt::Debug for RegionScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionScanExec")
            .field("scanner", &self.scanner)
            .field("arrow_schema", &self.arrow_schema)
            .field("output_ordering", &self.output_ordering)
            .field("metric", &self.metric)
            .field("properties", &self.properties)
            .field("append_mode", &self.append_mode)
            .field("total_rows", &self.total_rows)
            .field("is_partition_set", &self.is_partition_set)
            .field("distribution", &self.distribution)
            .field("explain_verbose", &self.explain_verbose)
            .field("pushed_dyn_filters", &self.pushed_dyn_filters)
            .finish()
    }
}

/// Distribute [`PartitionRange`]s to each partition.
///
/// Currently we assign ranges to partitions according to their rows so each partition
/// has similar number of rows. This method always return `expected_partition_num` partitions.
pub fn assign_partition_ranges(
    mut ranges: Vec<PartitionRange>,
    expected_partition_num: usize,
) -> Vec<Vec<PartitionRange>> {
    if ranges.is_empty() {
        // Returns a single partition with no range.
        return vec![vec![]; expected_partition_num];
    }

    if ranges.len() == 1 {
        let mut vec = vec![vec![]; expected_partition_num];
        vec[0] = ranges;
        return vec;
    }

    // Sort ranges by number of rows in descending order.
    ranges.sort_by_key(|b| std::cmp::Reverse(b.num_rows));
    let mut partition_ranges = vec![vec![]; expected_partition_num];

    #[derive(Eq, PartialEq)]
    struct HeapNode {
        num_rows: usize,
        partition_idx: usize,
    }

    impl Ord for HeapNode {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            // Reverse for min-heap.
            self.num_rows.cmp(&other.num_rows).reverse()
        }
    }

    impl PartialOrd for HeapNode {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut part_heap =
        BinaryHeap::from_iter((0..expected_partition_num).map(|partition_idx| HeapNode {
            num_rows: 0,
            partition_idx,
        }));

    // Assigns the range to the partition with the smallest number of rows.
    for range in ranges {
        let mut node = part_heap.pop().unwrap();
        let partition_idx = node.partition_idx;
        node.num_rows += range.num_rows;
        partition_ranges[partition_idx].push(range);
        part_heap.push(node);
    }

    partition_ranges
}

impl RegionScanExec {
    pub fn new(
        scanner: RegionScannerRef,
        request: ScanRequest,
        query_memory_tracker: Option<QueryMemoryTracker>,
    ) -> DfResult<Self> {
        let arrow_schema = scanner.schema().arrow_schema().clone();
        let scanner_props = scanner.properties();
        let mut num_output_partition = scanner_props.num_partitions();
        // The meaning of word "partition" is different in different context. For datafusion
        // it's about "parallelism" and for storage it's about "data range". Thus here we add
        // a special case to handle the situation where the number of storage partition is 0.
        if num_output_partition == 0 {
            num_output_partition = 1;
        }

        let metadata = scanner.metadata();
        // Keep the primary key columns in the order reported by the metadata (for a metric
        // region that is the physical row key order). Reordering them would let a projected
        // logical tag look like a prefix of the row key, which yields ordering and
        // partitioning claims the scanner cannot honour.
        let pk_names = metadata
            .primary_key_columns()
            .map(|col| col.column_schema.name.clone())
            .collect::<Vec<_>>();
        // A region emits rows ordered by its primary key columns and then by the time
        // index column. A primary key column that is not projected is absent from the
        // arrow schema, and it also breaks the ordering of every primary key column that
        // follows it (e.g. the physical `__table_id`/`__tsid` prefix of a metric region
        // when reading a logical table). Thus only the longest fully projected prefix of
        // the primary key can describe the output ordering, and that prefix can be empty.
        let projected_pk_prefix_len = pk_names
            .iter()
            .position(|name| Column::new_with_schema(name, &arrow_schema).is_err())
            .unwrap_or(pk_names.len());
        // Whether the projected prefix covers the whole row key.
        let all_pk_projected = projected_pk_prefix_len == pk_names.len();
        let projected_pk_names = &pk_names[..projected_pk_prefix_len];
        let pk_columns = pk_names
            .iter()
            .filter_map(
                |col| Some(Arc::new(Column::new_with_schema(col, &arrow_schema).ok()?) as _),
            )
            .collect::<Vec<_>>();
        let pk_sort_columns: Vec<PhysicalSortExpr> = projected_pk_names
            .iter()
            .filter_map(|col| {
                Some(PhysicalSortExpr::new(
                    Arc::new(Column::new_with_schema(col, &arrow_schema).ok()?) as _,
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                ))
            })
            .collect::<Vec<_>>();
        let ts_col: Option<PhysicalSortExpr> = (|| {
            Some(PhysicalSortExpr::new(
                Arc::new(
                    Column::new_with_schema(
                        &metadata.time_index_column().column_schema.name,
                        &arrow_schema,
                    )
                    .ok()?,
                ) as _,
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ))
        })();

        let eq_props = match request.distribution {
            Some(TimeSeriesDistribution::PerSeries) => {
                let mut orderings = Vec::with_capacity(2);

                let mut pk_time_ordering = pk_sort_columns.clone();
                // The time index column extends the ordering only when the whole row key
                // precedes it; otherwise rows of different series can interleave.
                if all_pk_projected && let Some(ts) = ts_col.clone() {
                    pk_time_ordering.push(ts);
                }
                orderings.push(pk_time_ordering);

                // Metric engine physical tables keep tag columns alongside `__tsid`. When `__tsid`
                // is present, the scan output is also ordered by `(__tsid, time index)` which can
                // help eliminate redundant sorts for promql plans (e.g. SeriesDivide).
                //
                // `pk_time_ordering` and the potential `(__tsid, time index)` ordering are actually
                // the same thing, thus present in one `OrderingEquivalenceClass`.
                if let (Ok(tsid_col), Some(ts)) = (
                    Column::new_with_schema(DATA_SCHEMA_TSID_COLUMN_NAME, &arrow_schema),
                    ts_col.clone(),
                ) {
                    orderings.push(vec![
                        PhysicalSortExpr::new(
                            Arc::new(tsid_col) as _,
                            SortOptions {
                                descending: false,
                                nulls_first: true,
                            },
                        ),
                        ts,
                    ]);
                }

                EquivalenceProperties::new_with_orderings(arrow_schema.clone(), orderings)
            }
            Some(TimeSeriesDistribution::TimeWindowed) => {
                let mut pk_time_ordering = pk_sort_columns.clone();
                if let Some(ts) = ts_col.clone() {
                    pk_time_ordering.insert(0, ts);
                }
                EquivalenceProperties::new_with_orderings(
                    arrow_schema.clone(),
                    vec![pk_time_ordering],
                )
            }
            None => EquivalenceProperties::new(arrow_schema.clone()),
        };

        let partitioning = match request.distribution {
            Some(TimeSeriesDistribution::PerSeries) => {
                match Column::new_with_schema(DATA_SCHEMA_TSID_COLUMN_NAME, &arrow_schema) {
                    Ok(tsid_col) => {
                        Partitioning::Hash(vec![Arc::new(tsid_col) as _], num_output_partition)
                    }
                    Err(_) if pk_columns.len() == pk_names.len() => {
                        Partitioning::Hash(pk_columns.clone(), num_output_partition)
                    }
                    Err(_) => Partitioning::UnknownPartitioning(num_output_partition),
                }
            }
            Some(TimeSeriesDistribution::TimeWindowed) | None => {
                Partitioning::UnknownPartitioning(num_output_partition)
            }
        };

        let properties = Arc::new(PlanProperties::new(
            eq_props,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let append_mode = scanner_props.append_mode();
        let total_rows = scanner_props.total_rows();

        Ok(Self {
            scanner: Arc::new(Mutex::new(scanner)),
            arrow_schema,
            output_ordering: None,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            append_mode,
            total_rows,
            is_partition_set: false,
            distribution: request.distribution,
            explain_verbose: false,
            query_memory_tracker,
            pushed_dyn_filters: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Get the partition ranges of the scanner. This method will collapse the ranges into
    /// a single vector.
    pub fn get_partition_ranges(&self) -> Vec<PartitionRange> {
        let scanner = self.scanner.lock().unwrap();
        let raw_ranges = &scanner.properties().partitions;

        // collapse the ranges
        let mut ranges = Vec::with_capacity(raw_ranges.len());
        for partition in raw_ranges {
            ranges.extend_from_slice(partition);
        }

        ranges
    }

    /// Similar to [`Self::get_partition_ranges`] but don't collapse the ranges.
    pub fn get_uncollapsed_partition_ranges(&self) -> Vec<Vec<PartitionRange>> {
        let scanner = self.scanner.lock().unwrap();
        scanner.properties().partitions.clone()
    }

    pub fn is_partition_set(&self) -> bool {
        self.is_partition_set
    }

    pub fn scanner_type(&self) -> String {
        let scanner = self.scanner.lock().unwrap();
        scanner.name().to_string()
    }

    /// Update the partition ranges of underlying scanner.
    pub fn with_new_partitions(
        &self,
        partitions: Vec<Vec<PartitionRange>>,
        target_partitions: usize,
    ) -> Result<Self, BoxedError> {
        if self.is_partition_set {
            warn!("Setting partition ranges more than once for RegionScanExec");
        }

        let mut properties = self.properties.as_ref().clone();
        let new_partitioning = match properties.partitioning {
            Partitioning::Hash(ref columns, _) => {
                Partitioning::Hash(columns.clone(), target_partitions)
            }
            _ => Partitioning::UnknownPartitioning(target_partitions),
        };
        properties.partitioning = new_partitioning;

        {
            let mut scanner = self.scanner.lock().unwrap();
            scanner.prepare(
                PrepareRequest::default()
                    .with_ranges(partitions)
                    .with_target_partitions(target_partitions),
            )?;
        }

        Ok(Self {
            scanner: self.scanner.clone(),
            arrow_schema: self.arrow_schema.clone(),
            output_ordering: self.output_ordering.clone(),
            metric: self.metric.clone(),
            properties: Arc::new(properties),
            append_mode: self.append_mode,
            total_rows: self.total_rows,
            is_partition_set: true,
            distribution: self.distribution,
            explain_verbose: self.explain_verbose,
            query_memory_tracker: self.query_memory_tracker.clone(),
            pushed_dyn_filters: self.pushed_dyn_filters.clone(),
        })
    }

    pub fn distribution(&self) -> Option<TimeSeriesDistribution> {
        self.distribution
    }

    pub fn with_distinguish_partition_range(&self, distinguish_partition_range: bool) {
        let mut scanner = self.scanner.lock().unwrap();
        // set distinguish_partition_range won't fail
        let _ = scanner.prepare(
            PrepareRequest::default().with_distinguish_partition_range(distinguish_partition_range),
        );
    }

    pub fn time_index(&self) -> String {
        self.scanner
            .lock()
            .unwrap()
            .metadata()
            .time_index_column()
            .column_schema
            .name
            .clone()
    }

    pub fn tag_columns(&self) -> Vec<String> {
        self.scanner
            .lock()
            .unwrap()
            .metadata()
            .primary_key_columns()
            .map(|col| col.column_schema.name.clone())
            .collect()
    }

    pub fn set_explain_verbose(&mut self, explain_verbose: bool) {
        self.explain_verbose = explain_verbose;
    }

    /// Adds dynamic filters directly to the underlying region scanner predicate.
    ///
    /// This is the same mutation path used by DataFusion child-filter pushdown. Remote dynamic
    /// filter updates install their shared runtime wrapper through this method at scan build time
    /// and later update the wrapper state out-of-band.
    pub fn add_dyn_filters_to_predicate(
        &self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        self.scanner
            .lock()
            .unwrap()
            .add_dyn_filter_to_predicate(filter_exprs)
    }

    pub fn query_load_region_id(&self) -> Option<u64> {
        self.scanner
            .lock()
            .unwrap()
            .properties()
            .query_load_region_id()
            .map(|region_id| region_id.as_u64())
    }

    pub fn query_stat_counters(&self) -> Option<RegionQueryStatCounters> {
        self.scanner
            .lock()
            .unwrap()
            .properties()
            .query_stat_counters()
    }

    /// Resolves authorized logical names against this prepared scanner's physical metadata,
    /// then selects a native raw-series leaf.
    ///
    /// The caller is responsible for authorizing the logical table and supplying its complete
    /// logical tag names and requested field names. This method resolves them only against this
    /// scanner's metadata snapshot.
    pub fn try_to_named_series_scan(
        self: Arc<Self>,
        metric_name: &str,
        logical_tags: &[String],
        field_names: &[String],
    ) -> DfResult<Option<Arc<SeriesScanExec>>> {
        if !self.is_partition_set || field_names.is_empty() {
            return Ok(None);
        }
        if !self.pushed_dyn_filters.lock().unwrap().is_empty() {
            return Ok(None);
        }
        let (metadata, expected_field_ids, fields, tags) = {
            let scanner = self.scanner.lock().unwrap();
            let metadata = scanner.metadata();
            let primary_key_ids = metadata
                .primary_key_columns()
                .map(|column| column.column_id)
                .collect::<HashSet<_>>();
            let mut names = HashSet::new();
            let mut column_ids = HashSet::new();
            let mut tags = Vec::with_capacity(logical_tags.len());
            for name in logical_tags {
                if !names.insert(name.as_str())
                    || name == "__name__"
                    || store_api::metric_engine_consts::is_metric_engine_internal_column(name)
                    || store_api::storage::consts::is_internal_column(name)
                {
                    return Ok(None);
                }
                let Some(column) = metadata.column_by_name(name) else {
                    return Ok(None);
                };
                if column.semantic_type != SemanticType::Tag
                    || store_api::storage::consts::ReservedColumnId::is_reserved(column.column_id)
                    || !primary_key_ids.contains(&column.column_id)
                    || !column_ids.insert(column.column_id)
                {
                    return Ok(None);
                }
                tags.push(BoundTag {
                    column_id: column.column_id,
                    name: Arc::from(name.as_str()),
                });
            }

            let mut fields = Vec::with_capacity(field_names.len());
            let mut expected_field_ids = Vec::with_capacity(field_names.len());
            for name in field_names {
                if !names.insert(name.as_str())
                    || name == "__name__"
                    || store_api::metric_engine_consts::is_metric_engine_internal_column(name)
                    || store_api::storage::consts::is_internal_column(name)
                {
                    return Ok(None);
                }
                let Some(column) = metadata.column_by_name(name) else {
                    return Ok(None);
                };
                if column.semantic_type != SemanticType::Field
                    || store_api::storage::consts::ReservedColumnId::is_reserved(column.column_id)
                    || !column_ids.insert(column.column_id)
                {
                    return Ok(None);
                }
                expected_field_ids.push(column.column_id);
                fields.push(FieldSpec {
                    name: Arc::from(name.as_str()),
                });
            }
            (metadata, expected_field_ids, fields, tags)
        };
        // The requested field sequence must exactly match the scanner's projected field IDs.
        let projected = self
            .arrow_schema
            .fields()
            .iter()
            .filter_map(|field| {
                metadata
                    .column_by_name(field.name())
                    .filter(|column| column.semantic_type == SemanticType::Field)
                    .map(|column| column.column_id)
            })
            .collect::<Vec<_>>();
        if expected_field_ids != projected {
            return Ok(None);
        }
        let source = RawLabelSource::PrimaryKey {
            metric_name: Arc::from(metric_name),
            physical_metadata: metadata,
            logical_tags: tags.into(),
        };
        let schema = Arc::new(SeriesSchema {
            fields: fields.into(),
            eval_grid: None,
        });
        let Some(native) = self.try_to_series_scan(source)? else {
            return Ok(None);
        };
        if native.series_schema.eval_grid.is_some()
            || native.series_schema.fields.len() != schema.fields.len()
            || native
                .series_schema
                .fields
                .iter()
                .zip(schema.fields.iter())
                .any(|(actual, expected)| actual.name != expected.name)
        {
            return Ok(None);
        }
        Ok(Some(native))
    }

    /// Builds a native raw-series leaf when the scanner's current preparation supports it.
    /// Finish partition preparation before selecting or executing this leaf. Callers must
    /// already have resolved the authorized scanner and logical binding; this method does not
    /// authenticate them.
    pub fn try_to_series_scan(
        self: Arc<Self>,
        label_source: RawLabelSource,
    ) -> DfResult<Option<Arc<SeriesScanExec>>> {
        if !self.pushed_dyn_filters.lock().unwrap().is_empty() {
            return Ok(None);
        }
        let scanner = self.scanner.lock().unwrap();
        let Some(schema) = scanner
            .raw_series_schema(&label_source)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        else {
            return Ok(None);
        };
        let ordering = scanner
            .raw_series_ordering()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let arrow_schema = raw_series_frame_schema_with_ordering(&schema, &label_source, ordering)?;
        let partition_count = scanner.properties().num_partitions().max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        drop(scanner);
        Ok(Some(Arc::new(SeriesScanExec {
            scan: self,
            label_source,
            series_schema: schema,
            arrow_schema,
            ordering,
            properties,
        })))
    }
}

/// Native physical leaf that packs a prepared region scanner's raw series chunks.
/// Do not prepare, reset, or execute concurrently through scanner aliases.
pub struct SeriesScanExec {
    scan: Arc<RegionScanExec>,
    label_source: RawLabelSource,
    series_schema: Arc<SeriesSchema>,
    arrow_schema: ArrowSchemaRef,
    ordering: SeriesOrdering,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for SeriesScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = match &self.label_source {
            RawLabelSource::Explicit => "explicit",
            RawLabelSource::PrimaryKey { .. } => "primary_key",
        };
        f.debug_struct("SeriesScanExec")
            .field("source", &source)
            .field("fields", &self.series_schema.fields.len())
            .field(
                "partitions",
                &self.properties.output_partitioning().partition_count(),
            )
            .finish()
    }
}

impl SeriesScanExec {
    pub fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }
    pub fn series_schema(&self) -> &Arc<SeriesSchema> {
        &self.series_schema
    }

    /// Returns the producer ordering captured when this scan was prepared.
    pub fn raw_series_ordering(&self) -> SeriesOrdering {
        self.ordering
    }

    pub fn query_load_region_id(&self) -> Option<u64> {
        self.scan.query_load_region_id()
    }

    pub fn query_stat_counters(&self) -> Option<RegionQueryStatCounters> {
        self.scan.query_stat_counters()
    }
}

fn same_label_source(left: &RawLabelSource, right: &RawLabelSource) -> bool {
    match (left, right) {
        (RawLabelSource::Explicit, RawLabelSource::Explicit) => true,
        (
            RawLabelSource::PrimaryKey {
                metric_name: lm,
                physical_metadata: lp,
                logical_tags: lt,
            },
            RawLabelSource::PrimaryKey {
                metric_name: rm,
                physical_metadata: rp,
                logical_tags: rt,
            },
        ) => {
            lm == rm
                && Arc::ptr_eq(lp, rp)
                && lt.len() == rt.len()
                && lt
                    .iter()
                    .zip(rt.iter())
                    .all(|(l, r)| l.name == r.name && l.column_id == r.column_id)
        }
        _ => false,
    }
}

impl DisplayAs for SeriesScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let source = match &self.label_source {
            RawLabelSource::Explicit => "explicit",
            RawLabelSource::PrimaryKey { .. } => "primary_key",
        };
        write!(
            f,
            "SeriesScanExec: source={source}, fields={}, partitions={}",
            self.series_schema.fields.len(),
            self.properties.output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for SeriesScanExec {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("SeriesScanExec is a leaf".into()));
        }
        Ok(self)
    }
    fn name(&self) -> &str {
        SERIES_SCAN_EXEC_NAME
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.scan.metric.clone_inner())
    }
    fn reset_state(self: Arc<Self>) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.scan.scanner.lock().unwrap().reset_state();
        self.scan.pushed_dyn_filters.lock().unwrap().clear();
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let partitions = self.properties.output_partitioning().partition_count();
        if partition >= partitions {
            return Err(DataFusionError::Execution(format!(
                "partition {partition} out of bounds for {partitions} partitions"
            )));
        }
        if !self.scan.pushed_dyn_filters.lock().unwrap().is_empty() {
            return Err(DataFusionError::Execution(
                "dynamic filters changed after native series planning".into(),
            ));
        }
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        let span = tracing_context.attach(common_telemetry::tracing::info_span!(
            "read_from_region",
            partition = partition
        ));
        let _enter = span.enter();
        let ctx = QueryScanContext {
            explain_verbose: self.scan.explain_verbose,
        };
        let scanner = self.scan.scanner.lock().unwrap();
        let current = scanner
            .raw_series_schema(&self.label_source)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| {
                DataFusionError::Execution("raw series capability changed after planning".into())
            })?;
        let current_ordering = scanner
            .raw_series_ordering()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let current_frame =
            raw_series_frame_schema_with_ordering(&current, &self.label_source, current_ordering)?;
        if current_frame != self.arrow_schema
            || scanner.properties().num_partitions().max(1) != partitions
        {
            return Err(DataFusionError::Execution(
                "raw series schema or prepared partitions changed after planning".into(),
            ));
        }
        let raw = scanner
            .scan_series_partition(&ctx, &self.scan.metric, partition, &self.label_source)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "raw series capability disappeared during execution".into(),
                )
            })?;
        if raw.ordering() != current_ordering {
            return Err(DataFusionError::Execution(
                "raw series stream ordering does not match its planned guarantee".into(),
            ));
        }
        if raw.schema().fields.len() != self.series_schema.fields.len()
            || raw
                .schema()
                .fields
                .iter()
                .zip(self.series_schema.fields.iter())
                .any(|(a, b)| a.name != b.name)
            || raw.schema().eval_grid.is_some()
            || !same_label_source(raw.label_source(), &self.label_source)
        {
            return Err(DataFusionError::Execution(
                "raw series stream does not match its planned binding".into(),
            ));
        }
        let packed = pack_raw_series(raw)?;
        let stream = RecordBatchStreamAdapter::try_new(packed)
            .map_err(|e| DataFusionError::External(Box::new(BoxedError::new(e))))?;
        let stream: SendableRecordBatchStream = Box::pin(stream);
        let stream = if let Some(tracker) = &self.scan.query_memory_tracker {
            Box::pin(MemoryTrackedStream::new(stream, tracker.clone())) as SendableRecordBatchStream
        } else {
            stream
        };
        drop(_enter);
        Ok(Box::pin(StreamWithMetricWrapper {
            stream,
            metric: StreamMetrics::new(&self.scan.metric, partition),
            span,
            await_timer: None,
            dyn_filter: None,
        }))
    }
}

impl ExecutionPlan for RegionScanExec {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        let pushed_dyn_filters = self.pushed_dyn_filters.lock().unwrap().clone();
        apply_expression_roots(
            self.output_ordering
                .iter()
                .flat_map(|ordering| ordering.iter().map(|sort_expr| &sort_expr.expr))
                .chain(pushed_dyn_filters.iter()),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        let span = tracing_context.attach(common_telemetry::tracing::info_span!(
            "read_from_region",
            partition = partition
        ));
        let enter = span.enter();

        let ctx = QueryScanContext {
            explain_verbose: self.explain_verbose,
        };
        let stream = self
            .scanner
            .lock()
            .unwrap()
            .scan_partition(&ctx, &self.metric, partition)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let stream = if let Some(tracker) = &self.query_memory_tracker {
            Box::pin(MemoryTrackedStream::new(stream, tracker.clone()))
        } else {
            stream
        };

        drop(enter);
        let dyn_filter = {
            let filters = self.pushed_dyn_filters.lock().unwrap().clone();
            let guarded_filters = filters.into_iter().map(|filter| {
                filter.children().iter().try_fold(
                    filter.clone() as Arc<dyn PhysicalExpr>,
                    |acc, child| {
                        let guarded: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                            acc,
                            Operator::Or,
                            is_null((*child).clone())?,
                        ));
                        Ok::<_, datafusion_common::DataFusionError>(guarded)
                    },
                )
            });
            let guarded_filters = guarded_filters.collect::<DfResult<Vec<_>>>()?;
            (!guarded_filters.is_empty()).then(|| conjunction(guarded_filters))
        };
        let stream_metrics = StreamMetrics::new(&self.metric, partition);
        Ok(Box::pin(StreamWithMetricWrapper {
            stream,
            metric: stream_metrics,
            span,
            await_timer: None,
            dyn_filter,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn reset_state(self: Arc<Self>) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.scanner.lock().unwrap().reset_state();
        self.pushed_dyn_filters.lock().unwrap().clear();
        Ok(self)
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DfResult<Arc<Statistics>> {
        if args.partition().is_some() || !self.append_mode {
            return Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())));
        }

        let scanner = self.scanner.lock().unwrap();
        let statistics = if scanner.properties().total_rows_is_exact()
            && !scanner.has_predicate_without_region()
        {
            let column_statistics = self
                .arrow_schema
                .fields
                .iter()
                .map(|_| ColumnStatistics {
                    distinct_count: Precision::Exact(self.total_rows),
                    null_count: Precision::Exact(0), // all null rows are counted for append-only table
                    ..Default::default()
                })
                .collect();
            Statistics {
                num_rows: Precision::Exact(self.total_rows),
                total_byte_size: Default::default(),
                column_statistics,
            }
        } else {
            Statistics::new_unknown(&self.arrow_schema)
        };
        Ok(Arc::new(statistics))
    }

    fn name(&self) -> &str {
        REGION_SCAN_EXEC_NAME
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> DfResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|f| f.filter)
            .collect::<Vec<_>>();

        // `supported` holds the final per-parent `PushedDown` answers. Dynamic filters are accepted
        // because this scan filters decoded rows; scanner support only determines statistics pruning.
        let mut scanner_filters = Vec::with_capacity(parent_filters.len());
        // Maps compact scanner callback positions back to their original parent-filter positions.
        let mut scanner_filter_indices = Vec::with_capacity(parent_filters.len());
        let mut supported = vec![false; parent_filters.len()];
        {
            let mut exact_filters = self.pushed_dyn_filters.lock().unwrap();
            for (index, filter) in parent_filters.iter().enumerate() {
                if filter.downcast_ref::<DynamicFilterPhysicalExpr>().is_some() {
                    if exact_filters.iter().any(|existing| existing == filter) {
                        supported[index] = true;
                    } else {
                        exact_filters.push(filter.clone());
                        supported[index] = true;
                        scanner_filter_indices.push(index);
                        scanner_filters.push(filter.clone());
                    }
                } else {
                    scanner_filter_indices.push(index);
                    scanner_filters.push(filter.clone());
                }
            }
        }

        // Release the exact-filter lock before calling into the scanner.
        let scanner_supported = self.add_dyn_filters_to_predicate(scanner_filters);
        for (index, is_supported) in scanner_filter_indices.into_iter().zip(scanner_supported) {
            if parent_filters[index]
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .is_none()
            {
                supported[index] = is_supported;
            }
        }

        // DataFusion requires an updated node after installing the filters.
        let new_self = Arc::new(self.clone());

        Ok(FilterPushdownPropagation {
            filters: supported
                .into_iter()
                .map(|s| if s { PushedDown::Yes } else { PushedDown::No })
                .collect(),
            updated_node: Some(new_self),
        })
    }
}

impl DisplayAs for RegionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // The scanner contains all information needed to display the plan.
        match self.scanner.try_lock() {
            Ok(scanner) => scanner.fmt_as(t, f),
            Err(_) => write!(f, "RegionScanExec <locked>"),
        }
    }
}

pub struct StreamWithMetricWrapper {
    stream: SendableRecordBatchStream,
    metric: StreamMetrics,
    span: Span,
    await_timer: Option<Instant>,
    dyn_filter: Option<Arc<dyn PhysicalExpr>>,
}

impl Stream for StreamWithMetricWrapper {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _enter = this.span.enter();
        let poll_timer = this.metric.poll_timer();
        this.await_timer.get_or_insert(Instant::now());
        let poll_result = this.stream.poll_next_unpin(cx);
        drop(poll_timer);
        match poll_result {
            Poll::Ready(Some(result)) => {
                if let Some(instant) = this.await_timer.take() {
                    let elapsed = instant.elapsed();
                    this.metric.record_await_duration(elapsed);
                }
                match result {
                    Ok(record_batch) => {
                        let record_batch = if let Some(predicate) = &this.dyn_filter {
                            let result = {
                                let _timer = this.metric.dynamic_filter_timer();
                                batch_filter(record_batch.df_record_batch(), predicate)
                            };
                            match result {
                                Ok(batch) => RecordBatch::from_df_record_batch(
                                    record_batch.schema.clone(),
                                    batch,
                                ),
                                Err(error) => return Poll::Ready(Some(Err(error))),
                            }
                        } else {
                            record_batch
                        };
                        this.metric
                            .record_output_bytes(record_batch.logical_slice_memory_size());
                        this.metric.record_output(record_batch.num_rows());
                        Poll::Ready(Some(Ok(record_batch.into_df_record_batch())))
                    }
                    Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl DfRecordBatchStream for StreamWithMetricWrapper {
    fn schema(&self) -> ArrowSchemaRef {
        self.stream.schema().arrow_schema().clone()
    }
}

#[cfg(test)]
#[path = "scan/series_tests.rs"]
mod series_tests;

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::fmt;
    use std::sync::{Arc, Mutex};

    use api::v1::SemanticType;
    use common_error::ext::BoxedError;
    use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion::physical_plan::execution_plan::reset_plan_states;
    use datafusion::physical_plan::filter_pushdown::{
        ChildFilterPushdownResult, ChildPushdownResult,
    };
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::physical_plan::{StatisticsArgs, StatisticsContext};
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::Array;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{Int32Vector, TimestampMillisecondVector, VectorRef};
    use futures::TryStreamExt;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
    use store_api::region_engine::{
        PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties, SinglePartitionScanner,
    };
    use store_api::storage::consts::ReservedColumnId;
    use store_api::storage::{RegionId, TimeSeriesDistribution};

    use super::*;

    #[derive(Debug)]
    struct RepeatableScanner {
        batches: RecordBatches,
        properties: ScannerProperties,
        metadata: store_api::metadata::RegionMetadataRef,
        dynamic_filters: Arc<Mutex<Vec<Arc<DynamicFilterPhysicalExpr>>>>,
    }

    impl fmt::Display for RepeatableScanner {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "RepeatableScanner")
        }
    }

    impl DisplayAs for RepeatableScanner {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{self}")
        }
    }

    impl RegionScanner for RepeatableScanner {
        fn name(&self) -> &str {
            "RepeatableScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> SchemaRef {
            self.batches.schema()
        }

        fn metadata(&self) -> store_api::metadata::RegionMetadataRef {
            self.metadata.clone()
        }

        fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics_set: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> Result<SendableRecordBatchStream, BoxedError> {
            Ok(self.batches.as_stream())
        }

        fn reset_state(&mut self) {
            self.dynamic_filters.lock().unwrap().clear();
        }

        fn has_predicate_without_region(&self) -> bool {
            false
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filters: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Vec<bool> {
            let mut dynamic_filters = self.dynamic_filters.lock().unwrap();
            filters
                .into_iter()
                .map(|filter| {
                    if let Ok(filter) = (filter as Arc<dyn Any + Send + Sync>)
                        .downcast::<DynamicFilterPhysicalExpr>()
                    {
                        dynamic_filters.push(filter);
                        true
                    } else {
                        false
                    }
                })
                .collect()
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }

        fn set_query_load_region_id(&mut self, region_id: RegionId) {
            self.properties.set_query_load_region_id(region_id);
        }
    }

    fn dynamic_filter_fixture(region_number: u32) -> (SchemaRef, RegionScanExec) {
        dynamic_filter_fixture_with_nulls(region_number, false)
    }

    fn dynamic_filter_test_data(
        region_number: u32,
        nullable: bool,
    ) -> (
        SchemaRef,
        RecordBatch,
        store_api::metadata::RegionMetadataRef,
    ) {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), nullable),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let a: VectorRef = if nullable {
            Arc::new(Int32Vector::from(vec![Some(1), None, Some(3), Some(4)]))
        } else {
            Arc::new(Int32Vector::from_slice([1, 2, 3, 4]))
        };
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                a,
                Arc::new(TimestampMillisecondVector::from_slice([1, 2, 3, 4])) as _,
            ],
        )
        .unwrap();
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, region_number));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int32_datatype(), nullable),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![]);
        (schema, batch, Arc::new(builder.build().unwrap()))
    }

    fn dynamic_filter_fixture_with_nulls(
        region_number: u32,
        nullable: bool,
    ) -> (SchemaRef, RegionScanExec) {
        let (schema, batch, metadata) = dynamic_filter_test_data(region_number, nullable);
        let recordbatches = RecordBatches::try_new(schema.clone(), vec![batch]).unwrap();
        let scanner = Box::new(SinglePartitionScanner::new(
            recordbatches.as_stream(),
            false,
            metadata,
            None,
        ));
        (
            schema,
            RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap(),
        )
    }

    #[test]
    fn assign_partition_ranges_balances_rows_deterministically() {
        let ranges: Vec<_> = [100, 200, 150, 250]
            .into_iter()
            .enumerate()
            .map(|(identifier, num_rows)| PartitionRange {
                start: common_time::Timestamp::new(
                    identifier as i64,
                    common_time::timestamp::TimeUnit::Second,
                ),
                end: common_time::Timestamp::new(
                    identifier as i64 + 1,
                    common_time::timestamp::TimeUnit::Second,
                ),
                num_rows,
                identifier: identifier + 1,
            })
            .collect();
        let assigned = assign_partition_ranges(ranges.clone(), 2);
        assert_eq!(
            assigned
                .iter()
                .map(|partition| {
                    partition
                        .iter()
                        .map(|range| range.identifier)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            vec![vec![4, 1], vec![2, 3]]
        );
        let mut assigned_identifiers = assigned
            .iter()
            .flatten()
            .map(|range| range.identifier)
            .collect::<Vec<_>>();
        assigned_identifiers.sort_unstable();
        assert_eq!(assigned_identifiers, vec![1, 2, 3, 4]);

        let assigned = assign_partition_ranges(ranges.clone(), 5);
        assert_eq!(
            assigned
                .iter()
                .map(|partition| {
                    partition
                        .iter()
                        .map(|range| range.identifier)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            vec![vec![4], vec![1], vec![2], vec![], vec![3]]
        );
        let mut assigned_identifiers = assigned
            .iter()
            .flatten()
            .map(|range| range.identifier)
            .collect::<Vec<_>>();
        assigned_identifiers.sort_unstable();
        assert_eq!(assigned_identifiers, vec![1, 2, 3, 4]);

        assert_eq!(assign_partition_ranges(vec![], 5), vec![vec![]; 5]);
    }

    #[test]
    fn named_series_scan_requires_prepared_scanner_and_valid_names() {
        let (_, plan) = dynamic_filter_fixture(5686);
        let plan = Arc::new(plan);
        assert!(
            plan.clone()
                .try_to_named_series_scan("cpu", &[], &["a".to_string()])
                .unwrap()
                .is_none()
        );

        let prepared = Arc::new(
            plan.with_new_partitions(vec![vec![]], 1)
                .expect("prepare scanner partitions"),
        );
        for (tags, fields) in [
            (vec![], vec!["missing".to_string()]),
            (vec!["a".to_string()], vec!["a".to_string()]),
            (
                vec![DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string()],
                vec!["a".to_string()],
            ),
        ] {
            assert!(
                prepared
                    .clone()
                    .try_to_named_series_scan("cpu", &tags, &fields)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn test_per_series_partitioning_requires_all_primary_keys() {
        let full_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("marker", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5683));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "marker",
                    ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1, 2]);
        let metadata = Arc::new(builder.build().unwrap());

        for (schema, expected) in [
            (
                full_schema.clone(),
                Partitioning::Hash(
                    vec![
                        Arc::new(
                            Column::new_with_schema("marker", full_schema.arrow_schema()).unwrap(),
                        ) as _,
                        Arc::new(
                            Column::new_with_schema("host", full_schema.arrow_schema()).unwrap(),
                        ) as _,
                    ],
                    2,
                ),
            ),
            (
                Arc::new(Schema::new(vec![
                    ColumnSchema::new("marker", ConcreteDataType::int32_datatype(), false),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                ])),
                Partitioning::UnknownPartitioning(2),
            ),
        ] {
            let batches = RecordBatches::try_new(schema, vec![]).unwrap();
            let scanner = Box::new(RepeatableScanner {
                batches,
                properties: ScannerProperties::new(vec![vec![], vec![]], false, 0),
                metadata: metadata.clone(),
                dynamic_filters: Arc::new(Mutex::new(Vec::new())),
            });
            let request = ScanRequest {
                distribution: Some(TimeSeriesDistribution::PerSeries),
                ..Default::default()
            };
            let plan = RegionScanExec::new(scanner, request, None).unwrap();
            let actual = plan.properties().output_partitioning();
            match (&actual, &expected) {
                (
                    Partitioning::UnknownPartitioning(actual_count),
                    Partitioning::UnknownPartitioning(expected_count),
                ) => assert_eq!(actual_count, expected_count),
                _ => assert_eq!(actual, &expected),
            }
        }
    }

    /// Returns whether the scan claims the given output ordering.
    fn claims_ordering(plan: &RegionScanExec, names: &[&str]) -> bool {
        plan.properties()
            .equivalence_properties()
            .ordering_satisfy(names.iter().map(|name| {
                PhysicalSortExpr::new(
                    Arc::new(Column::new_with_schema(name, &plan.arrow_schema).unwrap()),
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                )
            }))
            .unwrap()
    }

    /// Column schema used by the ordering fixtures below.
    fn column_schema_of(name: &str) -> ColumnSchema {
        let data_type = match name {
            "ts" => ConcreteDataType::timestamp_millisecond_datatype(),
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME => ConcreteDataType::uint32_datatype(),
            DATA_SCHEMA_TSID_COLUMN_NAME => ConcreteDataType::uint64_datatype(),
            _ => ConcreteDataType::string_datatype(),
        };
        ColumnSchema::new(name, data_type, false)
    }

    /// The scan must describe its output ordering with the primary key order reported by
    /// the metadata. Sorting the names (the removed logical-table workaround) would let a
    /// projected column look like a row key prefix and claim an ordering the scanner does
    /// not produce; the time index only extends a fully projected row key.
    #[test]
    fn test_per_series_ordering_follows_metadata_pk_order() {
        // Deliberately not alphabetical: sorting these names would give `[a, b, z]`.
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5690));
        for (index, name) in ["z", "a", "b", "ts"].into_iter().enumerate() {
            builder.push_column_metadata(ColumnMetadata {
                column_schema: column_schema_of(name),
                semantic_type: if name == "ts" {
                    SemanticType::Timestamp
                } else {
                    SemanticType::Tag
                },
                column_id: index as u32 + 1,
            });
        }
        builder.primary_key(vec![1, 2, 3]);
        let metadata = Arc::new(builder.build().unwrap());

        // The claims must not depend on the scanner reporting a logical region.
        for logical_region in [false, true] {
            for (projection, claimed, unclaimed) in [
                // Missing the first key: neither the remaining keys nor the time index are
                // ordered.
                (
                    vec!["a", "b", "ts"],
                    vec![],
                    vec![vec!["a"], vec!["b"], vec!["ts"], vec!["a", "b"]],
                ),
                // Missing a key in the middle: only the prefix before it is ordered.
                (
                    vec!["z", "b", "ts"],
                    vec![vec!["z"]],
                    vec![vec!["b"], vec!["z", "b"], vec!["z", "ts"]],
                ),
                // A valid but incomplete prefix: the time index is not ordered after it.
                (
                    vec!["z", "a", "ts"],
                    vec![vec!["z"], vec!["z", "a"]],
                    vec![vec!["a", "z"], vec!["z", "a", "ts"]],
                ),
                // The whole row key followed by the time index.
                (
                    vec!["z", "a", "b", "ts"],
                    vec![vec!["z", "a", "b"], vec!["z", "a", "b", "ts"]],
                    vec![vec!["a"], vec!["a", "b", "z"]],
                ),
            ] {
                let schema = Arc::new(Schema::new(
                    projection
                        .iter()
                        .map(|name| column_schema_of(name))
                        .collect(),
                ));
                let mut properties = ScannerProperties::new(vec![vec![], vec![]], false, 0);
                properties.set_logical_region(logical_region);
                let scanner = Box::new(RepeatableScanner {
                    batches: RecordBatches::try_new(schema, vec![]).unwrap(),
                    properties,
                    metadata: metadata.clone(),
                    dynamic_filters: Arc::new(Mutex::new(Vec::new())),
                });
                let plan = RegionScanExec::new(
                    scanner,
                    ScanRequest {
                        distribution: Some(TimeSeriesDistribution::PerSeries),
                        ..Default::default()
                    },
                    None,
                )
                .unwrap();

                for ordering in claimed {
                    assert!(
                        claims_ordering(&plan, &ordering),
                        "logical_region={logical_region} projection {projection:?} must claim \
                         ordering {ordering:?}"
                    );
                }
                for ordering in unclaimed {
                    assert!(
                        !claims_ordering(&plan, &ordering),
                        "logical_region={logical_region} projection {projection:?} must not \
                         claim ordering {ordering:?}"
                    );
                }
            }
        }
    }

    /// Metric TSID properties require projecting `__tsid` and restricting the scan to one
    /// `__table_id`; hiding the physical row-key prefix must not claim a visible tag or time as
    /// order or hash partition columns.
    #[test]
    fn test_metric_projection_claims_only_projected_row_key_prefix() {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5691));
        for (name, column_id) in [
            (
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ReservedColumnId::table_id(),
            ),
            (DATA_SCHEMA_TSID_COLUMN_NAME, ReservedColumnId::tsid()),
            ("A", 1),
            ("marker", 2),
            ("ts", 3),
        ] {
            builder.push_column_metadata(ColumnMetadata {
                column_schema: column_schema_of(name),
                semantic_type: if name == "ts" {
                    SemanticType::Timestamp
                } else {
                    SemanticType::Tag
                },
                column_id,
            });
        }
        // The physical row key order: `__table_id`, `__tsid`, then the tag columns.
        builder.primary_key(vec![
            ReservedColumnId::table_id(),
            ReservedColumnId::tsid(),
            1,
            2,
        ]);
        let metadata = Arc::new(builder.build().unwrap());

        for (projection, partitioned_by_tsid, claimed, unclaimed) in [
            // The logical projection drops the row key prefix, so `A` is not a prefix of the
            // row key and neither `A` nor `ts` is ordered.
            (
                vec!["A", "marker", "ts"],
                false,
                vec![],
                vec![vec!["A"], vec!["marker"], vec!["ts"], vec!["A", "ts"]],
            ),
            // Projecting internal `__tsid` plus the single-table filter preserves metric TSID properties.
            (
                vec![DATA_SCHEMA_TSID_COLUMN_NAME, "A", "marker", "ts"],
                true,
                vec![
                    vec![DATA_SCHEMA_TSID_COLUMN_NAME],
                    vec![DATA_SCHEMA_TSID_COLUMN_NAME, "ts"],
                ],
                vec![vec!["A"], vec!["ts"], vec!["A", "ts"]],
            ),
        ] {
            let schema = Arc::new(Schema::new(
                projection
                    .iter()
                    .map(|name| column_schema_of(name))
                    .collect(),
            ));
            let mut properties = ScannerProperties::new(vec![vec![], vec![]], false, 0);
            properties.set_logical_region(true);
            let scanner = Box::new(RepeatableScanner {
                batches: RecordBatches::try_new(schema, vec![]).unwrap(),
                properties,
                metadata: metadata.clone(),
                dynamic_filters: Arc::new(Mutex::new(Vec::new())),
            });
            let plan = RegionScanExec::new(
                scanner,
                ScanRequest {
                    distribution: Some(TimeSeriesDistribution::PerSeries),
                    filters: if partitioned_by_tsid {
                        vec![
                            datafusion_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                                .eq(datafusion_expr::lit(42u32)),
                        ]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                },
                None,
            )
            .unwrap();

            let expected_partitioning = if partitioned_by_tsid {
                Partitioning::Hash(
                    vec![Arc::new(
                        Column::new_with_schema(DATA_SCHEMA_TSID_COLUMN_NAME, &plan.arrow_schema)
                            .unwrap(),
                    ) as _],
                    2,
                )
            } else {
                Partitioning::UnknownPartitioning(2)
            };
            match (&plan.properties.partitioning, &expected_partitioning) {
                (
                    Partitioning::UnknownPartitioning(actual_count),
                    Partitioning::UnknownPartitioning(expected_count),
                ) => assert_eq!(actual_count, expected_count, "projection {projection:?}"),
                (actual, expected) => assert_eq!(actual, expected, "projection {projection:?}"),
            }

            for ordering in claimed {
                assert!(
                    claims_ordering(&plan, &ordering),
                    "projection {projection:?} must claim ordering {ordering:?}"
                );
            }
            for ordering in unclaimed {
                assert!(
                    !claims_ordering(&plan, &ordering),
                    "projection {projection:?} must not claim ordering {ordering:?}"
                );
            }
        }
    }

    #[test]
    fn test_count_statistics_require_exact_source_rows() {
        for (append_mode, exact, expected) in [
            (true, false, Precision::Absent),
            (true, true, Precision::Exact(3)),
            (false, true, Precision::Absent),
        ] {
            let (schema, batch, metadata) = dynamic_filter_test_data(5685, false);
            let scanner = Box::new(RepeatableScanner {
                batches: RecordBatches::try_new(schema, vec![batch]).unwrap(),
                properties: ScannerProperties::default()
                    .with_append_mode(append_mode)
                    .with_total_rows(3)
                    .with_total_rows_is_exact(exact),
                metadata,
                dynamic_filters: Arc::new(Mutex::new(Vec::new())),
            });
            let plan = RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap();
            assert_eq!(
                StatisticsContext::new()
                    .compute(&plan, &StatisticsArgs::new())
                    .unwrap()
                    .num_rows,
                expected,
            );
            assert_eq!(
                StatisticsContext::new()
                    .compute(&plan, &StatisticsArgs::new().with_partition(Some(0)))
                    .unwrap()
                    .num_rows,
                Precision::Absent,
            );
        }
    }

    #[tokio::test]
    async fn test_reset_state_clears_dynamic_filters_for_repeatable_scanner() {
        let ctx = SessionContext::new();
        let (schema, batch, metadata) = dynamic_filter_test_data(5684, false);
        let scanner_dynamic_filters = Arc::new(Mutex::new(Vec::new()));
        let scanner = RepeatableScanner {
            batches: RecordBatches::try_new(schema.clone(), vec![batch.clone()]).unwrap(),
            properties: ScannerProperties::default(),
            metadata,
            dynamic_filters: scanner_dynamic_filters.clone(),
        };
        let plan =
            Arc::new(RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None).unwrap());
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter: dynamic_filter.clone(),
                    child_results: vec![PushedDown::No],
                }],
                self_filters: vec![],
            },
            &datafusion::config::ConfigOptions::default(),
        )
        .unwrap();
        dynamic_filter
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                lit(2_i32),
            )))
            .unwrap();

        assert_eq!(
            plan.execute(0, ctx.task_ctx())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()[0]
                .num_rows(),
            2
        );
        assert_eq!(
            batch_filter(
                batch.df_record_batch(),
                &(scanner_dynamic_filters.lock().unwrap()[0].clone() as Arc<dyn PhysicalExpr>)
            )
            .unwrap()
            .num_rows(),
            2
        );

        let reset_plan = reset_plan_states(plan).unwrap();
        assert!(scanner_dynamic_filters.lock().unwrap().is_empty());
        assert_eq!(
            batch_filter(
                batch.df_record_batch(),
                &(dynamic_filter.clone() as Arc<dyn PhysicalExpr>),
            )
            .unwrap()
            .num_rows(),
            2
        );
        dynamic_filter.update(lit(false)).unwrap();
        assert_eq!(
            reset_plan
                .execute(0, ctx.task_ctx())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()[0]
                .num_rows(),
            4
        );

        let fresh_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        reset_plan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: fresh_filter.clone(),
                        child_results: vec![PushedDown::No],
                    }],
                    self_filters: vec![],
                },
                &datafusion::config::ConfigOptions::default(),
            )
            .unwrap();
        let stream = reset_plan.execute(0, ctx.task_ctx()).unwrap();
        fresh_filter
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                lit(3_i32),
            )))
            .unwrap();
        assert_eq!(
            stream.try_collect::<Vec<_>>().await.unwrap()[0].num_rows(),
            1
        );
    }

    #[test]
    fn test_apply_expressions_visits_pushed_dynamic_filters() {
        let (_, plan) = dynamic_filter_fixture(5685);
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let propagation = plan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: dynamic_filter.clone(),
                        child_results: vec![PushedDown::No],
                    }],
                    self_filters: vec![],
                },
                &datafusion::config::ConfigOptions::default(),
            )
            .unwrap();
        assert!(matches!(propagation.filters.as_slice(), [PushedDown::Yes]));

        let mut visited = 0;
        assert_eq!(
            plan.apply_expressions(&mut |expr| {
                visited += 1;
                assert!(plan.pushed_dyn_filters.try_lock().is_ok());
                assert_eq!(expr.expression_id(), dynamic_filter.expression_id());
                assert_eq!(
                    expr.downcast_ref::<DynamicFilterPhysicalExpr>().unwrap(),
                    dynamic_filter.as_ref()
                );
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap(),
            TreeNodeRecursion::Continue
        );
        assert_eq!(visited, 1);

        assert_eq!(
            plan.apply_expressions(&mut |_| Ok(TreeNodeRecursion::Stop))
                .unwrap(),
            TreeNodeRecursion::Stop
        );
    }

    #[tokio::test]
    async fn test_live_dynamic_filter_is_applied_after_stream_creation() {
        let ctx = SessionContext::new();
        let (schema, plan) = dynamic_filter_fixture(5679);
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let static_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(0_i32),
        ));
        let propagation = plan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![
                        ChildFilterPushdownResult {
                            filter: filter.clone(),
                            child_results: vec![PushedDown::No],
                        },
                        ChildFilterPushdownResult {
                            filter: static_filter,
                            child_results: vec![PushedDown::No],
                        },
                    ],
                    self_filters: vec![],
                },
                &datafusion::config::ConfigOptions::default(),
            )
            .unwrap();
        assert!(matches!(
            propagation.filters.as_slice(),
            [PushedDown::Yes, PushedDown::No]
        ));
        assert!(
            propagation
                .updated_node
                .as_ref()
                .unwrap()
                .downcast_ref::<RegionScanExec>()
                .is_some()
        );
        assert_eq!(plan.pushed_dyn_filters.lock().unwrap().len(), 1);
        let repeated = plan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: filter.clone(),
                        child_results: vec![PushedDown::No],
                    }],
                    self_filters: vec![],
                },
                &datafusion::config::ConfigOptions::default(),
            )
            .unwrap();
        assert!(matches!(repeated.filters.as_slice(), [PushedDown::Yes]));
        assert_eq!(plan.pushed_dyn_filters.lock().unwrap().len(), 1);
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        filter
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                lit(2_i32),
            )))
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[3, 4]);
        let metrics = plan.metrics().unwrap();
        assert_eq!(metrics.output_rows(), Some(2));
        let filter_cost = metrics.iter().find_map(|metric| match metric.value() {
            MetricValue::Time { name, time } if name == "dynamic_filter_cost" => Some(time.value()),
            _ => None,
        });
        assert!(filter_cost.is_some_and(|cost| cost > 0));
        let output_bytes = metrics.iter().find_map(|metric| match metric.value() {
            MetricValue::OutputBytes(count) => Some(count.value()),
            _ => None,
        });
        assert_eq!(
            output_bytes,
            Some(
                RecordBatch::from_df_record_batch(schema, batches[0].clone())
                    .logical_slice_memory_size(),
            )
        );
    }

    #[tokio::test]
    async fn test_dynamic_filter_keeps_null_rows() {
        let ctx = SessionContext::new();
        let (_, plan) = dynamic_filter_fixture_with_nulls(5681, true);
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter: filter.clone(),
                    child_results: vec![PushedDown::No],
                }],
                self_filters: vec![],
            },
            &datafusion::config::ConfigOptions::default(),
        )
        .unwrap();
        filter
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                lit(2_i32),
            )))
            .unwrap();

        let batches = plan
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 3);
        assert_eq!(values.value(2), 4);
    }

    #[tokio::test]
    async fn test_multi_child_dynamic_filter_guards_each_child() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Vector::from(vec![Some(1), None, Some(3)])) as _,
                Arc::new(Int32Vector::from(vec![Some(10), Some(20), None])) as _,
                Arc::new(TimestampMillisecondVector::from_slice([1, 2, 3])) as _,
            ],
        )
        .unwrap();
        let recordbatches = RecordBatches::try_new(schema.clone(), vec![batch]).unwrap();
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5683));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![]);
        let metadata = Arc::new(builder.build().unwrap());
        let scanner = Box::new(SinglePartitionScanner::new(
            recordbatches.as_stream(),
            false,
            metadata,
            None,
        ));
        let plan = RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap();
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![
                Arc::new(Column::new("a", 0)),
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("b", 1)),
                    Operator::Plus,
                    lit(1_i32),
                )),
            ],
            lit(true),
        ));
        plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter: filter.clone(),
                    child_results: vec![PushedDown::No],
                }],
                self_filters: vec![],
            },
            &datafusion::config::ConfigOptions::default(),
        )
        .unwrap();
        filter
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Eq,
                lit(datafusion_common::ScalarValue::Int32(None)),
            )))
            .unwrap();

        let batches = plan
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 3);
    }

    #[tokio::test]
    async fn test_multiple_dynamic_filters_are_conjoined() {
        let ctx = SessionContext::new();
        let (_, plan) = dynamic_filter_fixture(5680);
        let lower = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let upper = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let propagation = plan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![
                        ChildFilterPushdownResult {
                            filter: lower.clone(),
                            child_results: vec![PushedDown::No],
                        },
                        ChildFilterPushdownResult {
                            filter: upper.clone(),
                            child_results: vec![PushedDown::No],
                        },
                    ],
                    self_filters: vec![],
                },
                &datafusion::config::ConfigOptions::default(),
            )
            .unwrap();
        assert!(matches!(
            propagation.filters.as_slice(),
            [PushedDown::Yes, PushedDown::Yes]
        ));
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        lower
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                lit(2_i32),
            )))
            .unwrap();
        upper
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Lt,
                lit(3_i32),
            )))
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(plan.pushed_dyn_filters.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_simple_table_scan() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new(
                "b",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));

        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Vector::from_slice([1, 2])) as _,
                Arc::new(TimestampMillisecondVector::from_slice([1000, 2000])) as _,
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Vector::from_slice([3, 4, 5])) as _,
                Arc::new(TimestampMillisecondVector::from_slice([3000, 4000, 5000])) as _,
            ],
        )
        .unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "b",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![1]);
        let region_metadata = Arc::new(builder.build().unwrap());

        let scanner = Box::new(SinglePartitionScanner::new(
            stream,
            false,
            region_metadata,
            None,
        ));
        let plan = RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap();
        let actual: SchemaRef = Arc::new(
            plan.properties
                .eq_properties
                .schema()
                .clone()
                .try_into()
                .unwrap(),
        );
        assert_eq!(actual, schema);

        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let recordbatches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batch1.df_record_batch(), &recordbatches[0]);
        assert_eq!(batch2.df_record_batch(), &recordbatches[1]);

        let result = plan.execute(0, ctx.task_ctx());
        assert!(result.is_ok());
    }
}
