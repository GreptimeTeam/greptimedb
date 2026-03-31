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

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ahash::{HashMap, HashSet};
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, SortOptions};
use async_stream::stream;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_plugins::GREPTIME_EXEC_READ_COST;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_telemetry::tracing_context::TracingContext;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{Column as ColumnExpr, DataFusionError, Result};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, PhysicalSortExpr,
};
use futures_util::StreamExt;
use greptime_proto::v1::region::RegionRequestHeader;
use meter_core::data::ReadItem;
use meter_macros::read_meter;
use promql::extension_plan::{
    InstantManipulate, RangeManipulate, ScalarCalculate, SeriesDivide, SeriesNormalize,
};
use session::context::QueryContextRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
use store_api::storage::RegionId;
use table::table_name::TableName;
use tokio::time::Instant;
use tracing::{Instrument, Span};

use crate::dist_plan::analyzer::AliasMapping;
use crate::dist_plan::analyzer::utils::patch_batch_timezone;
use crate::metrics::{MERGE_SCAN_ERRORS_TOTAL, MERGE_SCAN_POLL_ELAPSED, MERGE_SCAN_REGIONS};
use crate::region_query::RegionQueryHandlerRef;

#[derive(Debug, Hash, PartialOrd, PartialEq, Eq, Clone)]
pub struct MergeScanLogicalPlan {
    /// In logical plan phase it only contains one input
    input: LogicalPlan,
    /// If this plan is a placeholder
    is_placeholder: bool,
    partition_cols: AliasMapping,
}

impl UserDefinedLogicalNodeCore for MergeScanLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    // Prevent further optimization.
    // The input can be retrieved by `self.input()`
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    // Prevent further optimization
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MergeScan [is_placeholder={}, remote_input=[\n{}\n]]",
            self.is_placeholder, self.input
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
    }
}

impl MergeScanLogicalPlan {
    pub fn new(input: LogicalPlan, is_placeholder: bool, partition_cols: AliasMapping) -> Self {
        Self {
            input,
            is_placeholder,
            partition_cols,
        }
    }

    pub fn name() -> &'static str {
        "MergeScan"
    }

    /// Create a [LogicalPlan::Extension] node from this merge scan plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn is_placeholder(&self) -> bool {
        self.is_placeholder
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn partition_cols(&self) -> &AliasMapping {
        &self.partition_cols
    }
}

pub struct MergeScanExec {
    table: TableName,
    regions: Vec<RegionId>,
    plan: LogicalPlan,
    arrow_schema: ArrowSchemaRef,
    region_query_handler: RegionQueryHandlerRef,
    metric: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
    /// Metrics from sub stages
    sub_stage_metrics: Arc<Mutex<HashMap<RegionId, RecordBatchMetrics>>>,
    /// Metrics for each partition
    partition_metrics: Arc<Mutex<HashMap<usize, PartitionMetrics>>>,
    query_ctx: QueryContextRef,
    target_partition: usize,
    partition_cols: AliasMapping,
}

impl std::fmt::Debug for MergeScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeScanExec")
            .field("table", &self.table)
            .field("regions", &self.regions)
            .field("plan", &self.plan)
            .finish()
    }
}

impl MergeScanExec {
    fn default_partition_exprs(
        session_state: &SessionState,
        plan: &LogicalPlan,
        partition_cols: &AliasMapping,
    ) -> Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
        partition_cols
            .iter()
            .filter_map(|col| Self::partition_expr_for_alias(session_state, plan, col.1.first()))
            .collect()
    }

    fn partition_expr_for_alias(
        session_state: &SessionState,
        plan: &LogicalPlan,
        column: Option<&ColumnExpr>,
    ) -> Option<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
        let column = column?;
        session_state
            .create_physical_expr(
                Expr::Column(ColumnExpr::new_unqualified(column.name().to_string())),
                plan.schema(),
            )
            .ok()
    }

    fn prefer_tsid_partition_exprs(
        session_state: &SessionState,
        plan: &LogicalPlan,
        partition_cols: &AliasMapping,
    ) -> Option<Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>> {
        Self::promql_tsid_ordered_time_index(plan)?;

        let tsid_aliases = partition_cols.get(DATA_SCHEMA_TSID_COLUMN_NAME)?;
        let tsid_expr = Self::partition_expr_for_alias(session_state, plan, tsid_aliases.first())?;
        Some(vec![tsid_expr])
    }

    fn promql_tsid_ordered_time_index(plan: &LogicalPlan) -> Option<String> {
        let time_index_column = match plan {
            LogicalPlan::Sort(sort) => {
                if sort.expr.len() != 2 {
                    return None;
                }

                let [tsid_sort, time_sort] = sort.expr.as_slice() else {
                    return None;
                };

                let tsid_column = Self::ascending_nulls_first_sort_column(tsid_sort)?;
                let time_column = Self::ascending_nulls_first_sort_column(time_sort)?;
                (tsid_column == DATA_SCHEMA_TSID_COLUMN_NAME).then_some(time_column)
            }
            LogicalPlan::Projection(projection) => {
                Self::promql_tsid_ordered_time_index(projection.input.as_ref())
            }
            LogicalPlan::Filter(filter) => {
                Self::promql_tsid_ordered_time_index(filter.input.as_ref())
            }
            LogicalPlan::SubqueryAlias(alias) => {
                Self::promql_tsid_ordered_time_index(alias.input.as_ref())
            }
            LogicalPlan::Extension(extension) if Self::is_promql_passthrough_node(extension) => {
                extension
                    .node
                    .inputs()
                    .first()
                    .and_then(|input| Self::promql_tsid_ordered_time_index(input))
            }
            _ => None,
        }?;

        let schema = plan.schema();
        let has_tsid = schema
            .index_of_column_by_name(None, DATA_SCHEMA_TSID_COLUMN_NAME)
            .is_some();
        let has_time_index = schema
            .index_of_column_by_name(None, &time_index_column)
            .is_some();

        (has_tsid && has_time_index).then_some(time_index_column)
    }

    fn is_promql_passthrough_node(extension: &Extension) -> bool {
        let node = extension.node.as_any();
        node.is::<InstantManipulate>()
            || node.is::<SeriesDivide>()
            || node.is::<SeriesNormalize>()
            || node.is::<ScalarCalculate>()
            || node.is::<RangeManipulate>()
    }

    fn ascending_nulls_first_sort_column(
        sort_expr: &datafusion_expr::expr::Sort,
    ) -> Option<String> {
        (sort_expr.asc && sort_expr.nulls_first)
            .then(|| sort_expr.expr.try_as_col().map(|col| col.name.clone()))
            .flatten()
    }

    fn schema_exposes_column(plan: &LogicalPlan, column_name: &str) -> bool {
        plan.schema()
            .index_of_column_by_name(None, column_name)
            .is_some()
    }

    pub(crate) fn logical_sort_ordering(
        session_state: &SessionState,
        plan: &LogicalPlan,
    ) -> Result<Option<LexOrdering>> {
        if let Some(time_index_column) = Self::promql_tsid_ordered_time_index(plan) {
            if !Self::schema_exposes_column(plan, DATA_SCHEMA_TSID_COLUMN_NAME)
                || !Self::schema_exposes_column(plan, &time_index_column)
            {
                return Ok(None);
            }

            let tsid_expr = session_state.create_physical_expr(
                Expr::Column(ColumnExpr::new_unqualified(
                    DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                )),
                plan.schema(),
            )?;
            let time_expr = session_state.create_physical_expr(
                Expr::Column(ColumnExpr::new_unqualified(time_index_column)),
                plan.schema(),
            )?;
            return Ok(LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    tsid_expr,
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                ),
                PhysicalSortExpr::new(
                    time_expr,
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                ),
            ]));
        }

        let LogicalPlan::Sort(sort) = plan else {
            return Ok(None);
        };

        let lex_ordering = LexOrdering::new(
            sort.expr
                .iter()
                .map(|sort_expr| {
                    let physical_expr = session_state
                        .create_physical_expr(sort_expr.expr.clone(), plan.schema())?;
                    Ok(PhysicalSortExpr::new(
                        physical_expr,
                        SortOptions {
                            descending: !sort_expr.asc,
                            nulls_first: sort_expr.nulls_first,
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        )
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Sort plan must contain at least one expression: {plan}"
            ))
        })?;
        Ok(Some(lex_ordering))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_state: &SessionState,
        table: TableName,
        regions: Vec<RegionId>,
        plan: LogicalPlan,
        remote_orderings: Vec<LexOrdering>,
        arrow_schema: &ArrowSchema,
        region_query_handler: RegionQueryHandlerRef,
        query_ctx: QueryContextRef,
        target_partition: usize,
        partition_cols: AliasMapping,
    ) -> Result<Self> {
        // TODO(CookiePieWw): Initially we removed the metadata from the schema in #2000, but we have to
        // keep it for #4619 to identify json type in src/datatypes/src/schema/column_schema.rs.
        // Reconsider if it's possible to remove it.
        let arrow_schema = Arc::new(arrow_schema.clone());

        // States the output ordering of the plan.
        //
        // When the input plan is a sort, we can use the sort ordering as the output ordering
        // if the target partition is greater than the number of regions, which means we won't
        // break the ordering on merging (of MergeScan).
        //
        // Otherwise, we need to use the default ordering.
        let eq_properties = if target_partition >= regions.len() {
            if !remote_orderings.is_empty() {
                EquivalenceProperties::new_with_orderings(arrow_schema.clone(), remote_orderings)
            } else if let Some(ordering) = Self::logical_sort_ordering(session_state, &plan)? {
                EquivalenceProperties::new_with_orderings(arrow_schema.clone(), vec![ordering])
            } else {
                EquivalenceProperties::new(arrow_schema.clone())
            }
        } else {
            EquivalenceProperties::new(arrow_schema.clone())
        };

        let partition_exprs =
            Self::prefer_tsid_partition_exprs(session_state, &plan, &partition_cols)
                .unwrap_or_else(|| {
                    Self::default_partition_exprs(session_state, &plan, &partition_cols)
                });
        let partitioning = Partitioning::Hash(partition_exprs, target_partition);

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table,
            regions,
            plan,
            arrow_schema,
            region_query_handler,
            metric: ExecutionPlanMetricsSet::new(),
            sub_stage_metrics: Arc::default(),
            partition_metrics: Arc::default(),
            properties,
            query_ctx,
            target_partition,
            partition_cols,
        })
    }

    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        // prepare states to move
        let regions = self.regions.clone();
        let region_query_handler = self.region_query_handler.clone();
        let metric = MergeScanMetric::new(&self.metric);
        let arrow_schema = self.arrow_schema.clone();
        let query_ctx = self.query_ctx.clone();
        let sub_stage_metrics_moved = self.sub_stage_metrics.clone();
        let partition_metrics_moved = self.partition_metrics.clone();
        let plan = self.plan.clone();
        let target_partition = self.target_partition;
        let dbname = context.task_id().unwrap_or_default();
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        let current_channel = self.query_ctx.channel();
        let read_preference = self.query_ctx.read_preference();
        let explain_verbose = self.query_ctx.explain_verbose();

        let stream = Box::pin(stream!({
            // only report metrics once for each MergeScan
            if partition == 0 {
                MERGE_SCAN_REGIONS.observe(regions.len() as f64);
            }

            let _finish_timer = metric.finish_time().timer();
            let mut ready_timer = metric.ready_time().timer();
            let mut first_consume_timer = Some(metric.first_consume_time().timer());

            for region_id in regions
                .iter()
                .skip(partition)
                .step_by(target_partition)
                .copied()
            {
                let region_span = tracing_context.attach(tracing::info_span!(
                    parent: &Span::current(),
                    "merge_scan_region",
                    region_id = %region_id,
                    partition = partition
                ));
                let request = QueryRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: tracing_context.to_w3c(),
                        dbname: dbname.clone(),
                        query_context: Some(query_ctx.as_ref().into()),
                    }),
                    region_id,
                    plan: plan.clone(),
                };
                let region_start = Instant::now();
                let do_get_start = Instant::now();

                if explain_verbose {
                    common_telemetry::info!(
                        "Merge scan one region, partition: {}, region_id: {}",
                        partition,
                        region_id
                    );
                }

                let mut stream = region_query_handler
                    .do_get(read_preference, request)
                    .instrument(region_span.clone())
                    .await
                    .map_err(|e| {
                        MERGE_SCAN_ERRORS_TOTAL.inc();
                        DataFusionError::External(Box::new(e))
                    })?;
                let do_get_cost = do_get_start.elapsed();

                ready_timer.stop();

                let mut poll_duration = Duration::ZERO;
                let mut poll_timer = Instant::now();
                while let Some(batch) = stream.next().instrument(region_span.clone()).await {
                    let poll_elapsed = poll_timer.elapsed();
                    poll_duration += poll_elapsed;

                    let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let batch = patch_batch_timezone(
                        arrow_schema.clone(),
                        batch.into_df_record_batch().columns().to_vec(),
                    )?;
                    metric.record_output_batch_rows(batch.num_rows());
                    if let Some(mut first_consume_timer) = first_consume_timer.take() {
                        first_consume_timer.stop();
                    }

                    if let Some(metrics) = stream.metrics() {
                        let mut sub_stage_metrics = sub_stage_metrics_moved.lock().unwrap();
                        sub_stage_metrics.insert(region_id, metrics);
                    }

                    yield Ok(batch);
                    // reset poll timer
                    poll_timer = Instant::now();
                }
                let total_cost = region_start.elapsed();

                // Record region metrics and push to global partition_metrics
                let region_metrics = RegionMetrics {
                    region_id,
                    poll_duration,
                    do_get_cost,
                    total_cost,
                };

                // Push RegionMetrics to global partition_metrics immediately after scanning this region
                {
                    let mut partition_metrics_guard = partition_metrics_moved.lock().unwrap();
                    let partition_metrics = partition_metrics_guard
                        .entry(partition)
                        .or_insert_with(|| PartitionMetrics::new(partition, explain_verbose));
                    partition_metrics.add_region_metrics(region_metrics);
                }

                if explain_verbose {
                    common_telemetry::info!(
                        "Merge scan finish one region, partition: {}, region_id: {}, poll_duration: {:?}, first_consume: {}, do_get_cost: {:?}",
                        partition,
                        region_id,
                        poll_duration,
                        metric.first_consume_time(),
                        do_get_cost
                    );
                }

                // process metrics after all data is drained.
                if let Some(metrics) = stream.metrics() {
                    let (c, s) = parse_catalog_and_schema_from_db_string(&dbname);
                    let value = read_meter!(
                        c,
                        s,
                        ReadItem {
                            cpu_time: metrics.elapsed_compute as u64,
                            table_scan: metrics.memory_usage as u64
                        },
                        current_channel as u8
                    );
                    metric.record_greptime_exec_cost(value as usize);

                    // record metrics from sub sgates
                    let mut sub_stage_metrics = sub_stage_metrics_moved.lock().unwrap();
                    sub_stage_metrics.insert(region_id, metrics);
                }

                MERGE_SCAN_POLL_ELAPSED.observe(poll_duration.as_secs_f64());
            }

            // Finish partition metrics and log results
            {
                let mut partition_metrics_guard = partition_metrics_moved.lock().unwrap();
                if let Some(partition_metrics) = partition_metrics_guard.get_mut(&partition) {
                    partition_metrics.finish();
                }
            }
        }));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.arrow_schema.clone(),
            stream,
        )))
    }

    pub fn try_with_new_distribution(&self, distribution: Distribution) -> Option<Self> {
        let Distribution::HashPartitioned(hash_exprs) = distribution else {
            // not applicable
            return None;
        };

        let all_partition_col_aliases: HashSet<_> = self
            .partition_cols
            .values()
            .flat_map(|aliases| aliases.iter().map(|c| c.name()))
            .collect();
        let mut overlaps = hash_exprs
            .iter()
            .filter(|expr| {
                expr.as_any()
                    .downcast_ref::<Column>()
                    .is_some_and(|col_expr| all_partition_col_aliases.contains(col_expr.name()))
            })
            .cloned()
            .collect::<Vec<_>>();

        // Metric-engine scans can satisfy any hash distribution that includes `__tsid`.
        // Equal requested keys must also share the same `__tsid`, and equal `__tsid` values are
        // guaranteed to stay co-located across MergeScan partitions. Advertise the full requested
        // distribution so EnforceDistribution can skip redundant reshuffles.
        if self
            .arrow_schema
            .column_with_name(DATA_SCHEMA_TSID_COLUMN_NAME)
            .is_some()
            && hash_exprs.iter().any(|expr| {
                expr.as_any()
                    .downcast_ref::<Column>()
                    .is_some_and(|col_expr| col_expr.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
            })
        {
            overlaps = hash_exprs.clone();
        }

        if overlaps.is_empty() {
            return None;
        }

        if let Partitioning::Hash(curr_dist, _) = &self.properties.partitioning
            && curr_dist == &overlaps
        {
            // No need to change the distribution.
            return None;
        }

        Some(Self {
            table: self.table.clone(),
            regions: self.regions.clone(),
            plan: self.plan.clone(),
            arrow_schema: self.arrow_schema.clone(),
            region_query_handler: self.region_query_handler.clone(),
            metric: self.metric.clone(),
            properties: Arc::new(PlanProperties::new(
                self.properties.eq_properties.clone(),
                Partitioning::Hash(overlaps, self.target_partition),
                self.properties.emission_type,
                self.properties.boundedness,
            )),
            sub_stage_metrics: self.sub_stage_metrics.clone(),
            partition_metrics: self.partition_metrics.clone(),
            query_ctx: self.query_ctx.clone(),
            target_partition: self.target_partition,
            partition_cols: self.partition_cols.clone(),
        })
    }

    pub fn sub_stage_metrics(&self) -> Vec<RecordBatchMetrics> {
        self.sub_stage_metrics
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn partition_count(&self) -> usize {
        self.target_partition
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    fn partition_metrics(&self) -> Vec<PartitionMetrics> {
        self.partition_metrics
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use async_trait::async_trait;
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{EmptyRelation, Extension, LogicalPlan, LogicalPlanBuilder, col};
    use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
    use promql::extension_plan::{InstantManipulate, SeriesDivide};
    use session::ReadPreference;
    use session::context::QueryContext;

    use super::*;
    use crate::error::Result as QueryResult;
    use crate::region_query::RegionQueryHandler;

    struct NoopRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for NoopRegionQueryHandler {
        async fn do_get(
            &self,
            _read_preference: ReadPreference,
            _request: QueryRequest,
        ) -> QueryResult<SendableRecordBatchStream> {
            unreachable!("merge scan distribution tests should not execute remote queries")
        }
    }

    #[test]
    fn try_with_new_distribution_satisfies_tsid_hash_requirements() {
        let merge_scan = test_merge_scan_exec(
            BTreeMap::from([
                (
                    "host".to_string(),
                    BTreeSet::from([ColumnExpr::from_name("host")]),
                ),
                (
                    DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                    BTreeSet::from([ColumnExpr::from_name(DATA_SCHEMA_TSID_COLUMN_NAME)]),
                ),
            ]),
            vec![
                partition_column("host", 0),
                partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 1),
            ],
        );

        let optimized = merge_scan
            .try_with_new_distribution(Distribution::HashPartitioned(vec![
                partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 1),
                partition_column("greptime_timestamp", 2),
            ]))
            .unwrap();

        let Partitioning::Hash(ref exprs, partition_count) = optimized.properties.partitioning
        else {
            panic!("expected hash partitioning");
        };
        assert_eq!(partition_count, 32);
        assert_eq!(
            column_names(exprs),
            vec![DATA_SCHEMA_TSID_COLUMN_NAME, "greptime_timestamp"]
        );
    }

    #[test]
    fn try_with_new_distribution_keeps_regular_partition_overlap() {
        let merge_scan = test_merge_scan_exec(
            BTreeMap::from([(
                "host".to_string(),
                BTreeSet::from([ColumnExpr::from_name("host")]),
            )]),
            vec![partition_column("greptime_timestamp", 2)],
        );

        let optimized = merge_scan
            .try_with_new_distribution(Distribution::HashPartitioned(vec![
                partition_column("host", 0),
                partition_column("greptime_timestamp", 2),
            ]))
            .unwrap();

        let Partitioning::Hash(ref exprs, partition_count) = optimized.properties.partitioning
        else {
            panic!("expected hash partitioning");
        };
        assert_eq!(partition_count, 32);
        assert_eq!(column_names(exprs), vec!["host"]);
    }

    #[test]
    fn new_prefers_tsid_partitioning_for_promql_tsid_sort() {
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let partition_cols = BTreeMap::from([
            (
                "host".to_string(),
                BTreeSet::from([ColumnExpr::from_name("host")]),
            ),
            (
                DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                BTreeSet::from([ColumnExpr::from_name(DATA_SCHEMA_TSID_COLUMN_NAME)]),
            ),
        ]);
        let plan = promql_tsid_sorted_plan(schema.clone(), "ts");

        let ordering = MergeScanExec::logical_sort_ordering(&session_state, &plan)
            .unwrap()
            .unwrap();

        let merge_scan = MergeScanExec::new(
            &session_state,
            TableName::new("greptime", "public", "test"),
            vec![RegionId::new(1, 0), RegionId::new(1, 1)],
            plan,
            vec![],
            schema.as_ref(),
            Arc::new(NoopRegionQueryHandler),
            QueryContext::arc(),
            32,
            partition_cols,
        )
        .unwrap();

        let Partitioning::Hash(ref exprs, partition_count) = merge_scan.properties.partitioning
        else {
            panic!("expected hash partitioning");
        };
        assert_eq!(partition_count, 32);
        assert_eq!(column_names(exprs), vec![DATA_SCHEMA_TSID_COLUMN_NAME]);
        assert_eq!(
            ordering_column_names(&ordering),
            vec![DATA_SCHEMA_TSID_COLUMN_NAME, "ts"]
        );
    }

    #[test]
    fn logical_sort_ordering_ignores_projected_away_tsid_columns() {
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let projected = LogicalPlanBuilder::from(promql_tsid_sorted_plan(schema, "ts"))
            .project(vec![col("host"), col("ts"), col("greptime_value")])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                0,
                10,
                1,
                1,
                "ts".to_string(),
                Some("greptime_value".to_string()),
                projected,
            )),
        });

        let ordering = MergeScanExec::logical_sort_ordering(&session_state, &plan).unwrap();

        assert!(ordering.is_none());
    }

    fn test_merge_scan_exec(
        partition_cols: AliasMapping,
        current_partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> MergeScanExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();

        MergeScanExec {
            table: TableName::new("greptime", "public", "test"),
            regions: vec![RegionId::new(1, 0), RegionId::new(1, 1)],
            plan,
            arrow_schema: schema.clone(),
            region_query_handler: Arc::new(NoopRegionQueryHandler),
            metric: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::Hash(current_partition_exprs, 32),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            sub_stage_metrics: Arc::default(),
            partition_metrics: Arc::default(),
            query_ctx: QueryContext::arc(),
            target_partition: 32,
            partition_cols,
        }
    }

    fn partition_column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn column_names(exprs: &[Arc<dyn PhysicalExpr>]) -> Vec<&str> {
        exprs
            .iter()
            .map(|expr| expr.as_any().downcast_ref::<Column>().unwrap().name())
            .collect()
    }

    fn ordering_column_names(ordering: &LexOrdering) -> Vec<&str> {
        ordering
            .iter()
            .map(|sort_expr| {
                sort_expr
                    .expr
                    .as_any()
                    .downcast_ref::<Column>()
                    .unwrap()
                    .name()
            })
            .collect()
    }

    fn promql_tsid_sorted_plan(schema: Arc<Schema>, time_index: &str) -> LogicalPlan {
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: schema.to_dfschema_ref().unwrap(),
        });
        let sorted = LogicalPlanBuilder::from(input)
            .sort(vec![
                col(DATA_SCHEMA_TSID_COLUMN_NAME).sort(true, true),
                col(time_index).sort(true, true),
            ])
            .unwrap()
            .build()
            .unwrap();

        LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec!["host".to_string()],
                time_index.to_string(),
                sorted,
            )),
        })
    }
}

/// Metrics for a region of a partition.
#[derive(Debug, Clone)]
struct RegionMetrics {
    region_id: RegionId,
    poll_duration: Duration,
    do_get_cost: Duration,
    /// Total cost to scan the region.
    total_cost: Duration,
}

/// Metrics for a partition of a MergeScanExec.
#[derive(Debug, Clone)]
struct PartitionMetrics {
    partition: usize,
    region_metrics: Vec<RegionMetrics>,
    total_poll_duration: Duration,
    total_do_get_cost: Duration,
    total_regions: usize,
    explain_verbose: bool,
    finished: bool,
}

impl PartitionMetrics {
    fn new(partition: usize, explain_verbose: bool) -> Self {
        Self {
            partition,
            region_metrics: Vec::new(),
            total_poll_duration: Duration::ZERO,
            total_do_get_cost: Duration::ZERO,
            total_regions: 0,
            explain_verbose,
            finished: false,
        }
    }

    fn add_region_metrics(&mut self, region_metrics: RegionMetrics) {
        self.total_poll_duration += region_metrics.poll_duration;
        self.total_do_get_cost += region_metrics.do_get_cost;
        self.total_regions += 1;
        self.region_metrics.push(region_metrics);
    }

    /// Finish the partition metrics and log the results.
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.log_metrics();
    }

    /// Log partition metrics based on explain_verbose level.
    fn log_metrics(&self) {
        if self.explain_verbose {
            common_telemetry::info!(
                "MergeScan partition {} finished: {} regions, total_poll_duration: {:?}, total_do_get_cost: {:?}",
                self.partition,
                self.total_regions,
                self.total_poll_duration,
                self.total_do_get_cost
            );
        } else {
            common_telemetry::debug!(
                "MergeScan partition {} finished: {} regions, total_poll_duration: {:?}, total_do_get_cost: {:?}",
                self.partition,
                self.total_regions,
                self.total_poll_duration,
                self.total_do_get_cost
            );
        }
    }
}

impl Drop for PartitionMetrics {
    fn drop(&mut self) {
        if !self.finished {
            self.log_metrics();
        }
    }
}

impl ExecutionPlan for MergeScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    // DataFusion will swap children unconditionally.
    // But since this node is leaf node, it's safe to just return self.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.to_stream(context, partition)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "MergeScanExec"
    }
}

impl DisplayAs for MergeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeScanExec: peers=[")?;
        for region_id in self.regions.iter() {
            write!(f, "{}, ", region_id)?;
        }
        write!(f, "]")?;

        if matches!(t, DisplayFormatType::Verbose) {
            let partition_metrics = self.partition_metrics();
            if !partition_metrics.is_empty() {
                write!(f, ", metrics={{")?;
                for (i, pm) in partition_metrics.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(
                        f,
                        "\"partition_{}\":{{\"regions\":{},\"total_poll_duration\":\"{:?}\",\"total_do_get_cost\":\"{:?}\",\"region_metrics\":[",
                        pm.partition,
                        pm.total_regions,
                        pm.total_poll_duration,
                        pm.total_do_get_cost
                    )?;
                    for (j, rm) in pm.region_metrics.iter().enumerate() {
                        if j > 0 {
                            write!(f, ",")?;
                        }
                        write!(
                            f,
                            "{{\"region_id\":\"{}\",\"poll_duration\":\"{:?}\",\"do_get_cost\":\"{:?}\",\"total_cost\":\"{:?}\"}}",
                            rm.region_id, rm.poll_duration, rm.do_get_cost, rm.total_cost
                        )?;
                    }
                    write!(f, "]}}")?;
                }
                write!(f, "}}")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MergeScanMetric {
    /// Nanosecond elapsed till the scan operator is ready to emit data
    ready_time: Time,
    /// Nanosecond elapsed till the first record batch emitted from the scan operator gets consumed
    first_consume_time: Time,
    /// Nanosecond elapsed till the scan operator finished execution
    finish_time: Time,
    /// Count of rows fetched from remote
    output_rows: Count,

    /// Gauge for greptime plan execution cost metrics for output
    greptime_exec_cost: Gauge,
}

impl MergeScanMetric {
    pub fn new(metric: &ExecutionPlanMetricsSet) -> Self {
        Self {
            ready_time: MetricBuilder::new(metric).subset_time("ready_time", 1),
            first_consume_time: MetricBuilder::new(metric).subset_time("first_consume_time", 1),
            finish_time: MetricBuilder::new(metric).subset_time("finish_time", 1),
            output_rows: MetricBuilder::new(metric).output_rows(1),
            greptime_exec_cost: MetricBuilder::new(metric).gauge(GREPTIME_EXEC_READ_COST, 1),
        }
    }

    pub fn ready_time(&self) -> &Time {
        &self.ready_time
    }

    pub fn first_consume_time(&self) -> &Time {
        &self.first_consume_time
    }

    pub fn finish_time(&self) -> &Time {
        &self.finish_time
    }

    pub fn record_output_batch_rows(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }

    pub fn record_greptime_exec_cost(&self, metrics: usize) {
        self.greptime_exec_cost.add(metrics);
    }
}
