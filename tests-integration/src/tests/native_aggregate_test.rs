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

//! A complete aggregate over one ordinary Mito table, executed by the real standalone frontend and
//! the real datanode in a single process.
//!
//! The table is ordinary - one region, no `PARTITION` clause - and its five series sit in four SST
//! files and one memtable range. With the scan asked for four partitions, each of them ends up with
//! at least one series; the per-partition metric assertion below checks that instead of assuming it.
//! Every query runs twice: once with the native route asked for per request, once on the plan the
//! existing planner builds. Both are compared with expectations worked out from the fixture, so
//! neither run is the other's oracle.
//!
//! The native run plans two trees, and both are checked in the plans the engines really executed.
//! The region's tree must hold one fused producer reading the raw series scan directly - the scan
//! itself, or the one transparent cooperative-scheduling wrapper the engine may put above it, but
//! never an exchange - and `native.root.output_values` for every one of the four scan partitions;
//! the metric is read after the result stream was drained, the only point at which it is complete.
//! The frontend's tree must hold the merge of the state those partitions export (so the answer is
//! not a fall back). It cannot reach the producer, because a region plans and runs its own subtree,
//! so the merge is followed down only to the `MergeScanExec` leaf that stands for it: the executed
//! relation is `AggregateFinalize` -> `MergeScanExec` on the frontend and
//! `FusedSeriesExec` -> `SeriesScanExec` in the region, one partition-local evaluation per scan
//! partition and one merge over all of their states. Plans are observed through
//! [`PhysicalPlanWrapper`], which both query engines install, so what is checked is the tree that ran
//! rather than a re-plan made for an `EXPLAIN`.
//!
//! Two tables carry the queries. [`COUNTER_TABLE`] holds the same five series in the same four
//! persisted files and one memtable range as [`GAUGE_TABLE`], but its samples are a counter's, so
//! `rate` over it is a nonzero number that is derived by hand in [`COUNTER_SERIES`]: the gauge's
//! constant value has a rate of exactly zero and cannot tell a computed rate from a dropped one. The
//! counter queries are the acceptance ones for the state bridge - four nonempty partitions fold
//! partial states that only a merge over *all* of them turns into `sum`, `avg`, `count` and
//! `stddev` - while the gauge queries keep covering the shapes with round answers.
//!
//! The counter table carries one tag the gauge table does not, `region`: the four flushed series -
//! the four files - are in one region and the series left in the memtable range is in another, so
//! one query can name a matcher (`{region="us-west-2"}`) that only four of the five scan ranges
//! satisfy. No other counter query reads that tag: an aggregate drops the labels it does not group
//! by, so their answers are the ones the untagged fixture gives.
//!
//! A query the native route declines is checked the other way round. [`check_fallback_case`] runs
//! shapes the compiler has no node for - `time()`, `scalar(v)`, a fold over two sources and a
//! subquery - with the native request on and with it off, and requires that no native node ran in
//! either plan and that both runs report rows derived from the fixture by hand. A hint that quietly
//! took the route for one of them would show a fused producer or a frontend merge in the plans that
//! executed, which is what is observed.
//!
//! A binary operation whose sibling is a literal scalar is carried by the split on either side of
//! the fold: below the aggregate it is computed in the per-series prefix (`sum(8 * rate(...))`,
//! `max(10 - rate(...))`), and above it the merge's suffix rebuilds it over the merged value with
//! the operands in the order they were written (`100 - avg(rate(...))`, `100 / sum(rate(...))`,
//! `-sum(rate(...))`). Deliberately not covered yet: the aggregate split carries a literal scalar
//! as its only binary sibling, so a computed scalar (`scalar(...)`, `time()`) or a whole-vector
//! combination such as `sort`, `absent` or `and`/`or`/`unless` beside the aggregate keeps the
//! existing plan. [`check_fallback_case`] pins that the computed scalars do, and that a fold over a
//! second source and a subquery keep it too.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use common_base::Plugins;
use common_query::OutputData;
use common_recordbatch::RecordBatches;
use common_recordbatch::util::collect_batches;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datatypes::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use frontend::instance::Instance;
use futures::StreamExt;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::dist_plan::MergeScanExec;
use query::options::QueryOptions;
use query::physical_wrapper::{PhysicalPlanWrapper, PhysicalPlanWrapperRef};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use table::table::scan::SeriesScanExec;

use crate::standalone::GreptimeDbStandaloneBuilder;

/// The per-request key that turns the native route on for one query. The planner's own constant is
/// crate-private, so it is repeated here.
const NATIVE_PROMQL_EXTENSION_KEY: &str = "query.experimental_native_promql";
/// The physical name of the fused producer.
const FUSED_EXEC_NAME: &str = "FusedSeriesExec";
/// What every name of the frontend-side merge node contains (`AggregateFinalize`, `...Exec`).
const FINALIZE_EXEC_NAME: &str = "AggregateFinalize";
/// The metric a fused producer registers for the values its own root node produced, per partition.
const ROOT_OUTPUT_VALUES_METRIC: &str = "native.root.output_values";
/// The parallelism the scan is asked to split its ranges into.
const SCAN_PARTITIONS: usize = 4;
/// The first sample of the fixture, in seconds since the epoch.
const BASE_SECS: i64 = 1_700_000_000;
/// The instant every query is evaluated at: `BASE_SECS + 240`, so the oldest sample is 240s old and
/// every sample is inside the default 5m lookback.
const EVAL_SECS: i64 = BASE_SECS + 240;
/// [`EVAL_SECS`] in milliseconds, the `ts` the results are reported at.
const EVAL_MILLIS: i64 = EVAL_SECS * 1000;
/// The table the aggregate queries read.
const GAUGE_TABLE: &str = "native_agg_gauge";
/// The table the `rate` queries read: the same five series as [`GAUGE_TABLE`], holding a counter.
const COUNTER_TABLE: &str = "native_agg_counter";
/// The table the range-window smoke test reads.
const RANGE_TABLE: &str = "native_agg_range";
/// The tolerance of the value comparisons: relative, with an absolute floor.
const FLOAT_TOLERANCE: f64 = 1e-10;

/// The five series of the gauge fixture in insert order: `job = a` holds 2, 6 and 10; `job = b`
/// holds 4 and 8, so every aggregate below has an answer that can be worked out by hand.
const GAUGE_SERIES: [(&str, &str, f64); 5] = [
    ("a", "sh1", 2.0),
    ("a", "h2", 6.0),
    ("a", "h3", 10.0),
    ("b", "h4", 4.0),
    ("b", "h5", 8.0),
];

/// The value both samples of every counter series start from.
///
/// A `rate` over a counter extrapolates the samples towards the window's ends, but never past the
/// point where the counter would have been zero: that clamp shortens the leading extrapolation to
/// `interval * first / increase`. Keeping the first value at least half the largest increase keeps
/// the clamp from ever binding here, so the factor below is the plain boundary one and nothing in
/// the fixture depends on a series' history before the window.
const COUNTER_FIRST_VALUE: f64 = 1_500.0;
/// The first sample of every counter series, in seconds since the epoch: 200s before [`EVAL_SECS`].
const COUNTER_FIRST_SECS: i64 = EVAL_SECS - 200;
/// The distance to every counter series' second sample, which is therefore 140s before
/// [`EVAL_SECS`].
const COUNTER_STEP_SECS: i64 = 60;

/// The five series of the counter fixture in insert order, as `(job, instance, increase)`: the same
/// keys as [`GAUGE_SERIES`], so both tables hold two groups of three and two series, but the
/// increases make `rate` nonzero and hand-derivable.
///
/// `rate` over `[5m]` at [`EVAL_SECS`] reads the two samples 60s apart, which are 100s and 140s away
/// from the window's start and end. Both distances exceed the extrapolation threshold of 1.1 average
/// intervals (66s), so both ends are extended by half an average interval - 30s - and the sampled
/// increase is scaled by `(60 + 30 + 30) / 60 = 2`. Dividing by the 300s window gives
/// `rate = increase * 2 / 300 = increase / 150`, so the five rates are 1, 3, 5 for `job = a` and 2, 4
/// for `job = b`. The counter-zero clamp stays out of it: `interval * first / increase` is between
/// 120s and 600s for these increases, never below the 30s boundary, so it cannot shorten the start.
const COUNTER_SERIES: [(&str, &str, f64); 5] = [
    ("a", "sh1", 150.0),
    ("a", "h2", 450.0),
    ("a", "h3", 750.0),
    ("b", "h4", 300.0),
    ("b", "h5", 600.0),
];

/// The `region` tag of the four counter series that were flushed, one series per file.
///
/// Only [`COUNTER_TABLE`] carries the tag, and only the matcher case reads it: the four flushed
/// series are in this region, so `{region="us-west-2"}` selects the four files and drops the series
/// the memtable range holds. Every other counter query aggregates the tag away.
const COUNTER_WEST_REGION: &str = "us-west-2";
/// The `region` tag of the counter series that stays in the memtable range: `job = b`,
/// `instance = h5`, whose rate is 4.
const COUNTER_EAST_REGION: &str = "us-east-1";

/// Observes the physical plan each query engine is about to execute, and keeps it.
///
/// The frontend and the datanode it reads its regions from install the same plugin instance, so both
/// trees of one query arrive here. They are kept in a list because one query produces one entry per
/// engine, and keeping a single plan would lose the region's.
#[derive(Default)]
struct NativePlanObserver {
    seen: Mutex<Vec<Arc<dyn ExecutionPlan>>>,
    record_original_streams: bool,
    partial_batches: Arc<Mutex<Vec<(usize, RecordBatch)>>>,
}

impl NativePlanObserver {
    /// Marks the current end of the list, for [`Self::since`].
    fn mark(&self) -> usize {
        self.seen.lock().unwrap().len()
    }

    /// The plans handed over since `mark`.
    fn since(&self, mark: usize) -> Vec<Arc<dyn ExecutionPlan>> {
        self.seen.lock().unwrap()[mark..].to_vec()
    }

    fn partial_batches(&self) -> Vec<(usize, RecordBatch)> {
        std::mem::take(&mut *self.partial_batches.lock().unwrap())
    }
}

impl PhysicalPlanWrapper for NativePlanObserver {
    fn wrap(
        &self,
        origin: Arc<dyn ExecutionPlan>,
        _ctx: QueryContextRef,
    ) -> Arc<dyn ExecutionPlan> {
        if origin.name() == FUSED_EXEC_NAME && self.record_original_streams {
            let recorder = Arc::new(PartialStateRecorder {
                input: Arc::clone(&origin),
                properties: Arc::clone(origin.properties()),
                batches: Arc::clone(&self.partial_batches),
            });
            self.seen
                .lock()
                .unwrap()
                .push(Arc::clone(&recorder) as Arc<dyn ExecutionPlan>);
            recorder
        } else {
            self.seen.lock().unwrap().push(Arc::clone(&origin));
            origin
        }
    }
}

/// Transparent test wrapper that records the fused producer's original partition streams.
#[derive(Debug)]
struct PartialStateRecorder {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    batches: Arc<Mutex<Vec<(usize, RecordBatch)>>>,
}

impl DisplayAs for PartialStateRecorder {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.input.fmt_as(format, f)
    }
}

impl ExecutionPlan for PartialStateRecorder {
    fn name(&self) -> &str {
        "PartialStateRecorder"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        self.input.apply_expressions(f)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "PartialStateRecorder expects one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            properties: Arc::clone(&self.properties),
            batches: Arc::clone(&self.batches),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let batches = Arc::clone(&self.batches);
        let schema = stream.schema();
        let recorded = stream.map(move |result| {
            if let Ok(batch) = &result {
                batches.lock().unwrap().push((partition, batch.clone()));
            }
            result
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, recorded)))
    }
}

/// The native nodes one query's plans hold: the partition-local producers, one per region the query
/// read, and the frontend-side merges of the state they export.
///
/// The two are observed in different trees, so the lists are never matched against each other by
/// `Arc`; the schema the producer exports is what ties them.
#[derive(Default)]
struct NativeCapture {
    fused: Vec<Arc<dyn ExecutionPlan>>,
    finalize: Vec<Arc<dyn ExecutionPlan>>,
}

impl NativeCapture {
    /// Picks the nodes of interest out of the plans one query handed over.
    fn from_plans(plans: &[Arc<dyn ExecutionPlan>]) -> Self {
        let mut capture = Self::default();
        let mut stack = plans.to_vec();
        while let Some(node) = stack.pop() {
            if node.name() == FUSED_EXEC_NAME {
                capture.fused.push(Arc::clone(&node));
            }
            if node.name().contains(FINALIZE_EXEC_NAME) {
                capture.finalize.push(Arc::clone(&node));
            }
            stack.extend(node.children().into_iter().cloned());
        }
        capture
    }
}

/// One result row, read by column role rather than by column name.
#[derive(Debug)]
struct ResultRow {
    /// The evaluation time, in milliseconds.
    ts_millis: i64,
    /// The string label columns by name; a null label is an absent entry.
    labels: BTreeMap<String, String>,
    /// The value column, [`None`] when the row carries no value for it.
    value: Option<f64>,
}

/// The rows of one PromQL query and the columns they were reported in.
struct QueryResult {
    /// Every column name, in schema order.
    columns: Vec<String>,
    /// The rows, in a canonical order.
    rows: Vec<ResultRow>,
}

impl QueryResult {
    /// Reads the batches of one PromQL result. The value column is found by type: the frontend names
    /// it after the expression it computes (`sum(v)` and the like), so that name is checked against
    /// the legacy run instead of being assumed here. `count` reports an integer, so every numeric
    /// type is accepted; the timestamp column was taken first and is therefore not a candidate.
    fn from_batches(batches: &RecordBatches, query: &str) -> Self {
        let schema = batches.schema();
        let fields = schema.arrow_schema().fields();
        let columns = fields
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let time_index = fields
            .iter()
            .position(|field| matches!(field.data_type(), DataType::Timestamp(_, _)))
            .unwrap_or_else(|| {
                panic!("`{query}`: the result carries no timestamp column: {columns:?}")
            });
        let label_indices = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                matches!(
                    field.data_type(),
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Dictionary(_, _)
                )
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let value_indices = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.data_type().is_numeric())
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert_eq!(
            1,
            value_indices.len(),
            "`{query}`: expected exactly one value column in {columns:?}"
        );

        let mut rows = Vec::new();
        for batch in batches.iter() {
            let ts = cast(batch.column(time_index), &DataType::Int64).unwrap();
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
            let labels = label_indices
                .iter()
                .map(|index| {
                    let array = cast(batch.column(*index), &DataType::Utf8).unwrap();
                    (columns[*index].clone(), array)
                })
                .collect::<Vec<(String, ArrayRef)>>();
            let value = cast(batch.column(value_indices[0]), &DataType::Float64).unwrap();
            let value = value.as_any().downcast_ref::<Float64Array>().unwrap();
            for row in 0..batch.num_rows() {
                let mut row_labels = BTreeMap::new();
                for (name, array) in &labels {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    if !array.is_null(row) {
                        row_labels.insert(name.clone(), array.value(row).to_string());
                    }
                }
                rows.push(ResultRow {
                    ts_millis: ts.value(row),
                    labels: row_labels,
                    value: (!value.is_null(row)).then_some(value.value(row)),
                });
            }
        }
        // Row order across partitions is not part of the answer, so both sides are ordered the same
        // way first. The sort is stable, so duplicate rows keep their multiplicity.
        rows.sort_by(|left, right| {
            (left.ts_millis, &left.labels).cmp(&(right.ts_millis, &right.labels))
        });

        Self { columns, rows }
    }
}

/// One query, and what it must answer.
struct AggregateCase {
    promql: String,
    expected: Vec<ResultRow>,
    /// Whether every one of the four scan partitions must report a folded value.
    per_partition: bool,
    /// The number of values the partitions' producers must report together, when the answer fixes
    /// that figure: one value per series that folded, which is what the counter fixture's queries
    /// make checkable. With `per_partition` those are all four partitions' values and their spread
    /// is bounded too. `None` leaves only "some value" to the assertion.
    root_values: Option<usize>,
    /// The `partial state:` line the producer must show, when the fixture fixes which aggregate the
    /// split cut out. The producer then exports that aggregate's *state*, not an answer, so a
    /// group's values are finished by the merge above it rather than once per partition.
    partial_state: Option<&'static str>,
}

/// Builds one case from a PromQL expression and its `(labels, value)` rows.
fn case(promql: String, expected: &[(&[(&str, &str)], f64)], per_partition: bool) -> AggregateCase {
    AggregateCase {
        promql,
        expected: expected
            .iter()
            .map(|(labels, value)| ResultRow {
                ts_millis: EVAL_MILLIS,
                labels: labels
                    .iter()
                    .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                    .collect(),
                value: Some(*value),
            })
            .collect(),
        per_partition,
        root_values: None,
        partial_state: None,
    }
}

/// A case over [`COUNTER_TABLE`], which additionally pins the values the four partitions fold and
/// the state the producer exports.
///
/// Every counter series is one series the scan keeps whole, and each of them decides exactly one
/// `rate` at the single grid point [`tql_eval`] evaluates, so the four partitions' producers together
/// report exactly as many values as there are counter series - and, because each partition folds at
/// least one of them, no partition can fold more than `series - (partitions - 1)`.
///
/// `partial_state` is the aggregate the split cut out, as the producer's plan line names it: the
/// innermost aggregate of the tree, whose state the partitions export.
fn counter_case(
    promql: String,
    expected: &[(&[(&str, &str)], f64)],
    partial_state: &'static str,
) -> AggregateCase {
    AggregateCase {
        root_values: Some(COUNTER_SERIES.len()),
        partial_state: Some(partial_state),
        ..case(promql, expected, true)
    }
}

/// The complete statement one case runs, reported in every assertion message.
fn tql_eval(promql: &str) -> String {
    format!("TQL EVAL ({EVAL_SECS}, {EVAL_SECS}, '1m') {promql}")
}

/// Runs `promql` at [`EVAL_SECS`] and returns its rows with the native nodes its plans held.
async fn run_query(
    instance: &Arc<Instance>,
    promql: &str,
    native: bool,
    observer: &NativePlanObserver,
) -> (QueryResult, NativeCapture) {
    let sql = tql_eval(promql);
    let mark = observer.mark();
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, SCAN_PARTITIONS.to_string());
    query_ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, native.to_string());
    let output = instance
        .do_query(&sql, Arc::new(query_ctx))
        .await
        .remove(0)
        .unwrap_or_else(|err| panic!("`{sql}` failed: {err}"));
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` produced no batches: {err}")),
        OutputData::RecordBatches(batches) => batches,
        other => panic!("`{sql}` produced no batches: {other:?}"),
    };
    // Read the plans only now: a producer starts reporting a partition's metrics when that partition
    // runs and stops updating them when it ends.
    (
        QueryResult::from_batches(&batches, &sql),
        NativeCapture::from_plans(&observer.since(mark)),
    )
}

/// Executes one statement and drops its result.
async fn execute_sql(instance: &Arc<Instance>, sql: &str) {
    for result in instance.do_query(sql, QueryContext::arc()).await {
        result.unwrap_or_else(|err| panic!("`{sql}` failed: {err}"));
    }
}

/// The DDL of the fixture tables: ordinary tables, no `PARTITION` clause, one region each.
///
/// TWCS keeps its files apart until a hundred exist, so flushing one file per series leaves each
/// fixture with four files instead of a compacted one.
fn create_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (ts TIMESTAMP(3) TIME INDEX, job STRING, instance STRING, v DOUBLE, \
         PRIMARY KEY (job, instance)) WITH ('compaction.type' = 'twcs', \
         'compaction.twcs.trigger_file_num' = '100')"
    )
}

/// The DDL of [`COUNTER_TABLE`]: [`create_table_sql`]'s columns plus the `region` tag.
///
/// The tag is part of the primary key, so it is a label the raw-series scan reports and a matcher
/// can select, and it costs the other counter queries nothing: an aggregate that does not group by
/// it drops it. Only the counter table carries it, because only one counter query reads it; the
/// gauge table keeps the shape [`create_table_sql`] builds for every fixture table.
fn counter_table_sql() -> String {
    format!(
        "CREATE TABLE {COUNTER_TABLE} (ts TIMESTAMP(3) TIME INDEX, job STRING, instance STRING, \
         \"region\" STRING, v DOUBLE, PRIMARY KEY (job, instance, \"region\")) WITH \
         ('compaction.type' = 'twcs', 'compaction.twcs.trigger_file_num' = '100')"
    )
}

/// Fills the fixture tables.
///
/// The gauge table gets one flush per series but the last, so four files and one memtable range
/// reach the scan; the two samples of series `index` are 10s apart and 40s after the previous
/// series'. The counter table is filled the same way, with the same keys and the same flush after
/// each of the first four series, so the scan sees the same five series in the same four files and
/// one memtable range - but its samples are the two of [`COUNTER_SERIES`], [`COUNTER_STEP_SECS`]
/// apart and 140s and 200s before [`EVAL_SECS`], and its rows carry the `region` tag of the file
/// they were flushed into ([`COUNTER_WEST_REGION`]) or, for the series left in the memtable range,
/// [`COUNTER_EAST_REGION`]. The range table gets one series whose samples are 390s apart: both are
/// inside a 10m range at [`EVAL_SECS`] and the older one is outside the default 5m lookback, so a
/// query that reads its own range counts two samples where one that used the lookback would count
/// one.
async fn setup_fixture(instance: &Arc<Instance>) {
    execute_sql(instance, &create_table_sql(GAUGE_TABLE)).await;
    for (index, (job, host, v)) in GAUGE_SERIES.iter().enumerate() {
        let first = (BASE_SECS + 40 * index as i64) * 1000;
        let second = first + 10_000;
        execute_sql(
            instance,
            &format!(
                "INSERT INTO {GAUGE_TABLE} (ts, job, instance, v) VALUES \
                 ({first}, '{job}', '{host}', {v:?}), ({second}, '{job}', '{host}', {v:?})"
            ),
        )
        .await;
        if index + 1 < GAUGE_SERIES.len() {
            execute_sql(instance, &format!("ADMIN FLUSH_TABLE('{GAUGE_TABLE}')")).await;
        }
    }

    execute_sql(instance, &counter_table_sql()).await;
    let (counter_first, counter_second) = (
        COUNTER_FIRST_SECS * 1000,
        (COUNTER_FIRST_SECS + COUNTER_STEP_SECS) * 1000,
    );
    for (index, (job, host, increase)) in COUNTER_SERIES.iter().enumerate() {
        let second_value = COUNTER_FIRST_VALUE + increase;
        // The four flushed series are the ones the matcher case selects; the series that stays in
        // the memtable range is the only one in the other region.
        let region = if index + 1 < COUNTER_SERIES.len() {
            COUNTER_WEST_REGION
        } else {
            COUNTER_EAST_REGION
        };
        execute_sql(
            instance,
            &format!(
                "INSERT INTO {COUNTER_TABLE} (ts, job, instance, \"region\", v) VALUES \
                 ({counter_first}, '{job}', '{host}', '{region}', {COUNTER_FIRST_VALUE:?}), \
                 ({counter_second}, '{job}', '{host}', '{region}', {second_value:?})"
            ),
        )
        .await;
        if index + 1 < COUNTER_SERIES.len() {
            execute_sql(instance, &format!("ADMIN FLUSH_TABLE('{COUNTER_TABLE}')")).await;
        }
    }

    execute_sql(instance, &create_table_sql(RANGE_TABLE)).await;
    execute_sql(
        instance,
        &format!(
            "INSERT INTO {RANGE_TABLE} (ts, job, instance, v) VALUES \
             ({}, 'r', 'r1', 1.0), ({}, 'r', 'r1', 1.0)",
            (EVAL_SECS - 400) * 1000,
            (EVAL_SECS - 10) * 1000
        ),
    )
    .await;
}

/// Whether two values agree within [`FLOAT_TOLERANCE`].
fn close_enough(left: f64, right: f64) -> bool {
    if left.is_nan() || right.is_nan() {
        return left.is_nan() && right.is_nan();
    }
    (left - right).abs() <= FLOAT_TOLERANCE * left.abs().max(right.abs()).max(1.0)
}

/// Asserts `actual` holds exactly `expected`, row order aside but multiplicity included.
fn assert_rows(actual: &[ResultRow], expected: &[ResultRow], context: &str) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "{context}: expected {} rows, got {}:\n{actual:#?}",
        expected.len(),
        actual.len()
    );
    for (want, got) in expected.iter().zip(actual.iter()) {
        assert_eq!(want.ts_millis, got.ts_millis, "{context}: wrong `ts`");
        assert_eq!(
            want.labels, got.labels,
            "{context}: wrong labels in the row at {}",
            want.ts_millis
        );
        match (want.value, got.value) {
            (Some(want_value), Some(got_value)) => assert!(
                close_enough(want_value, got_value),
                "{context}: expected {want_value}, got {got_value} for labels {:?}",
                want.labels
            ),
            (want_value, got_value) => panic!(
                "{context}: expected the value {want_value:?}, got {got_value:?} for labels {:?}",
                want.labels
            ),
        }
    }
}

/// Asserts both routes report the answer with the same columns.
///
/// The value column is named after the expression the frontend computes, which is the legacy naming:
/// a native run that renamed it, or dropped, added or reordered a column, fails here rather than
/// passing on values alone. Column order is not compared, only the answer's shape.
fn assert_same_columns(native: &QueryResult, legacy: &QueryResult, query: &str) {
    let sorted = |result: &QueryResult| {
        let mut columns = result.columns.clone();
        columns.sort();
        columns
    };
    assert_eq!(
        sorted(legacy),
        sorted(native),
        "`{query}`: the native route must report the columns the legacy plan reports"
    );
}

/// The `native.root.output_values` counts of the fused producers in `capture`, per partition.
///
/// An entry stays [`None`] when the producer never registered the metric for a partition, which is a
/// different failure from a partition that produced nothing; both are reported as they are.
fn fused_output_values_per_partition(capture: &NativeCapture, context: &str) -> Vec<Option<usize>> {
    let mut per_partition = vec![None; SCAN_PARTITIONS];
    for fused in &capture.fused {
        let plan = displayable(fused.as_ref()).indent(false).to_string();
        let metrics = fused
            .metrics()
            .unwrap_or_else(|| panic!("{context}: the fused producer reports no metrics:\n{plan}"));
        for metric in metrics.iter() {
            let MetricValue::Count { count, .. } = metric.value() else {
                continue;
            };
            if metric.value().name() != ROOT_OUTPUT_VALUES_METRIC {
                continue;
            }
            let partition = metric.partition().unwrap_or_else(|| {
                panic!(
                    "{context}: `{ROOT_OUTPUT_VALUES_METRIC}` is not labelled with its partition"
                )
            });
            assert!(
                partition < SCAN_PARTITIONS,
                "{context}: partition {partition} is beyond the {SCAN_PARTITIONS} the scan was \
                 asked for"
            );
            per_partition[partition] = Some(per_partition[partition].unwrap_or(0) + count.value());
        }
    }
    per_partition
}

/// Asserts the fused producer reads the raw series scan itself, with no exchange between them.
///
/// One node may sit in between: the cooperative-scheduling decorator clones its input's properties,
/// changes only the scheduling type, and forwards `execute` unchanged, so it neither re-partitions
/// nor re-orders the frames. It is accepted only as a single typed wrapper above the scan, never as
/// a wildcard. An exchange above the scan would re-partition the frames, and the producer's
/// per-partition fold would then no longer be one series at a time - so the plan would be wrong even
/// if a result appeared.
fn assert_fused_reads_scan_directly(fused: &Arc<dyn ExecutionPlan>, context: &str) {
    let plan = displayable(fused.as_ref()).indent(false).to_string();
    assert_eq!(
        SCAN_PARTITIONS,
        fused.properties().output_partitioning().partition_count(),
        "{context}: the fused producer must keep the {SCAN_PARTITIONS} scan partitions:\n{plan}"
    );
    let children = fused.children();
    assert_eq!(
        1,
        children.len(),
        "{context}: the fused producer must take exactly one input:\n{plan}"
    );
    let child = children[0];
    // At most one transparent wrapper is allowed between the producer and the scan.
    let scan = if child.is::<CooperativeExec>() {
        let wrapper_children = child.children();
        assert_eq!(
            1,
            wrapper_children.len(),
            "{context}: the cooperative wrapper must wrap exactly one input:\n{plan}"
        );
        assert_eq!(
            SCAN_PARTITIONS,
            child.properties().output_partitioning().partition_count(),
            "{context}: the cooperative wrapper must not change the {SCAN_PARTITIONS} scan \
             partitions:\n{plan}"
        );
        wrapper_children[0]
    } else {
        child
    };
    assert!(
        scan.is::<SeriesScanExec>(),
        "{context}: the fused producer must read the raw series scan directly, with no \
         `CoalesceExec` or `RepartitionExec` between them, but its input is `{}`:\n{plan}",
        scan.name()
    );
    assert_eq!(
        SCAN_PARTITIONS,
        scan.properties().output_partitioning().partition_count(),
        "{context}: `SeriesScanExec` must expose the {SCAN_PARTITIONS} scan partitions the fused \
         producer reads:\n{plan}"
    );
}

/// The node a transparent cooperative-scheduling wrapper stands in for, or `node` itself.
///
/// The decorator clones its input's properties, changes only the scheduling type and forwards
/// `execute` unchanged, so a chain that passes through it is the same chain: it neither gathers nor
/// re-partitions. Only a wrapper with exactly one input is looked through; anything else is returned
/// as it is, so the caller's own type check reports it.
fn through_cooperative(node: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    let children = node.children();
    if node.is::<CooperativeExec>() && children.len() == 1 {
        children[0]
    } else {
        node
    }
}

/// Asserts the frontend merge reads the state at the region boundary, not a producer of its own.
///
/// Only known transparent nodes are looked through: the projection that restores the producer's
/// schema (which a physical optimizer pass may remove, so it is optional), the cooperative wrapper,
/// and the `CoalescePartitionsExec` the merge itself builds for an input with more than one
/// partition. The boundary is the `MergeScanExec` leaf, and the merge's input schema must be the
/// state schema the captured region producer exports: finished values there would mean the merge
/// folded answers instead of the partitions' contributions.
fn assert_finalize_reads_region_state(
    finalize: &Arc<dyn ExecutionPlan>,
    producer: &Arc<dyn ExecutionPlan>,
    context: &str,
) {
    let plan = displayable(finalize.as_ref()).indent(false).to_string();
    let children = finalize.children();
    assert_eq!(
        1,
        children.len(),
        "{context}: the `{FINALIZE_EXEC_NAME}` merge must take exactly one input:\n{plan}"
    );
    let partitions = finalize
        .properties()
        .output_partitioning()
        .partition_count();
    assert_eq!(
        1, partitions,
        "{context}: the merge must answer one partition:\n{plan}"
    );
    let mut merge_input = children[0];
    if merge_input.is::<CoalescePartitionsExec>() {
        let gathered = merge_input.children();
        assert_eq!(
            1,
            gathered.len(),
            "{context}: `CoalescePartitionsExec` must gather exactly one input:\n{plan}"
        );
        let gathered_partitions = gathered[0]
            .properties()
            .output_partitioning()
            .partition_count();
        assert!(
            gathered_partitions > 1,
            "{context}: the merge gathers only an input with more than one partition, but this one \
             has {gathered_partitions}:\n{plan}"
        );
        merge_input = gathered[0];
    }
    let mut boundary = through_cooperative(merge_input);
    if boundary.is::<ProjectionExec>() {
        let projected = boundary.children();
        assert_eq!(
            1,
            projected.len(),
            "{context}: the schema-restoring projection must take exactly one input:\n{plan}"
        );
        boundary = through_cooperative(projected[0]);
    }
    assert!(
        boundary.is::<MergeScanExec>(),
        "{context}: the merge must read the region boundary `MergeScanExec`, not a producer the \
         frontend planned, but its input is `{}`:\n{plan}",
        boundary.name()
    );
    assert!(
        boundary.children().is_empty(),
        "{context}: `MergeScanExec` is a leaf, because the region plans the subtree it carries \
         separately:\n{plan}"
    );
    let boundary_partitions = boundary
        .properties()
        .output_partitioning()
        .partition_count();
    assert_eq!(
        1, boundary_partitions,
        "{context}: one region is read, so the boundary exposes one output partition:\n{plan}"
    );
    // What the producer exports is named here, column by column, before the merge's input schema is
    // compared with it: two schemas that agree on finished values would agree with each other just
    // as well, and a merge over answers is not the bridge this case is about. The state layout is
    // the metric name, one column per tag, the point and field a value belongs to, the four
    // accumulators and the incremental-mean flag; the tag count is read off the schema, so the
    // assertion holds for both fixtures without repeating either tag list.
    const STATE_ACCUMULATORS: [&str; 5] =
        ["value", "mean", "compensation", "count", "incremental_mean"];
    const STATE_POSITION: [&str; 2] = ["point", "field"];
    let state_columns = producer
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let fixed_columns = 1 + STATE_POSITION.len() + STATE_ACCUMULATORS.len();
    assert!(
        state_columns.len() >= fixed_columns,
        "{context}: the producer must export aggregate state, but its {} columns are \
         {state_columns:?}:\n{plan}",
        state_columns.len()
    );
    let tags = state_columns.len() - fixed_columns;
    let mut expected_columns = Vec::with_capacity(state_columns.len());
    expected_columns.push("metric_name".to_string());
    expected_columns.extend((0..tags).map(|at| format!("tag_{at}")));
    expected_columns.extend(STATE_POSITION.map(str::to_string));
    expected_columns.extend(STATE_ACCUMULATORS.map(str::to_string));
    assert_eq!(
        expected_columns, state_columns,
        "{context}: the producer must export the aggregate state layout, not one value per \
         group:\n{plan}"
    );
    assert_eq!(
        producer.schema(),
        merge_input.schema(),
        "{context}: the merge must read the state schema the region producer exports:\n{plan}"
    );
}

/// Checks one query the native route declines: it must stay declined on both routes, and both must
/// answer what the fixture derives by hand.
///
/// The native request is on for one run and off for the other, so the hint being asked for and the
/// hint being absent are both exercised: neither plan may hold a fused producer or a frontend merge,
/// and the rows - labels, evaluation time, values and their multiplicity - are the same hand-derived
/// ones for both. The observer is asked to have seen a plan at all, because a run that handed nothing
/// over would otherwise satisfy "no native node" without observing anything.
async fn check_fallback_case(
    instance: &Arc<Instance>,
    observer: &NativePlanObserver,
    case: &AggregateCase,
) {
    let promql = case.promql.as_str();
    let query = tql_eval(promql);
    for native in [false, true] {
        let route = if native { "native" } else { "legacy" };
        let mark = observer.mark();
        let (result, capture) = run_query(instance, promql, native, observer).await;
        assert!(
            observer.mark() > mark,
            "`{query}`: the {route} run handed no plan to the observer, so nothing about the plan \
             that ran was observed"
        );
        assert!(
            capture.fused.is_empty() && capture.finalize.is_empty(),
            "`{query}`: the {route} run must keep the existing plan, but the plans that ran held {} \
             fused producers and {} frontend merges",
            capture.fused.len(),
            capture.finalize.len()
        );
        assert_rows(&result.rows, &case.expected, &format!("{route} `{query}`"));
    }
}

/// Checks one case on both routes: the legacy run first, then the native one.
async fn check_case(instance: &Arc<Instance>, observer: &NativePlanObserver, case: &AggregateCase) {
    let promql = case.promql.as_str();
    let query = tql_eval(promql);
    let (legacy, capture) = run_query(instance, promql, false, observer).await;
    assert!(
        capture.fused.is_empty() && capture.finalize.is_empty(),
        "`{query}`: the legacy run must not plan a native node"
    );
    assert_rows(&legacy.rows, &case.expected, &format!("legacy `{query}`"));

    let (native, capture) = run_query(instance, promql, true, observer).await;
    assert_same_columns(&native, &legacy, &query);
    assert_rows(&native.rows, &case.expected, &format!("native `{query}`"));

    assert_eq!(
        1,
        capture.fused.len(),
        "`{query}`: one region is read, so exactly one fused producer must run, but the plans held \
         {} of them",
        capture.fused.len()
    );
    for fused in &capture.fused {
        assert_fused_reads_scan_directly(fused, &format!("native `{query}`"));
        if let Some(partial_state) = case.partial_state {
            let plan = displayable(fused.as_ref()).indent(false).to_string();
            assert!(
                plan.contains(partial_state),
                "`{query}`: the fused producer must export the partial state `{partial_state}`, so \
                 that the merge above it - and not one partition on its own - finishes the groups; \
                 its plan is:\n{plan}"
            );
        }
    }
    assert!(
        !capture.finalize.is_empty(),
        "`{query}`: the native run must plan the frontend-side `{FINALIZE_EXEC_NAME}` merge, \
         otherwise the answer came from a fall back to the legacy plan"
    );
    // The merge reads the region boundary; the producer is checked in the region's own tree.
    for finalize in &capture.finalize {
        assert_finalize_reads_region_state(
            finalize,
            &capture.fused[0],
            &format!("native `{query}`"),
        );
    }

    let per_partition = fused_output_values_per_partition(&capture, &format!("native `{query}`"));
    common_telemetry::info!(
        "`{query}` native per-partition `{ROOT_OUTPUT_VALUES_METRIC}` counts: {per_partition:?}"
    );
    if case.per_partition {
        for (partition, values) in per_partition.iter().enumerate() {
            assert!(
                values.is_some_and(|values| values > 0),
                "`{query}`: every one of the {SCAN_PARTITIONS} scan partitions must fold at least \
                 one value, but partition {partition} reported {values:?}; all partitions: \
                 {per_partition:?}"
            );
        }
    } else {
        assert!(
            per_partition.iter().flatten().sum::<usize>() > 0,
            "`{query}`: the fused producer must report folded values, got {per_partition:?}"
        );
    }
    if let Some(root_values) = case.root_values {
        // One value per series that folded, so the partitions' counts add up to the number of series
        // the scan selected - every counter series, or only the ones a matcher kept.
        let counts = per_partition
            .iter()
            .map(|values| values.unwrap_or(0))
            .collect::<Vec<_>>();
        assert_eq!(
            root_values,
            counts.iter().sum::<usize>(),
            "`{query}`: the partitions must fold one value per selected series, {root_values} in \
             all, got {counts:?}"
        );
        // A case whose four partitions are all asserted nonempty above splits those values as
        // evenly as four positive counts allow, so no partition holds more than
        // `root_values - (SCAN_PARTITIONS - 1)`: a group of three series (`count by (job)` below
        // reports three for `job = a`) therefore cannot sit in one partition, and the group's
        // states have to be merged across at least two of them. A case a matcher narrows pins only
        // the total: which partitions keep a selected series depends on how the scan ranges were
        // spread, and the answer itself - not a bound on the split - is what shows the unselected
        // series were dropped.
        if case.per_partition {
            let largest = counts.iter().copied().max().unwrap_or(0);
            assert_eq!(
                root_values - (SCAN_PARTITIONS - 1),
                largest,
                "`{query}`: {root_values} values over {SCAN_PARTITIONS} nonempty partitions leave \
                 no partition holding more than {}, got {counts:?}",
                root_values - (SCAN_PARTITIONS - 1)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_aggregate_topk_over_global_partitions() {
    common_telemetry::init_default_ut_logging();
    const TABLE: &str = "native_topk_counters";
    const T1: i64 = BASE_SECS + 180;
    const T2: i64 = BASE_SECS + 600;
    let observer = Arc::new(NativePlanObserver {
        record_original_streams: true,
        ..Default::default()
    });
    let plugins = Plugins::new();
    plugins.insert(Arc::clone(&observer) as PhysicalPlanWrapperRef);
    let standalone = GreptimeDbStandaloneBuilder::new("native_aggregate_topk")
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .with_plugin(plugins)
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    execute_sql(&instance, &format!(
        "CREATE TABLE {TABLE} (ts TIMESTAMP(3) TIME INDEX, hostname STRING, device STRING, \"region\" STRING, v DOUBLE, PRIMARY KEY (hostname, device, \"region\")) WITH ('compaction.type' = 'twcs', 'compaction.twcs.trigger_file_num' = '100')"
    )).await;
    // Flush host00/dev0 first and host00/dev1 last; flush each of the 28 series independently.
    let order = std::iter::once((0usize, 0usize))
        .chain((1..14).flat_map(|host| (0..2).map(move |device| (host, device))))
        .chain(std::iter::once((0usize, 1usize)));
    for (host, device) in order {
        let slope_before = 140 - 10 * host as i64;
        let slope_after = 10 + 10 * host as i64;
        let mut values = Vec::new();
        for elapsed in (0i64..=600).step_by(30) {
            let value =
                100_000 + slope_before * elapsed.min(300) + slope_after * (elapsed - 300).max(0);
            values.push(format!(
                "({}, 'host{host:02}', 'dev{device}', 'us-west-2', {value}.0)",
                (BASE_SECS + elapsed) * 1000
            ));
        }
        execute_sql(
            &instance,
            &format!(
                "INSERT INTO {TABLE} (ts, hostname, device, \"region\", v) VALUES {}",
                values.join(",")
            ),
        )
        .await;
        execute_sql(&instance, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
    }
    let promql = format!(
        "topk(10, sum by(hostname)(rate({TABLE}{{device!=\"lo\",region=\"us-west-2\"}}[5m])))"
    );
    let sql = format!("TQL EVAL ({T1}, {T2}, '7m') {promql}");
    let run = async |native: bool| {
        let mark = observer.mark();
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, SCAN_PARTITIONS.to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, native.to_string());
        let output = instance
            .do_query(&sql, Arc::new(ctx))
            .await
            .remove(0)
            .unwrap();
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            other => panic!("unexpected output: {other:?}"),
        };
        (
            QueryResult::from_batches(&batches, &sql),
            NativeCapture::from_plans(&observer.since(mark)),
        )
    };
    let (legacy, legacy_capture) = run(false).await;
    assert!(legacy_capture.fused.is_empty() && legacy_capture.finalize.is_empty());
    let (native, capture) = run(true).await;
    let partial_batches = observer.partial_batches();
    assert_same_columns(&native, &legacy, &sql);
    assert_rows(
        &native.rows,
        &legacy.rows,
        &format!("native vs legacy `{sql}`"),
    );
    assert_eq!(20, native.rows.len(), "both timestamps return ten rows");
    let distinct_timestamps = native
        .rows
        .iter()
        .map(|row| row.ts_millis)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        [T1 * 1000, T2 * 1000]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        distinct_timestamps
    );
    let mut expected = Vec::new();
    for (ts, hosts, post) in [(T1 * 1000, 0..10, false), (T2 * 1000, 4..14, true)] {
        for host in hosts {
            let slope = if post {
                10 + 10 * host
            } else {
                140 - 10 * host
            };
            let scale = if post { 2.0 } else { 1.3 };
            expected.push(ResultRow {
                ts_millis: ts,
                labels: [("hostname".to_string(), format!("host{host:02}"))].into(),
                value: Some(slope as f64 * scale),
            });
        }
    }
    expected.sort_by(|a, b| (a.ts_millis, &a.labels).cmp(&(b.ts_millis, &b.labels)));
    assert_rows(&native.rows, &expected, &format!("expected `{sql}`"));
    assert_eq!(1, capture.fused.len());
    assert_eq!(1, capture.finalize.len());
    let fused_plan = displayable(capture.fused[0].as_ref())
        .indent(false)
        .to_string();
    assert!(
        fused_plan.contains("partial state: sum by (hostname) Complete"),
        "{fused_plan}"
    );
    let producer_tree = fused_plan
        .split_once("NativeTree")
        .map(|(_, tree)| tree.split("partial state:").next().unwrap_or(tree))
        .unwrap_or(&fused_plan);
    assert!(
        !producer_tree.to_ascii_lowercase().contains("topk"),
        "{fused_plan}"
    );
    let finalize_plan = displayable(capture.finalize[0].as_ref())
        .indent(false)
        .to_string();
    assert!(
        finalize_plan.to_ascii_lowercase().contains("topk"),
        "{finalize_plan}"
    );
    let per_partition = fused_output_values_per_partition(&capture, &sql);
    assert!(
        per_partition
            .iter()
            .all(|count| count.is_some_and(|count| count > 0)),
        "{per_partition:?}"
    );
    assert_eq!(
        56,
        per_partition.iter().flatten().sum::<usize>(),
        "{per_partition:?}"
    );

    // Prove the partition-local states overlap without decoding or summing their values: at each
    // evaluation point, at least one selected hostname/point/field was emitted by distinct original
    // producer partitions.
    let mut owners = BTreeMap::<(String, u64, u32), BTreeSet<usize>>::new();
    for (partition, batch) in &partial_batches {
        let hostname = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let batch_point = batch
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        let field = batch
            .column(5)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            owners
                .entry((
                    hostname.value(row).to_string(),
                    batch_point.value(row),
                    field.value(row),
                ))
                .or_default()
                .insert(*partition);
        }
    }
    for (point, expected_hosts) in [(0u64, 0..10), (1u64, 4..14)] {
        assert!(
            expected_hosts.into_iter().any(|host| {
                owners
                    .get(&(format!("host{host:02}"), point, 0))
                    .is_some_and(|partitions| partitions.len() >= 2)
            }),
            "no selected hostname has rows from distinct original producer partitions at point {point}: {owners:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_aggregate_complete_over_four_nonempty_mito_partitions() {
    common_telemetry::init_default_ut_logging();

    let observer = Arc::new(NativePlanObserver::default());
    let plugins = Plugins::new();
    plugins.insert(Arc::clone(&observer) as PhysicalPlanWrapperRef);
    // The engine-wide switch alone leaves the route off: a query also has to ask for it, which every
    // native run below does and every legacy run does not.
    let standalone = GreptimeDbStandaloneBuilder::new("native_aggregate_complete")
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .with_plugin(plugins)
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    setup_fixture(&instance).await;

    /// The two expected rows of a query grouped by `job`.
    fn by_job(a: f64, b: f64) -> [(&'static [(&'static str, &'static str)], f64); 2] {
        const GROUP_A: &[(&str, &str)] = &[("job", "a")];
        const GROUP_B: &[(&str, &str)] = &[("job", "b")];
        [(GROUP_A, a), (GROUP_B, b)]
    }
    // Both tables have exactly one value field (`v`), so a bare table selector reads it. A
    // `__field__` matcher would narrow the field set, and the native route declines such a query.
    let cases = [
        case(
            format!("sum by (job) ({GAUGE_TABLE})"),
            &by_job(18.0, 12.0),
            true,
        ),
        // The gauge's groups average to 6 and 6, and the producer's plan line has to name the
        // `avg by (job)` the split cut out: what a partition exports is that aggregate's state, not
        // the group's mean. The counter table's cases below are the ones whose answers can tell a
        // merged state from a finished one.
        AggregateCase {
            partial_state: Some("partial state: avg by (job) Complete"),
            ..case(
                format!("avg by (job) ({GAUGE_TABLE})"),
                &by_job(6.0, 6.0),
                true,
            )
        },
        case(
            format!("count by (job) ({GAUGE_TABLE})"),
            &by_job(3.0, 2.0),
            true,
        ),
        // `a` deviates by sqrt(32/3) and `b` by 2.
        case(
            format!("stddev by (job) ({GAUGE_TABLE})"),
            &by_job((32.0f64 / 3.0).sqrt(), 2.0),
            true,
        ),
        case(
            format!("sum without (instance) ({GAUGE_TABLE})"),
            &by_job(18.0, 12.0),
            true,
        ),
        case(
            format!("abs(sum by (job) ({GAUGE_TABLE}))"),
            &by_job(18.0, 12.0),
            true,
        ),
        // The inner average is one value per group and the outer sum over the ungrouped result is a
        // single 12, so the merge's suffix really runs on merged values.
        case(
            format!("sum(avg({GAUGE_TABLE}) by (job))"),
            &[(&[], 12.0)],
            true,
        ),
        // A literal scalar above the fold: `sum` over the gauge's 2, 6, 10, 4 and 8 is 30, and the
        // merge's suffix doubles the merged value.
        case(format!("sum({GAUGE_TABLE}) * 2"), &[(&[], 60.0)], true),
        // A gauge over a constant value has a rate of exactly zero, so these two only cover the
        // zero-rate case: every series has both samples inside the 5m window, hence every partition
        // must fold a value. They cannot tell a computed rate from a dropped one, which is what the
        // counter table's cases below are for.
        case(format!("sum(rate({GAUGE_TABLE}[5m]))"), &[(&[], 0.0)], true),
        case(
            format!("sum by (job) (rate({GAUGE_TABLE}[5m]))"),
            &by_job(0.0, 0.0),
            true,
        ),
        // The counter table: the same five series in the same four files, twice each, with the
        // increases of `COUNTER_SERIES`, so every value below is a nonzero rate that
        // `increase / 150` derives by hand - 1, 3 and 5 for `job = a`, 2 and 4 for `job = b`.
        //
        // These are the acceptance cases for the partial/final state bridge. Each of the four
        // partitions folds the states of the series its own scan returned and the merge above it
        // folds them together, so the answers are only reachable when every partition's state
        // arrives: `count` reports 3 and 2, and `avg`/`stddev` fold states that do not decompose
        // into per-partition values. `job = a`'s three series cannot all be in one of the four
        // partitions - the per-partition counts asserted for the counter cases that read every
        // series rule that out - so its mean and its standard deviation are merged across at least
        // two partitions. A standard deviation finished per partition and then averaged would
        // report 0.5 or 1.0 for the 2+1 splits the counts leave possible, never sqrt(8/3).
        counter_case(
            format!("sum(rate({COUNTER_TABLE}[5m]))"),
            &[(&[], 15.0)],
            "partial state: sum by () Complete",
        ),
        // The same sum with a literal scalar above the fold: the merge's suffix doubles the merged
        // 15 to 30, where a suffix run over one partition's share - at most 10 of the 15 - would
        // answer at most 20, and a suffix that divided instead would answer 7.5. The cut is still
        // the ungrouped `sum`, which the producer's plan line names.
        counter_case(
            format!("sum(rate({COUNTER_TABLE}[5m])) * 2"),
            &[(&[], 30.0)],
            "partial state: sum by () Complete",
        ),
        counter_case(
            format!("sum by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job(9.0, 6.0),
            "partial state: sum by (job) Complete",
        ),
        // The matcher selects only `job = a`'s three series. Their rates are 1, 3 and 5, so the
        // native partial producer must still run and the merged result must be 9. Three selected
        // series cannot populate all four scan partitions, so this case checks that some producer
        // output exists without applying the unfiltered fixture's four-nonempty-partition assertion.
        AggregateCase {
            partial_state: Some("partial state: sum by () Complete"),
            ..case(
                format!("sum(rate({COUNTER_TABLE}{{job=\"a\"}}[5m]))"),
                &[(&[], 9.0)],
                false,
            )
        },
        // A matcher on the tag only the counter table carries. The four flushed series are the four
        // `region = us-west-2` files and the series left in the memtable range is `us-east-1`, so
        // this selects four of the five scan ranges: `job = a` keeps its 1, 3 and 5 - nine - while
        // `job = b` keeps only `h4`'s 2, where the case above answers 6. The pruned range still
        // runs and may leave its partition with nothing to fold, so `per_partition` is left off and
        // only the total is pinned: one value for each of the four selected series. The answer's
        // labels and values say which series survived, and every case here still requires the
        // producer to read the four-partition scan.
        AggregateCase {
            root_values: Some(COUNTER_SERIES.len() - 1),
            partial_state: Some("partial state: sum by (job) Complete"),
            ..case(
                format!(
                    "sum by (job) (rate({COUNTER_TABLE}{{region=\"{COUNTER_WEST_REGION}\"}}[5m]))"
                ),
                &by_job(9.0, 2.0),
                false,
            )
        },
        counter_case(
            format!("avg by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job(3.0, 3.0),
            "partial state: avg by (job) Complete",
        ),
        counter_case(
            format!("count by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job(3.0, 2.0),
            "partial state: count by (job) Complete",
        ),
        // `a`'s rates are 1, 3 and 5, whose population standard deviation is sqrt(8/3); `b`'s are 2
        // and 4, whose is 1. Both are unchanged when the group's states are merged, which is the
        // point: the merge folds sum-of-squared-deviation states, not completed answers.
        counter_case(
            format!("stddev by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job((8.0f64 / 3.0).sqrt(), 1.0),
            "partial state: stddev by (job) Complete",
        ),
        // Two nested aggregates and a map above them: the merge's suffix is `sum(abs(x))` over the
        // inner aggregate's merged values, so it really runs on merged values rather than on one
        // partition's answer. Each group's mean is 3, so the suffix sums to 6 - and the cut is the
        // inner `avg`, which is what the producer's own plan line has to name.
        counter_case(
            format!("sum(abs(avg(rate({COUNTER_TABLE}[5m])) by (job)))"),
            &[(&[], 6.0)],
            "partial state: avg by (job) Complete",
        ),
        // The same, with the map above both aggregates instead of between them.
        counter_case(
            format!("abs(sum(avg(rate({COUNTER_TABLE}[5m])) by (job)))"),
            &[(&[], 6.0)],
            "partial state: avg by (job) Complete",
        ),
        // A literal scalar below the fold is carried in the per-series prefix, so the state folds
        // `8 * rate`: the rates 1, 3, 5, 2 and 4 become 8, 24, 40, 16 and 32 and the merge answers
        // 120, where a state built without the multiplication would answer 15. The multiplication
        // distributes over the sum, so 120 alone cannot say which side of the merge computed it;
        // what this case pins down is that the constant is admitted below the cut and that the four
        // nonempty partitions still fold one value per series into that one state.
        counter_case(
            format!("sum(8 * rate({COUNTER_TABLE}[5m]))"),
            &[(&[], 120.0)],
            "partial state: sum by () Complete",
        ),
        // The same below-the-fold placement with an operator the fold does not commute with: the
        // maximum of the prefix-computed `10 - rate`. The rates 1, 3, 5, 2 and 4 become 9, 7, 5, 8
        // and 6, whose maximum is 9 - so the answer tells the prefix from the suffix, where
        // `10 - max(rate)` would answer 5, and from a prefix that swapped the operands, where
        // `max(rate - 10)` would answer -5.
        counter_case(
            format!("max(10 - rate({COUNTER_TABLE}[5m]))"),
            &[(&[], 9.0)],
            "partial state: max by () Complete",
        ),
        // A literal scalar above the fold is rebuilt in the merge's suffix over the merged value,
        // with the operands in the order the query wrote them: the mean rate is 3, so `100 - 3` is
        // 97 where a swapped pair would report -97, and `100 / 15` is the sum's own answer where
        // 15/100 would be the reciprocal. A negation is the same broadcast with `-1` on the left.
        counter_case(
            format!("100 - avg(rate({COUNTER_TABLE}[5m]))"),
            &[(&[], 97.0)],
            "partial state: avg by () Complete",
        ),
        counter_case(
            format!("100 / sum(rate({COUNTER_TABLE}[5m]))"),
            &[(&[], 100.0 / 15.0)],
            "partial state: sum by () Complete",
        ),
        counter_case(
            format!("-sum(rate({COUNTER_TABLE}[5m]))"),
            &[(&[], -15.0)],
            "partial state: sum by () Complete",
        ),
        // The range table holds one series in one partition, so this is a correctness smoke test of
        // one populated partition rather than a parallel acceptance. Its one series folds one
        // `count_over_time` value at the single grid point.
        case(
            format!("sum(count_over_time({RANGE_TABLE}[10m]))"),
            &[(&[], 2.0)],
            false,
        ),
    ];
    for case in &cases {
        check_case(&instance, &observer, case).await;
    }

    // The shapes the native compiler declines, each declined for its own reason, and each checked on
    // the plans that really ran: none may hold a fused producer or a frontend merge, and both routes
    // must answer what the fixture derives by hand.
    //
    // `time()` is the evaluation instant and nothing else, so the single row is at `EVAL_MILLIS` and
    // carries `EVAL_SECS`. `scalar` reports the one value present at a point only when exactly one
    // series is present at it, so the five counter series make it NaN; a route that folded one
    // partition at a time would report the number of whichever partition held a single series, so
    // the NaN also says the whole vector was read at once. The division folds two *different* tables
    // - 15 over 30 - and a subquery evaluates its own body on a grid of its own: both read across
    // series and neither can be answered by a producer per partition, so both keep the existing
    // plan. The subquery's window spans the two inner points `EVAL_SECS - 1` and `EVAL_SECS`, at each
    // of which the same extrapolated rate answers 15 - both samples stay further from the window's
    // ends than the 66s threshold - so the maximum is 15 as well.
    let fallback_cases = [
        case("time()".to_string(), &[(&[], EVAL_SECS as f64)], false),
        case(
            format!("scalar({COUNTER_TABLE})"),
            &[(&[], f64::NAN)],
            false,
        ),
        case(
            format!("sum(rate({COUNTER_TABLE}[5m])) / sum({GAUGE_TABLE})"),
            &[(&[], 15.0 / 30.0)],
            false,
        ),
        case(
            format!("max_over_time((sum(rate({COUNTER_TABLE}[5m])))[2s:1s])"),
            &[(&[], 15.0)],
            false,
        ),
    ];
    for case in &fallback_cases {
        check_fallback_case(&instance, &observer, case).await;
    }
}
