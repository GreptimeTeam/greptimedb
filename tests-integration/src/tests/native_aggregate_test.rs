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
//! The frontend tree holds a Final `FusedSeriesExec` over a `SeriesRepartitionExec` and its
//! `MergeScanExec` leaf. The Partial producer and its raw `SeriesScanExec` input are in the
//! separately captured region physical plan, not in that frontend tree. Both trees are checked
//! independently through [`PhysicalPlanWrapper`], which both query engines install, so what is
//! checked is the plan that ran rather than a re-plan made for an `EXPLAIN`.
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
//! shapes the compiler has no node for - `scalar(v)` and a subquery - with the native request on and
//! with it off, and requires that no native node ran in either plan and that both runs report rows
//! derived from the fixture by hand. A hint that quietly
//! took the route for one of them would show a fused producer or a frontend merge in the plans that
//! executed, which is what is observed.
//!
//! A binary operation whose sibling is a literal scalar is carried by the split on either side of
//! the fold: below the aggregate it is computed in the per-series prefix (`sum(8 * rate(...))`,
//! `max(10 - rate(...))`), and above it the merge's suffix rebuilds it over the merged value with
//! the operands in the order they were written (`100 - avg(rate(...))`, `100 / sum(rate(...))`,
//! `-sum(rate(...))`). Deliberately not covered yet: the aggregate split carries a literal scalar
//! as its only binary sibling, so a whole-vector combination such as `sort`, `absent` or
//! `and`/`or`/`unless` beside the aggregate keeps the existing plan. `time()` reads no data and runs
//! natively without the split.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use common_base::Plugins;
use common_query::OutputData;
use common_recordbatch::RecordBatches;
use common_recordbatch::util::collect_batches;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricValue;
use datatypes::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::series::frame::{FrameContent, SeriesFrameDeclaration};
use frontend::instance::Instance;
use promql::extension_plan::{FusedSeriesExec, SeriesRepartitionExec, SeriesRouting};
use promql::vector_match::MatchingLabels;
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
/// The metric a fused producer registers for the values its own root node produced, per partition.
const ROOT_OUTPUT_VALUES_METRIC: &str = "native.root.output_values";
/// The number of sample positions consumed by the root node, per partition.
const ROOT_INPUT_SAMPLES_METRIC: &str = "native.root.input_samples";
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
/// The right operand of the hybrid binary cases. It has one region but a partition column, so a
/// pair reading it is declined by the two-scan native route and planned by the hybrid one.
const COUNTER_RHS_TABLE: &str = "native_agg_counter_rhs";
/// The table the histogram queries read: one bucket series per `(job, le)`, with `le` in the primary
/// key so the raw-series scan hands the bound over as a label.
const HIST_TABLE: &str = "native_agg_hist_bucket";
/// The table the range-window smoke test reads.
const RANGE_TABLE: &str = "native_agg_range";
const HYBRID_MATCH_LEFT: &str = "native_hybrid_match_left";
/// Partitioned like [`COUNTER_RHS_TABLE`], so the hybrid route plans a pair reading it.
const HYBRID_MATCH_RIGHT: &str = "native_hybrid_match_right";
/// The same rows as [`HYBRID_MATCH_RIGHT`] without a partition column, so the two-scan route plans
/// a pair reading it.
const HYBRID_MATCH_RIGHT_UNPARTITIONED: &str = "native_hybrid_match_right_unpartitioned";
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

/// The count both samples of every histogram bucket series start from: the same value as
/// [`COUNTER_FIRST_VALUE`], and for the same reason - `rate`'s counter-zero clamp must not bind.
/// Here it is asked to cover increases of at most 6, so `interval * first / increase` is
/// `60 * 1500 / 6 = 15000s`, far above the 30s the boundary extrapolation adds.
const HIST_FIRST_VALUE: f64 = COUNTER_FIRST_VALUE;

/// The bucket series of the histogram fixture in insert order, as `(job, le, increase)`: three
/// bounds per job, each holding a cumulative count, so a bound's count is never below the count of
/// the bound beneath it - the monotonicity `histogram_quantile` and `histogram_fraction` require
/// before they interpolate. The increases are 1, 2, 3 for `job = a` and 4, 5, 6 for `job = b`, all
/// distinct, so a route that read a bucket twice or lost one answers a different number.
///
/// `rate` over the two samples at the counter fixture's instants divides by 150 exactly as
/// [`COUNTER_SERIES`] does, so `sum by (le)` over both jobs leaves 5/150, 7/150 and 9/150 in the
/// buckets `0.1`, `1` and `+Inf`. Those three counts put `histogram_quantile(0.99, ...)`'s rank
/// inside the `+Inf` bucket, where the answer is the bound below it - 1 - and
/// `histogram_quantile(0.5, ...)`'s nine tenths into the first bucket - 0.09 - while
/// `histogram_fraction(0, 1, ...)` closes under the `+Inf` count at 7/9.
const HIST_SERIES: [(&str, &str, f64); 6] = [
    ("a", "0.1", 1.0),
    ("a", "1", 2.0),
    ("a", "+Inf", 3.0),
    ("b", "0.1", 4.0),
    ("b", "1", 5.0),
    ("b", "+Inf", 6.0),
];

/// Observes the physical plan each query engine is about to execute, and keeps it.
///
/// The frontend and the datanode it reads its regions from install the same plugin instance, so both
/// trees of one query arrive here. They are kept in a list because one query produces one entry per
/// engine, and keeping a single plan would lose the region's.
#[derive(Default)]
struct NativePlanObserver {
    seen: Mutex<Vec<Arc<dyn ExecutionPlan>>>,
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
}

impl PhysicalPlanWrapper for NativePlanObserver {
    fn wrap(
        &self,
        origin: Arc<dyn ExecutionPlan>,
        _ctx: QueryContextRef,
    ) -> Arc<dyn ExecutionPlan> {
        self.seen.lock().unwrap().push(Arc::clone(&origin));
        origin
    }
}

/// The native nodes one query's captured plans hold: region-side Partial producers and frontend
/// Final merges. The two are observed in different trees, so the lists are never matched by `Arc`; the
/// region producer's typed state-frame schema and the frontend routing chain are checked separately.
#[derive(Default)]
struct NativeCapture {
    fused: Vec<Arc<dyn ExecutionPlan>>,
    finalize: Vec<Arc<dyn ExecutionPlan>>,
}

struct NodeDisplay<'a>(&'a dyn ExecutionPlan);

impl fmt::Display for NodeDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Default, formatter)
    }
}

impl NativeCapture {
    /// Picks the nodes of interest out of the plans one query handed over.
    fn from_plans(plans: &[Arc<dyn ExecutionPlan>]) -> Self {
        let mut capture = Self::default();
        let mut stack = plans.to_vec();
        while let Some(node) = stack.pop() {
            if node.downcast_ref::<FusedSeriesExec>().is_some() {
                // Invoke this node's formatter directly: the execution-plan display formatter
                // recursively includes children and can misclassify an ancestor as Final.
                let display = NodeDisplay(node.as_ref()).to_string();
                if display.contains(" final") {
                    capture.finalize.push(Arc::clone(&node));
                } else {
                    capture.fused.push(Arc::clone(&node));
                }
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
    /// Every column type, in schema order.
    column_types: Vec<DataType>,
    /// The rows, in a canonical order.
    rows: Vec<ResultRow>,
}

impl QueryResult {
    /// Reads the batches of one PromQL result. The value column is found by type rather than name;
    /// its schema name is preserved in `columns`. `count` reports an integer, so every numeric type
    /// is accepted; the timestamp column was taken first and is therefore not a candidate.
    fn from_batches(batches: &RecordBatches, query: &str) -> Self {
        let schema = batches.schema();
        let fields = schema.arrow_schema().fields();
        let columns = fields
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let column_types = fields
            .iter()
            .map(|field| field.data_type().clone())
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

        Self {
            columns,
            column_types,
            rows,
        }
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
    /// Whether this case's global aggregate has an explicitly specified single owner.
    single_owner: bool,
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
        single_owner: false,
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
        .unwrap_or_else(|err| {
            // The top-level Display is one line; the chain names what actually failed, and without
            // it a planning error inside the operator layer is indistinguishable from a scan error.
            let mut chain = String::new();
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&err);
            while let Some(error) = source {
                chain.push_str(&format!("\n  caused by: {error}"));
                source = error.source();
            }
            panic!("`{sql}` failed: {err}{chain}");
        });
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap_or_else(|err| {
            let mut chain = String::new();
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&err);
            while let Some(error) = source {
                chain.push_str(&format!("\n  caused by: {error}"));
                source = error.source();
            }
            panic!("`{sql}` produced no batches: {err}{chain}");
        }),
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

#[tokio::test(flavor = "multi_thread")]
async fn native_hybrid_binary_05_06_08() {
    common_telemetry::init_default_ut_logging();
    let observer = Arc::new(NativePlanObserver::default());
    let plugins = Plugins::new();
    plugins.insert(Arc::clone(&observer) as PhysicalPlanWrapperRef);
    let standalone = GreptimeDbStandaloneBuilder::new("native_hybrid_binary")
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .with_plugin(plugins)
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    setup_fixture(&instance).await;
    execute_sql(&instance, &format!("CREATE TABLE {HYBRID_MATCH_LEFT} (ts TIMESTAMP(3) TIME INDEX, job STRING, instance STRING, v DOUBLE, PRIMARY KEY (job, instance))")).await;
    execute_sql(&instance, &format!("CREATE TABLE {HYBRID_MATCH_RIGHT} (ts TIMESTAMP(3) TIME INDEX, job STRING, instance STRING, v DOUBLE, PRIMARY KEY (job, instance)) PARTITION ON COLUMNS (job) ()")).await;
    execute_sql(&instance, &format!("CREATE TABLE {HYBRID_MATCH_RIGHT_UNPARTITIONED} (ts TIMESTAMP(3) TIME INDEX, job STRING, instance STRING, v DOUBLE, PRIMARY KEY (job, instance))")).await;
    execute_sql(
        &instance,
        &format!("INSERT INTO {HYBRID_MATCH_LEFT} VALUES ({EVAL_MILLIS}, 'api', 'a1', 10), ({EVAL_MILLIS}, 'other', 'a2', 11), ({EVAL_MILLIS}, 'other', 'a3', 12)"),
    )
    .await;
    for table in [HYBRID_MATCH_RIGHT, HYBRID_MATCH_RIGHT_UNPARTITIONED] {
        execute_sql(&instance, &format!("INSERT INTO {table} VALUES ({EVAL_MILLIS}, 'api', 'b1', 2), ({EVAL_MILLIS}, 'other', 'b2', 3), ({EVAL_MILLIS}, 'other', 'b3', 4)" )).await;
    }
    let propagated = format!("{HYBRID_MATCH_LEFT}{{job=\"api\"}} / on(job) {HYBRID_MATCH_RIGHT}");
    let (legacy, _) = run_query(&instance, &propagated, false, &observer).await;
    assert_eq!(
        vec![5.0],
        legacy
            .rows
            .iter()
            .map(|row| row.value.unwrap())
            .collect::<Vec<_>>()
    );
    let mark = observer.mark();
    let (hybrid, capture) = run_query(&instance, &propagated, true, &observer).await;
    assert_eq!(
        vec![5.0],
        hybrid
            .rows
            .iter()
            .map(|row| row.value.unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        2,
        capture.fused.len(),
        "both binary operands use native execution"
    );
    let plans = observer.since(mark);
    let frontend = plans
        .iter()
        .find(|plan| find_binary_join(plan).is_some())
        .expect("frontend Join plan");
    let join = find_binary_join(frontend).unwrap();
    let uniqueness = join
        .children()
        .iter()
        .map(|branch| uniqueness_operators_above_merge(branch))
        .fold((false, false), |(window, filter), found| {
            (window || found.0, filter || found.1)
        });
    assert!(
        uniqueness.0 && uniqueness.1,
        "on(job) FE uniqueness operators: {}",
        displayable(frontend.as_ref()).indent(false)
    );

    // The left selector is a nonempty single `api` series, while the unpartitioned RHS also
    // contains the duplicated `other` group. Prometheus rejects duplicate match groups on the
    // RHS even when the left side does not select that group. Keep the preceding successful `api`
    // query as coverage for the matching pair itself.
    let unpartitioned =
        format!("{HYBRID_MATCH_LEFT}{{job=\"api\"}} / on(job) {HYBRID_MATCH_RIGHT_UNPARTITIONED}");
    let mark = observer.mark();
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, SCAN_PARTITIONS.to_string());
    query_ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, "true".to_string());
    let output = instance
        .do_query(&tql_eval(&unpartitioned), Arc::new(query_ctx))
        .await
        .remove(0)
        .expect("the query plans");
    let error = match output.data {
        OutputData::Stream(stream) => collect_batches(stream)
            .await
            .expect_err("a duplicate match group fails"),
        other => panic!("`{unpartitioned}` was expected to stream: {other:?}"),
    };
    assert!(
        error.to_string().contains(
            "found duplicate series for the match group {job=\"other\"} on the right hand-side"
        ),
        "{unpartitioned}: {error}"
    );

    // Inspect the *failing query's* binary node, not a re-plan or a separate probe. Its operand
    // counters count valid instant values after the matching routers and before duplicate checking.
    let capture = NativeCapture::from_plans(&observer.since(mark));
    let binary = capture
        .fused
        .iter()
        .filter(|node| node.children().len() == 2)
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        binary.len(),
        "{unpartitioned}: expected one native binary"
    );
    let binary = binary[0];
    let plan = displayable(binary.as_ref()).indent(false).to_string();
    assert_eq!(
        SCAN_PARTITIONS,
        binary.properties().output_partitioning().partition_count(),
        "{unpartitioned}: binary partition count:\n{plan}"
    );
    for (side, child) in binary.children().iter().enumerate() {
        let router = child
            .downcast_ref::<SeriesRepartitionExec>()
            .unwrap_or_else(|| panic!("{unpartitioned}: operand {side} has no router:\n{plan}"));
        assert_eq!(SCAN_PARTITIONS, router.outputs(), "{unpartitioned}: {plan}");
        assert_eq!(
            &SeriesRouting::Matching(MatchingLabels::On(vec![Arc::from("job")])),
            router.routing_policy(),
            "{unpartitioned}: operand {side} routing policy:\n{plan}"
        );
    }
    let left =
        fused_node_count_per_partition(binary, "native.root.left.output_values", &unpartitioned);
    let right =
        fused_node_count_per_partition(binary, "native.root.right.output_values", &unpartitioned);
    assert!(
        left.iter().chain(&right).all(Option::is_some),
        "{unpartitioned}: operand metrics must be registered for every partition: left={left:?}, right={right:?}"
    );
    // The fixture has one LHS api series, one RHS api series, and two RHS other series.
    // The on(job) routers co-locate each group, so the only way to get these exact counts is
    // api on the LHS partition and both other samples on a different matching partition.
    let left_partition = left
        .iter()
        .position(|count| *count == Some(1))
        .unwrap_or_else(|| panic!("{unpartitioned}: missing single left value: {left:?}"));
    let other_partition = right
        .iter()
        .position(|count| *count == Some(2))
        .unwrap_or_else(|| panic!("{unpartitioned}: missing duplicated right group: {right:?}"));
    assert_ne!(
        left_partition, other_partition,
        "{unpartitioned}: left={left:?}, right={right:?}"
    );
    let mut expected_left = vec![Some(0); SCAN_PARTITIONS];
    expected_left[left_partition] = Some(1);
    let mut expected_right = vec![Some(0); SCAN_PARTITIONS];
    expected_right[left_partition] = Some(1);
    expected_right[other_partition] = Some(2);
    // Exact vectors also fix totals at 1 and 3 and require every other partition to be empty.
    assert_eq!(
        expected_left, left,
        "{unpartitioned}: LHS values per partition"
    );
    assert_eq!(
        expected_right, right,
        "{unpartitioned}: RHS values per partition"
    );
    common_telemetry::info!(
        "`{unpartitioned}` operand values per matching partition: left={left:?}, right={right:?}"
    );

    // The identical RHS contains duplicates, but a globally empty LHS suppresses their error.
    let empty_left = format!(
        "{HYBRID_MATCH_LEFT}{{instance=\"missing\"}} / on(job) {HYBRID_MATCH_RIGHT_UNPARTITIONED}"
    );
    let (empty, capture) = run_query(&instance, &empty_left, true, &observer).await;
    assert_rows(&empty.rows, &[], &format!("empty left `{empty_left}`"));
    let binary = capture
        .fused
        .iter()
        .filter(|node| node.children().len() == 2)
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        binary.len(),
        "{empty_left}: expected native two-source binary"
    );
    let control_left =
        fused_node_count_per_partition(binary[0], "native.root.left.output_values", &empty_left);
    let control_right =
        fused_node_count_per_partition(binary[0], "native.root.right.output_values", &empty_left);
    assert_eq!(
        vec![Some(0); SCAN_PARTITIONS],
        control_left,
        "{empty_left}: left must be globally empty"
    );
    assert_eq!(
        expected_right, control_right,
        "{empty_left}: the same duplicated RHS must reach the binary"
    );
    common_telemetry::info!(
        "`{empty_left}` operand values per matching partition: left={control_left:?}, right={control_right:?}"
    );

    check_hybrid_binary_cases(&instance, &observer).await;
    let subquery = format!(
        "max_over_time((rate({COUNTER_TABLE}[5m]) / on(job, instance) rate({COUNTER_RHS_TABLE}[5m]))[2m:1m])"
    );
    let (legacy, legacy_capture) = run_query(&instance, &subquery, false, &observer).await;
    let (native, native_capture) = run_query(&instance, &subquery, true, &observer).await;
    assert!(
        legacy_capture.fused.is_empty() && native_capture.fused.is_empty(),
        "hybrid must be disabled inside subquery: {subquery}"
    );
    assert_eq!(
        legacy.rows.len(),
        native.rows.len(),
        "subquery legacy/native row count"
    );
    assert_rows(
        &native.rows,
        &legacy
            .rows
            .iter()
            .map(|row| ResultRow {
                ts_millis: row.ts_millis,
                labels: row.labels.clone(),
                value: row.value,
            })
            .collect::<Vec<_>>(),
        &format!("subquery fallback `{subquery}`"),
    );
    check_reduced_label_matching(&instance, &observer).await;
}

async fn check_reduced_label_matching(instance: &Arc<Instance>, observer: &NativePlanObserver) {
    let unique_query = format!(
        "{GAUGE_TABLE}{{instance=~\"h1|h4\"}} / on(job) {COUNTER_RHS_TABLE}{{instance=~\"h1|h4\"}}"
    );
    let (legacy, legacy_capture) = run_query(instance, &unique_query, false, observer).await;
    assert!(legacy_capture.fused.is_empty());
    let mark = observer.mark();
    let (native, capture) = run_query(instance, &unique_query, true, observer).await;
    assert_eq!(
        legacy.rows.len(),
        native.rows.len(),
        "{unique_query}: row count"
    );
    assert_rows(
        &native.rows,
        &legacy
            .rows
            .iter()
            .map(|row| ResultRow {
                ts_millis: row.ts_millis,
                labels: row.labels.clone(),
                value: row.value,
            })
            .collect::<Vec<_>>(),
        &format!("on(job) native == legacy `{unique_query}`"),
    );
    assert_eq!(
        1,
        native.rows.len(),
        "the matching instance filters leave one matching job"
    );
    assert_eq!(2, capture.fused.len(), "{unique_query}");
    let plans = observer.since(mark);
    let frontend = plans
        .iter()
        .find(|plan| find_binary_join(plan).is_some())
        .expect("frontend Join plan");
    let join = find_binary_join(frontend).unwrap();
    let uniqueness = join
        .children()
        .iter()
        .map(|branch| uniqueness_operators_above_merge(branch))
        .fold((false, false), |(window, assertion_filter), found| {
            (window || found.0, assertion_filter || found.1)
        });
    assert!(
        uniqueness.0,
        "on(job) requires a frontend uniqueness WindowAggExec above MergeScan: {}",
        displayable(frontend.as_ref()).indent(false)
    );
    assert!(
        uniqueness.1,
        "on(job) requires a frontend prom_assert_unique_match_group FilterExec above MergeScan: {}",
        displayable(frontend.as_ref()).indent(false)
    );
    for branch in join.children() {
        assert!(
            contains_execution_node(branch, "MergeScanExec"),
            "on(job) FE branch must retain remote boundary: {}",
            displayable(frontend.as_ref()).indent(false)
        );
        assert!(
            !contains_execution_node(branch, FUSED_EXEC_NAME),
            "on(job) fused node must stay in region"
        );
    }
    for fused in &capture.fused {
        assert_fused_reads_scan_directly(fused, &unique_query);
    }

    let duplicate_query = format!("{GAUGE_TABLE} / on(job) {COUNTER_RHS_TABLE}");
    for enabled in [false, true] {
        let sql = tql_eval(&duplicate_query);
        let mut query_ctx = QueryContext::with_db_name(None);
        query_ctx.set_extension(QUERY_PARALLELISM_HINT, SCAN_PARTITIONS.to_string());
        query_ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, enabled.to_string());
        let output = instance
            .do_query(&sql, Arc::new(query_ctx))
            .await
            .remove(0)
            .expect("duplicate query should produce an execution stream");
        let error = match output.data {
            OutputData::Stream(stream) => collect_batches(stream)
                .await
                .expect_err("duplicate on(job) matches must fail"),
            OutputData::RecordBatches(_) => panic!("duplicate on(job) matches must fail"),
            other => panic!("unexpected duplicate-query output: {other:?}"),
        };
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            message.contains("many-to-many")
                || message.contains("multiple matches")
                || message.contains("duplicate"),
            "`{sql}` should fail for duplicate matching groups, got: {error}"
        );
    }
}

async fn check_hybrid_binary_cases(instance: &Arc<Instance>, observer: &NativePlanObserver) {
    let cases = [
        (
            format!("{GAUGE_TABLE} / on(job, instance) {COUNTER_RHS_TABLE}{{instance!=\"h5\"}}"),
            4usize,
        ),
        (
            format!(
                "100 * (1 - ({GAUGE_TABLE} / on(job, instance) {COUNTER_RHS_TABLE}{{instance!=\"h5\"}}))"
            ),
            4usize,
        ),
        (
            format!("rate({COUNTER_TABLE}[5m]) / on(job, instance) rate({COUNTER_RHS_TABLE}[5m])"),
            5usize,
        ),
    ];
    for (index, (promql, expected_rows)) in cases.into_iter().enumerate() {
        let (legacy, legacy_capture) = run_query(instance, &promql, false, observer).await;
        assert!(legacy_capture.fused.is_empty());
        let mark = observer.mark();
        let (native, capture) = run_query(instance, &promql, true, observer).await;
        assert_eq!(
            legacy.rows.len(),
            native.rows.len(),
            "{promql}: full row count"
        );
        assert_rows(
            &native.rows,
            &legacy
                .rows
                .iter()
                .map(|row| ResultRow {
                    ts_millis: row.ts_millis,
                    labels: row.labels.clone(),
                    value: row.value,
                })
                .collect::<Vec<_>>(),
            &format!("native == legacy `{promql}`"),
        );
        assert_eq!(expected_rows, native.rows.len(), "{promql}");
        assert_eq!(
            legacy
                .columns
                .iter()
                .filter(|column| column.ends_with("ts"))
                .count(),
            1,
            "{promql}: legacy timestamp column"
        );
        assert_eq!(
            native
                .columns
                .iter()
                .filter(|column| column.ends_with("ts"))
                .count(),
            1,
            "{promql}: native timestamp column"
        );
        if index == 2 {
            let expected = [0.5; 5];
            assert!(
                native.rows.iter().zip(expected).all(|(row, expected)| row
                    .value
                    .is_some_and(|value| close_enough(value, expected))),
                "{promql}: independently scaled RHS rates must have expected ratios: {:#?}",
                native.rows
            );
        }
        if index < 2 {
            assert!(
                native
                    .rows
                    .iter()
                    .all(|row| row.labels.get("instance").is_none_or(|v| v != "h5")),
                "one-to-one match must drop unmatched series"
            );
        }
        assert_eq!(2, capture.fused.len(), "{promql}: plans since {mark}");
        let region_plans = observer
            .since(mark)
            .into_iter()
            .filter(|plan| contains_execution_node(plan, "SeriesScanExec"))
            .collect::<Vec<_>>();
        let scan_tables = region_plans
            .iter()
            .map(|plan| {
                let text = displayable(plan.as_ref()).indent(false).to_string();
                if text.contains("native_agg_gauge") {
                    GAUGE_TABLE
                } else if text.contains(COUNTER_RHS_TABLE) {
                    COUNTER_RHS_TABLE
                } else if text.contains(COUNTER_TABLE) {
                    COUNTER_TABLE
                } else {
                    panic!("{promql}: unable to identify scan table: {text}")
                }
            })
            .collect::<Vec<_>>();
        assert!(
            scan_tables.contains(&GAUGE_TABLE) || scan_tables.contains(&COUNTER_TABLE),
            "{promql}: gauge/counter source missing: {scan_tables:?}"
        );
        assert!(
            scan_tables.contains(&COUNTER_RHS_TABLE),
            "{promql}: independent RHS scan missing: {scan_tables:?}"
        );
        for fused in &capture.fused {
            assert_fused_reads_scan_directly(fused, &format!("hybrid `{promql}`"));
        }
        for fused in &capture.fused {
            let per_fused = NativeCapture {
                fused: vec![Arc::clone(fused)],
                finalize: Vec::new(),
            };
            let counts = fused_root_metric_per_partition(
                &per_fused,
                ROOT_OUTPUT_VALUES_METRIC,
                &format!("hybrid `{promql}`"),
            );
            assert!(
                counts.iter().all(|count| count.is_some_and(|n| n > 0)),
                "{promql}: each scan partition must emit values, got {counts:?}"
            );
        }
        let plans = observer.since(mark);
        let frontend = plans
            .iter()
            .find(|plan| find_binary_join(plan).is_some())
            .unwrap_or_else(|| panic!("{promql}: FE JoinExec missing; plans: {plans:#?}"));
        let join = find_binary_join(frontend).unwrap();
        assert_eq!(
            2,
            join.children().len(),
            "{promql}: FE join branches: {}",
            displayable(frontend.as_ref()).indent(false)
        );
        for branch in join.children() {
            assert!(
                contains_execution_node(branch, "MergeScanExec"),
                "{promql}: FE Join branch lacks MergeScanExec: {}",
                displayable(frontend.as_ref()).indent(false)
            );
            assert!(
                !contains_execution_node(branch, FUSED_EXEC_NAME),
                "{promql}: FusedSeriesExec leaked onto frontend: {}",
                displayable(frontend.as_ref()).indent(false)
            );
        }
        assert!(
            !is_nested_under_merge(frontend, "HashJoinExec"),
            "{promql}: FE Join is under MergeScan: {}",
            displayable(frontend.as_ref()).indent(false)
        );
        if index == 1 {
            assert!(
                has_projection_parent_of_join(frontend),
                "06 outer scalar arithmetic must project above JoinExec: {}",
                displayable(frontend.as_ref()).indent(false)
            );
        }
    }
}

fn find_binary_join(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    if matches!(
        plan.name(),
        "HashJoinExec" | "NestedLoopJoinExec" | "CrossJoinExec"
    ) {
        return Some(plan);
    }
    plan.children()
        .iter()
        .find_map(|child| find_binary_join(child))
}

fn find_execution_node<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    name: &str,
) -> Option<&'a Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(plan);
    }
    plan.children()
        .iter()
        .find_map(|child| find_execution_node(child, name))
}

fn contains_execution_node(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
    find_execution_node(plan, name).is_some()
}

/// Finds the PromQL uniqueness validation operators on the frontend side of a remote scan.
/// Traversal stops at each MergeScan so it cannot accidentally match region-side execution.
fn uniqueness_operators_above_merge(plan: &Arc<dyn ExecutionPlan>) -> (bool, bool) {
    if plan.name() == "MergeScanExec" {
        return (false, false);
    }

    let mut has_window = matches!(plan.name(), "WindowAggExec" | "BoundedWindowAggExec");
    let mut has_assertion_filter = plan.name() == "FilterExec"
        && format!("{plan:?}").contains("prom_assert_unique_match_group");
    for child in plan.children() {
        let (child_window, child_filter) = uniqueness_operators_above_merge(child);
        has_window |= child_window;
        has_assertion_filter |= child_filter;
    }
    (has_window, has_assertion_filter)
}

fn is_nested_under_merge(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
    fn walk(plan: &Arc<dyn ExecutionPlan>, under_merge: bool, name: &str) -> bool {
        if under_merge && plan.name() == name {
            return true;
        }
        plan.children()
            .iter()
            .any(|child| walk(child, under_merge || plan.name() == "MergeScanExec", name))
    }
    walk(plan, false, name)
}

fn has_projection_parent_of_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.name() == "ProjectionExec"
        && plan
            .children()
            .iter()
            .any(|child| find_binary_join(child).is_some())
        || plan
            .children()
            .iter()
            .any(|child| has_projection_parent_of_join(child))
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

/// The DDL of [`HIST_TABLE`]: [`create_table_sql`]'s shape with the instance replaced by the bucket
/// bound.
///
/// `le` is part of the primary key so that it is a label of the series the raw-series scan reads,
/// not a field of one: the histogram functions find a bucket by a `le` label whose value parses as a
/// bound, and a bound that arrived as a field would leave them no bucket to read. TWCS is set as on
/// every other fixture table, so one flush per bucket series leaves several files for the scan to
/// split.
fn hist_table_sql() -> String {
    format!(
        "CREATE TABLE {HIST_TABLE} (ts TIMESTAMP(3) TIME INDEX, job STRING, le STRING, v DOUBLE, \
         PRIMARY KEY (job, le)) WITH ('compaction.type' = 'twcs', \
         'compaction.twcs.trigger_file_num' = '100')"
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
/// [`COUNTER_EAST_REGION`]. The histogram table holds the six bucket series of [`HIST_SERIES`] at
/// the counter samples' instants, flushed after each series but the last, so its buckets reach the
/// scan the same way - several files, one memtable range - and a bucket's partial sum is one
/// partition's to fold. The range table gets one series whose samples are 390s apart: both are
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
    execute_sql(instance, &format!("CREATE TABLE {COUNTER_RHS_TABLE} (ts TIMESTAMP(3) TIME INDEX, job STRING, instance STRING, v DOUBLE, PRIMARY KEY (job, instance)) PARTITION ON COLUMNS (job) () WITH ('compaction.type' = 'twcs', 'compaction.twcs.trigger_file_num' = '100')")).await;
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
        let rhs_second_value = COUNTER_FIRST_VALUE + 2.0 * increase;
        execute_sql(instance, &format!("INSERT INTO {COUNTER_RHS_TABLE} (ts, job, instance, v) VALUES ({counter_first}, '{job}', '{host}', {COUNTER_FIRST_VALUE:?}), ({counter_second}, '{job}', '{host}', {rhs_second_value:?})")).await;
        if index + 1 < COUNTER_SERIES.len() {
            execute_sql(instance, &format!("ADMIN FLUSH_TABLE('{COUNTER_TABLE}')")).await;
            execute_sql(
                instance,
                &format!("ADMIN FLUSH_TABLE('{COUNTER_RHS_TABLE}')"),
            )
            .await;
        }
    }

    // The bucket series sit at the counter samples' instants, so `rate` over them extrapolates
    // exactly as derived for [`COUNTER_SERIES`]; only the counts differ, and those are cumulative
    // per bound.
    execute_sql(instance, &hist_table_sql()).await;
    for (index, (job, bound, increase)) in HIST_SERIES.iter().enumerate() {
        let second_value = HIST_FIRST_VALUE + increase;
        execute_sql(
            instance,
            &format!(
                "INSERT INTO {HIST_TABLE} (ts, job, le, v) VALUES \
                 ({counter_first}, '{job}', '{bound}', {HIST_FIRST_VALUE:?}), \
                 ({counter_second}, '{job}', '{bound}', {second_value:?})"
            ),
        )
        .await;
        if index + 1 < HIST_SERIES.len() {
            execute_sql(instance, &format!("ADMIN FLUSH_TABLE('{HIST_TABLE}')")).await;
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

/// Asserts the native boundary's ordered names and types, independently of legacy names.
fn assert_native_columns(native: &QueryResult, tags: &[&str], query: &str) {
    let mut expected = vec!["ts", "v"];
    expected.extend_from_slice(tags);
    assert_eq!(
        expected, native.columns,
        "`{query}`: native boundary columns"
    );
    assert_eq!(
        DataType::Timestamp(TimeUnit::Millisecond, None),
        native.column_types[0],
        "`{query}`: native time type"
    );
    assert_eq!(
        DataType::Float64,
        native.column_types[1],
        "`{query}`: native value type"
    );
    for ty in &native.column_types[2..] {
        assert_eq!(DataType::Utf8, *ty, "`{query}`: native label type");
    }
}

/// The `native.root.output_values` counts of the fused producers in `capture`, per partition.
///
/// An entry stays [`None`] when the producer never registered the metric for a partition, which is a
/// different failure from a partition that produced nothing; both are reported as they are.
fn fused_node_metric_per_partition(
    fused: &Arc<dyn ExecutionPlan>,
    metric_name: &str,
    context: &str,
) -> Vec<Option<usize>> {
    fused_node_count_per_partition(fused, metric_name, context)
}

/// Reads a named native count from the fused node's registered per-partition metrics.
fn fused_node_count_per_partition(
    fused: &Arc<dyn ExecutionPlan>,
    metric_name: &str,
    context: &str,
) -> Vec<Option<usize>> {
    let mut per_partition = vec![None; SCAN_PARTITIONS];
    let plan = displayable(fused.as_ref()).indent(false).to_string();
    let metrics = fused
        .metrics()
        .unwrap_or_else(|| panic!("{context}: the fused producer reports no metrics:\n{plan}"));
    for metric in metrics.iter() {
        let MetricValue::Count { count, .. } = metric.value() else {
            continue;
        };
        if metric.value().name() != metric_name {
            continue;
        }
        let partition = metric.partition().unwrap_or_else(|| {
            panic!("{context}: `{metric_name}` is not labelled with its partition")
        });
        assert!(
            partition < SCAN_PARTITIONS,
            "{context}: partition {partition} is beyond the {SCAN_PARTITIONS} the scan was \
             asked for; plan:\n{plan}"
        );
        per_partition[partition] = Some(per_partition[partition].unwrap_or(0) + count.value());
    }
    per_partition
}

/// The named root metric of all fused producers in `capture`, per partition.
fn fused_root_metric_per_partition(
    capture: &NativeCapture,
    metric_name: &str,
    context: &str,
) -> Vec<Option<usize>> {
    let mut per_partition = vec![None; SCAN_PARTITIONS];
    for fused in &capture.fused {
        for (partition, count) in fused_node_metric_per_partition(fused, metric_name, context)
            .into_iter()
            .enumerate()
        {
            if let Some(count) = count {
                per_partition[partition] = Some(per_partition[partition].unwrap_or(0) + count);
            }
        }
    }
    per_partition
}

/// The Final tree's aggregate input-sample counts, summed across its runtime nodes per partition.
fn final_input_samples_per_partition(capture: &NativeCapture, context: &str) -> Vec<usize> {
    let mut per_partition = vec![0; SCAN_PARTITIONS];
    for finalize in &capture.finalize {
        let plan = displayable(finalize.as_ref()).indent(false).to_string();
        let metrics = finalize
            .metrics()
            .unwrap_or_else(|| panic!("{context}: Final reports no metrics:\n{plan}"));
        for metric in metrics.iter() {
            let MetricValue::Count { count, .. } = metric.value() else {
                continue;
            };
            let name = metric.value().name();
            if !name.starts_with("native.") || !name.ends_with(".input_samples") {
                continue;
            }
            let partition = metric.partition().unwrap_or_else(|| {
                panic!("{context}: Final `{name}` is not labelled with its partition")
            });
            assert!(
                partition < SCAN_PARTITIONS,
                "{context}: Final `{name}` partition {partition} exceeds {SCAN_PARTITIONS}:\n{plan}"
            );
            per_partition[partition] += count.value();
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

/// Asserts the frontend Final node routes through the unified series repartitioner into the region boundary.
fn assert_finalize_reads_region_state(
    finalize: &Arc<dyn ExecutionPlan>,
    producer: &Arc<dyn ExecutionPlan>,
    expect_single_owner: bool,
    context: &str,
) {
    let plan = displayable(finalize.as_ref()).indent(false).to_string();
    let children = finalize.children();
    assert_eq!(
        1,
        children.len(),
        "{context}: Final node must have one input:\n{plan}"
    );
    let input = through_cooperative(children[0]);
    if expect_single_owner {
        assert!(
            input.is::<MergeScanExec>(),
            "{context}: the specified single-owner Final must read MergeScanExec directly, got {}:\n{plan}",
            input.name()
        );
        assert_eq!(
            1,
            input.properties().output_partitioning().partition_count(),
            "{context}: single-owner Final input must have one partition:\n{plan}"
        );
        assert_eq!(
            1,
            finalize
                .properties()
                .output_partitioning()
                .partition_count(),
            "{context}: single-owner Final must have one partition:\n{plan}"
        );
        assert!(
            input.children().is_empty(),
            "{context}: MergeScanExec must be a leaf:\n{plan}"
        );
    } else {
        let repartition = input;
        assert!(
            repartition.is::<SeriesRepartitionExec>(),
            "{context}: Final must route its input through SeriesRepartitionExec, got {}:\n{plan}",
            repartition.name()
        );
        assert_eq!(
            SCAN_PARTITIONS,
            repartition
                .properties()
                .output_partitioning()
                .partition_count(),
            "{context}: grouped Final input router must have {SCAN_PARTITIONS} partitions:\n{plan}"
        );
        assert_eq!(
            SCAN_PARTITIONS,
            finalize
                .properties()
                .output_partitioning()
                .partition_count(),
            "{context}: grouped Final must have {SCAN_PARTITIONS} partitions:\n{plan}"
        );
        let routed = repartition.children();
        assert_eq!(
            1,
            routed.len(),
            "{context}: repartition must have one input:\n{plan}"
        );
        let boundary = through_cooperative(routed[0]);
        assert!(
            boundary.is::<MergeScanExec>(),
            "{context}: Final repartition must read the region MergeScanExec, got {}:\n{plan}",
            boundary.name()
        );
        assert!(
            boundary.children().is_empty(),
            "{context}: MergeScanExec must be a leaf:\n{plan}"
        );
        assert_eq!(
            producer.schema(),
            repartition.schema(),
            "{context}: frontend router must preserve the Partial state-frame schema:\n{plan}"
        );
    }

    let producer_schema = producer.schema();
    assert_eq!(
        producer_schema,
        input.schema(),
        "{context}: Final input must preserve the Partial state-frame schema:\n{plan}"
    );
    let producer_plan = NodeDisplay(producer.as_ref()).to_string();
    let root_line = producer_plan
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("root ") || line.starts_with("root:"))
        .unwrap_or_default();
    assert!(
        root_line.contains("Aggregate") && root_line.contains("partial"),
        "{context}: captured region producer must have a Partial root, got `{root_line}`:\n{producer_plan}"
    );
    let declaration =
        SeriesFrameDeclaration::parse(producer_schema.as_ref()).unwrap_or_else(|error| {
            panic!("{context}: invalid Partial frame declaration: {error}:\n{producer_plan}")
        });
    assert_eq!(
        FrameContent::AggregateState,
        declaration.content(),
        "{context}: Partial declaration must be aggregate-state content:\n{producer_plan}"
    );
    let state = declaration.aggregate_state().unwrap_or_else(|| {
        panic!("{context}: Partial declaration lacks aggregate kind/components:\n{producer_plan}")
    });
    let fields = producer_schema
        .field_with_name("fields")
        .unwrap_or_else(|error| {
            panic!("{context}: state frame lacks `fields` column: {error}:\n{producer_plan}")
        });
    let DataType::Struct(field_values) = fields.data_type() else {
        panic!(
            "{context}: state frame `fields` column must be Struct, got {:?}:\n{producer_plan}",
            fields.data_type()
        );
    };
    assert!(
        !field_values.is_empty(),
        "{context}: state frame must declare at least one field:\n{producer_plan}"
    );
    for field in field_values {
        let DataType::List(item) = field.data_type() else {
            panic!(
                "{context}: state field `{}` must be List, got {:?}:\n{producer_plan}",
                field.name(),
                field.data_type()
            );
        };
        let DataType::Struct(components) = item.data_type() else {
            panic!(
                "{context}: state field `{}` must contain Struct values, got {:?}:\n{producer_plan}",
                field.name(),
                item.data_type()
            );
        };
        assert_eq!(
            state
                .components
                .iter()
                .map(|component| component.as_str())
                .collect::<Vec<_>>(),
            components
                .iter()
                .map(|component| component.name().as_str())
                .collect::<Vec<_>>(),
            "{context}: state field `{}` components disagree with declared {:?} kind:\n{producer_plan}",
            field.name(),
            state.kind
        );
    }
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

/// Checks a split across two independently scanned sources.
async fn check_two_source_segregation(
    instance: &Arc<Instance>,
    observer: &NativePlanObserver,
    promql: &str,
    expected: &[ResultRow],
) {
    let query = tql_eval(promql);
    let (legacy, legacy_capture) = run_query(instance, promql, false, observer).await;
    assert!(
        legacy_capture.fused.is_empty() && legacy_capture.finalize.is_empty(),
        "`{query}`: legacy run must not plan native nodes"
    );
    assert_rows(&legacy.rows, expected, &format!("legacy `{query}`"));

    let (native, capture) = run_query(instance, promql, true, observer).await;
    assert_eq!(
        vec![
            "ts".to_string(),
            "native_agg_counter.sum(prom_rate(ts_range,v,ts,Int64(300000))) / \
             native_agg_gauge.sum(native_agg_gauge.v)"
                .to_string(),
        ],
        legacy.columns,
        "`{query}`: the legacy plan names the value column after its expression"
    );
    assert_native_columns(&native, &[], &query);
    assert_rows(
        &native.rows,
        &legacy.rows,
        &format!("native `{query}` vs legacy"),
    );
    assert_rows(&native.rows, expected, &format!("native `{query}`"));
    let parent_plan = capture
        .fused
        .iter()
        .find(|node| node.children().len() == 2)
        .unwrap_or_else(|| {
            panic!(
                "`{query}`: no two-child fused parent; plans: {:#?}",
                capture.fused
            )
        });
    let parent_display = displayable(parent_plan.as_ref()).indent(false).to_string();
    assert_eq!(
        2,
        parent_plan.children().len(),
        "`{query}`: frontend fused parent must have two source segments:\n{parent_display}"
    );
    assert_eq!(
        3,
        capture.fused.len(),
        "`{query}`: expected 3 fused nodes: one frontend parent plus one segment per source, \
         so a regression merging them back into one node is visible; plans: {:#?}",
        capture
            .fused
            .iter()
            .map(|node| displayable(node.as_ref()).indent(false).to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        capture.finalize.is_empty(),
        "`{query}`: two-source native execution must not create a frontend merge; parent:\n{parent_display}"
    );
    let segments = capture
        .fused
        .iter()
        .filter(|node| !Arc::ptr_eq(node, parent_plan))
        .collect::<Vec<_>>();
    assert_eq!(
        2,
        segments.len(),
        "`{query}`: expected two source segments; parent:\n{parent_display}"
    );
    let mut vectors = Vec::with_capacity(segments.len());
    for (index, segment) in segments.iter().enumerate() {
        let plan = displayable(segment.as_ref()).indent(false).to_string();
        assert_eq!(
            1,
            segment.children().len(),
            "`{query}`: segment {index} must have one child; plan:\n{plan}"
        );
        assert_fused_reads_scan_directly(segment, &format!("native `{query}` segment {index}"));
        vectors.push(fused_node_metric_per_partition(
            segment,
            ROOT_OUTPUT_VALUES_METRIC,
            &format!("native `{query}` segment {index}"),
        ));
    }
    common_telemetry::info!(
        "`{query}` segmented native per-partition `{ROOT_OUTPUT_VALUES_METRIC}` counts: \
         {vectors:?}"
    );
    for (index, values) in vectors.iter().enumerate() {
        let plan = displayable(segments[index].as_ref())
            .indent(false)
            .to_string();
        if values.iter().all(Option::is_none) {
            panic!(
                "`{query}`: segment {index} does not publish `{ROOT_OUTPUT_VALUES_METRIC}`; \
                 both segment vectors: {vectors:?}; parent plan:\n{parent_display}; segment plan:\n{plan}"
            );
        }
        assert!(
            values
                .iter()
                .all(|count| count.is_some_and(|count| count > 0)),
            "`{query}`: each source segment must produce values on all {SCAN_PARTITIONS} partitions; \
             both segment vectors: {vectors:?}; parent plan:\n{parent_display}; segment plan:\n{plan}"
        );
    }
}

/// Checks one case on both routes: the legacy run first, then the native one.
async fn check_nested_aggregate_case(
    instance: &Arc<Instance>,
    observer: &NativePlanObserver,
    case: &AggregateCase,
) {
    let query = tql_eval(&case.promql);
    let (legacy, legacy_capture) = run_query(instance, &case.promql, false, observer).await;
    assert!(legacy_capture.fused.is_empty() && legacy_capture.finalize.is_empty());
    assert_rows(&legacy.rows, &case.expected, &format!("legacy `{query}`"));

    let (native, capture) = run_query(instance, &case.promql, true, observer).await;
    assert_native_columns(&native, &[], &query);
    assert_rows(
        &native.rows,
        &legacy.rows,
        &format!("native `{query}` vs legacy"),
    );
    assert_rows(&native.rows, &case.expected, &format!("native `{query}`"));
    assert!(
        !capture.fused.is_empty(),
        "`{query}` must execute native fused nodes"
    );
    assert!(
        capture.finalize.is_empty(),
        "`{query}` must use the native fused path without a Final node"
    );
}

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
    let tags = case.expected[0]
        .labels
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_native_columns(&native, &tags, &query);
    if promql.starts_with("count ") {
        assert!(
            legacy.column_types.contains(&DataType::Int64),
            "`{query}` legacy count type"
        );
    }
    assert_rows(
        &native.rows,
        &legacy.rows,
        &format!("native `{query}` vs legacy"),
    );
    assert_rows(&native.rows, &case.expected, &format!("native `{query}`"));

    assert_eq!(
        1,
        capture.fused.len(),
        "`{query}`: expected one inner Partial node"
    );
    assert_eq!(
        1,
        capture.finalize.len(),
        "`{query}`: expected one outer Final node"
    );
    for fused in &capture.fused {
        let producer_plan = NodeDisplay(fused.as_ref()).to_string();
        assert_fused_reads_scan_directly(fused, &format!("native `{query}`"));
        let declaration =
            SeriesFrameDeclaration::parse(fused.schema().as_ref()).unwrap_or_else(|error| {
                panic!("`{query}`: invalid Partial frame declaration: {error}:\n{producer_plan}")
            });
        assert_eq!(
            FrameContent::AggregateState,
            declaration.content(),
            "`{query}`: captured Partial must declare aggregate-state content:\n{producer_plan}"
        );
        let state = declaration.aggregate_state().unwrap_or_else(|| {
            panic!("`{query}`: Partial declaration must include aggregate kind/components:\n{producer_plan}")
        });
        let fused_schema = fused.schema();
        let fields = fused_schema.field_with_name("fields").unwrap_or_else(|error| {
            panic!("`{query}`: Partial state frame has no `fields` column: {error}:\n{producer_plan}")
        });
        let DataType::Struct(field_values) = fields.data_type() else {
            panic!(
                "`{query}`: Partial `fields` column must be Struct, got {:?}:\n{producer_plan}",
                fields.data_type()
            );
        };
        assert!(
            !field_values.is_empty(),
            "`{query}`: Partial must declare state fields:\n{producer_plan}"
        );
        for (index, field) in field_values.iter().enumerate() {
            let DataType::List(item) = field.data_type() else {
                panic!(
                    "`{query}`: state field {} `{}` must be List, got {:?}:\n{producer_plan}",
                    index,
                    field.name(),
                    field.data_type()
                );
            };
            let DataType::Struct(components) = item.data_type() else {
                panic!(
                    "`{query}`: state field {} `{}` must contain a Struct, got {:?}:\n{producer_plan}",
                    index,
                    field.name(),
                    item.data_type()
                );
            };
            assert_eq!(
                state
                    .components
                    .iter()
                    .map(|component| component.as_str())
                    .collect::<Vec<_>>(),
                components
                    .iter()
                    .map(|component| component.name().as_str())
                    .collect::<Vec<_>>(),
                "`{query}`: state field {} `{}` components must match the declared {:?} state kind:\n{producer_plan}",
                index,
                field.name(),
                state.kind
            );
        }
        assert!(
            !state.kind.as_str().is_empty(),
            "`{query}`: state declaration must name an aggregate kind"
        );
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
    // Validate the outer-to-inner state chain and the Partial node's region boundary.
    for finalize in &capture.finalize {
        assert_finalize_reads_region_state(
            finalize,
            &capture.fused[0],
            case.single_owner,
            &format!("native `{query}`"),
        );
    }

    if !case.single_owner {
        // These counters belong to the Final runtime tree, not the Partial producer. The Final
        // aggregate consumes routed AggregateState chunks at `native.root.input_samples`.
        let final_samples =
            final_input_samples_per_partition(&capture, &format!("native `{query}`"));
        common_telemetry::info!(
            "`{query}` native Final per-partition `.input_samples` counts: {final_samples:?}"
        );
        assert!(
            final_samples.iter().sum::<usize>() > 0,
            "`{query}`: Final must consume aggregate state, got {final_samples:?}"
        );
        assert!(
            final_samples.iter().filter(|&&samples| samples > 0).count() >= 2,
            "`{query}`: aggregate state must reach at least two of the {SCAN_PARTITIONS} Final partitions, got {final_samples:?}"
        );
    }

    let per_partition = fused_root_metric_per_partition(
        &capture,
        ROOT_OUTPUT_VALUES_METRIC,
        &format!("native `{query}`"),
    );
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
        let input_samples = fused_root_metric_per_partition(
            &capture,
            ROOT_INPUT_SAMPLES_METRIC,
            &format!("native `{query}`"),
        );
        common_telemetry::info!(
            "`{query}` native per-partition `{ROOT_INPUT_SAMPLES_METRIC}` counts: {input_samples:?}"
        );
        assert!(
            input_samples
                .iter()
                .all(|samples| samples.is_some_and(|n| n > 0)),
            "`{query}`: every partition must report positive root input samples, got {input_samples:?}"
        );
        assert_eq!(
            root_values,
            input_samples.iter().flatten().sum::<usize>(),
            "`{query}`: root input sample total must match the selected fixture series"
        );
        if root_values == COUNTER_SERIES.len() {
            assert_eq!(2, input_samples.iter().flatten().copied().max().unwrap());
        }
        // Root output counts are grid points grouped into output series, not input-series
        // contributions. Keep the per-partition activity check above; contribution totals belong
        // to the exact native root input_samples metric checked here.
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
            partial_state: Some("avg by [job] partial"),
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
        // A literal scalar above the fold: `sum` over the gauge's 2, 6, 10, 4 and 8 is 30, and the
        // merge's suffix doubles the merged value. The global aggregate has one owner.
        AggregateCase {
            single_owner: true,
            ..case(format!("sum({GAUGE_TABLE}) * 2"), &[(&[], 60.0)], true)
        },
        // A gauge over a constant value has a rate of exactly zero, so these two only cover the
        // zero-rate case: every series has both samples inside the 5m window, hence every partition
        // must fold a value. They cannot tell a computed rate from a dropped one, which is what the
        // counter table's cases below are for. The global aggregate has one owner.
        AggregateCase {
            single_owner: true,
            ..case(format!("sum(rate({GAUGE_TABLE}[5m]))"), &[(&[], 0.0)], true)
        },
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
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("sum(rate({COUNTER_TABLE}[5m]))"),
                &[(&[], 15.0)],
                "sum by [] partial",
            )
        },
        // The same sum with a literal scalar above the fold: the merge's suffix doubles the merged
        // 15 to 30, where a suffix run over one partition's share - at most 10 of the 15 - would
        // answer at most 20, and a suffix that divided instead would answer 7.5. The cut is still
        // the ungrouped `sum`, which the producer's plan line names. The global aggregate has one owner.
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("sum(rate({COUNTER_TABLE}[5m])) * 2"),
                &[(&[], 30.0)],
                "sum by [] partial",
            )
        },
        counter_case(
            format!("sum by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job(9.0, 6.0),
            "sum by [job] partial",
        ),
        // The matcher selects only `job = a`'s three series. Their rates are 1, 3 and 5, so the
        // native partial producer must still run and the merged result must be 9. Three selected
        // series cannot populate all four scan partitions, so this case checks that some producer
        // output exists without applying the unfiltered fixture's four-nonempty-partition assertion.
        AggregateCase {
            single_owner: true,
            partial_state: Some("sum by [] partial"),
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
            partial_state: Some("sum by [job] partial"),
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
            "avg by [job] partial",
        ),
        counter_case(
            format!("count by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job(3.0, 2.0),
            "count by [job] partial",
        ),
        // `a`'s rates are 1, 3 and 5, whose population standard deviation is sqrt(8/3); `b`'s are 2
        // and 4, whose is 1. Both are unchanged when the group's states are merged, which is the
        // point: the merge folds sum-of-squared-deviation states, not completed answers.
        counter_case(
            format!("stddev by (job) (rate({COUNTER_TABLE}[5m]))"),
            &by_job((8.0f64 / 3.0).sqrt(), 1.0),
            "stddev by [job] partial",
        ),
        // Two nested aggregates and a map above them: the merge's suffix is `sum(abs(x))` over the
        // inner aggregate's merged values, so it really runs on merged values rather than on one
        // partition's answer. Each group's mean is 3, so the suffix sums to 6 - and the cut is the
        // inner `avg`, which is what the producer's own plan line has to name.
        // A literal scalar below the fold is carried in the per-series prefix, so the state folds
        // `8 * rate`: the rates 1, 3, 5, 2 and 4 become 8, 24, 40, 16 and 32 and the merge answers
        // 120, where a state built without the multiplication would answer 15. The multiplication
        // distributes over the sum, so 120 alone cannot say which side of the merge computed it;
        // what this case pins down is that the constant is admitted below the cut and that the four
        // nonempty partitions still fold one value per series into that one state.
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("sum(8 * rate({COUNTER_TABLE}[5m]))"),
                &[(&[], 120.0)],
                "sum by [] partial",
            )
        },
        // The same below-the-fold placement with an operator the fold does not commute with: the
        // maximum of the prefix-computed `10 - rate`. The rates 1, 3, 5, 2 and 4 become 9, 7, 5, 8
        // and 6, whose maximum is 9 - so the answer tells the prefix from the suffix, where
        // `10 - max(rate)` would answer 5, and from a prefix that swapped the operands, where
        // `max(rate - 10)` would answer -5.
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("max(10 - rate({COUNTER_TABLE}[5m]))"),
                &[(&[], 9.0)],
                "max by [] partial",
            )
        },
        // A literal scalar above the fold is rebuilt in the merge's suffix over the merged value,
        // with the operands in the order the query wrote them: the mean rate is 3, so `100 - 3` is
        // 97 where a swapped pair would report -97, and `100 / 15` is the sum's own answer where
        // 15/100 would be the reciprocal. A negation is the same broadcast with `-1` on the left.
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("100 - avg(rate({COUNTER_TABLE}[5m]))"),
                &[(&[], 97.0)],
                "avg by [] partial",
            )
        },
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("100 / sum(rate({COUNTER_TABLE}[5m]))"),
                &[(&[], 100.0 / 15.0)],
                "sum by [] partial",
            )
        },
        AggregateCase {
            single_owner: true,
            ..counter_case(
                format!("-sum(rate({COUNTER_TABLE}[5m]))"),
                &[(&[], -15.0)],
                "sum by [] partial",
            )
        },
        // The range table holds one series in one partition, so this is a correctness smoke test of
        // one populated partition rather than a parallel acceptance. Its one series folds one
        // `count_over_time` value at the single grid point.
        AggregateCase {
            single_owner: true,
            ..case(
                format!("sum(count_over_time({RANGE_TABLE}[10m]))"),
                &[(&[], 2.0)],
                false,
            )
        },
    ];
    for case in &cases {
        check_case(&instance, &observer, case).await;
    }

    // Nested aggregates require actual fused execution without a frontend Final merge.
    let nested_cases = [
        AggregateCase {
            single_owner: true,
            ..case(
                format!("sum(avg({GAUGE_TABLE}) by (job))"),
                &[(&[], 12.0)],
                true,
            )
        },
        counter_case(
            format!("sum(abs(avg(rate({COUNTER_TABLE}[5m])) by (job)))"),
            &[(&[], 6.0)],
            "avg by [job] partial",
        ),
        counter_case(
            format!("abs(sum(avg(rate({COUNTER_TABLE}[5m])) by (job)))"),
            &[(&[], 6.0)],
            "avg by [job] partial",
        ),
    ];
    for case in &nested_cases {
        check_nested_aggregate_case(&instance, &observer, case).await;
    }

    let time = case("time()".to_string(), &[(&[], EVAL_SECS as f64)], false);
    let (native, capture) = run_query(&instance, &time.promql, true, &observer).await;
    assert_rows(&native.rows, &time.expected, "native `time()`");
    assert_eq!(1, capture.fused.len(), "`time()` runs one fused node");
    assert!(capture.finalize.is_empty(), "`time()` needs no merge");

    let two_source_promql = format!("sum(rate({COUNTER_TABLE}[5m])) / sum({GAUGE_TABLE})");
    check_two_source_segregation(
        &instance,
        &observer,
        &two_source_promql,
        &case(two_source_promql.clone(), &[(&[], 15.0 / 30.0)], false).expected,
    )
    .await;

    // The shapes the native compiler declines, each declined for its own reason, and each checked on
    // the plans that really ran: none may hold a fused producer or a frontend merge, and both routes
    // must answer what the fixture derives by hand.
    //
    // `scalar` reports the one value present at a point only when exactly one series is present at
    // it, so the five counter series make it NaN; a route that folded one partition at a time would
    // report the number of whichever partition held a single series, so the NaN also says the whole
    // vector was read at once. A subquery evaluates its own body on a grid of its own: it reads
    // across series and cannot be answered by a producer per partition, so it keeps the existing
    // plan. The subquery's window
    // spans the two inner points `EVAL_SECS - 1` and `EVAL_SECS`, at each of which the same
    // extrapolated rate answers 15 - both samples stay further from the window's ends than the 66s
    // threshold - so the maximum is 15 as well.
    let fallback_cases = [
        case(
            format!("scalar({COUNTER_TABLE})"),
            &[(&[], f64::NAN)],
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

/// Histogram functions above the cut merge Partial state before evaluating the complete buckets.
/// Routing excludes `le` so each histogram's bucket groups meet on the same Final owner.
#[tokio::test(flavor = "multi_thread")]
async fn native_histogram_over_the_cut_splits_and_matches_legacy() {
    common_telemetry::init_default_ut_logging();

    let observer = Arc::new(NativePlanObserver::default());
    let plugins = Plugins::new();
    plugins.insert(Arc::clone(&observer) as PhysicalPlanWrapperRef);
    let standalone = GreptimeDbStandaloneBuilder::new("native_histogram_over_cut")
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .with_plugin(plugins)
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    setup_fixture(&instance).await;

    // A histogram function above the cut is admitted because the merge has already folded every
    // partition's `(..., le)` group into one complete series, so the buckets it reads are the ones
    // the statement's input would have produced in one execution. That claim is checked on the rows
    // two real executions report, in addition to the split plans: the bucket series of [`HIST_SERIES`]
    // are spread over several files and a memtable range, and both routes compute real rates over them.
    // The quantiles cover the two ends of the classic interpolation - 0.99's rank is above the last
    // finite bucket's, where the answer is that bucket's upper bound rather than an interpolation,
    // and 0.5 interpolates inside the first bucket - and the fraction reads the same buckets a third
    // way.
    let queries = [
        (
            format!("histogram_quantile(0.99, sum by (le) (rate({HIST_TABLE}[5m])))"),
            true,
        ),
        (
            format!("histogram_quantile(0.5, sum by (le) (rate({HIST_TABLE}[5m])))"),
            true,
        ),
        (
            format!("histogram_fraction(0.0, 1.0, sum by (le) (rate({HIST_TABLE}[5m])))"),
            true,
        ),
        (
            format!("histogram_quantile(0.99, sum by (le, job) (rate({HIST_TABLE}[5m])))"),
            false,
        ),
        (
            format!("histogram_fraction(0.0, 1.0, sum by (le, job) (rate({HIST_TABLE}[5m])))"),
            false,
        ),
    ];
    for (promql, single_owner) in &queries {
        let query = tql_eval(promql);
        let (legacy, legacy_capture) = run_query(&instance, promql, false, &observer).await;
        assert!(
            legacy_capture.fused.is_empty() && legacy_capture.finalize.is_empty(),
            "`{query}`: the legacy run must not plan a native node, but it planned {} fused \
             producers and {} frontend merges",
            legacy_capture.fused.len(),
            legacy_capture.finalize.len()
        );
        // Without this the comparison below would also pass over two empty answers, which says
        // nothing about either route.
        assert!(
            !legacy.rows.is_empty(),
            "`{query}`: the fixture must produce a value on the complete input, otherwise the two \
             routes would agree on nothing"
        );
        if !*single_owner {
            assert_eq!(
                vec![Some("a"), Some("b")],
                legacy
                    .rows
                    .iter()
                    .map(|row| row.labels.get("job").map(String::as_str))
                    .collect::<Vec<_>>(),
                "`{query}`: the grouped fixture must retain both histogram owners"
            );
        }

        let mark = observer.mark();
        let (native, capture) = run_query(&instance, promql, true, &observer).await;
        assert!(
            !capture.finalize.is_empty(),
            "`{query}`: a histogram suffix must run above a Final aggregate"
        );
        let partial = capture
            .fused
            .iter()
            .find(|fused| NodeDisplay(fused.as_ref()).to_string().contains(" partial"))
            .unwrap_or_else(|| panic!("`{query}`: no Partial state producer found"));
        assert_finalize_reads_region_state(
            &capture.finalize[0],
            partial,
            *single_owner,
            &format!("native `{query}`"),
        );
        let tags: &[&str] = if *single_owner { &[] } else { &["job"] };
        assert_native_columns(&native, tags, &query);
        // The equivalence the admission rests on: the rows the same statement reports over the
        // complete input, labels included.
        assert_rows(
            &native.rows,
            &legacy.rows,
            &format!("native `{query}` vs legacy"),
        );

        let plans = observer
            .since(mark)
            .iter()
            .map(|plan| displayable(plan.as_ref()).indent(true).to_string())
            .collect::<Vec<_>>();
        common_telemetry::info!(
            "`{query}`\n native rows: {:#?}\n legacy rows: {:#?}\n plans:\n{}",
            native.rows,
            legacy.rows,
            plans.join("\n")
        );
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn native_bare_selector_aggregate_value_columns() {
    common_telemetry::init_default_ut_logging();
    let observer = Arc::new(NativePlanObserver::default());
    let plugins = Plugins::new();
    plugins.insert(Arc::clone(&observer) as PhysicalPlanWrapperRef);
    let standalone = GreptimeDbStandaloneBuilder::new("native_bare_selector_aggregate")
        .with_query_options(QueryOptions {
            experimental_native_promql: true,
            ..Default::default()
        })
        .with_plugin(plugins)
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    setup_fixture(&instance).await;

    const ONE: &str = "native_bare_one_value";
    const TWO: &str = "native_bare_two_values";
    // Report the whole error chain: the top-level Display of a DDL failure names nothing useful.
    async fn statement(instance: &Arc<Instance>, sql: &str) {
        for result in instance.do_query(sql, QueryContext::arc()).await {
            if let Err(err) = result {
                let mut chain = String::new();
                let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&err);
                while let Some(error) = source {
                    chain.push_str(&format!("\n  caused by: {error}"));
                    source = error.source();
                }
                panic!("`{sql}` failed: {err}{chain}");
            }
        }
    }
    statement(
        &instance,
        &format!("CREATE TABLE {ONE} (ts TIMESTAMP(3) TIME INDEX, job STRING, v DOUBLE, PRIMARY KEY (job))"),
    )
    .await;
    statement(
        &instance,
        &format!("CREATE TABLE {TWO} (ts TIMESTAMP(3) TIME INDEX, job STRING, a DOUBLE, b DOUBLE, PRIMARY KEY (job))"),
    )
    .await;
    statement(
        &instance,
        &format!(
            "INSERT INTO {ONE} VALUES ({EVAL_MILLIS}, 'api', 3), ({EVAL_MILLIS}, 'worker', 7)"
        ),
    )
    .await;
    statement(
        &instance,
        &format!("INSERT INTO {TWO} VALUES ({EVAL_MILLIS}, 'api', 2, 20), ({EVAL_MILLIS}, 'worker', 5, 30)"),
    )
    .await;

    // Direct batch reader: unlike QueryResult, each row intentionally carries all numeric value
    // columns, as a bare selector over a multi-field table may return more than one.
    async fn read(
        instance: &Arc<Instance>,
        observer: &NativePlanObserver,
        promql: &str,
        native: bool,
    ) -> (
        Vec<String>,
        Vec<DataType>,
        Vec<(Vec<(String, String)>, Vec<f64>)>,
        NativeCapture,
    ) {
        let sql = tql_eval(promql);
        let mark = observer.mark();
        let mut ctx = QueryContext::with_db_name(None);
        ctx.set_extension(QUERY_PARALLELISM_HINT, SCAN_PARTITIONS.to_string());
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, native.to_string());
        let output = instance
            .do_query(&sql, Arc::new(ctx))
            .await
            .remove(0)
            .unwrap_or_else(|err| panic!("`{sql}` failed: {err}"));
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            other => panic!("`{sql}` produced no batches: {other:?}"),
        };
        let schema = batches.schema();
        let fields = schema.arrow_schema().fields();
        let names = fields.iter().map(|f| f.name().clone()).collect::<Vec<_>>();
        let types = fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let labels = fields
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                matches!(
                    f.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                )
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let numbers = fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.data_type().is_numeric())
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        assert!(
            !numbers.is_empty(),
            "`{sql}` has no numeric value fields: {names:?}"
        );
        for &index in &numbers {
            assert_eq!(
                DataType::Float64,
                types[index],
                "`{sql}` value column {} must be Float64",
                names[index]
            );
        }
        let mut rows = Vec::new();
        for batch in batches.iter() {
            let label_arrays = labels
                .iter()
                .map(|&i| {
                    let a = cast(batch.column(i), &DataType::Utf8).unwrap();
                    (names[i].clone(), a)
                })
                .collect::<Vec<_>>();
            let value_arrays = numbers
                .iter()
                .map(|&i| {
                    (
                        names[i].clone(),
                        cast(batch.column(i), &DataType::Float64).unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            for row in 0..batch.num_rows() {
                let mut row_labels = label_arrays
                    .iter()
                    .filter_map(|(name, a)| {
                        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
                        (!a.is_null(row)).then(|| (name.clone(), a.value(row).to_string()))
                    })
                    .collect::<Vec<_>>();
                // Labels are an identity set here, not an ordered vector.
                row_labels.sort();
                let row_values = value_arrays
                    .iter()
                    .map(|(_, a)| {
                        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
                        assert!(!a.is_null(row), "`{sql}` null in value column");
                        a.value(row)
                    })
                    .collect::<Vec<_>>();
                rows.push((row_labels, row_values));
            }
        }
        rows.sort_by(|l, r| l.0.cmp(&r.0));
        (
            names,
            types,
            rows,
            NativeCapture::from_plans(&observer.since(mark)),
        )
    }

    let cases = [
        (format!("sum({ONE})"), vec![(vec![], vec![10.0])], true, 1),
        (
            format!("sum by (job) ({ONE})"),
            vec![
                (vec![("job".into(), "api".into())], vec![3.0]),
                (vec![("job".into(), "worker".into())], vec![7.0]),
            ],
            false,
            1,
        ),
        (
            format!("sum({TWO})"),
            vec![(vec![], vec![7.0, 50.0])],
            true,
            2,
        ),
        (
            format!("sum by (job) ({TWO})"),
            vec![
                (vec![("job".into(), "api".into())], vec![2.0, 20.0]),
                (vec![("job".into(), "worker".into())], vec![5.0, 30.0]),
            ],
            false,
            2,
        ),
    ];
    for (promql, expected, single_owner, expected_values) in cases {
        let query = tql_eval(&promql);
        let (_legacy_names, legacy_types, legacy_rows, legacy_capture) =
            read(&instance, &observer, &promql, false).await;
        assert!(
            legacy_capture.fused.is_empty() && legacy_capture.finalize.is_empty(),
            "`{query}` legacy plan unexpectedly contains native nodes"
        );
        assert_eq!(
            expected_values,
            legacy_types.iter().filter(|t| t.is_numeric()).count(),
            "`{query}` legacy value count"
        );
        let mark = observer.mark();
        let (native_names, native_types, native_rows, capture) =
            read(&instance, &observer, &promql, true).await;
        let expected_names = match (expected_values, single_owner) {
            (1, true) => vec!["ts", "v"],
            (1, false) => vec!["ts", "v", "job"],
            (2, true) => vec!["ts", "a", "b"],
            (2, false) => vec!["ts", "a", "b", "job"],
            _ => unreachable!(),
        };
        assert_eq!(
            expected_names, native_names,
            "`{query}` native boundary columns"
        );
        assert_eq!(
            DataType::Timestamp(TimeUnit::Millisecond, None),
            native_types[0],
            "`{query}` native time type"
        );
        assert!(
            native_types[1..=expected_values]
                .iter()
                .all(|ty| *ty == DataType::Float64),
            "`{query}` native value types"
        );
        if !single_owner {
            assert_eq!(
                DataType::Utf8,
                native_types[expected_values + 1],
                "`{query}` native label type"
            );
        }
        assert_eq!(
            expected_values,
            native_types.iter().filter(|t| t.is_numeric()).count(),
            "`{query}` native value count"
        );
        assert_eq!(
            expected, legacy_rows,
            "`{query}` legacy independent fixture result"
        );
        assert_eq!(
            expected, native_rows,
            "`{query}` native independent fixture result"
        );
        assert_eq!(legacy_rows, native_rows, "`{query}` native == legacy");
        assert!(
            observer.mark() > mark,
            "`{query}` native execution must be observed"
        );

        // Every case here is a shape the native route answers: one- and two-value-field bare
        // selectors, global and grouped. A silent fall back to legacy would therefore be a
        // regression, so the plan assertions below are unconditional.
        assert_eq!(
            1,
            capture.fused.len(),
            "`{query}` must have one Partial producer"
        );
        assert_eq!(
            1,
            capture.finalize.len(),
            "`{query}` must have one Final merge"
        );
        assert_fused_reads_scan_directly(&capture.fused[0], &format!("native `{query}`"));
        assert_finalize_reads_region_state(
            &capture.finalize[0],
            &capture.fused[0],
            single_owner,
            &format!("native `{query}`"),
        );
        let schema = capture.fused[0].schema();
        let declaration = SeriesFrameDeclaration::parse(schema.as_ref())
            .expect("Partial state frame declaration");
        assert_eq!(
            FrameContent::AggregateState,
            declaration.content(),
            "`{query}` Partial frame content"
        );
        let fields = schema
            .field_with_name("fields")
            .expect("aggregate state fields");
        assert!(
            matches!(fields.data_type(), DataType::Struct(_)),
            "`{query}` state components must be Struct"
        );
        for field in match fields.data_type() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        } {
            assert!(
                matches!(field.data_type(), DataType::List(item) if matches!(item.data_type(), DataType::Struct(_))),
                "`{query}` state component {} must be List<Struct>",
                field.name()
            );
        }
        let plans = observer.since(mark);
        assert!(
            !plans.is_empty(),
            "`{query}` observer should capture frontend and region plans"
        );
    }
}
