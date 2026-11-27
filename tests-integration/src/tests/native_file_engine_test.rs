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

//! Native PromQL over a file-engine table.
//!
//! The table reads two CSV files whose rows are in no particular order, and one sample of series
//! `{job="api", instance="a"}` is present in both files with different values. Expected answers are
//! worked out by hand from Prometheus semantics, which keep one sample per timestamp per series.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use common_base::Plugins;
use common_datasource::object_store::LocalFileAccess;
use common_query::OutputData;
use common_recordbatch::util::collect_batches;
use common_test_util::find_workspace_path;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::DataType;
use frontend::instance::Instance;
use query::options::QueryOptions;
use query::physical_wrapper::{PhysicalPlanWrapper, PhysicalPlanWrapperRef};
use query::promql::planner::NATIVE_PROMQL_EXTENSION_KEY;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::cluster::GreptimeDbClusterBuilder;
use crate::standalone::GreptimeDbStandaloneBuilder;
use crate::tests::test_util::{create_local_file_test_dir, prepare_path};

const TABLE: &str = "fe_metric";
const BASE_SECS: i64 = 1_700_000_000;
const RELATIVE_TOLERANCE: f64 = 1e-9;

/// `(offset_secs, job, instance, value)` rows of the two files, in file order.
///
/// The three series are `{api,a}`: 0, 60, 120, 180, 240 (a counter rising 1/s), `{api,b}`: 10, 40,
/// 70, 100, 130 and `{db,c}`: 100, 160, 220, 280, 340, each sampled every 60s from `BASE_SECS`.
/// `{api,a}` at offset 120 appears in both files, as 125 and as 120.
const FILE_A: &[(i64, &str, &str, f64)] = &[
    (240, "api", "a", 240.0),
    (0, "db", "c", 100.0),
    (120, "api", "b", 70.0),
    (0, "api", "a", 0.0),
    (120, "api", "a", 125.0),
    (180, "db", "c", 280.0),
    (0, "api", "b", 10.0),
];
const FILE_B: &[(i64, &str, &str, f64)] = &[
    (240, "api", "b", 130.0),
    (60, "api", "a", 60.0),
    (120, "db", "c", 220.0),
    (120, "api", "a", 120.0),
    (60, "api", "b", 40.0),
    (240, "db", "c", 340.0),
    (180, "api", "a", 180.0),
    (60, "db", "c", 160.0),
    (180, "api", "b", 100.0),
];

/// Keeps the frontend plans a query executes.
#[derive(Default)]
struct PlanObserver {
    plans: Mutex<Vec<Arc<dyn ExecutionPlan>>>,
}

impl PlanObserver {
    fn take_node_names(&self) -> Vec<String> {
        let mut stack = std::mem::take(&mut *self.plans.lock().unwrap());
        let mut names = Vec::new();
        while let Some(node) = stack.pop() {
            names.push(node.name().to_string());
            stack.extend(node.children().into_iter().cloned());
        }
        names
    }
}

impl PhysicalPlanWrapper for PlanObserver {
    fn wrap(
        &self,
        origin: Arc<dyn ExecutionPlan>,
        _ctx: QueryContextRef,
    ) -> Arc<dyn ExecutionPlan> {
        self.plans.lock().unwrap().push(Arc::clone(&origin));
        origin
    }
}

/// One answer row: evaluation time in milliseconds, `name=value` labels without `__name__`, value.
type Row = (i64, String, f64);

struct Case {
    promql: &'static str,
    start: i64,
    end: i64,
    expected: Vec<Row>,
    /// Whether the answer reads the duplicated sample, which the existing planner does not reduce
    /// by a fixed rule: it answers whichever row its sort happens to leave last.
    reads_duplicate: bool,
}

fn row(offset_secs: i64, labels: &str, value: f64) -> Row {
    ((BASE_SECS + offset_secs) * 1000, labels.to_string(), value)
}

fn instant(promql: &'static str, at: i64, expected: Vec<Row>) -> Case {
    Case {
        promql,
        start: at,
        end: at,
        expected,
        reads_duplicate: false,
    }
}

fn cases() -> Vec<Case> {
    let a = "instance=a,job=api";
    let b = "instance=b,job=api";
    let c = "instance=c,job=db";
    // `rate` over [5m] at offset 240 reads all five samples, 240s apart end to end. The window
    // starts 60s before the first sample, under the 66s extrapolation threshold, so extrapolation
    // reaches the window start unless the counter would cross zero sooner:
    // - {api,a}: first value 0, so the zero point is the first sample: 240 / 300 = 0.8.
    // - {api,b}: zero point 240 * 10 / 120 = 20s back: 120 * 260 / 240 / 300.
    // - {db,c}: zero point 100s back, beyond 60s: 240 * 300 / 240 / 300 = 1.
    let rate_a = 0.8;
    let rate_b = 120.0 * 260.0 / 240.0 / 300.0;
    let rate_c = 1.0;
    let mut range_selector = Vec::new();
    let mut range_sum = Vec::new();
    for step in 0..5 {
        let offset = step * 60;
        let (va, vb, vc) = (
            offset as f64,
            10.0 + step as f64 * 30.0,
            100.0 + offset as f64,
        );
        range_selector.extend([row(offset, a, va), row(offset, b, vb), row(offset, c, vc)]);
        range_sum.extend([row(offset, "job=api", va + vb), row(offset, "job=db", vc)]);
    }
    vec![
        instant(
            TABLE,
            240,
            vec![row(240, a, 240.0), row(240, b, 130.0), row(240, c, 340.0)],
        ),
        // The duplicated sample: the smaller of the two values is the one kept.
        Case {
            reads_duplicate: true,
            ..instant("fe_metric{instance=\"a\"}", 120, vec![row(120, a, 120.0)])
        },
        instant(
            "rate(fe_metric[5m])",
            240,
            vec![
                row(240, a, rate_a),
                row(240, b, rate_b),
                row(240, c, rate_c),
            ],
        ),
        instant(
            "sum by (job) (rate(fe_metric[5m]))",
            240,
            vec![
                row(240, "job=api", rate_a + rate_b),
                row(240, "job=db", rate_c),
            ],
        ),
        instant(
            "rate(fe_metric[5m]) * 60",
            240,
            vec![
                row(240, a, rate_a * 60.0),
                row(240, b, rate_b * 60.0),
                row(240, c, rate_c * 60.0),
            ],
        ),
        instant(
            "fe_metric / fe_metric",
            240,
            vec![row(240, a, 1.0), row(240, b, 1.0), row(240, c, 1.0)],
        ),
        Case {
            promql: TABLE,
            start: 0,
            end: 240,
            expected: range_selector,
            reads_duplicate: true,
        },
        Case {
            promql: "sum by (job) (fe_metric)",
            start: 0,
            end: 240,
            expected: range_sum,
            reads_duplicate: true,
        },
    ]
}

fn write_csv(path: &std::path::Path, rows: &[(i64, &str, &str, f64)]) {
    let mut text = String::from("ts,job,instance,val\n");
    for (offset, job, instance, value) in rows {
        let ts = chrono::DateTime::from_timestamp(BASE_SECS + offset, 0).unwrap();
        text.push_str(&format!(
            "{},{job},{instance},{value}\n",
            ts.format("%Y-%m-%dT%H:%M:%S%.3f")
        ));
    }
    std::fs::write(path, text).unwrap();
}

async fn create_table(instance: &Arc<Instance>, dir: &std::path::Path) {
    write_csv(&dir.join("a.csv"), FILE_A);
    write_csv(&dir.join("b.csv"), FILE_B);
    let location = prepare_path(&format!("{}/", dir.display()));
    let sql = format!(
        "CREATE EXTERNAL TABLE {TABLE} (
            ts TIMESTAMP(3) TIME INDEX,
            job STRING,
            instance STRING,
            val DOUBLE,
            PRIMARY KEY (job, instance)
        ) WITH (location = '{location}', format = 'csv')"
    );
    instance
        .do_query(&sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
}

async fn run(instance: &Arc<Instance>, case: &Case, native: bool) -> Vec<Row> {
    let sql = format!(
        "TQL EVAL ({}, {}, '60s') {}",
        BASE_SECS + case.start,
        BASE_SECS + case.end,
        case.promql
    );
    let mut ctx = QueryContext::with_db_name(None);
    ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, native.to_string());
    let output = instance
        .do_query(&sql, Arc::new(ctx))
        .await
        .remove(0)
        .unwrap_or_else(|error| panic!("`{sql}` (native={native}) failed: {error:?}"));
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        other => panic!("`{sql}` returned {other:?}"),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let df = batch.df_record_batch();
        let schema = df.schema();
        let mut ts = None;
        let mut value = None;
        let mut labels = BTreeMap::new();
        for (index, field) in schema.fields().iter().enumerate() {
            let column = df.column(index);
            match field.data_type() {
                DataType::Timestamp(_, _) => ts = Some(cast(column, &DataType::Int64).unwrap()),
                DataType::Float64 => {
                    assert!(value.is_none(), "`{sql}`: more than one value column");
                    value = Some(Arc::clone(column));
                }
                _ if field.name() == "__name__" => {}
                _ => {
                    labels.insert(field.name().clone(), cast(column, &DataType::Utf8).unwrap());
                }
            }
        }
        let ts = ts.expect("a time column");
        let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
        let value = value.expect("a value column");
        let value = value.as_any().downcast_ref::<Float64Array>().unwrap();
        for index in 0..df.num_rows() {
            let row_labels = labels
                .iter()
                .filter_map(|(name, values)| {
                    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                    values
                        .is_valid(index)
                        .then(|| format!("{name}={}", values.value(index)))
                })
                .collect::<Vec<_>>()
                .join(",");
            rows.push((ts.value(index), row_labels, value.value(index)));
        }
    }
    rows.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    rows
}

fn same_rows(actual: &[Row], expected: &[Row]) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            actual.0 == expected.0
                && actual.1 == expected.1
                && (actual.2 - expected.2).abs()
                    <= RELATIVE_TOLERANCE * actual.2.abs().max(expected.2.abs())
        })
}

async fn check_native_answers(instance: &Arc<Instance>, observer: &PlanObserver) {
    for case in cases() {
        observer.take_node_names();
        let native = run(instance, &case, true).await;
        let nodes = observer.take_node_names();
        assert!(
            nodes.iter().any(|node| node == "FusedSeriesExec")
                && nodes.iter().any(|node| node == "RowSeriesSourceExec"),
            "`{}` did not run on the native route: {nodes:?}",
            case.promql
        );
        assert!(
            same_rows(&native, &case.expected),
            "`{}` native answer {native:?}, expected {:?}",
            case.promql,
            case.expected
        );
        let legacy = run(instance, &case, false).await;
        if case.reads_duplicate {
            common_telemetry::info!(
                "`{}`: legacy answer {legacy:?}, native answer {native:?}",
                case.promql
            );
        } else {
            assert!(
                same_rows(&legacy, &case.expected),
                "`{}` legacy answer {legacy:?}, expected {:?}",
                case.promql,
                case.expected
            );
        }
    }
}

fn observer_plugins() -> (Arc<PlanObserver>, Plugins) {
    let observer = Arc::new(PlanObserver::default());
    let plugins = Plugins::new();
    plugins.insert(Arc::clone(&observer) as PhysicalPlanWrapperRef);
    (observer, plugins)
}

fn native_query_options() -> QueryOptions {
    QueryOptions {
        experimental_native_promql: true,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_over_file_engine_standalone() {
    common_telemetry::init_default_ut_logging();
    let (observer, plugins) = observer_plugins();
    let standalone = GreptimeDbStandaloneBuilder::new("native_promql_file_engine")
        .with_query_options(native_query_options())
        .with_plugin(plugins)
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    let dir = create_local_file_test_dir("native_promql_file_engine");
    create_table(&instance, dir.path()).await;
    check_native_answers(&instance, &observer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn native_promql_over_file_engine_distributed() {
    common_telemetry::init_default_ut_logging();
    let (observer, plugins) = observer_plugins();
    let cluster = GreptimeDbClusterBuilder::new("native_promql_file_engine_distributed")
        .await
        .with_query_options(native_query_options())
        .with_frontend_plugins(plugins)
        .with_local_file_access(LocalFileAccess::sandboxed(find_workspace_path(".")).unwrap())
        .build(false)
        .await;
    let instance = Arc::clone(cluster.fe_instance());
    let dir = create_local_file_test_dir("native_promql_file_engine_distributed");
    create_table(&instance, dir.path()).await;
    check_native_answers(&instance, &observer).await;
}
