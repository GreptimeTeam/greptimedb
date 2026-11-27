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

//! Native PromQL over file-engine tables.
//!
//! `fe_metric` reads two CSV files whose rows are in no particular order, and one sample of series
//! `{job="api", instance="a"}` is present in both files with different values. `fe_labels` holds a tag that is `NULL` on some
//! rows and `''` on others, and an integer tag. Expected answers are worked out by hand from Prometheus
//! semantics, which keep one sample per timestamp per series, read an empty label as an absent one
//! and compare label values as strings.

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
    promql: String,
    /// Evaluation range, in seconds since the epoch.
    start: i64,
    end: i64,
    expected: Vec<Row>,
    /// Whether the existing planner answers this case differently: it keeps whichever duplicated
    /// sample its sort leaves last, keeps `NULL` and `''` apart, and fails on an integer tag
    /// (`Unsupported tag column type: Int64`).
    legacy_differs: bool,
}

fn row(offset_secs: i64, labels: &str, value: f64) -> Row {
    ((BASE_SECS + offset_secs) * 1000, labels.to_string(), value)
}

fn instant(promql: impl Into<String>, offset_secs: i64, expected: Vec<Row>) -> Case {
    Case {
        promql: promql.into(),
        start: BASE_SECS + offset_secs,
        end: BASE_SECS + offset_secs,
        expected,
        legacy_differs: false,
    }
}

fn differs(case: Case) -> Case {
    Case {
        legacy_differs: true,
        ..case
    }
}

fn metric_cases(table: &str) -> Vec<Case> {
    let q = |promql: &str| promql.replace("fe_metric", table);
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
            q("fe_metric"),
            240,
            vec![row(240, a, 240.0), row(240, b, 130.0), row(240, c, 340.0)],
        ),
        // The duplicated sample: the smaller of the two values is the one kept.
        differs(instant(
            q("fe_metric{instance=\"a\"}"),
            120,
            vec![row(120, a, 120.0)],
        )),
        instant(
            q("rate(fe_metric[5m])"),
            240,
            vec![
                row(240, a, rate_a),
                row(240, b, rate_b),
                row(240, c, rate_c),
            ],
        ),
        instant(
            q("sum by (job) (rate(fe_metric[5m]))"),
            240,
            vec![
                row(240, "job=api", rate_a + rate_b),
                row(240, "job=db", rate_c),
            ],
        ),
        instant(
            q("rate(fe_metric[5m]) * 60"),
            240,
            vec![
                row(240, a, rate_a * 60.0),
                row(240, b, rate_b * 60.0),
                row(240, c, rate_c * 60.0),
            ],
        ),
        instant(
            q("fe_metric / fe_metric"),
            240,
            vec![row(240, a, 1.0), row(240, b, 1.0), row(240, c, 1.0)],
        ),
        Case {
            promql: q("fe_metric"),
            start: BASE_SECS,
            end: BASE_SECS + 240,
            expected: range_selector,
            legacy_differs: true,
        },
        Case {
            promql: q("sum by (job) (fe_metric)"),
            start: BASE_SECS,
            end: BASE_SECS + 240,
            expected: range_sum,
            legacy_differs: true,
        },
    ]
}

/// `fe_labels`: `{code="200"}` holds 1 at offset 0 with `instance` NULL, 2 at 60 with `instance`
/// `''`, and at 120 both a NULL row (3) and a `''` row (4); `{code="500", instance="x"}` holds 10,
/// 20, 30. The first is one series without an `instance` label, whose sample at 120 is 3.
fn label_cases() -> Vec<Case> {
    let ok = "code=200";
    let err = "code=500,instance=x";
    vec![
        Case {
            promql: "fe_labels{code=\"200\"}".to_string(),
            start: BASE_SECS,
            end: BASE_SECS + 120,
            expected: vec![row(0, ok, 1.0), row(60, ok, 2.0), row(120, ok, 3.0)],
            legacy_differs: true,
        },
        differs(instant("count(fe_labels)", 120, vec![row(120, "", 2.0)])),
        differs(instant(
            "fe_labels{instance=\"\"}",
            120,
            vec![row(120, ok, 3.0)],
        )),
        differs(instant(
            "sum by (code) (fe_labels)",
            120,
            vec![row(120, "code=200", 3.0), row(120, "code=500", 30.0)],
        )),
        // Label values are strings: "0200" is not "200".
        differs(instant("fe_labels{code=\"0200\"}", 120, vec![])),
        differs(instant(
            "fe_labels{code=\"500\"}",
            120,
            vec![row(120, err, 30.0)],
        )),
        differs(instant(
            "fe_labels{code=~\"5..\"}",
            120,
            vec![row(120, err, 30.0)],
        )),
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

async fn execute(instance: &Arc<Instance>, sql: &str) {
    instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error:?}"));
}

fn location(dir: &std::path::Path) -> String {
    prepare_path(&format!("{}/", dir.display()))
}

async fn create_tables(instance: &Arc<Instance>, dir: &std::path::Path) {
    let metric_dir = dir.join("metric");
    std::fs::create_dir(&metric_dir).unwrap();
    write_csv(&metric_dir.join("a.csv"), FILE_A);
    write_csv(&metric_dir.join("b.csv"), FILE_B);
    for (table, precision) in [("fe_metric", 3)] {
        execute(
            instance,
            &format!(
                "CREATE EXTERNAL TABLE {table} (
                    ts TIMESTAMP({precision}) TIME INDEX,
                    job STRING,
                    instance STRING,
                    val DOUBLE,
                    PRIMARY KEY (job, instance)
                ) WITH (location = '{}', format = 'csv')",
                location(&metric_dir)
            ),
        )
        .await;
    }

    let labels_dir = dir.join("labels");
    std::fs::create_dir(&labels_dir).unwrap();
    let mut lines = String::new();
    for (offset, instance, code, value) in [
        (0, "null", 200, 1.0),
        (60, "\"\"", 200, 2.0),
        (120, "\"\"", 200, 4.0),
        (120, "null", 200, 3.0),
        (0, "\"x\"", 500, 10.0),
        (60, "\"x\"", 500, 20.0),
        (120, "\"x\"", 500, 30.0),
    ] {
        let ts = chrono::DateTime::from_timestamp(BASE_SECS + offset, 0).unwrap();
        lines.push_str(&format!(
            "{{\"ts\":\"{}\",\"instance\":{instance},\"code\":{code},\"val\":{value}}}\n",
            ts.format("%Y-%m-%dT%H:%M:%S%.3f")
        ));
    }
    std::fs::write(labels_dir.join("rows.json"), lines).unwrap();
    execute(
        instance,
        &format!(
            "CREATE EXTERNAL TABLE fe_labels (
                ts TIMESTAMP(3) TIME INDEX,
                instance STRING NULL,
                code BIGINT NULL,
                val DOUBLE,
                PRIMARY KEY (instance, code)
            ) WITH (location = '{}', format = 'json')",
            location(&labels_dir)
        ),
    )
    .await;
}

async fn run(instance: &Arc<Instance>, case: &Case, native: bool) -> Result<Vec<Row>, String> {
    let sql = format!(
        "TQL EVAL ({}, {}, '60s') {}",
        case.start, case.end, case.promql
    );
    let mut ctx = QueryContext::with_db_name(None);
    ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY, native.to_string());
    let output = instance
        .do_query(&sql, Arc::new(ctx))
        .await
        .remove(0)
        .map_err(|error| format!("`{sql}` (native={native}) failed: {error:?}"))?;
    let batches = match output.data {
        OutputData::Stream(stream) => collect_batches(stream)
            .await
            .map_err(|error| format!("`{sql}` (native={native}) failed: {error:?}"))?,
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
                    (values.is_valid(index) && !values.value(index).is_empty())
                        .then(|| format!("{name}={}", values.value(index)))
                })
                .collect::<Vec<_>>()
                .join(",");
            rows.push((ts.value(index), row_labels, value.value(index)));
        }
    }
    rows.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    Ok(rows)
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

/// Runs every case and reports all failures together, so one run shows which shapes miss.
async fn check_native_answers(instance: &Arc<Instance>, observer: &PlanObserver) {
    let cases = metric_cases("fe_metric").into_iter().chain(label_cases());
    let mut failures = Vec::new();
    for case in cases {
        observer.take_node_names();
        let native = run(instance, &case, true).await;
        let nodes = observer.take_node_names();
        let native = match native {
            Ok(native) => native,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        if !(nodes.iter().any(|node| node == "FusedSeriesExec")
            && nodes.iter().any(|node| node == "RowSeriesSourceExec"))
        {
            failures.push(format!(
                "`{}` did not run on the native route: {nodes:?}",
                case.promql
            ));
            continue;
        }
        if !same_rows(&native, &case.expected) {
            failures.push(format!(
                "`{}` native answer {native:?}, expected {:?}",
                case.promql, case.expected
            ));
        }
        let legacy = run(instance, &case, false).await;
        if case.legacy_differs {
            common_telemetry::info!(
                "`{}`: legacy answer {legacy:?}, native answer {native:?}",
                case.promql
            );
        } else if !legacy
            .as_ref()
            .is_ok_and(|legacy| same_rows(legacy, &case.expected))
        {
            failures.push(format!(
                "`{}` legacy answer {legacy:?}, expected {:?}",
                case.promql, case.expected
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
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
    create_tables(&instance, dir.path()).await;
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
    create_tables(&instance, dir.path()).await;
    check_native_answers(&instance, &observer).await;
}
