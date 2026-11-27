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
//! `{job="api", instance="a"}` is present in both files with different values; `fe_metric_s`,
//! `fe_metric_us` and `fe_metric_ns` read the same files with a second, microsecond and nanosecond
//! time index. `fe_sub_us` and `fe_sub_ns` read one file whose samples are less than a millisecond
//! off whole milliseconds and from each other. `fe_labels` holds a tag that is `NULL` on some
//! rows and `''` on others, and an integer tag. `fe_histogram` mirrors the sqlness case
//! `promql/delta_exponential_histogram`. Expected answers are worked out by hand from Prometheus
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
    /// The lookback delta of the statement, when not the default.
    lookback: Option<&'static str>,
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
        lookback: None,
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
            lookback: None,
        },
        Case {
            promql: q("sum by (job) (fe_metric)"),
            start: BASE_SECS,
            end: BASE_SECS + 240,
            expected: range_sum,
            legacy_differs: true,
            lookback: None,
        },
    ]
}

/// `(offset_micros, value)` of the one series `{host="h"}` of `fe_sub_us` and `fe_sub_ns`.
const SUB_MILLISECOND: &[(i64, f64)] = &[
    (59_999_500, 1.0),
    (60_000_500, 2.0),
    (61_000_000, 4.0),
    (61_000_200, 8.0),
];

/// Windows and lookbacks are decided on the stored time, so a bound at a whole millisecond falls
/// between two samples that share that millisecond once truncated, and samples less than a
/// millisecond apart stay two samples. A kernel reads the time truncated to milliseconds.
fn sub_millisecond_cases(table: &str) -> Vec<Case> {
    let q = |promql: &str| promql.replace("fe_sub", table);
    let h = "host=h";
    vec![
        // 60.0005s is after 60s, so 59.9995s is the latest sample at 60s.
        instant(q("fe_sub"), 60, vec![row(60, h, 1.0)]),
        instant(
            q("timestamp(fe_sub)"),
            60,
            vec![row(60, h, (BASE_SECS * 1000 + 59_999) as f64 / 1000.0)],
        ),
        // 61.0002s is after 61s.
        instant(q("fe_sub"), 61, vec![row(61, h, 4.0)]),
        instant(
            q("timestamp(fe_sub)"),
            62,
            vec![row(62, h, (BASE_SECS + 61) as f64)],
        ),
        // (60s, 61s] holds 60.0005s and 61s.
        instant(q("count_over_time(fe_sub[1s])"), 61, vec![row(61, h, 2.0)]),
        // (60s, 62s] holds 60.0005s, 61s and 61.0002s.
        instant(q("sum_over_time(fe_sub[2s])"), 62, vec![row(62, h, 14.0)]),
        // The last two samples in (59s, 61s] are 60.0005s and 61s.
        instant(q("idelta(fe_sub[2s])"), 61, vec![row(61, h, 2.0)]),
        // The last two samples in (60s, 62s] share the millisecond 61000, so there is no interval
        // to take a difference over.
        instant(q("idelta(fe_sub[2s])"), 62, vec![]),
        // A zero lookback selects a sample stored exactly at the instant: 59.9995s is not at 60s.
        zero_lookback(instant(q("fe_sub"), 60, vec![])),
        zero_lookback(instant(q("fe_sub"), 61, vec![row(61, h, 4.0)])),
    ]
}

fn zero_lookback(case: Case) -> Case {
    Case {
        lookback: Some("0"),
        ..case
    }
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
            lookback: None,
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

/// The statements and answers of the sqlness case `promql/delta_exponential_histogram`.
fn histogram_cases() -> Vec<Case> {
    let at = |secs: i64, promql: &str, value: Option<f64>| Case {
        promql: promql.to_string(),
        start: secs,
        end: secs,
        expected: value
            .map(|value| vec![(secs * 1000, String::new(), value)])
            .unwrap_or_default(),
        legacy_differs: false,
        lookback: None,
    };
    vec![
        at(
            180,
            "histogram_count(sum_over_time(fe_histogram[3m]))",
            Some(45.0),
        ),
        at(
            180,
            "histogram_sum(sum_over_time(fe_histogram[3m]))",
            Some(67.5),
        ),
        at(
            180,
            "histogram_count(sum_over_time(fe_histogram[3m]) / 180)",
            Some(0.25),
        ),
        at(
            180,
            "histogram_fraction(1, 2, sum_over_time(fe_histogram[3m]))",
            Some(1.0),
        ),
        at(
            180,
            "histogram_count(sum_over_time(fe_histogram[1m]))",
            Some(15.0),
        ),
        at(
            360,
            "histogram_count(sum_over_time(fe_histogram[3m]))",
            None,
        ),
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
    for (table, precision) in [
        ("fe_metric", 3),
        ("fe_metric_s", 0),
        ("fe_metric_us", 6),
        ("fe_metric_ns", 9),
    ] {
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

    let sub_dir = dir.join("sub");
    std::fs::create_dir(&sub_dir).unwrap();
    let mut text = String::from("ts,host,val\n");
    for (offset_micros, value) in SUB_MILLISECOND {
        let ts =
            chrono::DateTime::from_timestamp_micros(BASE_SECS * 1_000_000 + offset_micros).unwrap();
        text.push_str(&format!(
            "{},h,{value}\n",
            ts.format("%Y-%m-%dT%H:%M:%S%.6f")
        ));
    }
    std::fs::write(sub_dir.join("rows.csv"), text).unwrap();
    for (table, precision) in [("fe_sub_us", 6), ("fe_sub_ns", 9)] {
        execute(
            instance,
            &format!(
                "CREATE EXTERNAL TABLE {table} (
                    ts TIMESTAMP({precision}) TIME INDEX,
                    host STRING,
                    val DOUBLE,
                    PRIMARY KEY (host)
                ) WITH (location = '{}', format = 'csv')",
                location(&sub_dir)
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

    // Two series that differ only in a tag column named `__name__`.
    let named_dir = dir.join("named");
    std::fs::create_dir(&named_dir).unwrap();
    let ts = chrono::DateTime::from_timestamp(BASE_SECS, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%S%.3f");
    std::fs::write(
        named_dir.join("rows.csv"),
        format!("ts,__name__,host,val\n{ts},a,h,1\n{ts},b,h,2\n"),
    )
    .unwrap();
    execute(
        instance,
        &format!(
            "CREATE EXTERNAL TABLE fe_named (
                ts TIMESTAMP(3) TIME INDEX,
                \"__name__\" STRING,
                host STRING,
                val DOUBLE,
                PRIMARY KEY (\"__name__\", host)
            ) WITH (location = '{}', format = 'csv')",
            location(&named_dir)
        ),
    )
    .await;

    // The fixture of `promql/delta_exponential_histogram`.
    let histogram_file = prepare_path(&format!("{}/histogram.parquet", dir.display()));
    execute(
        instance,
        "CREATE TABLE fe_histogram_source (
            greptime_timestamp TIMESTAMP TIME INDEX,
            histogram_schema INT, zero_threshold DOUBLE, sample_sum DOUBLE, reset_hint INT,
            start_timestamp TIMESTAMP, count_i64 BIGINT, zero_count_i64 BIGINT,
            absent_float DOUBLE
        )",
    )
    .await;
    execute(
        instance,
        "INSERT INTO fe_histogram_source VALUES
            (60000, 0, 0.001, 15.0, 3, 0, 10, 0, NULL),
            (120000, 0, 0.001, 30.0, 3, 60000, 20, 0, NULL),
            (180000, 0, 0.001, 22.5, 3, 120000, 15, 0, NULL)",
    )
    .await;
    execute(
        instance,
        &format!(
            "COPY (
                SELECT greptime_timestamp,
                    named_struct(
                        'schema', histogram_schema,
                        'zero_threshold', zero_threshold,
                        'sum', sample_sum,
                        'reset_hint', reset_hint,
                        'start_timestamp', start_timestamp,
                        'custom_values', array_slice([sample_sum], 1, 0),
                        'positive_span_offsets', [CAST(histogram_schema + 1 AS INT)],
                        'positive_span_lengths', [CAST(histogram_schema + 1 AS INT)],
                        'negative_span_offsets', array_slice([histogram_schema], 1, 0),
                        'negative_span_lengths', array_slice([histogram_schema], 1, 0),
                        'count_i64', count_i64,
                        'zero_count_i64', zero_count_i64,
                        'positive_buckets_i64', [count_i64],
                        'negative_buckets_i64', array_slice([count_i64], 1, 0),
                        'count_f64', absent_float,
                        'zero_count_f64', absent_float,
                        'positive_buckets_f64', array_slice([sample_sum], 1, 0),
                        'negative_buckets_f64', array_slice([sample_sum], 1, 0)
                    ) AS greptime_native_histogram
                FROM fe_histogram_source
            ) TO '{histogram_file}'"
        ),
    )
    .await;
    execute(
        instance,
        &format!(
            "CREATE EXTERNAL TABLE fe_histogram WITH (location = '{histogram_file}', format = 'parquet')"
        ),
    )
    .await;
}

async fn run(instance: &Arc<Instance>, case: &Case, native: bool) -> Result<Vec<Row>, String> {
    let lookback = case
        .lookback
        .map(|lookback| format!(", '{lookback}'"))
        .unwrap_or_default();
    let sql = format!(
        "TQL EVAL ({}, {}, '60s'{lookback}) {}",
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
    let cases = metric_cases("fe_metric")
        .into_iter()
        .chain(metric_cases("fe_metric_s"))
        .chain(metric_cases("fe_metric_us"))
        .chain(metric_cases("fe_metric_ns"))
        .chain(sub_millisecond_cases("fe_sub_us"))
        .chain(sub_millisecond_cases("fe_sub_ns"))
        .chain(label_cases())
        .chain(histogram_cases());
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

/// A tag column named `__name__` has no label to be read as, so the native route keeps the existing
/// plan rather than merging the two series it tells apart.
async fn check_metric_name_tag_declines(instance: &Arc<Instance>, observer: &PlanObserver) {
    let case = instant("fe_named", 0, vec![]);
    observer.take_node_names();
    let answer = run(instance, &case, true).await;
    let nodes = observer.take_node_names();
    assert!(
        !nodes.iter().any(|node| node == "RowSeriesSourceExec"),
        "a `__name__` tag column must not reach a row source: {nodes:?}"
    );
    assert!(answer.is_ok(), "{answer:?}");
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
    check_metric_name_tag_declines(&instance, &observer).await;
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
    check_metric_name_tag_declines(&instance, &observer).await;
}
