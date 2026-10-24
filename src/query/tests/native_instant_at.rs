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

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use promql::native::execute::{NativeExecutionState, NativeExecutor};
use promql_parser::parser::EvalStmt;
use query::promql::native_compile::compile;

fn stmt(query: &str, start: u64, end: u64, lookback: u64) -> EvalStmt {
    EvalStmt {
        expr: promql_parser::parser::parse(query).unwrap(),
        start: UNIX_EPOCH + Duration::from_millis(start),
        end: UNIX_EPOCH + Duration::from_millis(end),
        interval: Duration::from_millis(1000),
        lookback_delta: Duration::from_millis(lookback),
    }
}
fn schema(fields: usize) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: (0..fields)
            .map(|i| FieldSpec {
                name: if i == 0 {
                    "value".into()
                } else {
                    "other".into()
                },
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}
fn raw(times: &[i64], values: &[Option<f64>], fields: usize, metric: MetricName) -> RawSeriesChunk {
    let vals = Samples::from_float(Float64Array::from(values.to_vec()));
    let all = (0..fields)
        .map(|i| {
            if i == 0 {
                vals.clone()
            } else {
                Samples::from_float(Float64Array::from(vec![None; values.len()]))
            }
        })
        .collect();
    RawSeriesChunk::try_new(
        &schema(fields),
        Buffer::from(vec![1]),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).unwrap(),
        SeriesFields::try_from_vec(all).unwrap(),
        true,
    )
    .unwrap()
}
fn metric(name: &str) -> MetricName {
    MetricName::try_new(
        Some(name.into()),
        vec![Label {
            name: "job".into(),
            value: "x".into(),
        }],
    )
    .unwrap()
}
/// One result series: its values, the grid timestamps it holds, its metric name and its tags.
type ResultSeries = (Vec<f64>, Vec<i64>, Option<String>, Vec<(String, String)>);

fn run(
    query: &str,
    start: u64,
    end: u64,
    lookback: u64,
    values: &[Option<f64>],
    times: &[i64],
    fields: usize,
) -> Vec<ResultSeries> {
    let stmt = stmt(query, start, end, lookback);
    let tree = compile(&stmt, schema(fields), SeriesOrdering::KeyThenTime).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 24));
    let state =
        NativeExecutionState::try_new(&tree, pool, &ExecutionPlanMetricsSet::new(), 0).unwrap();
    let mut executor = NativeExecutor::new(state);
    executor
        .push_raw(raw(times, values, fields, metric("a")))
        .unwrap();
    executor
        .finish()
        .unwrap()
        .into_iter()
        .map(|chunk| {
            let positions: Vec<i64> = chunk
                .series
                .time
                .positions()
                .map(|(p, _)| chunk.series.time.grid().unwrap().timestamps().unwrap()[p])
                .collect();
            let vals: Vec<f64> = chunk
                .series
                .fields
                .get(0)
                .unwrap()
                .iter()
                .filter_map(|v| match v {
                    datatypes::series::samples::SampleRef::Float(x) => Some(x),
                    _ => None,
                })
                .collect();
            (
                vals,
                positions,
                chunk.series.metric.name().map(str::to_string),
                chunk
                    .series
                    .metric
                    .tags()
                    .iter()
                    .map(|l| (l.name.to_string(), l.value.to_string()))
                    .collect(),
            )
        })
        .filter(|(values, positions, _, _)| !values.is_empty() || !positions.is_empty())
        .collect()
}
#[test]
fn native_instant_at_timestamp_values_and_labels() {
    let times = [-1500, 500, 1500, 3500];
    let values = [Some(7.0), Some(1.0), Some(2.0), Some(3.0)];
    assert_eq!(
        run("a", 0, 4000, 3000, &values, &times, 1)[0].0,
        [7.0, 1.0, 2.0, 2.0, 3.0]
    );
    assert_eq!(
        run("a @ 1", 0, 4000, 3000, &values, &times, 1)[0].0,
        [1.0; 5]
    );
    assert_eq!(
        run("a @ 1 offset 800ms", 0, 4000, 3000, &values, &times, 1)[0].0,
        [7.0; 5]
    );
    assert_eq!(
        run("a @ start()", 0, 4000, 3000, &values, &times, 1)[0].0,
        [7.0; 5]
    );
    assert_eq!(
        run("a @ end()", 0, 4000, 3000, &values, &times, 1)[0].0,
        [3.0; 5]
    );
    assert_eq!(
        run("timestamp(a)", 0, 4000, 3000, &values, &times, 1)[0].0,
        [-1.5, 0.5, 1.5, 1.5, 3.5]
    );
    let out = run("timestamp(a @ 1)", 0, 4000, 3000, &values, &times, 1);
    assert_eq!(out[0].0, [0.5; 5]);
    assert_eq!(out[0].2, None);
    assert_eq!(out[0].3, [("job".into(), "x".into())]);
    assert_eq!(
        run("timestamp(abs(a @ 1))", 0, 4000, 3000, &values, &times, 1)[0].0,
        [0.0, 1.0, 2.0, 3.0, 4.0]
    );
    assert_eq!(
        run("a @ -1.5", 2000, 6000, 1000, &values, &times, 1)[0].0,
        [7.0; 5]
    );
    assert!(run("a @ 6.5", 0, 4000, 3000, &values, &times, 1).is_empty());
    assert_eq!(
        run("a @ 0.5", 0, 4000, 0, &values, &times, 1)[0].0,
        [1.0; 5]
    );
    assert!(run("a @ 1", 0, 4000, 0, &values, &times, 1).is_empty());
    let stale = f64::from_bits(common_query::prometheus::PROMETHEUS_STALE_NAN_BITS);
    assert!(
        run(
            "a @ 1",
            0,
            4000,
            3000,
            &[Some(5.0), Some(stale)],
            &[-1500, 500],
            1
        )
        .is_empty()
    );
}

/// Two source series that differ only by a metadata label `timestamp()` drops must not be emitted
/// as two rows of one label set: the second is refused at the position it collides on, which is the
/// same rule a map follows when it drops the metric name.
#[test]
fn timestamp_refuses_two_series_that_collapse_to_one_label_set() {
    let stmt = stmt("timestamp(a @ 1)", 0, 4000, 3000);
    let tree = compile(&stmt, schema(1), SeriesOrdering::KeyThenTime).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 24));
    let state =
        NativeExecutionState::try_new(&tree, pool, &ExecutionPlanMetricsSet::new(), 0).unwrap();
    let mut executor = NativeExecutor::new(state);
    let times = [-1500i64, 500];
    let values = [Some(7.0), Some(1.0)];
    for (key, metadata) in [("first", "gauge"), ("second", "counter")] {
        let metric = MetricName::try_new(
            Some("a".into()),
            vec![
                Label {
                    name: "job".into(),
                    value: "x".into(),
                },
                Label {
                    name: "__type__".into(),
                    value: metadata.into(),
                },
            ],
        )
        .unwrap();
        let chunk = RawSeriesChunk::try_new(
            &schema(1),
            Buffer::from(key.as_bytes()),
            Some(metric),
            RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).unwrap(),
            SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
                values.to_vec(),
            ))])
            .unwrap(),
            true,
        )
        .unwrap();
        // A chunk is queued on the input node; the selection sees it when it is pulled, so the
        // collision surfaces on the read that gathers the second series, not on the push.
        executor
            .push_raw(chunk)
            .unwrap_or_else(|error| panic!("the {key} chunk was refused on the way in: {error}"));
    }
    let error = executor
        .finish()
        .expect_err("two series collapsed onto one label set were accepted");
    assert!(
        error.to_string().contains("job"),
        "the refusal did not name the label set it collided on: {error}"
    );
}
