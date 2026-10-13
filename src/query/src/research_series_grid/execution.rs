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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion_common::{DataFusionError, Result};
use futures::{TryStreamExt, stream};
use promql_parser::parser::Expr;

use crate::research_series_grid::evaluate::{Evaluator, SelectorInputs, Value};
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::source::SnapshotIndex;
use crate::research_series_grid::vector::SeriesVector;

#[derive(Debug, Clone)]
pub(crate) struct GridPartition {
    pub expr: Expr,
    pub grid: Arc<TimeGrid>,
    pub source: Arc<SnapshotIndex>,
    pub path: PathBuf,
    pub tag_orders: Arc<BTreeMap<String, Vec<String>>>,
    pub schema: SchemaRef,
    pub lookback: i64,
}

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl PartitionStream for GridPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let partition = self.clone();
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let work = stream::once(async move {
            let cancelled = guard.0.clone();
            let result = common_runtime::spawn_blocking_query(move || {
                let workers = match std::env::var("GT_RESEARCH_SERIES_WORKERS") {
                    Ok(value) => value
                        .parse::<usize>()
                        .ok()
                        .filter(|n| (1..=32).contains(n))
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "Research worker count must be in 1..=32".into(),
                            )
                        })?,
                    Err(std::env::VarError::NotPresent) => 1,
                    Err(error) => return Err(DataFusionError::Execution(error.to_string())),
                };
                let block_evaluation = match std::env::var("GT_RESEARCH_BLOCK_EVALUATION") {
                    Ok(value) if value == "1" => true,
                    Ok(value) if value == "0" => false,
                    Err(std::env::VarError::NotPresent) => false,
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Research block evaluation must be 0 or 1".into(),
                        ));
                    }
                };
                let mut evaluator = Evaluator {
                    inputs: SelectorInputs::Snapshot {
                        source: &partition.source,
                        path: &partition.path,
                    },
                    workers,
                    block_evaluation,
                    grid: &partition.grid,
                    lookback: partition.lookback,
                    tag_orders: &partition.tag_orders,
                    cancelled: &cancelled,
                    memory: None,
                };
                let series = match evaluator.evaluate(&partition.expr)? {
                    Value::Vector(result) => result.series,
                    Value::Scalar(value) => vec![SeriesVector {
                        labels: BTreeMap::new(),
                        values: vec![Some(value); partition.grid.instants.len()],
                    }],
                };
                into_batches(series, &partition.grid, &partition.schema, &cancelled)
            })
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            drop(guard);
            result
        });
        let batches = work
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
    }
}

pub(crate) fn into_batches(
    series: Vec<SeriesVector>,
    grid: &TimeGrid,
    schema: &SchemaRef,
    cancelled: &AtomicBool,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(series.len());
    for series in series {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DataFusionError::Execution(
                "Research series query cancelled".into(),
            ));
        }
        if series.values.len() != grid.instants.len() {
            return Err(DataFusionError::Execution(
                "Output does not match time grid".into(),
            ));
        }
        for label in series
            .labels
            .keys()
            .filter(|name| name.as_str() != "__name__")
        {
            if !schema
                .fields()
                .iter()
                .skip(2)
                .any(|field| field.name() == label)
            {
                return Err(DataFusionError::Execution(format!(
                    "Output schema lost label: {label}"
                )));
            }
        }
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for (&timestamp, value) in grid.instants.iter().zip(series.values) {
            if let Some(value) = value {
                timestamps.push(timestamp);
                values.push(value);
            }
        }
        if timestamps.is_empty() {
            continue;
        }
        let count = timestamps.len();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ];
        for field in schema.fields().iter().skip(2) {
            let label = series.labels.get(field.name()).map(String::as_str);
            columns.push(Arc::new(StringArray::from_iter(std::iter::repeat_n(
                label, count,
            ))));
        }
        batches.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn output_preserves_presence_and_label_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("empty", DataType::Utf8, true),
            Field::new("absent", DataType::Utf8, true),
        ]));
        let grid = TimeGrid::new(0, 2, 1).unwrap();
        let batches = into_batches(
            vec![SeriesVector {
                labels: BTreeMap::from([("empty".into(), String::new())]),
                values: vec![Some(f64::NAN), None, Some(3.0)],
            }],
            &grid,
            &schema,
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].column(2).null_count(), 0);
        assert_eq!(batches[0].column(3).null_count(), 2);
        let times = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(times.values().as_ref(), &[0, 2]);
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(values.value(0).is_nan());
    }

    #[test]
    fn dropping_consumer_signals_blocking_work() {
        let flag = Arc::new(AtomicBool::new(false));
        {
            let _guard = CancelOnDrop(flag.clone());
        }
        assert!(flag.load(Ordering::Relaxed));
    }
}
