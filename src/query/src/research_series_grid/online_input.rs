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

use datafusion::arrow::array::{Array, Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::LogicalPlan;
use datatypes::arrow_array::string_array_value_at_index;
use promql::extension_plan::SeriesDivide;
use promql_parser::parser::Expr;

use crate::research_series_grid::evaluate::SelectorKey;
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::range;
use crate::research_series_grid::vector::SeriesVector;

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

/// Keeps each selector's window in its own main-planner request.
pub(crate) fn selector_requests(expr: &Expr) -> Result<BTreeMap<SelectorKey, Expr>> {
    struct Requests(BTreeMap<SelectorKey, Expr>);
    impl Requests {
        fn visit(&mut self, expr: &Expr) -> Result<()> {
            let (selector, function) = match expr {
                Expr::VectorSelector(selector) => (selector, None),
                Expr::Call(call)
                    if matches!(
                        call.func.name,
                        "rate" | "increase" | "irate" | "max_over_time"
                    ) =>
                {
                    let [argument] = call.args.args.as_slice() else {
                        return Err(invalid("Invalid online range arguments"));
                    };
                    let Expr::MatrixSelector(matrix) = argument.as_ref() else {
                        return Err(invalid("Online range input requires a matrix selector"));
                    };
                    if matrix.range.subsec_nanos() % 1_000_000 != 0 {
                        return Err(invalid("Online range requires millisecond precision"));
                    }
                    let window = i64::try_from(matrix.range.as_millis())
                        .map_err(|_| invalid("Online range duration overflow"))?;
                    (&matrix.vs, Some((call.func.name, window)))
                }
                Expr::MatrixSelector(_) | Expr::Subquery(_) => {
                    return Err(invalid("Unsupported online selector shape"));
                }
                _ => {
                    match expr {
                        Expr::Aggregate(aggregate) => {
                            self.visit(&aggregate.expr)?;
                            if let Some(param) = &aggregate.param {
                                self.visit(param)?;
                            }
                        }
                        Expr::Binary(binary) => {
                            self.visit(&binary.lhs)?;
                            self.visit(&binary.rhs)?;
                        }
                        Expr::Paren(paren) => self.visit(&paren.expr)?,
                        Expr::Unary(unary) => self.visit(&unary.expr)?,
                        Expr::Call(call) => {
                            for argument in &call.args.args {
                                self.visit(argument)?;
                            }
                        }
                        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
                        _ => return Err(invalid("Unsupported online expression")),
                    }
                    return Ok(());
                }
            };
            if selector.at.is_some() || !selector.matchers.or_matchers.is_empty() {
                return Err(invalid("Online selector does not support @ or OR matchers"));
            }
            self.0
                .entry(SelectorKey::new(selector, function))
                .or_insert_with(|| expr.clone());
            Ok(())
        }
    }
    let mut requests = Requests(BTreeMap::new());
    requests.visit(expr)?;
    Ok(requests.0)
}

/// Retains the main planner's filters, ordering and series division, but not window expansion.
pub(crate) fn raw_series_input(plan: &LogicalPlan) -> Result<LogicalPlan> {
    fn visit(plan: &LogicalPlan, found: &mut Option<LogicalPlan>) -> Result<()> {
        if let LogicalPlan::Extension(extension) = plan
            && extension.node.as_any().is::<SeriesDivide>()
        {
            if found.replace(plan.clone()).is_some() {
                return Err(invalid("Online selector has multiple series inputs"));
            }
            return Ok(());
        }
        for input in plan.inputs() {
            visit(input, found)?;
        }
        Ok(())
    }
    let mut found = None;
    visit(plan, &mut found)?;
    found.ok_or_else(|| invalid("Online selector has no SeriesDivide input"))
}

#[derive(Clone, Debug)]
pub(crate) struct SeriesInput {
    pub key: SelectorKey,
    pub metric: String,
    pub time_column: String,
    pub value_column: String,
    pub tags: Vec<String>,
    pub offset: i64,
}

impl SeriesInput {
    /// Consumes one complete series from SeriesDivide, after storage visibility and dedup.
    pub(crate) fn evaluate(
        &self,
        batch: &RecordBatch,
        grid: &TimeGrid,
        lookback: i64,
    ) -> Result<Option<SeriesVector>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| invalid(format!("Missing online column: {name}")))
        };
        let timestamps = column(&self.time_column)?
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| invalid("Online prototype requires millisecond timestamps"))?;
        let samples = column(&self.value_column)?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Online prototype requires Float64 samples"))?;
        if timestamps.null_count() != 0
            || timestamps.values().windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(invalid("Online series timestamps are null or unordered"));
        }
        let mut labels = BTreeMap::new();
        for name in &self.tags {
            let array = column(name)?;
            let string_type = match array.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
                DataType::Dictionary(key, value) => key.is_integer() && value.is_string(),
                _ => false,
            };
            if !string_type {
                return Err(invalid(format!(
                    "Unsupported online label type: {}",
                    array.data_type()
                )));
            }
            // SeriesDivide guarantees a single series; nullable and empty labels remain distinct.
            if let Some(value) = string_array_value_at_index(array, 0) {
                labels.insert(name.clone(), value.to_owned());
            }
        }
        let values = if let Some((name, window)) = &self.key.function {
            let mut times = Vec::with_capacity(batch.num_rows());
            let mut values = Vec::with_capacity(batch.num_rows());
            for (timestamp, value) in timestamps.values().iter().zip(samples.iter()) {
                if let Some(value) = value {
                    times.push(*timestamp);
                    values.push(value);
                }
            }
            range::evaluate(name, grid, times, values, *window, self.offset)?
        } else {
            labels.insert("__name__".into(), self.metric.clone());
            // A null latest sample must not resurrect an older value. Keep its timestamp
            // during instant selection; range inputs instead omit null samples entirely.
            let values: Vec<_> = samples
                .iter()
                .map(|value| value.unwrap_or_else(|| f64::from_bits(0x7ff0_0000_0000_0002)))
                .collect();
            grid.instant_indices(timestamps.values(), &values, lookback, self.offset)
                .map_err(invalid)?
                .into_iter()
                .map(|index| index.map(|i| values[i]))
                .collect()
        };
        Ok(Some(SeriesVector { labels, values }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use promql_parser::parser::parse;

    use super::*;

    #[test]
    fn requests_keep_windows_and_offsets_distinct() {
        let requests = selector_requests(&parse(
            "sum(rate(x[5m])) + sum(rate(x[1h])) + sum(rate(x[5m] offset 30s)) + sum(rate(x[5m]))"
        ).unwrap()).unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests
                .keys()
                .filter(|key| key.function.as_ref().unwrap().1 == 300_000)
                .count(),
            2
        );
        assert!(selector_requests(&parse("rate(x[5m:1m])").unwrap()).is_err());
        assert!(selector_requests(&parse("x @ 10").unwrap()).is_err());
    }

    #[tokio::test]
    async fn batch_keeps_null_and_stale_instant_suppression() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::prelude::SessionContext;
        use datafusion_common::ToDFSchema;
        use datafusion_expr::EmptyRelation;
        use promql::extension_plan::InstantManipulate;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 10, 20, 30])),
                Arc::new(Float64Array::from(vec![
                    Some(2.0),
                    None,
                    Some(f64::from_bits(0x7ff0_0000_0000_0002)),
                    Some(-0.0),
                ])),
                Arc::new(StringArray::from(vec![Some(""); 4])),
            ],
        )
        .unwrap();
        let Expr::VectorSelector(selector) = parse("x").unwrap() else {
            panic!("selector")
        };
        let mut input = SeriesInput {
            key: SelectorKey::new(&selector, None),
            metric: "x".into(),
            time_column: "ts".into(),
            value_column: "value".into(),
            tags: vec!["tag".into()],
            offset: 10,
        };
        let grid = TimeGrid::new(10, 40, 10).unwrap();
        let output = input.evaluate(&batch, &grid, 30).unwrap().unwrap();
        let reference = InstantManipulate::new(
            10,
            40,
            30,
            10,
            10,
            "ts".into(),
            vec!["tag".into()],
            Some("value".into()),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: batch.schema().to_dfschema_ref().unwrap(),
            }),
        );
        let memory = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch.clone()]], batch.schema(), None).unwrap(),
        )));
        let reference = datafusion::physical_plan::collect(
            reference.to_execution_plan(memory),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap();
        let mut expected = vec![None; grid.instants.len()];
        for batch in reference {
            let times = batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for (timestamp, value) in times.values().iter().zip(values.iter()) {
                expected[grid.instants.binary_search(timestamp).unwrap()] = value.map(f64::to_bits);
            }
        }
        assert_eq!(
            output
                .values
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            output
                .values
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>(),
            vec![
                Some(2.0f64.to_bits()),
                None,
                None,
                Some((-0.0f64).to_bits())
            ]
        );
        assert_eq!(output.labels.get("tag").unwrap(), "");
        input.key.function = Some(("max_over_time".into(), 30));
        let output = input.evaluate(&batch, &grid, 30).unwrap().unwrap();
        assert_eq!(
            output.values,
            vec![Some(2.0), Some(2.0), Some(2.0), Some(-0.0)]
        );
        assert!(!output.labels.contains_key("__name__"));
    }
}
