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

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Expr, Extension, LogicalPlan, Operator, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn, Literal};
use serde::{Deserialize, Serialize};

use crate::research_series_grid::online_frontier::PACKED_POINTS;
use crate::research_series_grid::scan_range::{PackedRangeSpec, RangeOutput, RangeSpec};
use crate::research_series_grid::scan_range_aggregate::RangeAggregateSpec;
use crate::research_series_grid::series_range::{SeriesRange, SeriesRangeExec};

// The wire expression refers to the window result, never to raw input columns.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum Formula {
    Rate,
    Constant(u64),
    Binary(u8, Box<Formula>, Box<Formula>),
}

impl Formula {
    fn recognize(expr: &Expr, schema: &DFSchema, value: usize) -> Option<Self> {
        match bare(expr) {
            Expr::Column(column) if schema.index_of_column(column).ok() == Some(value) => {
                Some(Self::Rate)
            }
            Expr::Literal(ScalarValue::Float64(Some(value)), _) => {
                Some(Self::Constant(value.to_bits()))
            }
            Expr::Cast(cast) if cast.field.data_type() == &DataType::Float64 => {
                // Every admitted leaf and arithmetic result is already Float64.
                // A cast through another type fails recursive recognition.
                Self::recognize(&cast.expr, schema, value)
            }
            Expr::BinaryExpr(binary) => {
                let op = match binary.op {
                    Operator::Plus => 0,
                    Operator::Minus => 1,
                    Operator::Multiply => 2,
                    Operator::Divide => 3,
                    _ => return None,
                };
                Some(Self::Binary(
                    op,
                    Box::new(Self::recognize(&binary.left, schema, value)?),
                    Box::new(Self::recognize(&binary.right, schema, value)?),
                ))
            }
            _ => None,
        }
    }

    fn contains_rate(&self) -> bool {
        match self {
            Self::Rate => true,
            Self::Constant(_) => false,
            Self::Binary(_, left, right) => left.contains_rate() || right.contains_rate(),
        }
    }

    fn physical(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(match self {
            Self::Rate => Arc::new(PhysicalColumn::new("rate", 0)),
            Self::Constant(bits) => Arc::new(Literal::new(ScalarValue::Float64(Some(
                f64::from_bits(*bits),
            )))),
            Self::Binary(op, left, right) => Arc::new(BinaryExpr::new(
                left.physical()?,
                match op {
                    0 => Operator::Plus,
                    1 => Operator::Minus,
                    2 => Operator::Multiply,
                    3 => Operator::Divide,
                    _ => return Err(invalid("Unknown fragment arithmetic operation")),
                },
                right.physical()?,
            )),
        })
    }
}

/// A region-local rate/scalar/sum program. Its only consumer merges partial grids.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SeriesFragment {
    range: SeriesRange,
    labels: Vec<(usize, String)>,
    formula: Formula,
    sum: bool,
    schema: DFSchemaRef,
}

#[derive(Serialize, Deserialize)]
struct WireFragment {
    version: u8,
    range: Vec<u8>,
    labels: Vec<(usize, String)>,
    formula: Formula,
    sum: bool,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

pub(crate) fn bare(expr: &Expr) -> &Expr {
    if let Expr::Alias(alias) = expr {
        bare(&alias.expr)
    } else {
        expr
    }
}

pub(crate) fn column_index(expr: &Expr, plan: &LogicalPlan) -> Option<usize> {
    if let Expr::Column(column) = bare(expr) {
        plan.schema().index_of_column(column).ok()
    } else {
        None
    }
}

pub(crate) fn peel_projections<'a>(
    mut plan: &'a LogicalPlan,
    expressions: &mut [Expr],
) -> Result<&'a LogicalPlan> {
    while let LogicalPlan::Projection(projection) = plan {
        for expression in expressions.iter_mut() {
            *expression = expression
                .clone()
                .transform_up(|expr| {
                    Ok(if let Expr::Column(column) = &expr {
                        let index = projection.schema.index_of_column(column)?;
                        Transformed::yes(bare(&projection.expr[index]).clone())
                    } else {
                        Transformed::no(expr)
                    })
                })?
                .data;
        }
        plan = projection.input.as_ref();
    }
    Ok(plan)
}

impl SeriesFragment {
    pub(crate) fn shift_time(&self, delta: i64) -> Result<Self> {
        let mut rebound = self.clone();
        rebound.range = self.range.shift_time(delta)?;
        Ok(rebound)
    }

    pub(crate) const NAME: &str = "ResearchSeriesFragment";

    pub(crate) fn recognize(
        plan: &LogicalPlan,
        tags: &[String],
        time: &str,
        value: &str,
    ) -> Result<Option<Self>> {
        let mut expressions = tags
            .iter()
            .map(String::as_str)
            .chain([time, value])
            .map(|name| {
                let index = plan
                    .schema()
                    .index_of_column_by_name(None, name)
                    .ok_or_else(|| invalid("Missing fragment output"))?;
                let (qualifier, field) = plan.schema().qualified_field(index);
                Ok(Expr::Column(Column::new(qualifier.cloned(), field.name())))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut input = peel_projections(plan, &mut expressions)?;
        let sum = if let LogicalPlan::Aggregate(aggregate) = input {
            if aggregate.aggr_expr.len() != 1 || aggregate.group_expr.len() != tags.len() + 1 {
                return Ok(None);
            }
            let Some(value_index) = column_index(&expressions[tags.len() + 1], input) else {
                return Ok(None);
            };
            if value_index != aggregate.group_expr.len() {
                return Ok(None);
            }
            let Expr::AggregateFunction(function) = bare(&aggregate.aggr_expr[0]) else {
                return Ok(None);
            };
            if function.func.name() != "sum"
                || function.params.distinct
                || function.params.filter.is_some()
                || !function.params.order_by.is_empty()
                || function.params.args.len() != 1
            {
                return Ok(None);
            }
            let mut used = Vec::new();
            for expression in &mut expressions[..tags.len() + 1] {
                let Some(index) = column_index(expression, input) else {
                    return Ok(None);
                };
                let Some(group) = aggregate.group_expr.get(index) else {
                    return Ok(None);
                };
                used.push(index);
                *expression = bare(group).clone();
            }
            used.sort_unstable();
            used.dedup();
            if used.len() != aggregate.group_expr.len() {
                return Ok(None);
            }
            expressions[tags.len() + 1] = function.params.args[0].clone();
            input = peel_projections(aggregate.input.as_ref(), &mut expressions)?;
            true
        } else {
            false
        };
        let LogicalPlan::Extension(extension) = input else {
            return Ok(None);
        };
        let Some(range) = extension.node.as_any().downcast_ref::<SeriesRange>() else {
            return Ok(None);
        };
        let spec = range.specification(Arc::new(range.inputs()[0].schema().as_arrow().clone()))?;
        if sum && spec.function == super::scan_range::RangeFunction::Instant {
            // Raw packed selectors omit null points. Aggregates must retain
            // the distinction between a present null and an absent series.
            return Ok(None);
        }
        if !sum && !super::dense_budget::admits(spec.grid.instants.len()) {
            // Without a partial sum the fragment ships one full grid per source series;
            // over long ranges with many (e.g. churned short-lived) series those exceed
            // the query pool. Leave such inputs to row streaming.
            return Ok(None);
        }
        let Some(value_index) = spec
            .output
            .iter()
            .position(|output| matches!(output, RangeOutput::Value))
        else {
            return Ok(None);
        };
        let Some(time_index) = column_index(&expressions[tags.len()], input) else {
            return Ok(None);
        };
        if !matches!(spec.output.get(time_index), Some(RangeOutput::Timestamp)) {
            return Ok(None);
        }
        let Some(formula) =
            Formula::recognize(&expressions[tags.len() + 1], range.schema(), value_index)
        else {
            return Ok(None);
        };
        if !formula.contains_rate() {
            return Ok(None);
        }
        let mut labels = Vec::new();
        for (expression, name) in expressions[..tags.len()].iter().zip(tags) {
            let Some(index) = column_index(expression, input) else {
                return Ok(None);
            };
            if !matches!(spec.output.get(index), Some(RangeOutput::Tag(_))) {
                return Ok(None);
            }
            labels.push((index, name.clone()));
        }
        Self::new(range.clone(), labels, formula, sum).map(Some)
    }

    fn new(
        range: SeriesRange,
        labels: Vec<(usize, String)>,
        formula: Formula,
        sum: bool,
    ) -> Result<Self> {
        if !formula.contains_rate() {
            return Err(invalid("Fragment must depend on its window result"));
        }
        let mut fields = Vec::new();
        for (index, name) in &labels {
            let Some(field) = range.schema().fields().get(*index) else {
                return Err(invalid("Invalid fragment label index"));
            };
            let datatype = match field.data_type() {
                DataType::Dictionary(_, value) => value.as_ref(),
                datatype => datatype,
            };
            if name == PACKED_POINTS || !datatype.is_string() {
                return Err(invalid("Invalid fragment label mapping"));
            }
            fields.push((None, Arc::new(field.as_ref().clone().with_name(name))));
        }
        fields.push((
            None,
            Arc::new(Field::new(
                PACKED_POINTS,
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            )),
        ));
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            range.schema().as_ref().metadata().clone(),
        )?);
        Ok(Self {
            range,
            labels,
            formula,
            sum,
            schema,
        })
    }

    pub(crate) fn partial_sum(&self) -> bool {
        self.sum
    }

    pub(crate) fn plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&WireFragment {
            version: 1,
            range: self.range.serialize()?,
            labels: self.labels.clone(),
            formula: self.formula.clone(),
            sum: self.sum,
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        let wire: WireFragment =
            serde_json::from_slice(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
        if wire.version != 1 {
            return Err(invalid("Unknown series fragment version"));
        }
        Ok(Self {
            range: SeriesRange::deserialize(&wire.range)?,
            labels: wire.labels,
            formula: wire.formula,
            sum: wire.sum,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    pub(crate) fn specification(&self, input_schema: SchemaRef) -> Result<RangeSpec> {
        let mut spec = self.range.specification(input_schema)?;
        let labels = self
            .labels
            .iter()
            .map(|(index, _)| {
                spec.output[..*index]
                    .iter()
                    .filter(|output| matches!(output, RangeOutput::Tag(_)))
                    .count()
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(self.schema.as_arrow().clone()))
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let expression = self.formula.physical()?;
        if self.sum {
            let function = AggregateExprBuilder::new(
                datafusion::functions_aggregate::sum::sum_udaf(),
                vec![Arc::new(PhysicalColumn::new("rate", 0))],
            )
            .schema(Arc::new(Schema::new(vec![Field::new(
                "rate",
                DataType::Float64,
                true,
            )])))
            .alias("sum")
            .build()?;
            spec.aggregate = Some(Arc::new(RangeAggregateSpec {
                schema: schema.clone(),
                groups: labels
                    .iter()
                    .copied()
                    .map(Some)
                    .chain(std::iter::once(None))
                    .collect(),
                expression: expression.clone(),
                function: Arc::new(function),
            }));
        }
        spec.packed = Some(Arc::new(PackedRangeSpec {
            batch_output: true,
            schema,
            labels,
            expression,
        }));
        common_telemetry::debug!(
            partial_sum = self.sum,
            "Research native series fragment planned"
        );
        Ok(spec)
    }

    pub(crate) fn physical(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let spec = self.specification(input.schema())?;
        let offset = spec.offset;
        Ok(Arc::new(SeriesRangeExec::new(
            input,
            Arc::new(spec),
            offset,
        )))
    }
}

impl PartialOrd for SeriesFragment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.range, &self.labels, &self.formula, self.sum).partial_cmp(&(
            &other.range,
            &other.labels,
            &other.formula,
            other.sum,
        ))
    }
}

impl UserDefinedLogicalNodeCore for SeriesFragment {
    fn name(&self) -> &str {
        Self::NAME
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.range.inputs()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        self.range.expressions()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: rate, partial_sum={}, shared_grid=true",
            Self::NAME,
            self.sum
        )
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let range = self.range.with_exprs_and_inputs(expressions, inputs)?;
        if range.schema() == self.range.schema() {
            let mut node = self.clone();
            node.range = range;
            return Ok(node);
        }
        Self::new(range, self.labels.clone(), self.formula.clone(), self.sum)
    }
}
