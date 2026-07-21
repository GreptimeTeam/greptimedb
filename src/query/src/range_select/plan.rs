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
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use ahash::RandomState;
use arrow::compute::{self, CastOptions, cast_with_options, take_arrays};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
#[cfg(test)]
use common_function::aggrs::aggr_wrapper::StateMergeHelper;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::common::Result as DataFusionResult;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
#[cfg(test)]
use datafusion::functions_aggregate::{
    average::Avg,
    count::Count,
    min_max::{Max, Min},
    sum::Sum,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
#[cfg(test)]
use datafusion_common::Column;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{
    Accumulator, Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore, lit,
};
#[cfg(test)]
use datafusion_expr::{Aggregate, Extension, Projection};
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalSortExpr,
    create_physical_expr, create_physical_sort_expr,
};
use datatypes::arrow::array::{
    Array, ArrayRef, StructArray, TimestampMillisecondArray, TimestampMillisecondBuilder,
    UInt32Builder,
};
use datatypes::arrow::datatypes::{ArrowPrimitiveType, TimestampMillisecondType};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::arrow::row::{OwnedRow, RowConverter, SortField};
use futures::{Stream, ready};
use futures_util::StreamExt;
use snafu::ensure;

use crate::error::{RangeQuerySnafu, Result};

type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum Fill {
    Null,
    Prev,
    Linear,
    Const(ScalarValue),
}

impl Display for Fill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fill::Null => write!(f, "NULL"),
            Fill::Prev => write!(f, "PREV"),
            Fill::Linear => write!(f, "LINEAR"),
            Fill::Const(x) => write!(f, "{}", x),
        }
    }
}

impl Fill {
    pub fn try_from_str(value: &str, datatype: &DataType) -> DfResult<Option<Self>> {
        let s = value.to_uppercase();
        match s.as_str() {
            "" => Ok(None),
            "NULL" => Ok(Some(Self::Null)),
            "PREV" => Ok(Some(Self::Prev)),
            "LINEAR" => {
                if datatype.is_numeric() {
                    Ok(Some(Self::Linear))
                } else {
                    Err(DataFusionError::Plan(format!(
                        "Use FILL LINEAR on Non-numeric DataType {}",
                        datatype
                    )))
                }
            }
            _ => ScalarValue::try_from_string(s.clone(), datatype)
                .map_err(|err| {
                    DataFusionError::Plan(format!(
                        "{} is not a valid fill option, fail to convert to a const value. {{ {} }}",
                        s, err
                    ))
                })
                .map(|x| Some(Fill::Const(x))),
        }
    }

    /// The input `data` contains data on a complete time series.
    /// If the filling strategy is `PREV` or `LINEAR`, caller must be ensured that the incoming `ts`&`data` is ascending time order.
    pub fn apply_fill_strategy(&self, ts: &[i64], data: &mut [ScalarValue]) -> DfResult<()> {
        // No calculation need in `Fill::Null`
        if matches!(self, Fill::Null) {
            return Ok(());
        }
        let len = data.len();
        if *self == Fill::Linear {
            return Self::fill_linear(ts, data);
        }
        for i in 0..len {
            if data[i].is_null() {
                match self {
                    Fill::Prev => {
                        if i != 0 {
                            data[i] = data[i - 1].clone()
                        }
                    }
                    // The calculation of linear interpolation is relatively complicated.
                    // `Self::fill_linear` is used to dispose `Fill::Linear`.
                    // No calculation need in `Fill::Null`
                    Fill::Linear | Fill::Null => unreachable!(),
                    Fill::Const(v) => data[i] = v.clone(),
                }
            }
        }
        Ok(())
    }

    fn fill_linear(ts: &[i64], data: &mut [ScalarValue]) -> DfResult<()> {
        let not_null_num = data
            .iter()
            .fold(0, |acc, x| if x.is_null() { acc } else { acc + 1 });
        // We need at least two non-empty data points to perform linear interpolation
        if not_null_num < 2 {
            return Ok(());
        }
        let mut index = 0;
        let mut head: Option<usize> = None;
        let mut tail: Option<usize> = None;
        while index < data.len() {
            // find null interval [start, end)
            // start is null, end is not-null
            let start = data[index..]
                .iter()
                .position(ScalarValue::is_null)
                .unwrap_or(data.len() - index)
                + index;
            if start == data.len() {
                break;
            }
            let end = data[start..]
                .iter()
                .position(|r| !r.is_null())
                .unwrap_or(data.len() - start)
                + start;
            index = end + 1;
            // head or tail null dispose later, record start/end first
            if start == 0 {
                head = Some(end);
            } else if end == data.len() {
                tail = Some(start);
            } else {
                linear_interpolation(ts, data, start - 1, end, start, end)?;
            }
        }
        // dispose head null interval
        if let Some(end) = head {
            linear_interpolation(ts, data, end, end + 1, 0, end)?;
        }
        // dispose tail null interval
        if let Some(start) = tail {
            linear_interpolation(ts, data, start - 2, start - 1, start, data.len())?;
        }
        Ok(())
    }
}

/// use `(ts[i1], data[i1])`, `(ts[i2], data[i2])` as endpoint, linearly interpolates element over the interval `[start, end)`
fn linear_interpolation(
    ts: &[i64],
    data: &mut [ScalarValue],
    i1: usize,
    i2: usize,
    start: usize,
    end: usize,
) -> DfResult<()> {
    let (x0, x1) = (ts[i1] as f64, ts[i2] as f64);
    let (y0, y1, is_float32) = match (&data[i1], &data[i2]) {
        (ScalarValue::Float64(Some(y0)), ScalarValue::Float64(Some(y1))) => (*y0, *y1, false),
        (ScalarValue::Float32(Some(y0)), ScalarValue::Float32(Some(y1))) => {
            (*y0 as f64, *y1 as f64, true)
        }
        _ => {
            return Err(DataFusionError::Execution(
                "RangePlan: Apply Fill LINEAR strategy on Non-floating type".to_string(),
            ));
        }
    };
    // To avoid divide zero error, kind of defensive programming
    if x1 == x0 {
        return Err(DataFusionError::Execution(
            "RangePlan: Linear interpolation using the same coordinate points".to_string(),
        ));
    }
    for i in start..end {
        let val = y0 + (y1 - y0) / (x1 - x0) * (ts[i] as f64 - x0);
        data[i] = if is_float32 {
            ScalarValue::Float32(Some(val as f32))
        } else {
            ScalarValue::Float64(Some(val))
        }
    }
    Ok(())
}

#[derive(Eq, Clone, Debug)]
pub struct RangeFn {
    /// with format like `max(a) RANGE 300s [FILL NULL]`
    pub name: String,
    pub data_type: DataType,
    pub expr: Expr,
    pub range: Duration,
    pub fill: Option<Fill>,
    /// If the `FIll` strategy is `Linear` and the output is an integer,
    /// it is possible to calculate a floating point number.
    /// So for `FILL==LINEAR`, the entire data will be implicitly converted to Float type
    /// If `need_cast==true`, `data_type` may not consist with type `expr` generated.
    pub need_cast: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
enum RangeSelectMode {
    #[default]
    Complete,
    Partial(PartialRangeSelectSpec),
    Final(FinalRangeSelectSpec),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CanonicalRangeAggregate {
    Min,
    Max,
    Sum,
    Count,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PartialRangeSelectSpec {
    state_columns: Vec<String>,
    state_types: Vec<DataType>,
    by_fields: Vec<FieldContract>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FinalRangeSelectSpec {
    state_columns: Vec<String>,
    bucket_column: String,
    bucket_field: FieldContract,
    state_types: Vec<DataType>,
    by_input_fields: Vec<FieldContract>,
    raw_merge_result_fields: Vec<FieldContract>,
    legacy_range_fields: Vec<FieldContract>,
    legacy_time_field: FieldContract,
    legacy_by_fields: Vec<FieldContract>,
    legacy_metadata: BTreeMap<String, String>,
    schema_project: Option<Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FieldContract {
    qualifier: Option<datafusion_common::TableReference>,
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: BTreeMap<String, String>,
}

impl FieldContract {
    fn from_qualified_field(
        qualifier: Option<&datafusion_common::TableReference>,
        field: &Field,
    ) -> Self {
        Self {
            qualifier: qualifier.cloned(),
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            metadata: field.metadata().clone().into_iter().collect(),
        }
    }

    #[cfg(test)]
    fn from_schema(schema: &DFSchema, index: usize) -> Self {
        let (qualifier, field) = schema.qualified_field(index);
        Self::from_qualified_field(qualifier, field)
    }

    fn to_qualified_field(&self) -> (Option<datafusion_common::TableReference>, Arc<Field>) {
        (
            self.qualifier.clone(),
            Arc::new(
                Field::new(self.name.clone(), self.data_type.clone(), self.nullable)
                    .with_metadata(self.metadata.clone().into_iter().collect()),
            ),
        )
    }

    fn matches(
        &self,
        qualifier: Option<&datafusion_common::TableReference>,
        field: &Field,
    ) -> bool {
        self == &Self::from_qualified_field(qualifier, field)
    }
}

impl PartialEq for RangeFn {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for RangeFn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RangeFn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl std::hash::Hash for RangeFn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Display for RangeFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RangeSelect {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// all range expressions
    pub range_expr: Vec<RangeFn>,
    pub align: Duration,
    pub align_to: i64,
    pub time_index: String,
    pub time_expr: Expr,
    pub by: Vec<Expr>,
    pub schema: DFSchemaRef,
    pub by_schema: DFSchemaRef,
    /// If the `schema` of the `RangeSelect` happens to be the same as the content of the upper-level Projection Plan,
    /// the final output needs to be `project` through `schema_project`,
    /// so that we can omit the upper-level Projection Plan.
    pub schema_project: Option<Vec<usize>>,
    /// The schema before run projection, follow the order of `range expr | time index | by columns`
    /// `schema_before_project  ----  schema_project ----> schema`
    /// if `schema_project==None` then `schema_before_project==schema`
    pub schema_before_project: DFSchemaRef,
    mode: RangeSelectMode,
}

impl PartialOrd for RangeSelect {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Compare fields in order excluding `schema`, `by_schema`, `schema_before_project`.
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.range_expr.partial_cmp(&other.range_expr) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.align.partial_cmp(&other.align) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.align_to.partial_cmp(&other.align_to) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_index.partial_cmp(&other.time_index) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_expr.partial_cmp(&other.time_expr) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.by.partial_cmp(&other.by) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.schema_project.partial_cmp(&other.schema_project) {
            Some(Ordering::Equal) => self.mode.partial_cmp(&other.mode),
            other => other,
        }
    }
}

impl RangeSelect {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        range_expr: Vec<RangeFn>,
        align: Duration,
        align_to: i64,
        time_index: Expr,
        by: Vec<Expr>,
        projection_expr: &[Expr],
    ) -> Result<Self> {
        ensure!(
            align.as_millis() != 0,
            RangeQuerySnafu {
                msg: "Can't use 0 as align in Range Query"
            }
        );
        for expr in &range_expr {
            ensure!(
                expr.range.as_millis() != 0,
                RangeQuerySnafu {
                    msg: format!(
                        "Invalid Range expr `{}`, Can't use 0 as range in Range Query",
                        expr.name
                    )
                }
            );
        }
        let mut fields = range_expr
            .iter()
            .map(
                |RangeFn {
                     name,
                     data_type,
                     fill,
                     ..
                 }| {
                    let field = Field::new(
                        name,
                        data_type.clone(),
                        // Only when data fill with Const option, the data can't be null
                        !matches!(fill, Some(Fill::Const(..))),
                    );
                    Ok((None, Arc::new(field)))
                },
            )
            .collect::<DfResult<Vec<_>>>()?;
        // add align_ts
        let ts_field = time_index.to_field(input.schema().as_ref())?;
        let time_index_name = ts_field.1.name().clone();
        fields.push(ts_field);
        // add by
        let by_fields = range_by_fields(&by, &input)?;
        fields.extend(by_fields.clone());
        let schema_before_project = Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        )?);
        let by_schema = Arc::new(DFSchema::new_with_metadata(
            by_fields,
            input.schema().metadata().clone(),
        )?);
        // If the results of project plan can be obtained directly from range plan without any additional
        // calculations, no project plan is required. We can simply project the final output of the range
        // plan to produce the final result.
        let schema_project = projection_expr
            .iter()
            .map(|project_expr| {
                if let Expr::Column(column) = project_expr {
                    schema_before_project
                        .index_of_column_by_name(column.relation.as_ref(), &column.name)
                        .ok_or(())
                } else {
                    let (qualifier, field) = project_expr
                        .to_field(input.schema().as_ref())
                        .map_err(|_| ())?;
                    schema_before_project
                        .index_of_column_by_name(qualifier.as_ref(), field.name())
                        .ok_or(())
                }
            })
            .collect::<std::result::Result<Vec<usize>, ()>>()
            .ok();
        let schema = if let Some(project) = &schema_project {
            let project_field = project
                .iter()
                .map(|i| {
                    let f = schema_before_project.qualified_field(*i);
                    (f.0.cloned(), f.1.clone())
                })
                .collect();
            Arc::new(DFSchema::new_with_metadata(
                project_field,
                input.schema().metadata().clone(),
            )?)
        } else {
            schema_before_project.clone()
        };
        Ok(Self {
            input,
            range_expr,
            align,
            align_to,
            time_index: time_index_name,
            time_expr: time_index,
            schema,
            by_schema,
            by,
            schema_project,
            schema_before_project,
            mode: RangeSelectMode::Complete,
        })
    }

    /// Builds query-local Complete -> Partial -> Final stages for unit tests.
    /// Production lowering and wire representation intentionally live in later
    /// changes, so this must not become a planning API yet.
    #[cfg(test)]
    fn split_for_local_test(&self) -> Option<(Self, Self)> {
        if !matches!(self.mode, RangeSelectMode::Complete)
            || !matches!(self.time_expr, Expr::Column(_))
            || self.by.iter().any(|expr| !matches!(expr, Expr::Column(_)))
        {
            return None;
        }
        let aggregate = StateMergeHelper::coerce_aggr_node(
            Aggregate::try_new(
                self.input.clone(),
                vec![],
                self.range_expr
                    .iter()
                    .map(|range| range.expr.clone())
                    .collect(),
            )
            .ok()?,
        )
        .ok()?;
        let mut projection_expr = vec![self.time_expr.clone()];
        projection_expr.extend(self.by.clone());
        let mut partial_ranges = Vec::with_capacity(self.range_expr.len());
        let mut final_ranges = Vec::with_capacity(self.range_expr.len());
        let mut state_columns = Vec::with_capacity(self.range_expr.len());
        let mut state_types = Vec::with_capacity(self.range_expr.len());
        let mut state_fields = Vec::with_capacity(self.range_expr.len() + self.by.len() + 1);
        let mut raw_merge_result_fields = Vec::with_capacity(self.range_expr.len());

        for (index, (range, expr)) in self.range_expr.iter().zip(&aggregate.aggr_expr).enumerate() {
            let Expr::AggregateFunction(aggr) = strip_alias(expr) else {
                return None;
            };
            let [argument] = aggr.params.args.as_slice() else {
                return None;
            };
            canonical_range_aggregate(aggr)?;
            let argument_name = format!("__range_arg_{index}");
            if aggregate
                .input
                .schema()
                .index_of_column_by_name(None, &argument_name)
                .is_some()
            {
                return None;
            }
            let contract =
                StateMergeHelper::split_coerced_aggregate_expr(expr, aggregate.input.as_ref())
                    .ok()?;
            projection_expr.push(argument.clone().alias(argument_name));
            let state_column = format!("__range_state_{index}");
            let final_result_field = contract.final_result_field.clone();
            let final_expr = contract.merge_expr(Column::new_unqualified(state_column.clone()));
            let state_expr = materialize_state_argument(contract.partial_state_expr, index)?
                .alias(state_column.clone());
            let materialized_schema = Arc::new(
                DFSchema::new_with_metadata(
                    projection_expr
                        .iter()
                        .map(|expr| expr.to_field(aggregate.input.schema()).ok())
                        .collect::<Option<Vec<_>>>()?,
                    aggregate.input.schema().metadata().clone(),
                )
                .ok()?,
            );
            let (_, state_field) = state_expr.to_field(materialized_schema.as_ref()).ok()?;
            if !matches!(state_field.data_type(), DataType::Struct(_)) {
                return None;
            }
            let state_type = state_field.data_type().clone();
            let mut output_field = state_field.as_ref().clone();
            output_field.set_nullable(false);
            state_fields.push((None, Arc::new(output_field)));
            state_columns.push(state_column.clone());
            state_types.push(state_type.clone());
            partial_ranges.push(RangeFn {
                name: state_column.clone(),
                data_type: state_type,
                expr: state_expr,
                range: range.range,
                fill: None,
                need_cast: false,
            });
            let Expr::Alias(alias) = final_expr else {
                return None;
            };
            let final_expr = alias.expr.as_ref().clone().alias(range.name.clone());
            raw_merge_result_fields.push(FieldContract::from_qualified_field(
                None,
                &Field::new(
                    range.name.clone(),
                    final_result_field.data_type().clone(),
                    final_result_field.is_nullable(),
                )
                .with_metadata(final_result_field.metadata().clone()),
            ));
            final_ranges.push(RangeFn {
                expr: final_expr,
                ..range.clone()
            });
        }
        let projection = Arc::new(LogicalPlan::Projection(
            Projection::try_new(projection_expr, aggregate.input).ok()?,
        ));
        let time_expr = column_expr(projection.schema(), 0);
        let by = (0..self.by.len())
            .map(|index| column_expr(projection.schema(), index + 1))
            .collect::<Vec<_>>();
        let (_, time_field) = time_expr.to_field(projection.schema()).ok()?;
        state_fields.push((
            None,
            Arc::new(Field::new(
                "__range_bucket_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                time_field.is_nullable(),
            )),
        ));
        let by_fields = (0..self.by_schema.fields().len())
            .map(|index| {
                let (qualifier, field) = self.by_schema.qualified_field(index);
                (qualifier.cloned(), field.clone())
            })
            .collect::<Vec<_>>();
        state_fields.extend(by_fields.clone());
        let metadata = projection.schema().metadata().clone();
        let partial_schema =
            Arc::new(DFSchema::new_with_metadata(state_fields, metadata.clone()).ok()?);
        let partial_by_schema = Arc::new(DFSchema::new_with_metadata(by_fields, metadata).ok()?);
        let partial = Self {
            input: projection,
            range_expr: partial_ranges,
            align: self.align,
            align_to: self.align_to,
            time_index: time_field.name().clone(),
            time_expr,
            by,
            schema: partial_schema.clone(),
            by_schema: partial_by_schema.clone(),
            schema_project: None,
            schema_before_project: partial_schema,
            mode: RangeSelectMode::Partial(PartialRangeSelectSpec {
                state_columns: state_columns.clone(),
                state_types: state_types.clone(),
                by_fields: (0..partial_by_schema.fields().len())
                    .map(|index| FieldContract::from_schema(&partial_by_schema, index))
                    .collect(),
            }),
        };
        let partial = partial
            .with_exprs_and_inputs(partial.expressions(), vec![partial.input.as_ref().clone()])
            .ok()?;
        let partial_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(partial.clone()),
        });
        let final_by = partial
            .by_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let (qualifier, field) = partial.by_schema.qualified_field(index);
                Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect();
        let final_node = Self {
            input: Arc::new(partial_plan),
            range_expr: final_ranges,
            align: self.align,
            align_to: self.align_to,
            time_index: "__range_bucket_ms".to_string(),
            time_expr: Expr::Column(Column::new_unqualified("__range_bucket_ms")),
            by: final_by,
            schema: self.schema.clone(),
            by_schema: self.by_schema.clone(),
            schema_project: self.schema_project.clone(),
            schema_before_project: self.schema_before_project.clone(),
            mode: RangeSelectMode::Final(FinalRangeSelectSpec {
                state_columns,
                bucket_column: "__range_bucket_ms".to_string(),
                bucket_field: FieldContract::from_schema(&partial.schema, self.range_expr.len()),
                state_types,
                by_input_fields: (0..partial.by_schema.fields().len())
                    .map(|index| FieldContract::from_schema(&partial.by_schema, index))
                    .collect(),
                raw_merge_result_fields,
                legacy_range_fields: (0..self.range_expr.len())
                    .map(|index| FieldContract::from_schema(&self.schema_before_project, index))
                    .collect(),
                legacy_time_field: FieldContract::from_schema(
                    &self.schema_before_project,
                    self.range_expr.len(),
                ),
                legacy_by_fields: (0..self.by_schema.fields().len())
                    .map(|index| FieldContract::from_schema(&self.by_schema, index))
                    .collect(),
                legacy_metadata: self
                    .schema_before_project
                    .metadata()
                    .clone()
                    .into_iter()
                    .collect(),
                schema_project: self.schema_project.clone(),
            }),
        };
        let final_node = final_node
            .with_exprs_and_inputs(
                final_node.expressions(),
                vec![final_node.input.as_ref().clone()],
            )
            .ok()?;
        Some((partial, final_node))
    }
}

#[cfg(test)]
fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(&alias.expr),
        expr => expr,
    }
}

#[cfg(test)]
fn canonical_range_aggregate(
    aggregate: &datafusion_expr::expr::AggregateFunction,
) -> Option<CanonicalRangeAggregate> {
    if aggregate.params.distinct
        || aggregate.params.filter.is_some()
        || !aggregate.params.order_by.is_empty()
        || aggregate.params.null_treatment.is_some()
    {
        return None;
    }
    let kind = if aggregate.func.inner().as_any().is::<Min>() {
        CanonicalRangeAggregate::Min
    } else if aggregate.func.inner().as_any().is::<Max>() {
        CanonicalRangeAggregate::Max
    } else if aggregate.func.inner().as_any().is::<Sum>() {
        CanonicalRangeAggregate::Sum
    } else if aggregate.func.inner().as_any().is::<Count>() {
        CanonicalRangeAggregate::Count
    } else if aggregate.func.inner().as_any().is::<Avg>() {
        CanonicalRangeAggregate::Avg
    } else {
        return None;
    };
    if kind == CanonicalRangeAggregate::Count
        && !matches!(aggregate.params.args.as_slice(), [Expr::Column(_)])
    {
        return None;
    }
    Some(kind)
}

#[cfg(test)]
fn column_expr(schema: &DFSchemaRef, index: usize) -> Expr {
    let (qualifier, field) = schema.qualified_field(index);
    Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
}

#[cfg(test)]
fn materialize_state_argument(expr: Expr, index: usize) -> Option<Expr> {
    let Expr::AggregateFunction(mut aggregate) = expr else {
        return None;
    };
    if aggregate.params.args.len() != 1 {
        return None;
    }
    aggregate.params.args = vec![Expr::Column(Column::new_unqualified(format!(
        "__range_arg_{index}"
    )))];
    Some(Expr::AggregateFunction(aggregate))
}

fn range_by_fields(
    by: &[Expr],
    input: &LogicalPlan,
) -> DfResult<Vec<(Option<datafusion_common::TableReference>, Arc<Field>)>> {
    by.iter()
        .map(|expr| match expr {
            Expr::Column(column) => {
                let index = input
                    .schema()
                    .index_of_column_by_name(column.relation.as_ref(), &column.name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "RangeSelect BY column {} not found",
                            column.flat_name()
                        ))
                    })?;
                let (qualifier, field) = input.schema().qualified_field(index);
                Ok((qualifier.cloned(), field.clone()))
            }
            _ => expr.to_field(input.schema()),
        })
        .collect()
}

impl UserDefinedLogicalNodeCore for RangeSelect {
    fn name(&self) -> &str {
        "RangeSelect"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.range_expr
            .iter()
            .map(|expr| expr.expr.clone())
            .chain([self.time_expr.clone()])
            .chain(self.by.clone())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeSelect: range_exprs=[{}], align={}ms, align_to={}ms, align_by=[{}], time_index={}",
            self.range_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.align.as_millis(),
            self.align_to,
            self.by
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.time_index
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "RangeSelect expects exactly one input, got {}",
                inputs.len()
            )));
        }
        if exprs.len() != self.range_expr.len() + self.by.len() + 1 {
            return Err(DataFusionError::Plan(
                "RangeSelect: exprs length not match".to_string(),
            ));
        }

        let range_expr = exprs
            .iter()
            .zip(self.range_expr.iter())
            .map(|(e, range)| RangeFn {
                name: range.name.clone(),
                data_type: range.data_type.clone(),
                expr: e.clone(),
                range: range.range,
                fill: range.fill.clone(),
                need_cast: range.need_cast,
            })
            .collect();
        let time_expr = exprs[self.range_expr.len()].clone();
        let by = exprs[self.range_expr.len() + 1..].to_vec();
        let input = Arc::new(inputs.into_iter().next().unwrap());
        match &self.mode {
            RangeSelectMode::Complete => Ok(Self {
                align: self.align,
                align_to: self.align_to,
                range_expr,
                input,
                time_index: self.time_index.clone(),
                time_expr,
                schema: self.schema.clone(),
                by,
                by_schema: self.by_schema.clone(),
                schema_project: self.schema_project.clone(),
                schema_before_project: self.schema_before_project.clone(),
                mode: RangeSelectMode::Complete,
            }),
            RangeSelectMode::Partial(spec) => {
                rebuild_partial(self, input, range_expr, time_expr, by, spec)
            }
            RangeSelectMode::Final(spec) => {
                rebuild_final(self, input, range_expr, time_expr, by, spec)
            }
        }
    }
}

fn rebuild_partial(
    node: &RangeSelect,
    input: Arc<LogicalPlan>,
    range_expr: Vec<RangeFn>,
    time_expr: Expr,
    by: Vec<Expr>,
    spec: &PartialRangeSelectSpec,
) -> DfResult<RangeSelect> {
    if range_expr.len() != spec.state_columns.len()
        || range_expr.len() != spec.state_types.len()
        || by.len() != spec.by_fields.len()
        || !matches!(input.as_ref(), LogicalPlan::Projection(_))
    {
        return Err(DataFusionError::Plan(
            "RangeSelect Partial expressions do not match its contract".into(),
        ));
    }
    let (_, time_field) = time_expr.to_field(input.schema())?;
    let actual_by_fields = range_by_fields(&by, input.as_ref())?;
    if actual_by_fields.len() != spec.by_fields.len() {
        return Err(DataFusionError::Plan(
            "RangeSelect Partial BY expressions do not match its contract".into(),
        ));
    }
    let mut fields = Vec::with_capacity(range_expr.len() + by.len() + 1);
    for (index, (range, (state_column, state_type))) in range_expr
        .iter()
        .zip(spec.state_columns.iter().zip(&spec.state_types))
        .enumerate()
    {
        let Expr::Alias(alias) = &range.expr else {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial state expressions must be aliases".into(),
            ));
        };
        let Expr::AggregateFunction(aggregate) = alias.expr.as_ref() else {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial state aliases must wrap aggregate functions".into(),
            ));
        };
        if !aggregate
            .func
            .inner()
            .as_any()
            .is::<common_function::aggrs::aggr_wrapper::StateWrapper>()
            || aggregate.params.args.len() != 1
            || aggregate.params.filter.is_some()
            || aggregate.params.distinct
            || !aggregate.params.order_by.is_empty()
            || aggregate.params.null_treatment.is_some()
            || !matches!(aggregate.params.args.as_slice(), [Expr::Column(column)] if column.relation.is_none() && column.name == format!("__range_arg_{index}"))
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial state wrapper contract is invalid".into(),
            ));
        }
        let (qualifier, field) = range.expr.to_field(input.schema())?;
        if alias.name != *state_column
            || qualifier.is_some()
            || field.data_type() != state_type
            || !matches!(field.data_type(), DataType::Struct(_))
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial state schema contract is invalid".into(),
            ));
        }
        let mut field = field.as_ref().clone();
        field.set_nullable(false);
        fields.push((None, Arc::new(field)));
    }
    fields.push((
        None,
        Arc::new(Field::new(
            "__range_bucket_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            time_field.is_nullable(),
        )),
    ));
    let by_fields = spec
        .by_fields
        .iter()
        .map(FieldContract::to_qualified_field)
        .collect::<Vec<_>>();
    fields.extend(by_fields.clone());
    let metadata = input.schema().metadata().clone();
    let schema = Arc::new(DFSchema::new_with_metadata(fields, metadata.clone())?);
    let by_schema = Arc::new(DFSchema::new_with_metadata(by_fields, metadata)?);
    Ok(RangeSelect {
        input,
        range_expr,
        align: node.align,
        align_to: node.align_to,
        time_index: time_field.name().clone(),
        time_expr,
        by,
        schema: schema.clone(),
        by_schema,
        schema_project: None,
        schema_before_project: schema,
        mode: RangeSelectMode::Partial(spec.clone()),
    })
}

fn rebuild_final(
    node: &RangeSelect,
    input: Arc<LogicalPlan>,
    range_expr: Vec<RangeFn>,
    time_expr: Expr,
    by: Vec<Expr>,
    spec: &FinalRangeSelectSpec,
) -> DfResult<RangeSelect> {
    if range_expr.len() != spec.state_columns.len()
        || range_expr.len() != spec.state_types.len()
        || range_expr.len() != spec.raw_merge_result_fields.len()
        || by.len() != spec.by_input_fields.len()
    {
        return Err(DataFusionError::Plan(
            "RangeSelect Final expressions do not match its contract".into(),
        ));
    }
    for (state_column, state_type) in spec.state_columns.iter().zip(&spec.state_types) {
        let index = input
            .schema()
            .index_of_column_by_name(None, state_column)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "RangeSelect Final is missing state column {state_column}"
                ))
            })?;
        let (qualifier, field) = input.schema().qualified_field(index);
        if qualifier.is_some() || field.is_nullable() || field.data_type() != state_type {
            return Err(DataFusionError::Plan(
                "RangeSelect Final state schema contract is invalid".into(),
            ));
        }
    }
    let bucket_index = input
        .schema()
        .index_of_column_by_name(None, &spec.bucket_column)
        .ok_or_else(|| {
            DataFusionError::Plan("RangeSelect Final is missing bucket column".into())
        })?;
    let (bucket_qualifier, bucket_field) = input.schema().qualified_field(bucket_index);
    if !spec.bucket_field.matches(bucket_qualifier, bucket_field)
        || bucket_field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
    {
        return Err(DataFusionError::Plan(
            "RangeSelect Final bucket schema contract is invalid".into(),
        ));
    }
    let Expr::Column(bucket) = &time_expr else {
        return Err(DataFusionError::Plan(
            "RangeSelect Final time expression must be the bucket column".into(),
        ));
    };
    if bucket.relation.is_some() || bucket.name != spec.bucket_column {
        return Err(DataFusionError::Plan(
            "RangeSelect Final bucket contract is invalid".into(),
        ));
    }
    for ((range, state_column), output) in range_expr
        .iter()
        .zip(&spec.state_columns)
        .zip(&spec.raw_merge_result_fields)
    {
        let Some(aggregate) = common_function::aggrs::aggr_wrapper::get_aggr_func(&range.expr)
        else {
            return Err(DataFusionError::Plan(
                "RangeSelect Final merge expression must be an aggregate".into(),
            ));
        };
        if !aggregate
            .func
            .inner()
            .as_any()
            .is::<common_function::aggrs::aggr_wrapper::MergeWrapper>()
            || aggregate.params.distinct
            || aggregate.params.filter.is_some()
            || !aggregate.params.order_by.is_empty()
            || aggregate.params.null_treatment.is_some()
            || !matches!(aggregate.params.args.as_slice(), [Expr::Column(column)] if column.relation.is_none() && column.name == *state_column)
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Final merge wrapper contract is invalid".into(),
            ));
        }
        let (qualifier, field) = range.expr.to_field(input.schema())?;
        if !output.matches(qualifier.as_ref(), &field) {
            return Err(DataFusionError::Plan(
                "RangeSelect Final merge output contract is invalid".into(),
            ));
        }
    }
    for (expr, expected) in by.iter().zip(&spec.by_input_fields) {
        let (qualifier, field) = expr.to_field(input.schema())?;
        if !expected.matches(qualifier.as_ref(), &field) {
            return Err(DataFusionError::Plan(
                "RangeSelect Final BY contract is invalid".into(),
            ));
        }
    }
    let fields = spec
        .legacy_range_fields
        .iter()
        .map(FieldContract::to_qualified_field)
        .chain(std::iter::once(spec.legacy_time_field.to_qualified_field()))
        .chain(
            spec.legacy_by_fields
                .iter()
                .map(FieldContract::to_qualified_field),
        )
        .collect();
    let metadata = spec.legacy_metadata.clone().into_iter().collect();
    let schema_before_project = Arc::new(DFSchema::new_with_metadata(fields, metadata)?);
    Ok(RangeSelect {
        input,
        range_expr,
        align: node.align,
        align_to: node.align_to,
        time_index: node.time_index.clone(),
        time_expr,
        by,
        schema: node.schema.clone(),
        by_schema: node.by_schema.clone(),
        schema_project: spec.schema_project.clone(),
        schema_before_project,
        mode: RangeSelectMode::Final(spec.clone()),
    })
}

impl RangeSelect {
    fn create_physical_expr_list(
        &self,
        is_count_aggr: bool,
        exprs: &[Expr],
        df_schema: &Arc<DFSchema>,
        session_state: &SessionState,
    ) -> DfResult<Vec<Arc<dyn PhysicalExpr>>> {
        exprs
            .iter()
            .map(|e| match e {
                // `count(*)` will be rewritten by `CountWildcardRule` into `count(1)` when optimizing logical plan.
                // The modification occurs after range plan rewrite.
                // At this time, aggregate plan has been replaced by a custom range plan,
                // so `CountWildcardRule` has not been applied.
                // We manually modify it when creating the physical plan.
                #[expect(deprecated)]
                Expr::Wildcard { .. } if is_count_aggr => create_physical_expr(
                    &lit(COUNT_STAR_EXPANSION),
                    df_schema.as_ref(),
                    session_state.execution_props(),
                ),
                _ => create_physical_expr(e, df_schema.as_ref(), session_state.execution_props()),
            })
            .collect::<DfResult<Vec<_>>>()
    }

    pub fn to_execution_plan(
        &self,
        logical_input: &LogicalPlan,
        exec_input: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mode = match self.mode {
            RangeSelectMode::Complete => RangeSelectExecMode::Complete,
            RangeSelectMode::Partial(_) => RangeSelectExecMode::Partial,
            RangeSelectMode::Final(_) => RangeSelectExecMode::Final,
        };
        if mode == RangeSelectExecMode::Partial && self.schema_project.is_some() {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial must not apply a schema projection".into(),
            ));
        }
        let fields: Vec<_> = self
            .schema_before_project
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
            .collect();
        let by_fields: Vec<_> = self
            .by_schema
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
            .collect();
        let input_dfschema = logical_input.schema();
        let input_schema = exec_input.schema();
        let range_exec: Vec<RangeFnExec> = self
            .range_expr
            .iter()
            .map(|range_fn| {
                let name = range_fn.expr.schema_name().to_string();
                let range_expr = match &range_fn.expr {
                    Expr::Alias(expr) => expr.expr.as_ref(),
                    others => others,
                };

                let expr = match &range_expr {
                    Expr::AggregateFunction(aggr)
                        if (aggr.func.name() == "last_value"
                            || aggr.func.name() == "first_value") =>
                    {
                        let order_by = if !aggr.params.order_by.is_empty() {
                            aggr.params
                                .order_by
                                .iter()
                                .map(|x| {
                                    create_physical_sort_expr(
                                        x,
                                        input_dfschema.as_ref(),
                                        session_state.execution_props(),
                                    )
                                })
                                .collect::<DfResult<Vec<_>>>()?
                        } else {
                            // if user not assign order by, time index is needed as default ordering
                            let time_index = create_physical_expr(
                                &self.time_expr,
                                input_dfschema.as_ref(),
                                session_state.execution_props(),
                            )?;
                            vec![PhysicalSortExpr {
                                expr: time_index,
                                options: SortOptions {
                                    descending: false,
                                    nulls_first: false,
                                },
                            }]
                        };
                        let arg = self.create_physical_expr_list(
                            false,
                            &aggr.params.args,
                            input_dfschema,
                            session_state,
                        )?;
                        // first_value/last_value has only one param.
                        // The param have been checked by datafusion in logical plan stage.
                        // We can safely assume that there is only one element here.
                        AggregateExprBuilder::new(aggr.func.clone(), arg)
                            .schema(input_schema.clone())
                            .order_by(order_by)
                            .alias(name)
                            .build()
                    }
                    Expr::AggregateFunction(aggr) => {
                        let order_by = if !aggr.params.order_by.is_empty() {
                            aggr.params
                                .order_by
                                .iter()
                                .map(|x| {
                                    create_physical_sort_expr(
                                        x,
                                        input_dfschema.as_ref(),
                                        session_state.execution_props(),
                                    )
                                })
                                .collect::<DfResult<Vec<_>>>()?
                        } else {
                            vec![]
                        };
                        let distinct = aggr.params.distinct;
                        // TODO(discord9): add default null treatment?

                        let input_phy_exprs = self.create_physical_expr_list(
                            aggr.func.name() == "count",
                            &aggr.params.args,
                            input_dfschema,
                            session_state,
                        )?;
                        AggregateExprBuilder::new(aggr.func.clone(), input_phy_exprs)
                            .schema(input_schema.clone())
                            .order_by(order_by)
                            .with_distinct(distinct)
                            .alias(name)
                            .build()
                    }
                    _ => Err(DataFusionError::Plan(format!(
                        "Unexpected Expr: {} in RangeSelect",
                        range_fn.expr
                    ))),
                }?;
                Ok(RangeFnExec {
                    expr: Arc::new(expr),
                    range: range_fn.range.as_millis() as Millisecond,
                    fill: range_fn.fill.clone(),
                    need_cast: if range_fn.need_cast {
                        Some(range_fn.data_type.clone())
                    } else {
                        None
                    },
                })
            })
            .collect::<DfResult<Vec<_>>>()?;
        let schema_before_project = Arc::new(Schema::new(fields));
        let schema = if let Some(project) = &self.schema_project {
            Arc::new(schema_before_project.project(project)?)
        } else {
            schema_before_project.clone()
        };
        let by = self.create_physical_expr_list(false, &self.by, input_dfschema, session_state)?;
        let output_partitioning = match mode {
            RangeSelectExecMode::Partial => Partitioning::UnknownPartitioning(
                exec_input
                    .properties()
                    .output_partitioning()
                    .partition_count(),
            ),
            RangeSelectExecMode::Complete | RangeSelectExecMode::Final => {
                Partitioning::UnknownPartitioning(1)
            }
        };
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            output_partitioning,
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(RangeSelectExec {
            input: exec_input,
            range_exec,
            align: self.align.as_millis() as Millisecond,
            align_to: self.align_to,
            by,
            time_index: self.time_index.clone(),
            schema,
            by_schema: Arc::new(Schema::new(by_fields)),
            metric: ExecutionPlanMetricsSet::new(),
            schema_before_project,
            schema_project: self.schema_project.clone(),
            cache,
            mode,
        }))
    }
}

/// Range function expression.
#[derive(Debug, Clone)]
struct RangeFnExec {
    expr: Arc<AggregateFunctionExpr>,
    range: Millisecond,
    fill: Option<Fill>,
    need_cast: Option<DataType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeSelectExecMode {
    Complete,
    Partial,
    Final,
}

impl RangeFnExec {
    /// Returns the expressions to pass to the aggregator.
    /// It also adds the order by expressions to the list of expressions.
    /// Order-sensitive aggregators, such as `FIRST_VALUE(x ORDER BY y)` requires this.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut exprs = self.expr.expressions();
        exprs.extend(self.expr.order_bys().iter().map(|sort| sort.expr.clone()));
        exprs
    }
}

impl Display for RangeFnExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(fill) = &self.fill {
            write!(
                f,
                "{} RANGE {}s FILL {}",
                self.expr.name(),
                self.range / 1000,
                fill
            )
        } else {
            write!(f, "{} RANGE {}s", self.expr.name(), self.range / 1000)
        }
    }
}

#[derive(Debug)]
pub struct RangeSelectExec {
    input: Arc<dyn ExecutionPlan>,
    range_exec: Vec<RangeFnExec>,
    align: Millisecond,
    align_to: i64,
    time_index: String,
    by: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    by_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,
    schema_project: Option<Vec<usize>>,
    schema_before_project: SchemaRef,
    cache: Arc<PlanProperties>,
    mode: RangeSelectExecMode,
}

impl DisplayAs for RangeSelectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "RangeSelectExec: ")?;
                let range_expr_strs: Vec<String> =
                    self.range_exec.iter().map(RangeFnExec::to_string).collect();
                let by: Vec<String> = self.by.iter().map(|e| e.to_string()).collect();
                write!(
                    f,
                    "range_expr=[{}], align={}ms, align_to={}ms, align_by=[{}], time_index={}",
                    range_expr_strs.join(", "),
                    self.align,
                    self.align_to,
                    by.join(", "),
                    self.time_index,
                )?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for RangeSelectExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            RangeSelectExecMode::Partial => vec![Distribution::UnspecifiedDistribution],
            RangeSelectExecMode::Complete | RangeSelectExecMode::Final => {
                vec![Distribution::SinglePartition]
            }
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "RangeSelectExec expects exactly one child, got {}",
                children.len()
            )));
        }
        let input = children.into_iter().next().unwrap();
        let partitioning = match self.mode {
            RangeSelectExecMode::Partial => Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            RangeSelectExecMode::Complete | RangeSelectExecMode::Final => {
                Partitioning::UnknownPartitioning(1)
            }
        };
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.schema.clone()),
            partitioning,
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            input,
            range_exec: self.range_exec.clone(),
            time_index: self.time_index.clone(),
            by: self.by.clone(),
            align: self.align,
            align_to: self.align_to,
            schema: self.schema.clone(),
            by_schema: self.by_schema.clone(),
            metric: self.metric.clone(),
            schema_before_project: self.schema_before_project.clone(),
            schema_project: self.schema_project.clone(),
            cache,
            mode: self.mode,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let time_index = schema
            .column_with_name(&self.time_index)
            .ok_or(DataFusionError::Execution(
                "time index column not found".into(),
            ))?
            .0;
        let row_converter = RowConverter::new(
            self.by_schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, value_type) => {
                        SortField::new(value_type.as_ref().clone())
                    }
                    data_type => SortField::new(data_type.clone()),
                })
                .collect(),
        )?;
        Ok(Box::pin(RangeSelectStream {
            batch_size,
            schema: self.schema.clone(),
            range_exec: self.range_exec.clone(),
            input,
            random_state: RandomState::new(),
            time_index,
            align: self.align,
            align_to: self.align_to,
            by: self.by.clone(),
            series_map: HashMap::new(),
            exec_state: ExecutionState::ReadingInput,
            num_not_null_rows: 0,
            row_converter,
            modify_map: HashMap::new(),
            metric: baseline_metric,
            schema_project: self.schema_project.clone(),
            schema_before_project: self.schema_before_project.clone(),
            output_batch: None,
            output_batch_offset: 0,
            mode: self.mode,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "RanegSelectExec"
    }
}

struct RangeSelectStream {
    batch_size: usize,
    /// the schema of output column
    schema: SchemaRef,
    range_exec: Vec<RangeFnExec>,
    input: SendableRecordBatchStream,
    /// Column index of TIME INDEX column's position in the input schema
    time_index: usize,
    /// the unit of `align` is millisecond
    align: Millisecond,
    align_to: i64,
    by: Vec<Arc<dyn PhysicalExpr>>,
    exec_state: ExecutionState,
    /// Converter for the by values
    row_converter: RowConverter,
    random_state: RandomState,
    /// key: time series's hash value
    /// value: time series's state on different align_ts
    series_map: HashMap<u64, SeriesState>,
    /// key: `(hash of by rows, align_ts)`
    /// value: `[row_ids]`
    /// It is used to record the data that needs to be aggregated in each time slot during the data update process
    modify_map: HashMap<(u64, Millisecond), Vec<u32>>,
    /// The number of rows of not null rows in the final output
    num_not_null_rows: usize,
    metric: BaselineMetrics,
    schema_project: Option<Vec<usize>>,
    schema_before_project: SchemaRef,
    output_batch: Option<RecordBatch>,
    output_batch_offset: usize,
    mode: RangeSelectExecMode,
}

#[derive(Debug)]
struct SeriesState {
    /// by values written by `RowWriter`
    row: OwnedRow,
    /// key: align_ts
    /// value: a vector, each element is a range_fn follow the order of `range_exec`
    align_ts_accumulator: BTreeMap<Millisecond, Vec<Box<dyn Accumulator>>>,
}

/// Use `align_to` as time origin.
/// According to `align` as time interval, produces aligned time.
/// Combining the parameters related to the range query,
/// determine for each `Accumulator` `(hash, align_ts)` define,
/// which rows of data will be applied to it.
fn produce_align_time(
    align_to: i64,
    range: Millisecond,
    align: Millisecond,
    ts_column: &TimestampMillisecondArray,
    by_columns_hash: &[u64],
    modify_map: &mut HashMap<(u64, Millisecond), Vec<u32>>,
) {
    modify_map.clear();
    // make modify_map for range_fn[i]
    for (row, hash) in by_columns_hash.iter().enumerate() {
        let ts = ts_column.value(row);
        let ith_slot = (ts - align_to).div_floor(align);
        let mut align_ts = ith_slot * align + align_to;
        while align_ts <= ts && ts < align_ts + range {
            modify_map
                .entry((*hash, align_ts))
                .or_default()
                .push(row as u32);
            align_ts -= align;
        }
    }
}

fn cast_scalar_values(values: &mut [ScalarValue], data_type: &DataType) -> DfResult<()> {
    let array = ScalarValue::iter_to_array(values.to_vec())?;
    let cast_array = cast_with_options(&array, data_type, &CastOptions::default())?;
    for (i, value) in values.iter_mut().enumerate() {
        *value = ScalarValue::try_from_array(&cast_array, i)?;
    }
    Ok(())
}

impl RangeSelectStream {
    fn evaluate_many(
        &self,
        batch: &RecordBatch,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> DfResult<Vec<ArrayRef>> {
        exprs
            .iter()
            .map(|expr| {
                let value = expr.evaluate(batch)?;
                value.into_array(batch.num_rows())
            })
            .collect::<DfResult<Vec<_>>>()
    }

    fn normalize_by_arrays(&self, arrays: Vec<ArrayRef>) -> DfResult<Vec<ArrayRef>> {
        arrays
            .into_iter()
            .map(|array| match array.data_type() {
                DataType::Dictionary(_, value_type) => {
                    compute::cast(array.as_ref(), value_type.as_ref())
                        .map_err(DataFusionError::from)
                }
                _ => Ok(array),
            })
            .collect()
    }

    fn update_range_context(&mut self, batch: RecordBatch) -> DfResult<()> {
        let metric = self.metric.clone();
        let _timer = metric.elapsed_compute().timer();
        if self.mode == RangeSelectExecMode::Final {
            return self.merge_partial_context(batch);
        }
        let num_rows = batch.num_rows();
        let by_arrays = self.normalize_by_arrays(self.evaluate_many(&batch, &self.by)?)?;
        let mut hashes = vec![0; num_rows];
        create_hashes(&by_arrays, &self.random_state, &mut hashes)?;
        let by_rows = self.row_converter.convert_columns(&by_arrays)?;
        let mut ts_column = batch.column(self.time_index).clone();
        if !matches!(
            ts_column.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ) {
            ts_column = compute::cast(
                ts_column.as_ref(),
                &DataType::Timestamp(TimeUnit::Millisecond, None),
            )?;
        }
        let ts_column_ref = ts_column
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Time index Column downcast to TimestampMillisecondArray failed".into(),
                )
            })?;
        for i in 0..self.range_exec.len() {
            let args = self.evaluate_many(&batch, &self.range_exec[i].expressions())?;
            // use self.modify_map record (hash, align_ts) => [row_nums]
            produce_align_time(
                self.align_to,
                self.range_exec[i].range,
                self.align,
                ts_column_ref,
                &hashes,
                &mut self.modify_map,
            );
            // build modify_rows/modify_index/offsets for batch update
            let mut modify_rows = UInt32Builder::with_capacity(0);
            // (hash, align_ts, row_num)
            // row_num use to find a by value
            // So we just need to record the row_num of a modify row randomly, because they all have the same by value
            let mut modify_index = Vec::with_capacity(self.modify_map.len());
            let mut offsets = vec![0];
            let mut offset_so_far = 0;
            for ((hash, ts), modify) in &self.modify_map {
                modify_rows.append_slice(modify);
                offset_so_far += modify.len();
                offsets.push(offset_so_far);
                modify_index.push((*hash, *ts, modify[0]));
            }
            let modify_rows = modify_rows.finish();
            let args = take_arrays(&args, &modify_rows, None)?;
            modify_index.iter().zip(offsets.windows(2)).try_for_each(
                |((hash, ts, row), offset)| {
                    let (offset, length) = (offset[0], offset[1] - offset[0]);
                    let sliced_arrays: Vec<ArrayRef> = args
                        .iter()
                        .map(|array| array.slice(offset, length))
                        .collect();
                    let accumulators_map =
                        self.series_map.entry(*hash).or_insert_with(|| SeriesState {
                            row: by_rows.row(*row as usize).owned(),
                            align_ts_accumulator: BTreeMap::new(),
                        });
                    match accumulators_map.align_ts_accumulator.entry(*ts) {
                        Entry::Occupied(mut e) => {
                            let accumulators = e.get_mut();
                            accumulators[i].update_batch(&sliced_arrays)
                        }
                        Entry::Vacant(e) => {
                            self.num_not_null_rows += 1;
                            let mut accumulators = self
                                .range_exec
                                .iter()
                                .map(|range| range.expr.create_accumulator())
                                .collect::<DfResult<Vec<_>>>()?;
                            let result = accumulators[i].update_batch(&sliced_arrays);
                            e.insert(accumulators);
                            result
                        }
                    }
                },
            )?;
        }
        Ok(())
    }

    fn merge_partial_context(&mut self, batch: RecordBatch) -> DfResult<()> {
        let num_rows = batch.num_rows();
        let by_arrays = self.normalize_by_arrays(self.evaluate_many(&batch, &self.by)?)?;
        let state_args = self
            .range_exec
            .iter()
            .map(|range| self.evaluate_many(&batch, &range.expressions()))
            .collect::<DfResult<Vec<_>>>()?;
        let mut hashes = vec![0; num_rows];
        create_hashes(&by_arrays, &self.random_state, &mut hashes)?;
        let by_rows = self.row_converter.convert_columns(&by_arrays)?;
        let mut timestamp = batch.column(self.time_index).clone();
        if !matches!(
            timestamp.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ) {
            timestamp = compute::cast(
                timestamp.as_ref(),
                &DataType::Timestamp(TimeUnit::Millisecond, None),
            )?;
        }
        let timestamp = timestamp
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Time index Column downcast to TimestampMillisecondArray failed".into(),
                )
            })?;
        for (row, hash) in hashes.iter().enumerate().take(num_rows) {
            let bucket = timestamp.value(row);
            let series = self.series_map.entry(*hash).or_insert_with(|| SeriesState {
                row: by_rows.row(row).owned(),
                align_ts_accumulator: BTreeMap::new(),
            });
            let accumulators = match series.align_ts_accumulator.entry(bucket) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    self.num_not_null_rows += 1;
                    entry.insert(
                        self.range_exec
                            .iter()
                            .map(|range| range.expr.create_accumulator())
                            .collect::<DfResult<Vec<_>>>()?,
                    )
                }
            };
            for (index, accumulator) in accumulators.iter_mut().enumerate() {
                let states = &state_args[index];
                if states.len() != 1 {
                    return Err(DataFusionError::Execution(format!(
                        "RangeSelect Final expected one state argument for aggregate {index}"
                    )));
                }
                let state = states[0]
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "RangeSelect Final expected Struct state for aggregate {index}"
                        ))
                    })?;
                if state.is_null(row) {
                    return Err(DataFusionError::Execution(format!(
                        "RangeSelect Final received NULL outer state for aggregate {index}"
                    )));
                }
                accumulator.update_batch(&[states[0].slice(row, 1)])?;
            }
        }
        Ok(())
    }

    fn generate_output(&mut self) -> DfResult<RecordBatch> {
        if self.mode == RangeSelectExecMode::Partial {
            return self.generate_partial_output();
        }
        let _timer = self.metric.elapsed_compute().timer();
        if self.series_map.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        // 1 for time index column
        let mut columns: Vec<Arc<dyn Array>> =
            Vec::with_capacity(1 + self.range_exec.len() + self.by.len());
        let mut ts_builder = TimestampMillisecondBuilder::with_capacity(self.num_not_null_rows);
        let mut all_scalar =
            vec![Vec::with_capacity(self.num_not_null_rows); self.range_exec.len()];
        let mut by_rows = Vec::with_capacity(self.num_not_null_rows);
        let mut start_index = 0;
        // If any range expr need fill, we need fill both the missing align_ts and null value.
        let need_fill_output = self.range_exec.iter().any(|range| range.fill.is_some());
        // The padding value for each accumulator
        let padding_values = self
            .range_exec
            .iter()
            .map(|e| e.expr.create_accumulator()?.evaluate())
            .collect::<DfResult<Vec<_>>>()?;
        for SeriesState {
            row,
            align_ts_accumulator,
        } in self.series_map.values_mut()
        {
            // skip empty time series
            if align_ts_accumulator.is_empty() {
                continue;
            }
            // find the first and last align_ts
            let begin_ts = *align_ts_accumulator.first_key_value().unwrap().0;
            let end_ts = *align_ts_accumulator.last_key_value().unwrap().0;
            let align_ts = if need_fill_output {
                // we need to fill empty align_ts which not data in that solt
                (begin_ts..=end_ts).step_by(self.align as usize).collect()
            } else {
                align_ts_accumulator.keys().copied().collect::<Vec<_>>()
            };
            for ts in &align_ts {
                if let Some(slot) = align_ts_accumulator.get_mut(ts) {
                    for (column, acc) in all_scalar.iter_mut().zip(slot.iter_mut()) {
                        column.push(acc.evaluate()?);
                    }
                } else {
                    // fill null in empty time solt
                    for (column, padding) in all_scalar.iter_mut().zip(padding_values.iter()) {
                        column.push(padding.clone())
                    }
                }
            }
            ts_builder.append_slice(&align_ts);
            // apply fill strategy on time series
            for (
                i,
                RangeFnExec {
                    fill, need_cast, ..
                },
            ) in self.range_exec.iter().enumerate()
            {
                let time_series_data =
                    &mut all_scalar[i][start_index..start_index + align_ts.len()];
                if let Some(data_type) = need_cast {
                    cast_scalar_values(time_series_data, data_type)?;
                }
                if let Some(fill) = fill {
                    fill.apply_fill_strategy(&align_ts, time_series_data)?;
                }
            }
            by_rows.resize(by_rows.len() + align_ts.len(), row.row());
            start_index += align_ts.len();
        }
        for column_scalar in all_scalar {
            columns.push(ScalarValue::iter_to_array(column_scalar)?);
        }
        let ts_column = ts_builder.finish();
        // output schema before project follow the order of range expr | time index | by columns
        let ts_column = compute::cast(
            &ts_column,
            self.schema_before_project.field(columns.len()).data_type(),
        )?;
        columns.push(ts_column);
        // RowConverter decodes dictionary sort fields to their value arrays. Re-encode them so
        // the physical batch continues to match the logical output schema.
        for by_column in self.row_converter.convert_rows(by_rows)? {
            let output_type = self.schema_before_project.field(columns.len()).data_type();
            if by_column.data_type() == output_type {
                columns.push(by_column);
            } else {
                columns.push(compute::cast(by_column.as_ref(), output_type)?);
            }
        }
        let output = RecordBatch::try_new(self.schema_before_project.clone(), columns)?;
        let project_output = if let Some(project) = &self.schema_project {
            output.project(project)?
        } else {
            output
        };
        Ok(project_output)
    }

    fn generate_partial_output(&mut self) -> DfResult<RecordBatch> {
        if self.series_map.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let row_count = self
            .series_map
            .values()
            .map(|series| series.align_ts_accumulator.len())
            .sum();
        let mut states = vec![Vec::with_capacity(row_count); self.range_exec.len()];
        let mut timestamps = TimestampMillisecondBuilder::with_capacity(row_count);
        let mut by_rows = Vec::with_capacity(row_count);
        for series in self.series_map.values_mut() {
            for (timestamp, accumulators) in &mut series.align_ts_accumulator {
                for (state, accumulator) in states.iter_mut().zip(accumulators.iter_mut()) {
                    state.push(accumulator.evaluate()?);
                }
                timestamps.append_value(*timestamp);
                by_rows.push(series.row.row());
            }
        }
        let mut columns = states
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<DfResult<Vec<_>>>()?;
        columns.push(compute::cast(
            &timestamps.finish(),
            self.schema.field(columns.len()).data_type(),
        )?);
        for by_column in self.row_converter.convert_rows(by_rows)? {
            let output_type = self.schema.field(columns.len()).data_type();
            if by_column.data_type() == output_type {
                columns.push(by_column);
            } else {
                columns.push(compute::cast(by_column.as_ref(), output_type)?);
            }
        }
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn next_output_batch(&mut self) -> DfResult<Option<RecordBatch>> {
        if self.output_batch.is_none() {
            self.output_batch = Some(self.generate_output()?);
            self.output_batch_offset = 0;
        }

        let num_rows = self.output_batch.as_ref().unwrap().num_rows();
        if num_rows == 0 {
            self.output_batch = None;
            self.output_batch_offset = 0;
            return Ok(None);
        }

        if self.output_batch_offset == 0 && num_rows <= self.batch_size {
            return Ok(self.output_batch.take());
        }

        let offset = self.output_batch_offset;
        let len = (num_rows - offset).min(self.batch_size);
        let batch = self.output_batch.as_ref().unwrap().slice(offset, len);
        self.output_batch_offset += len;

        if self.output_batch_offset >= num_rows {
            self.output_batch = None;
            self.output_batch_offset = 0;
        }

        Ok(Some(batch))
    }
}

enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

impl RecordBatchStream for RangeSelectStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RangeSelectStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // new batch to aggregate
                        Some(Ok(batch)) => {
                            if let Err(e) = self.update_range_context(batch) {
                                common_telemetry::debug!(
                                    "RangeSelectStream cannot update range context, schema: {:?}, err: {:?}",
                                    self.schema,
                                    e
                                );
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        // inner had error, return to caller
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        // inner is done, producing output
                        None => {
                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }
                ExecutionState::ProducingOutput => {
                    let result = self.next_output_batch();
                    return match result {
                        // made output
                        Ok(Some(batch)) => {
                            if self.output_batch.is_none() {
                                self.exec_state = ExecutionState::Done;
                            }
                            Poll::Ready(Some(Ok(batch)))
                        }
                        Ok(None) => {
                            self.exec_state = ExecutionState::Done;
                            Poll::Ready(None)
                        }
                        // error making output
                        Err(error) => Poll::Ready(Some(Err(error))),
                    };
                }
                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}

#[cfg(test)]
mod test {
    macro_rules! nullable_array {
        ($builder:ident,) => {
        };
        ($array_type:ident ; $($tail:tt)*) => {
            paste::item! {
                {
                    let mut builder = arrow::array::[<$array_type Builder>]::new();
                    nullable_array!(builder, $($tail)*);
                    builder.finish()
                }
            }
        };
        ($builder:ident, null) => {
            $builder.append_null();
        };
        ($builder:ident, null, $($tail:tt)*) => {
            $builder.append_null();
            nullable_array!($builder, $($tail)*);
        };
        ($builder:ident, $value:literal) => {
            $builder.append_value($value);
        };
        ($builder:ident, $value:literal, $($tail:tt)*) => {
            $builder.append_value($value);
            nullable_array!($builder, $($tail)*);
        };
    }

    use std::sync::Arc;

    use arrow::array::StringDictionaryBuilder;
    use arrow_schema::SortOptions;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_aggregate::min_max;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::Column;
    use datatypes::arrow::array::{
        Float64Array, Int64Array, TimestampMillisecondArray, TimestampSecondArray,
    };
    use datatypes::arrow::datatypes::Int8Type;
    use datatypes::arrow_array::StringArray;

    use super::*;

    const TIME_INDEX_COLUMN: &str = "timestamp";

    fn prepare_test_data(is_float: bool, is_gap: bool) -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new(
                "value",
                if is_float {
                    DataType::Float64
                } else {
                    DataType::Int64
                },
                true,
            ),
            Field::new("host", DataType::Utf8, true),
        ]));
        let timestamp_column: Arc<dyn Array> = if !is_gap {
            Arc::new(TimestampMillisecondArray::from(vec![
                0, 5_000, 10_000, 15_000, 20_000, // host 1 every 5s
                0, 5_000, 10_000, 15_000, 20_000, // host 2 every 5s
            ])) as _
        } else {
            Arc::new(TimestampMillisecondArray::from(vec![
                0, 15_000, // host 1 every 5s, missing data on 5_000, 10_000
                0, 15_000, // host 2 every 5s, missing data on 5_000, 10_000
            ])) as _
        };
        let mut host = vec!["host1"; timestamp_column.len() / 2];
        host.extend(vec!["host2"; timestamp_column.len() / 2]);
        let mut value_column: Arc<dyn Array> = if is_gap {
            Arc::new(nullable_array!(Int64;
                0, 6, // data for host 1
                6, 12 // data for host 2
            )) as _
        } else {
            Arc::new(nullable_array!(Int64;
                0, null, 1, null, 2, // data for host 1
                3, null, 4, null, 5 // data for host 2
            )) as _
        };
        if is_float {
            value_column =
                cast_with_options(&value_column, &DataType::Float64, &CastOptions::default())
                    .unwrap();
        }
        let host_column: Arc<dyn Array> = Arc::new(StringArray::from(host)) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, value_column, host_column],
        )
        .unwrap();

        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
        ))
    }

    fn prepare_empty_test_data(is_float: bool) -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new(
                "value",
                if is_float {
                    DataType::Float64
                } else {
                    DataType::Int64
                },
                true,
            ),
            Field::new("host", DataType::Utf8, true),
        ]));
        let timestamp_column: Arc<dyn Array> =
            Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())) as _;
        let value_column: Arc<dyn Array> = if is_float {
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())) as _
        } else {
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as _
        };
        let host_column: Arc<dyn Array> =
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, value_column, host_column],
        )
        .unwrap();

        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
        ))
    }

    async fn collect_range_select_test(
        range1: Millisecond,
        range2: Millisecond,
        align: Millisecond,
        fill: Option<Fill>,
        is_float: bool,
        is_gap: bool,
        batch_size: usize,
    ) -> Vec<RecordBatch> {
        let data_type = if is_float {
            DataType::Float64
        } else {
            DataType::Int64
        };
        let (need_cast, schema_data_type) = if !is_float && matches!(fill, Some(Fill::Linear)) {
            // data_type = DataType::Float64;
            (Some(DataType::Float64), DataType::Float64)
        } else {
            (None, data_type.clone())
        };
        let memory_exec = Arc::new(prepare_test_data(is_float, is_gap));
        let schema = Arc::new(Schema::new(vec![
            Field::new("MIN(value)", schema_data_type.clone(), true),
            Field::new("MAX(value)", schema_data_type, true),
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let input_schema = memory_exec.schema().clone();
        let range_select_exec = Arc::new(RangeSelectExec {
            input: memory_exec,
            range_exec: vec![
                RangeFnExec {
                    expr: Arc::new(
                        AggregateExprBuilder::new(
                            min_max::min_udaf(),
                            vec![Arc::new(Column::new("value", 1))],
                        )
                        .schema(input_schema.clone())
                        .alias("MIN(value)")
                        .build()
                        .unwrap(),
                    ),
                    range: range1,
                    fill: fill.clone(),
                    need_cast: need_cast.clone(),
                },
                RangeFnExec {
                    expr: Arc::new(
                        AggregateExprBuilder::new(
                            min_max::max_udaf(),
                            vec![Arc::new(Column::new("value", 1))],
                        )
                        .schema(input_schema.clone())
                        .alias("MAX(value)")
                        .build()
                        .unwrap(),
                    ),
                    range: range2,
                    fill,
                    need_cast,
                },
            ],
            align,
            align_to: 0,
            by: vec![Arc::new(Column::new("host", 2))],
            time_index: TIME_INDEX_COLUMN.to_string(),
            schema: schema.clone(),
            schema_before_project: schema.clone(),
            schema_project: None,
            by_schema: Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            metric: ExecutionPlanMetricsSet::new(),
            cache,
            mode: RangeSelectExecMode::Complete,
        });
        let sort_exec = SortExec::new(
            [
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("host", 3)),
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new(TIME_INDEX_COLUMN, 2)),
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                },
            ]
            .into(),
            range_select_exec,
        );
        let session_context = SessionContext::new_with_config(
            datafusion::execution::config::SessionConfig::new().with_batch_size(batch_size),
        );
        datafusion::physical_plan::collect(Arc::new(sort_exec), session_context.task_ctx())
            .await
            .unwrap()
    }

    async fn do_range_select_test(
        range1: Millisecond,
        range2: Millisecond,
        align: Millisecond,
        fill: Option<Fill>,
        is_float: bool,
        is_gap: bool,
        expected: String,
    ) {
        let result =
            collect_range_select_test(range1, range2, align, fill, is_float, is_gap, 8192).await;

        let result_literal = arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn range_10s_align_1000s() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            10_000,
            1_000_000,
            Some(Fill::Null),
            true,
            false,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_fill_null() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        |            | 1969-12-31T23:59:55 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 1.0        |            | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 2.0        |            | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 3.0        |            | 1969-12-31T23:59:55 | host2 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 4.0        |            | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 5.0        |            | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            5_000,
            5_000,
            Some(Fill::Null),
            true,
            false,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_fill_prev() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        |            | 1969-12-31T23:59:55 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 1.0        | 0.0        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 2.0        | 1.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 3.0        |            | 1969-12-31T23:59:55 | host2 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 4.0        | 3.0        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 5.0        | 4.0        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            5_000,
            5_000,
            Some(Fill::Prev),
            true,
            false,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_fill_linear() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | -0.5       | 1969-12-31T23:59:55 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 1.0        | 0.5        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 2.0        | 1.5        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 3.0        | 2.5        | 1969-12-31T23:59:55 | host2 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 4.0        | 3.5        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 5.0        | 4.5        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            5_000,
            5_000,
            Some(Fill::Linear),
            true,
            false,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_fill_integer_linear() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | -0.5       | 1969-12-31T23:59:55 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 1.0        | 0.5        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 2.0        | 1.5        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 3.0        | 2.5        | 1969-12-31T23:59:55 | host2 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 4.0        | 3.5        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 5.0        | 4.5        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            5_000,
            5_000,
            Some(Fill::Linear),
            false,
            false,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_fill_const() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 6.6        | 1969-12-31T23:59:55 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 1.0        | 6.6        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 2.0        | 6.6        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 3.0        | 6.6        | 1969-12-31T23:59:55 | host2 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 4.0        | 6.6        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 5.0        | 6.6        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            5_000,
            5_000,
            Some(Fill::Const(ScalarValue::Float64(Some(6.6)))),
            true,
            false,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_fill_gap() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 12.0       | 12.0       | 1970-01-01T00:00:15 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(5_000, 5_000, 5_000, None, true, true, expected).await;
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n|            |            | 1970-01-01T00:00:05 | host1 |\
            \n|            |            | 1970-01-01T00:00:10 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:00 | host2 |\
            \n|            |            | 1970-01-01T00:00:05 | host2 |\
            \n|            |            | 1970-01-01T00:00:10 | host2 |\
            \n| 12.0       | 12.0       | 1970-01-01T00:00:15 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(5_000, 5_000, 5_000, Some(Fill::Null), true, true, expected).await;
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:05 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:05 | host2 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 12.0       | 12.0       | 1970-01-01T00:00:15 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(5_000, 5_000, 5_000, Some(Fill::Prev), true, true, expected).await;
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:05 | host1 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 8.0        | 8.0        | 1970-01-01T00:00:05 | host2 |\
            \n| 10.0       | 10.0       | 1970-01-01T00:00:10 | host2 |\
            \n| 12.0       | 12.0       | 1970-01-01T00:00:15 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            5_000,
            5_000,
            5_000,
            Some(Fill::Linear),
            true,
            true,
            expected,
        )
        .await;
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:05 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:05 | host2 |\
            \n| 6.0        | 6.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 12.0       | 12.0       | 1970-01-01T00:00:15 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            5_000,
            5_000,
            5_000,
            Some(Fill::Const(ScalarValue::Float64(Some(6.0)))),
            true,
            true,
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn range_select_respects_session_batch_size() {
        let result =
            collect_range_select_test(10_000, 5_000, 5_000, Some(Fill::Null), true, false, 3).await;

        let row_counts = result
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>();
        assert_eq!(vec![3, 3, 3, 3], row_counts);
    }

    #[tokio::test]
    async fn range_select_skips_empty_output_batch() {
        let memory_exec = Arc::new(prepare_empty_test_data(true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("MIN(value)", DataType::Float64, true),
            Field::new("MAX(value)", DataType::Float64, true),
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let input_schema = memory_exec.schema().clone();
        let range_select_exec = Arc::new(RangeSelectExec {
            input: memory_exec,
            range_exec: vec![
                RangeFnExec {
                    expr: Arc::new(
                        AggregateExprBuilder::new(
                            min_max::min_udaf(),
                            vec![Arc::new(Column::new("value", 1))],
                        )
                        .schema(input_schema.clone())
                        .alias("MIN(value)")
                        .build()
                        .unwrap(),
                    ),
                    range: 10_000,
                    fill: Some(Fill::Null),
                    need_cast: None,
                },
                RangeFnExec {
                    expr: Arc::new(
                        AggregateExprBuilder::new(
                            min_max::max_udaf(),
                            vec![Arc::new(Column::new("value", 1))],
                        )
                        .schema(input_schema)
                        .alias("MAX(value)")
                        .build()
                        .unwrap(),
                    ),
                    range: 5_000,
                    fill: Some(Fill::Null),
                    need_cast: None,
                },
            ],
            align: 5_000,
            align_to: 0,
            by: vec![Arc::new(Column::new("host", 2))],
            time_index: TIME_INDEX_COLUMN.to_string(),
            schema: schema.clone(),
            schema_before_project: schema.clone(),
            schema_project: None,
            by_schema: Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            metric: ExecutionPlanMetricsSet::new(),
            cache,
            mode: RangeSelectExecMode::Complete,
        });
        let session_context = SessionContext::new();
        let result =
            datafusion::physical_plan::collect(range_select_exec, session_context.task_ctx())
                .await
                .unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn fill_test() {
        assert!(Fill::try_from_str("", &DataType::UInt8).unwrap().is_none());
        assert!(Fill::try_from_str("Linear", &DataType::UInt8).unwrap() == Some(Fill::Linear));
        assert_eq!(
            Fill::try_from_str("Linear", &DataType::Boolean)
                .unwrap_err()
                .to_string(),
            "Error during planning: Use FILL LINEAR on Non-numeric DataType Boolean"
        );
        assert_eq!(
            Fill::try_from_str("WHAT", &DataType::UInt8)
                .unwrap_err()
                .to_string(),
            "Error during planning: WHAT is not a valid fill option, fail to convert to a const value. { Arrow error: Cast error: Cannot cast string 'WHAT' to value of UInt8 type }"
        );
        assert_eq!(
            Fill::try_from_str("8.0", &DataType::UInt8)
                .unwrap_err()
                .to_string(),
            "Error during planning: 8.0 is not a valid fill option, fail to convert to a const value. { Arrow error: Cast error: Cannot cast string '8.0' to value of UInt8 type }"
        );
        assert!(
            Fill::try_from_str("8", &DataType::UInt8).unwrap()
                == Some(Fill::Const(ScalarValue::UInt8(Some(8))))
        );
        let mut test1 = vec![
            ScalarValue::UInt8(Some(8)),
            ScalarValue::UInt8(None),
            ScalarValue::UInt8(Some(9)),
        ];
        Fill::Null.apply_fill_strategy(&[], &mut test1).unwrap();
        assert_eq!(test1[1], ScalarValue::UInt8(None));
        Fill::Prev.apply_fill_strategy(&[], &mut test1).unwrap();
        assert_eq!(test1[1], ScalarValue::UInt8(Some(8)));
        test1[1] = ScalarValue::UInt8(None);
        Fill::Const(ScalarValue::UInt8(Some(10)))
            .apply_fill_strategy(&[], &mut test1)
            .unwrap();
        assert_eq!(test1[1], ScalarValue::UInt8(Some(10)));
    }

    #[test]
    fn test_fill_linear() {
        let ts = vec![1, 2, 3, 4, 5];
        let mut test = vec![
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(None),
            ScalarValue::Float32(Some(3.0)),
            ScalarValue::Float32(None),
            ScalarValue::Float32(Some(5.0)),
        ];
        Fill::Linear.apply_fill_strategy(&ts, &mut test).unwrap();
        let mut test1 = vec![
            ScalarValue::Float32(None),
            ScalarValue::Float32(Some(2.0)),
            ScalarValue::Float32(None),
            ScalarValue::Float32(Some(4.0)),
            ScalarValue::Float32(None),
        ];
        Fill::Linear.apply_fill_strategy(&ts, &mut test1).unwrap();
        assert_eq!(test, test1);
        // test linear interpolation on irregularly spaced ts/data
        let ts = vec![
            1,   // None
            3,   // 1.0
            8,   // 11.0
            30,  // None
            88,  // 10.0
            108, // 5.0
            128, // None
        ];
        let mut test = vec![
            ScalarValue::Float64(None),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Float64(Some(11.0)),
            ScalarValue::Float64(None),
            ScalarValue::Float64(Some(10.0)),
            ScalarValue::Float64(Some(5.0)),
            ScalarValue::Float64(None),
        ];
        Fill::Linear.apply_fill_strategy(&ts, &mut test).unwrap();
        let data: Vec<_> = test
            .into_iter()
            .map(|x| {
                let ScalarValue::Float64(Some(f)) = x else {
                    unreachable!()
                };
                f
            })
            .collect();
        assert_eq!(data, vec![-3.0, 1.0, 11.0, 10.725, 10.0, 5.0, 0.0]);
        // test corner case
        let ts = vec![1];
        let test = vec![ScalarValue::Float32(None)];
        let mut test1 = test.clone();
        Fill::Linear.apply_fill_strategy(&ts, &mut test1).unwrap();
        assert_eq!(test, test1);
    }

    fn local_stage_input(fields: Vec<Field>) -> LogicalPlan {
        let schema = Arc::new(Schema::new(fields));
        let source = Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema),
        )));
        datafusion_expr::LogicalPlanBuilder::scan("range_select_local_stage", source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn local_stage_complete(expression: Expr, name: &str, result_type: DataType) -> RangeSelect {
        let input = local_stage_input(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("other", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
        ]);
        RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: name.into(),
                data_type: result_type,
                expr: expression,
                range: Duration::from_secs(10),
                fill: (name == "sum").then_some(Fill::Prev),
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            Expr::Column(datafusion_common::Column::new_unqualified("timestamp")),
            vec![Expr::Column(datafusion_common::Column::new_unqualified(
                "host",
            ))],
            &[],
        )
        .unwrap()
    }

    #[test]
    fn local_stages_materialize_compound_arguments_and_reject_collisions() {
        let complete = local_stage_complete(
            datafusion::functions_aggregate::expr_fn::avg(
                datafusion::prelude::col("value") + datafusion::prelude::col("other"),
            ),
            "avg",
            DataType::Float64,
        );
        let (partial, final_node) = complete.split_for_local_test().unwrap();
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!()
        };
        assert_eq!(projection.schema.field(2).name(), "__range_arg_0");
        assert!(matches!(partial.mode, RangeSelectMode::Partial(_)));
        assert!(matches!(final_node.mode, RangeSelectMode::Final(_)));
        let Expr::Alias(alias) = &partial.range_expr[0].expr else {
            unreachable!()
        };
        let Expr::AggregateFunction(state) = alias.expr.as_ref() else {
            unreachable!()
        };
        assert!(
            matches!(state.params.args.as_slice(), [Expr::Column(column)] if column.name == "__range_arg_0")
        );

        let collision = local_stage_input(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("other", DataType::Float64, true),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("__range_arg_0", DataType::Float64, true),
        ]);
        let collision = RangeSelect::try_new(
            Arc::new(collision),
            vec![RangeFn {
                name: "sum".into(),
                data_type: DataType::Float64,
                expr: datafusion::functions_aggregate::expr_fn::sum(datafusion::prelude::col(
                    "value",
                )),
                range: Duration::from_secs(5),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            Expr::Column(datafusion_common::Column::new_unqualified("timestamp")),
            vec![Expr::Column(datafusion_common::Column::new_unqualified(
                "host",
            ))],
            &[],
        )
        .unwrap();
        assert!(collision.split_for_local_test().is_none());
    }

    #[test]
    fn local_stages_reject_noncanonical_aggregate_contracts() {
        let value = || datafusion::prelude::col("value");
        let mut distinct = datafusion::functions_aggregate::expr_fn::sum(value());
        let Expr::AggregateFunction(function) = &mut distinct else {
            unreachable!()
        };
        function.params.distinct = true;
        let mut filtered = datafusion::functions_aggregate::expr_fn::sum(value());
        let Expr::AggregateFunction(function) = &mut filtered else {
            unreachable!()
        };
        function.params.filter = Some(Box::new(Expr::Literal(
            ScalarValue::Boolean(Some(true)),
            None,
        )));
        let mut ordered = datafusion::functions_aggregate::expr_fn::sum(value());
        let Expr::AggregateFunction(function) = &mut ordered else {
            unreachable!()
        };
        function.params.order_by = vec![datafusion_expr::expr::Sort::new(value(), false, false)];
        let mut null_treated = datafusion::functions_aggregate::expr_fn::sum(value());
        let Expr::AggregateFunction(function) = &mut null_treated else {
            unreachable!()
        };
        function.params.null_treatment = Some(datafusion_expr::expr::NullTreatment::IgnoreNulls);
        let cases = [
            distinct,
            filtered,
            ordered,
            null_treated,
            datafusion::functions_aggregate::expr_fn::count(datafusion_expr::lit(1_i64)),
            datafusion::functions_aggregate::expr_fn::first_value(value(), vec![]),
        ];
        for expression in cases {
            assert!(
                local_stage_complete(expression, "result", DataType::Float64)
                    .split_for_local_test()
                    .is_none()
            );
        }
    }

    fn local_raw_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("other", DataType::Float64, true),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ]))
    }

    fn local_raw_batch(rows: &[(i64, Option<f64>, Option<f64>)]) -> RecordBatch {
        let mut hosts = StringDictionaryBuilder::<Int8Type>::new();
        for _ in rows {
            hosts.append("g").unwrap();
        }
        RecordBatch::try_new(
            local_raw_schema(),
            vec![
                Arc::new(TimestampSecondArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(hosts.finish()),
            ],
        )
        .unwrap()
    }

    fn local_memory_exec(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], schema, None).unwrap(),
        )))
    }

    fn local_materialized_exec(
        partial: &RangeSelect,
        input: Arc<dyn ExecutionPlan>,
        state: &SessionState,
    ) -> Arc<dyn ExecutionPlan> {
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!()
        };
        let expressions = projection
            .expr
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                Ok((
                    create_physical_expr(
                        expr,
                        projection.input.schema().as_ref(),
                        state.execution_props(),
                    )?,
                    projection.schema.field(index).name().clone(),
                ))
            })
            .collect::<DfResult<Vec<_>>>()
            .unwrap();
        Arc::new(
            datafusion::physical_plan::projection::ProjectionExec::try_new(expressions, input)
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn local_builtin_stages_execute_two_partial_inputs_then_final() {
        let input = local_stage_input(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("other", DataType::Float64, true),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        let value = || datafusion::prelude::col("value");
        let ranges = vec![
            (
                "min",
                datafusion::functions_aggregate::expr_fn::min(value()),
                DataType::Float64,
            ),
            (
                "max",
                datafusion::functions_aggregate::expr_fn::max(value()),
                DataType::Float64,
            ),
            (
                "sum",
                datafusion::functions_aggregate::expr_fn::sum(value()),
                DataType::Float64,
            ),
            (
                "count",
                datafusion::functions_aggregate::expr_fn::count(value()),
                DataType::Int64,
            ),
            (
                "avg",
                datafusion::functions_aggregate::expr_fn::avg(value()),
                DataType::Float64,
            ),
            (
                "compound_avg",
                datafusion::functions_aggregate::expr_fn::avg(
                    value() + datafusion::prelude::col("other"),
                ),
                DataType::Float64,
            ),
        ]
        .into_iter()
        .map(|(name, expr, data_type)| RangeFn {
            name: name.into(),
            data_type,
            expr,
            range: Duration::from_secs(10),
            fill: (name == "sum").then_some(Fill::Prev),
            need_cast: false,
        })
        .collect::<Vec<_>>();
        let time_expr = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"));
        let by_expr = Expr::Column(datafusion_common::Column::new_unqualified("host"));
        let schema_project = ranges
            .iter()
            .map(|range| range.expr.clone().alias(range.name.clone()))
            .chain([time_expr.clone(), by_expr.clone()])
            .collect::<Vec<_>>();
        let complete = RangeSelect::try_new(
            Arc::new(input),
            ranges,
            Duration::from_secs(5),
            0,
            time_expr,
            vec![by_expr],
            &schema_project,
        )
        .unwrap();
        let (partial, final_node) = complete.split_for_local_test().unwrap();
        let inputs = vec![
            local_raw_batch(&[(0, Some(1.0), Some(3.0)), (5, Some(3.0), Some(5.0))]),
            local_raw_batch(&[(5, Some(5.0), Some(7.0)), (20, None, None)]),
            local_raw_batch(&[]),
        ];
        let session = SessionContext::new();
        let state = session.state();
        let two_partition_raw: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(
                &[vec![inputs[0].clone()], vec![inputs[1].clone()]],
                local_raw_schema(),
                None,
            )
            .unwrap(),
        )));
        let two_partition_partial = partial
            .to_execution_plan(
                partial.input.as_ref(),
                local_materialized_exec(&partial, two_partition_raw, &state),
                &state,
            )
            .unwrap();
        assert_eq!(
            two_partition_partial
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );
        let complete_exec = complete
            .to_execution_plan(
                complete.input.as_ref(),
                local_memory_exec(local_raw_schema(), inputs.clone()),
                &state,
            )
            .unwrap();
        assert!(matches!(
            complete_exec.required_input_distribution().as_slice(),
            [Distribution::SinglePartition]
        ));
        assert_eq!(
            complete_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        assert_eq!(
            complete_exec.properties().emission_type,
            EmissionType::Final
        );
        let expected = datafusion::physical_plan::collect(complete_exec, session.task_ctx())
            .await
            .unwrap();
        assert!(
            expected
                .iter()
                .any(|batch| batch.num_columns() == 8 && batch.num_rows() > 0)
        );
        assert_eq!(expected.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
        let gap_sum = expected
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(move |row| (batch, row)))
            .find_map(|(batch, row)| {
                (batch.num_columns() == 8
                    && ScalarValue::try_from_array(batch.column(6), row).ok()
                        == Some(ScalarValue::TimestampSecond(Some(10), None)))
                .then(|| ScalarValue::try_from_array(batch.column(2), row).unwrap())
            });
        assert_eq!(gap_sum, Some(ScalarValue::Float64(Some(8.0))));
        let mut states = Vec::new();
        for input in inputs {
            let raw = local_memory_exec(local_raw_schema(), vec![input]);
            let exec = partial
                .to_execution_plan(
                    partial.input.as_ref(),
                    local_materialized_exec(&partial, raw, &state),
                    &state,
                )
                .unwrap();
            assert!(matches!(
                exec.required_input_distribution().as_slice(),
                [Distribution::UnspecifiedDistribution]
            ));
            assert_eq!(exec.properties().emission_type, EmissionType::Final);
            for field in exec.schema().fields().iter().take(6) {
                assert!(matches!(field.data_type(), DataType::Struct(_)));
                assert!(!field.is_nullable());
            }
            assert_eq!(
                exec.schema().field(6).data_type(),
                &DataType::Timestamp(TimeUnit::Millisecond, None)
            );
            assert_eq!(
                exec.schema(),
                Arc::new(Schema::new(
                    partial
                        .schema
                        .fields()
                        .iter()
                        .map(|field| field.as_ref().clone())
                        .collect::<Vec<_>>()
                ))
            );
            states.extend(
                datafusion::physical_plan::collect(exec, session.task_ctx())
                    .await
                    .unwrap(),
            );
        }
        let final_exec = final_node
            .to_execution_plan(
                final_node.input.as_ref(),
                local_memory_exec(states[0].schema(), states),
                &state,
            )
            .unwrap();
        let actual = datafusion::physical_plan::collect(final_exec.clone(), session.task_ctx())
            .await
            .unwrap();
        assert!(matches!(
            final_exec.required_input_distribution().as_slice(),
            [Distribution::SinglePartition]
        ));
        assert_eq!(
            final_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        assert_eq!(final_exec.properties().emission_type, EmissionType::Final);
        assert_eq!(final_exec.schema(), expected[0].schema());
        assert_eq!(expected, actual);
    }

    #[test]
    fn local_stage_rebuild_rejects_malformed_state_bucket_and_merge_contracts() {
        let complete = local_stage_complete(
            datafusion::functions_aggregate::expr_fn::sum(datafusion::prelude::col("value")),
            "sum",
            DataType::Float64,
        );
        let (partial, final_node) = complete.split_for_local_test().unwrap();
        let assert_rejected = |node: RangeSelect| {
            assert!(
                node.with_exprs_and_inputs(node.expressions(), vec![node.input.as_ref().clone()])
                    .is_err()
            );
        };
        let mut malformed_state = partial.clone();
        malformed_state.range_expr[0].expr = datafusion::functions_aggregate::expr_fn::sum(
            datafusion::prelude::col("__range_arg_0"),
        );
        assert_rejected(malformed_state);

        let mut malformed_bucket = final_node.clone();
        let RangeSelectMode::Final(spec) = &mut malformed_bucket.mode else {
            unreachable!()
        };
        spec.bucket_field.data_type = DataType::Timestamp(TimeUnit::Second, None);
        assert_rejected(malformed_bucket);

        let mut malformed_merge = final_node;
        let Expr::Alias(alias) = &mut malformed_merge.range_expr[0].expr else {
            unreachable!()
        };
        let Expr::AggregateFunction(merge) = alias.expr.as_mut() else {
            unreachable!()
        };
        merge.params.distinct = true;
        assert_rejected(malformed_merge);
    }
}
