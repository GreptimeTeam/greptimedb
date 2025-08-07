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
use arrow::compute::{self, cast_with_options, take_arrays, CastOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::common::{Result as DataFusionResult, Statistics};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::utils::{exprlist_to_fields, COUNT_STAR_EXPANSION};
use datafusion_expr::{
    lit, Accumulator, Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore,
};
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::{
    create_physical_expr, create_physical_sort_expr, Distribution, EquivalenceProperties,
    Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datatypes::arrow::array::{
    Array, ArrayRef, TimestampMillisecondArray, TimestampMillisecondBuilder, UInt32Builder,
};
use datatypes::arrow::datatypes::{ArrowPrimitiveType, TimestampMillisecondType};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::arrow::row::{OwnedRow, RowConverter, SortField};
use futures::{ready, Stream};
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
        self.schema_project.partial_cmp(&other.schema_project)
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
        let by_fields = exprlist_to_fields(&by, &input)?;
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
                    (f.0.cloned(), Arc::new(f.1.clone()))
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
        })
    }
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
        if inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "RangeSelect: inputs is empty".to_string(),
            ));
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
        Ok(Self {
            align: self.align,
            align_to: self.align_to,
            range_expr,
            input: Arc::new(inputs[0].clone()),
            time_index: self.time_index.clone(),
            time_expr,
            schema: self.schema.clone(),
            by,
            by_schema: self.by_schema.clone(),
            schema_project: self.schema_project.clone(),
            schema_before_project: self.schema_before_project.clone(),
        })
    }
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
                #[allow(deprecated)]
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
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
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
    cache: PlanProperties,
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
        vec![Distribution::SinglePartition]
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        Ok(Arc::new(Self {
            input: children[0].clone(),
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
            cache: self.cache.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
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
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        Ok(Box::pin(RangeSelectStream {
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
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
    }

    fn name(&self) -> &str {
        "RanegSelectExec"
    }
}

struct RangeSelectStream {
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

    fn update_range_context(&mut self, batch: RecordBatch) -> DfResult<()> {
        let _timer = self.metric.elapsed_compute().timer();
        let num_rows = batch.num_rows();
        let by_arrays = self.evaluate_many(&batch, &self.by)?;
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

    fn generate_output(&mut self) -> DfResult<RecordBatch> {
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
        columns.extend(self.row_converter.convert_rows(by_rows)?);
        let output = RecordBatch::try_new(self.schema_before_project.clone(), columns)?;
        let project_output = if let Some(project) = &self.schema_project {
            output.project(project)?
        } else {
            output
        };
        Ok(project_output)
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
                                    "RangeSelectStream cannot update range context, schema: {:?}, err: {:?}", self.schema, e
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
                    let result = self.generate_output();
                    return match result {
                        // made output
                        Ok(batch) => {
                            self.exec_state = ExecutionState::Done;
                            Poll::Ready(Some(Ok(batch)))
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

    use arrow_schema::SortOptions;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_aggregate::min_max;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datatypes::arrow::array::TimestampMillisecondArray;
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

    async fn do_range_select_test(
        range1: Millisecond,
        range2: Millisecond,
        align: Millisecond,
        fill: Option<Fill>,
        is_float: bool,
        is_gap: bool,
        expected: String,
    ) {
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
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
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
        let session_context = SessionContext::default();
        let result =
            datafusion::physical_plan::collect(Arc::new(sort_exec), session_context.task_ctx())
                .await
                .unwrap();

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
}
