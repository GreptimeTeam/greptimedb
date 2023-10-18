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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use ahash::RandomState;
use arrow::compute::{self, cast_with_options, CastOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use common_query::DfPhysicalPlan;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::common::{Result as DataFusionResult, Statistics};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::udaf::create_aggregate_expr as create_aggr_udf_expr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::physical_planner::create_physical_sort_expr;
use datafusion_common::utils::get_arrayref_at_indices;
use datafusion_common::{DFField, DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::utils::exprlist_to_fields;
use datafusion_expr::{Accumulator, Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::expressions::create_aggregate_expr as create_aggr_expr;
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_physical_expr::{create_physical_expr, AggregateExpr, PhysicalExpr};
use datatypes::arrow::array::{
    Array, ArrayRef, TimestampMillisecondArray, TimestampMillisecondBuilder, UInt32Builder,
};
use datatypes::arrow::datatypes::{ArrowPrimitiveType, TimestampMillisecondType};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::arrow::row::{OwnedRow, RowConverter, SortField};
use futures::{ready, Stream};
use futures_util::StreamExt;
use snafu::ResultExt;

use crate::error::{DataFusionSnafu, Result};

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
    pub fn try_from_str(value: &str, datatype: &DataType) -> DfResult<Self> {
        let s = value.to_uppercase();
        match s.as_str() {
            "NULL" | "" => Ok(Self::Null),
            "PREV" => Ok(Self::Prev),
            "LINEAR" => {
                if datatype.is_numeric() {
                    Ok(Self::Linear)
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
                .map(Fill::Const),
        }
    }

    /// The input `data` contains data on a complete time series.
    /// If the filling strategy is `PREV` or `LINEAR`, caller must be ensured that the incoming `data` is ascending time order.
    pub fn apply_fill_strategy(&self, data: &mut [ScalarValue]) -> DfResult<()> {
        let len = data.len();
        for i in 0..len {
            if data[i].is_null() {
                match self {
                    Fill::Null => continue,
                    Fill::Prev => {
                        if i != 0 {
                            data[i] = data[i - 1].clone()
                        }
                    }
                    Fill::Linear => {
                        if 0 < i && i < len - 1 {
                            match (&data[i - 1], &data[i + 1]) {
                                (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
                                    data[i] = ScalarValue::Float64(Some((a + b) / 2.0));
                                }
                                (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
                                    data[i] = ScalarValue::Float32(Some((a + b) / 2.0));
                                }
                                (a, b) => {
                                    if !a.is_null() && !b.is_null() {
                                        return Err(DataFusionError::Execution(
                                            "RangePlan: Apply Fill LINEAR strategy on Non-floating type".to_string()));
                                    } else {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    Fill::Const(v) => data[i] = v.clone(),
                }
            }
        }
        Ok(())
    }
}

#[derive(Eq, Clone, Debug)]
pub struct RangeFn {
    /// with format like `max(a) 300s null`
    pub name: String,
    pub data_type: DataType,
    pub expr: Expr,
    pub range: Duration,
    pub fill: Fill,
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
    pub time_index: String,
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

impl RangeSelect {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        range_expr: Vec<RangeFn>,
        align: Duration,
        time_index: Expr,
        by: Vec<Expr>,
        projection_expr: &[Expr],
    ) -> Result<Self> {
        let mut fields = range_expr
            .iter()
            .map(
                |RangeFn {
                     name,
                     data_type,
                     fill,
                     ..
                 }| {
                    Ok(DFField::new_unqualified(
                        name,
                        data_type.clone(),
                        // Only when data fill with Const option, the data can't be null
                        !matches!(fill, Fill::Const(..)),
                    ))
                },
            )
            .collect::<DfResult<Vec<_>>>()
            .context(DataFusionSnafu)?;
        // add align_ts
        let ts_field = time_index
            .to_field(input.schema().as_ref())
            .context(DataFusionSnafu)?;
        let time_index_name = ts_field.name().clone();
        fields.push(ts_field);
        // add by
        let by_fields =
            exprlist_to_fields(by.iter().collect::<Vec<_>>(), &input).context(DataFusionSnafu)?;
        fields.extend(by_fields.clone());
        let schema_before_project = Arc::new(
            DFSchema::new_with_metadata(fields, input.schema().metadata().clone())
                .context(DataFusionSnafu)?,
        );
        let by_schema = Arc::new(
            DFSchema::new_with_metadata(by_fields, input.schema().metadata().clone())
                .context(DataFusionSnafu)?,
        );
        // If the results of project plan can be obtained directly from range plan without any additional calculations, no project plan is required.
        // We can simply project the final output of the range plan to produce the final result.
        let schema_project = projection_expr
            .iter()
            .map(|project_expr| {
                if let Expr::Column(column) = project_expr {
                    schema_before_project
                        .index_of_column_by_name(column.relation.as_ref(), &column.name)
                        .unwrap_or(None)
                        .ok_or(())
                } else {
                    Err(())
                }
            })
            .collect::<std::result::Result<Vec<usize>, ()>>()
            .ok();
        let schema = if let Some(project) = &schema_project {
            let project_field = project
                .iter()
                .map(|i| schema_before_project.fields()[*i].clone())
                .collect();
            Arc::new(
                DFSchema::new_with_metadata(project_field, input.schema().metadata().clone())
                    .context(DataFusionSnafu)?,
            )
        } else {
            schema_before_project.clone()
        };
        Ok(Self {
            input,
            range_expr,
            align,
            time_index: time_index_name,
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
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeSelect: range_exprs=[{}], align={}s time_index={}",
            self.range_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.align.as_secs(),
            self.time_index
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert!(!inputs.is_empty());

        Self {
            align: self.align,
            range_expr: self.range_expr.clone(),
            input: Arc::new(inputs[0].clone()),
            time_index: self.time_index.clone(),
            schema: self.schema.clone(),
            by: self.by.clone(),
            by_schema: self.by_schema.clone(),
            schema_project: self.schema_project.clone(),
            schema_before_project: self.schema_before_project.clone(),
        }
    }
}

impl RangeSelect {
    fn create_physical_expr_list(
        &self,
        exprs: &[Expr],
        df_schema: &Arc<DFSchema>,
        schema: &Schema,
        session_state: &SessionState,
    ) -> DfResult<Vec<Arc<dyn PhysicalExpr>>> {
        exprs
            .iter()
            .map(|by| create_physical_expr(by, df_schema, schema, session_state.execution_props()))
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
                let expr = match &range_fn.expr {
                    Expr::AggregateFunction(aggr) => {
                        let order_by = if let Some(exprs) = &aggr.order_by {
                            exprs
                                .iter()
                                .map(|x| {
                                    create_physical_sort_expr(
                                        x,
                                        input_dfschema,
                                        &input_schema,
                                        session_state.execution_props(),
                                    )
                                })
                                .collect::<DfResult<Vec<_>>>()?
                        } else {
                            vec![]
                        };
                        let expr = create_aggr_expr(
                            &aggr.fun,
                            false,
                            &self.create_physical_expr_list(
                                &aggr.args,
                                input_dfschema,
                                &input_schema,
                                session_state,
                            )?,
                            &order_by,
                            &input_schema,
                            range_fn.expr.display_name()?,
                        )?;
                        Ok(expr)
                    }
                    Expr::AggregateUDF(aggr_udf) => {
                        let expr = create_aggr_udf_expr(
                            &aggr_udf.fun,
                            &self.create_physical_expr_list(
                                &aggr_udf.args,
                                input_dfschema,
                                &input_schema,
                                session_state,
                            )?,
                            &input_schema,
                            range_fn.expr.display_name()?,
                        )?;
                        Ok(expr)
                    }
                    _ => Err(DataFusionError::Plan(format!(
                        "Unexpected Expr:{} in RangeSelect",
                        range_fn.expr.display_name()?
                    ))),
                }?;
                let args = expr.expressions();
                Ok(RangeFnExec {
                    expr,
                    args,
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
        Ok(Arc::new(RangeSelectExec {
            input: exec_input,
            range_exec,
            align: self.align.as_millis() as Millisecond,
            by: self.create_physical_expr_list(
                &self.by,
                input_dfschema,
                &input_schema,
                session_state,
            )?,
            time_index: self.time_index.clone(),
            schema,
            by_schema: Arc::new(Schema::new(by_fields)),
            metric: ExecutionPlanMetricsSet::new(),
            schema_before_project,
            schema_project: self.schema_project.clone(),
        }))
    }
}

#[derive(Debug, Clone)]
struct RangeFnExec {
    pub expr: Arc<dyn AggregateExpr>,
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    pub range: Millisecond,
    pub fill: Fill,
    pub need_cast: Option<DataType>,
}

#[derive(Debug)]
pub struct RangeSelectExec {
    input: Arc<dyn ExecutionPlan>,
    range_exec: Vec<RangeFnExec>,
    align: Millisecond,
    time_index: String,
    by: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    by_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,
    schema_project: Option<Vec<usize>>,
    schema_before_project: SchemaRef,
}

impl DisplayAs for RangeSelectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RangeSelectExec: ")?;
                let range_expr_strs: Vec<String> = self
                    .range_exec
                    .iter()
                    .map(|e| format!("RangeFnExec{{ {}, range: {:?}}}", e.expr.name(), e.range))
                    .collect();
                let by: Vec<String> = self.by.iter().map(|e| e.to_string()).collect();
                write!(
                    f,
                    "range_expr=[{}], align={}, time_index={}, by=[{}]",
                    range_expr_strs.join(", "),
                    self.align,
                    self.time_index,
                    by.join(", ")
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

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn DfPhysicalPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn DfPhysicalPlan>>,
    ) -> datafusion_common::Result<Arc<dyn DfPhysicalPlan>> {
        assert!(!children.is_empty());
        Ok(Arc::new(Self {
            input: children[0].clone(),
            range_exec: self.range_exec.clone(),
            time_index: self.time_index.clone(),
            by: self.by.clone(),
            align: self.align,
            schema: self.schema.clone(),
            by_schema: self.by_schema.clone(),
            metric: self.metric.clone(),
            schema_before_project: self.schema_before_project.clone(),
            schema_project: self.schema_project.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<common_query::physical_plan::TaskContext>,
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
            by: self.by.clone(),
            series_map: HashMap::new(),
            exec_state: ExecutionState::ReadingInput,
            output_num_rows: 0,
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

    fn statistics(&self) -> Statistics {
        self.input.statistics()
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
    /// The number of rows of the final output
    output_num_rows: usize,
    metric: BaselineMetrics,
    schema_project: Option<Vec<usize>>,
    schema_before_project: SchemaRef,
}

struct SeriesState {
    /// by values written by `RowWriter`
    row: OwnedRow,
    /// key: align_ts
    /// value: a vector, each element is a range_fn follow the order of `range_exec`
    align_ts_accumulator: HashMap<Millisecond, Vec<Box<dyn Accumulator>>>,
}

/// According to `align`, produces a calendar-based aligned time.
/// Combining the parameters related to the range query,
/// determine for each `Accumulator` `(hash, align_ts)` define,
/// which rows of data will be applied to it.
fn align_to_calendar(
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
        let mut align_ts = ((ts + align - 1) / align) * align;
        while align_ts - range < ts && ts <= align_ts {
            modify_map
                .entry((*hash, align_ts))
                .or_default()
                .push(row as u32);
            align_ts += align;
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
                Ok(value.into_array(batch.num_rows()))
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
            .ok_or(DataFusionError::Execution(
                "Time index Column downcast to TimestampMillisecondArray failed".into(),
            ))?;
        for i in 0..self.range_exec.len() {
            let args = self.evaluate_many(&batch, &self.range_exec[i].args)?;
            // use self.modify_map record (hash, align_ts) => [row_nums]
            align_to_calendar(
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
            let args = get_arrayref_at_indices(&args, &modify_rows)?;
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
                            align_ts_accumulator: HashMap::new(),
                        });
                    match accumulators_map.align_ts_accumulator.entry(*ts) {
                        Entry::Occupied(mut e) => {
                            let accumulators = e.get_mut();
                            accumulators[i].update_batch(&sliced_arrays)
                        }
                        Entry::Vacant(e) => {
                            self.output_num_rows += 1;
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
        let mut ts_builder = TimestampMillisecondBuilder::with_capacity(self.output_num_rows);
        let mut all_scalar = vec![Vec::with_capacity(self.output_num_rows); self.range_exec.len()];
        let mut by_rows = Vec::with_capacity(self.output_num_rows);
        let mut start_index = 0;
        // RangePlan is calculated on a row basis. If a column uses the PREV or LINEAR filling strategy,
        // we must arrange the data in the entire data row to determine the NULL filling value.
        let need_sort_output = self
            .range_exec
            .iter()
            .any(|range| range.fill == Fill::Linear || range.fill == Fill::Prev);
        for SeriesState {
            row,
            align_ts_accumulator,
        } in self.series_map.values()
        {
            // collect data on time series
            if !need_sort_output {
                for (ts, accumulators) in align_ts_accumulator {
                    for (i, accumulator) in accumulators.iter().enumerate() {
                        all_scalar[i].push(accumulator.evaluate()?);
                    }
                    ts_builder.append_value(*ts);
                }
            } else {
                let mut keys = align_ts_accumulator.keys().copied().collect::<Vec<_>>();
                keys.sort();
                for key in &keys {
                    for (i, accumulator) in
                        align_ts_accumulator.get(key).unwrap().iter().enumerate()
                    {
                        all_scalar[i].push(accumulator.evaluate()?);
                    }
                }
                ts_builder.append_slice(&keys);
            }
            // apply fill strategy on time series
            for (
                i,
                RangeFnExec {
                    fill, need_cast, ..
                },
            ) in self.range_exec.iter().enumerate()
            {
                let time_series_data =
                    &mut all_scalar[i][start_index..start_index + align_ts_accumulator.len()];
                if let Some(data_type) = need_cast {
                    cast_scalar_values(time_series_data, data_type)?;
                }
                fill.apply_fill_strategy(time_series_data)?;
            }
            by_rows.resize(by_rows.len() + align_ts_accumulator.len(), row.row());
            start_index += align_ts_accumulator.len();
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

    use arrow_schema::SortOptions;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
    };
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use datafusion_physical_expr::expressions::{self, Column};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use datatypes::arrow_array::StringArray;

    use super::*;

    const TIME_INDEX_COLUMN: &str = "timestamp";

    fn prepare_test_data(is_float: bool) -> MemoryExec {
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
        let timestamp_column: Arc<dyn Array> = Arc::new(TimestampMillisecondArray::from(vec![
            0, 5_000, 10_000, 15_000, 20_000, // host 1 every 5s
            0, 5_000, 10_000, 15_000, 20_000, // host 2 every 5s
        ])) as _;
        let mut host = vec!["host1"; 5];
        host.extend(vec!["host2"; 5]);
        let value_column: Arc<dyn Array> = if is_float {
            Arc::new(nullable_array!(Float64;
                0.0, null, 1.0, null, 2.0, // data for host 1
                3.0, null, 4.0, null, 5.0 // data for host 2
            )) as _
        } else {
            Arc::new(nullable_array!(Int64;
                0, null, 1, null, 2, // data for host 1
                3, null, 4, null, 5 // data for host 2
            )) as _
        };
        let host_column: Arc<dyn Array> = Arc::new(StringArray::from(host)) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, value_column, host_column],
        )
        .unwrap();

        MemoryExec::try_new(&[vec![data]], schema, None).unwrap()
    }

    async fn do_range_select_test(
        range1: Millisecond,
        range2: Millisecond,
        align: Millisecond,
        fill: Fill,
        is_float: bool,
        expected: String,
    ) {
        let data_type = if is_float {
            DataType::Float64
        } else {
            DataType::Int64
        };
        let (need_cast, schema_data_type) = if !is_float && fill == Fill::Linear {
            // data_type = DataType::Float64;
            (Some(DataType::Float64), DataType::Float64)
        } else {
            (None, data_type.clone())
        };
        let memory_exec = Arc::new(prepare_test_data(is_float));
        let schema = Arc::new(Schema::new(vec![
            Field::new("MIN(value)", schema_data_type.clone(), true),
            Field::new("MAX(value)", schema_data_type, true),
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let range_select_exec = Arc::new(RangeSelectExec {
            input: memory_exec,
            range_exec: vec![
                RangeFnExec {
                    expr: Arc::new(expressions::Min::new(
                        Arc::new(Column::new("value", 1)),
                        "MIN(value)",
                        data_type.clone(),
                    )),
                    args: vec![Arc::new(Column::new("value", 1))],
                    range: range1,
                    fill: fill.clone(),
                    need_cast: need_cast.clone(),
                },
                RangeFnExec {
                    expr: Arc::new(expressions::Max::new(
                        Arc::new(Column::new("value", 1)),
                        "MAX(value)",
                        data_type,
                    )),
                    args: vec![Arc::new(Column::new("value", 1))],
                    range: range2,
                    fill,
                    need_cast,
                },
            ],
            align,
            by: vec![Arc::new(Column::new("host", 2))],
            time_index: TIME_INDEX_COLUMN.to_string(),
            schema: schema.clone(),
            schema_before_project: schema.clone(),
            schema_project: None,
            by_schema: Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            metric: ExecutionPlanMetricsSet::new(),
        });
        let sort_exec = SortExec::new(
            vec![
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
            ],
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
        do_range_select_test(10_000, 10_000, 1_000_000, Fill::Null, true, expected).await;
    }

    #[tokio::test]
    async fn range_fill_null() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 0.0        |            | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 1.0        |            | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 2.0        |            | 1970-01-01T00:00:25 | host1 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 3.0        |            | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 4.0        |            | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n| 5.0        |            | 1970-01-01T00:00:25 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(10_000, 5_000, 5_000, Fill::Null, true, expected).await;
    }

    #[tokio::test]
    async fn range_fill_prev() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:25 | host1 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:25 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(10_000, 5_000, 5_000, Fill::Prev, true, expected).await;
    }

    #[tokio::test]
    async fn range_fill_linear() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 0.0        | 0.5        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 1.0        | 1.5        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 2.0        |            | 1970-01-01T00:00:25 | host1 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 3.0        | 3.5        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 4.0        | 4.5        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n| 5.0        |            | 1970-01-01T00:00:25 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(10_000, 5_000, 5_000, Fill::Linear, true, expected).await;
    }

    #[tokio::test]
    async fn range_fill_integer_null() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0          | 0          | 1970-01-01T00:00:00 | host1 |\
            \n| 0          |            | 1970-01-01T00:00:05 | host1 |\
            \n| 1          | 1          | 1970-01-01T00:00:10 | host1 |\
            \n| 1          |            | 1970-01-01T00:00:15 | host1 |\
            \n| 2          | 2          | 1970-01-01T00:00:20 | host1 |\
            \n| 2          |            | 1970-01-01T00:00:25 | host1 |\
            \n| 3          | 3          | 1970-01-01T00:00:00 | host2 |\
            \n| 3          |            | 1970-01-01T00:00:05 | host2 |\
            \n| 4          | 4          | 1970-01-01T00:00:10 | host2 |\
            \n| 4          |            | 1970-01-01T00:00:15 | host2 |\
            \n| 5          | 5          | 1970-01-01T00:00:20 | host2 |\
            \n| 5          |            | 1970-01-01T00:00:25 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(10_000, 5_000, 5_000, Fill::Null, false, expected).await;
    }

    #[tokio::test]
    async fn range_fill_integer_linear() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 0.0        | 0.5        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 1.0        | 1.5        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 2.0        |            | 1970-01-01T00:00:25 | host1 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 3.0        | 3.5        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 4.0        | 4.5        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n| 5.0        |            | 1970-01-01T00:00:25 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(10_000, 5_000, 5_000, Fill::Linear, false, expected).await;
    }

    #[tokio::test]
    async fn range_fill_const() {
        let expected = String::from(
            "+------------+------------+---------------------+-------+\
            \n| MIN(value) | MAX(value) | timestamp           | host  |\
            \n+------------+------------+---------------------+-------+\
            \n| 0.0        | 0.0        | 1970-01-01T00:00:00 | host1 |\
            \n| 0.0        | 6.6        | 1970-01-01T00:00:05 | host1 |\
            \n| 1.0        | 1.0        | 1970-01-01T00:00:10 | host1 |\
            \n| 1.0        | 6.6        | 1970-01-01T00:00:15 | host1 |\
            \n| 2.0        | 2.0        | 1970-01-01T00:00:20 | host1 |\
            \n| 2.0        | 6.6        | 1970-01-01T00:00:25 | host1 |\
            \n| 3.0        | 3.0        | 1970-01-01T00:00:00 | host2 |\
            \n| 3.0        | 6.6        | 1970-01-01T00:00:05 | host2 |\
            \n| 4.0        | 4.0        | 1970-01-01T00:00:10 | host2 |\
            \n| 4.0        | 6.6        | 1970-01-01T00:00:15 | host2 |\
            \n| 5.0        | 5.0        | 1970-01-01T00:00:20 | host2 |\
            \n| 5.0        | 6.6        | 1970-01-01T00:00:25 | host2 |\
            \n+------------+------------+---------------------+-------+",
        );
        do_range_select_test(
            10_000,
            5_000,
            5_000,
            Fill::Const(ScalarValue::Float64(Some(6.6))),
            true,
            expected,
        )
        .await;
    }

    #[test]
    fn fill_test() {
        assert!(Fill::try_from_str("Linear", &DataType::UInt8).unwrap() == Fill::Linear);
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
                == Fill::Const(ScalarValue::UInt8(Some(8)))
        );
        let mut test1 = vec![
            ScalarValue::UInt8(Some(8)),
            ScalarValue::UInt8(None),
            ScalarValue::UInt8(Some(9)),
        ];
        Fill::Null.apply_fill_strategy(&mut test1).unwrap();
        assert_eq!(test1[1], ScalarValue::UInt8(None));
        Fill::Prev.apply_fill_strategy(&mut test1).unwrap();
        assert_eq!(test1[1], ScalarValue::UInt8(Some(8)));
        test1[1] = ScalarValue::UInt8(None);
        Fill::Const(ScalarValue::UInt8(Some(10)))
            .apply_fill_strategy(&mut test1)
            .unwrap();
        assert_eq!(test1[1], ScalarValue::UInt8(Some(10)));
        test1[1] = ScalarValue::UInt8(None);
        assert_eq!(
            Fill::Linear
                .apply_fill_strategy(&mut test1)
                .unwrap_err()
                .to_string(),
            "Execution error: RangePlan: Apply Fill LINEAR strategy on Non-floating type"
        );
        let mut test2 = vec![
            ScalarValue::Float32(Some(8.0)),
            ScalarValue::Float32(None),
            ScalarValue::Float32(Some(9.0)),
        ];
        Fill::Linear.apply_fill_strategy(&mut test2).unwrap();
        assert_eq!(test2[1], ScalarValue::Float32(Some(8.5)));
    }
}
