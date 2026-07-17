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
use common_function::aggrs::aggr_wrapper::StateMergeHelper;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::common::Result as DataFusionResult;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::utils::{COUNT_STAR_EXPANSION, exprlist_to_fields};
use datafusion_expr::{
    Accumulator, Aggregate, Expr, ExprSchemable, Extension, LogicalPlan, Projection,
    UserDefinedLogicalNodeCore, lit,
};
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

/// Execution role for a RangeSelect stage.
///
/// `Complete` is the legacy single-stage behavior. `Partial` consumes raw
/// aggregate arguments and emits accumulator state. `Final` consumes those
/// states and produces the legacy RangeSelect result. Keeping this as a sum
/// type makes a stage's input and output contract explicit instead of relying
/// on a combination of optional state schemas and flags.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) enum RangeSelectMode {
    #[default]
    Complete,
    Partial(PartialRangeSelectSpec),
    Final(FinalRangeSelectSpec),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PartialRangeSelectSpec {
    state_columns: Vec<String>,
    state_types: Vec<DataType>,
    by_fields: Vec<FieldContract>,
    wire: PartialRangeWireMetadata,
}

/// Canonical built-in aggregate identity for the Partial wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RangeAggregateKind {
    Min,
    Max,
    Sum,
    Count,
    Avg,
}

/// Schema-indexed metadata for serializing a materialized RangeSelect Partial.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PartialRangeWireMetadata {
    align: Duration,
    align_to: i64,
    time_index: usize,
    by_indices: Vec<usize>,
    functions: Vec<PartialRangeWireFunction>,
}

/// Wire-stable contract for one materialized RangeSelect aggregate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PartialRangeWireFunction {
    kind: RangeAggregateKind,
    argument_index: usize,
    range: Duration,
}

impl PartialRangeWireMetadata {
    pub(crate) fn try_new(
        align: Duration,
        align_to: i64,
        time_index: usize,
        by_indices: Vec<usize>,
        functions: Vec<PartialRangeWireFunction>,
    ) -> DataFusionResult<Self> {
        validate_wire_duration(align, "align")?;
        if functions.is_empty() {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire metadata requires at least one function".to_string(),
            ));
        }
        Ok(Self {
            align,
            align_to,
            time_index,
            by_indices,
            functions,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn align(&self) -> Duration {
        self.align
    }

    #[allow(dead_code)]
    pub(crate) fn align_to(&self) -> i64 {
        self.align_to
    }

    #[allow(dead_code)]
    pub(crate) fn time_index(&self) -> usize {
        self.time_index
    }

    #[allow(dead_code)]
    pub(crate) fn by_indices(&self) -> &[usize] {
        &self.by_indices
    }

    #[allow(dead_code)]
    pub(crate) fn functions(&self) -> &[PartialRangeWireFunction] {
        &self.functions
    }
}

impl PartialRangeWireFunction {
    #[allow(dead_code)]
    pub(crate) fn try_new(
        kind: RangeAggregateKind,
        argument_index: usize,
        range: Duration,
    ) -> DataFusionResult<Self> {
        validate_wire_duration(range, "range")?;
        Ok(Self {
            kind,
            argument_index,
            range,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn kind(&self) -> RangeAggregateKind {
        self.kind
    }

    #[allow(dead_code)]
    pub(crate) fn argument_index(&self) -> usize {
        self.argument_index
    }

    #[allow(dead_code)]
    pub(crate) fn range(&self) -> Duration {
        self.range
    }
}

fn validate_wire_duration(duration: Duration, name: &str) -> DataFusionResult<()> {
    if duration.as_millis() == 0 || duration.as_millis() > i64::MAX as u128 {
        return Err(DataFusionError::Plan(format!(
            "RangeSelect Partial wire {name} must be a positive millisecond duration"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FinalRangeSelectSpec {
    state_columns: Vec<String>,
    bucket_column: String,
    state_types: Vec<DataType>,
    by_input_fields: Vec<FieldContract>,
    raw_merge_result_fields: Vec<FieldContract>,
    legacy_range_fields: Vec<FieldContract>,
    legacy_time_field: FieldContract,
    legacy_by_fields: Vec<FieldContract>,
    legacy_metadata: BTreeMap<String, String>,
    schema_project: Option<Vec<usize>>,
}

/// An Eq/Hash-safe logical field contract used by RangeSelect mode payloads.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FieldContract {
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
            metadata: field
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }

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

    fn matches_qualified_field(
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
            ord => ord,
        }
    }
}

impl RangeSelect {
    /// Returns this RangeSelect's execution role for query-local planning.
    pub(crate) fn mode(&self) -> &RangeSelectMode {
        &self.mode
    }

    #[allow(dead_code)]
    pub(crate) fn partial_wire_metadata(&self) -> Option<&PartialRangeWireMetadata> {
        let RangeSelectMode::Partial(spec) = &self.mode else {
            return None;
        };
        Some(&spec.wire)
    }

    /// Reconstructs a validated Partial RangeSelect from decoder-owned wire metadata.
    #[allow(dead_code)]
    pub(crate) fn try_new_partial_from_wire(
        input: Arc<LogicalPlan>,
        wire: PartialRangeWireMetadata,
    ) -> DataFusionResult<Self> {
        let LogicalPlan::Projection(projection) = input.as_ref() else {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire input must be a materialization Projection".to_string(),
            ));
        };
        validate_wire_duration(wire.align, "align")?;
        if wire.functions.is_empty() {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire metadata requires at least one function".to_string(),
            ));
        }
        let input_schema = input.schema();
        let expected_slots = 1 + wire.by_indices.len() + wire.functions.len();
        if projection.expr.len() != expected_slots || input_schema.fields().len() != expected_slots
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire Projection has an invalid number of slots".to_string(),
            ));
        }
        let projection_child_schema = projection.input.schema();
        if wire.time_index != 0 || wire.time_index >= input_schema.fields().len() {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire time index is invalid".to_string(),
            ));
        }
        validate_direct_projection_column(
            &projection.expr[wire.time_index],
            projection_child_schema,
            input_schema,
            wire.time_index,
            "time",
        )?;
        let time_expr = column_expr(input_schema, wire.time_index);
        let (_, time_field) = time_expr.to_field(input_schema.as_ref())?;
        if !matches!(time_field.data_type(), DataType::Timestamp(_, _)) {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire time column must be a timestamp".to_string(),
            ));
        }
        let mut by = Vec::with_capacity(wire.by_indices.len());
        for (position, index) in wire.by_indices.iter().enumerate() {
            if *index != position + 1 || *index >= input_schema.fields().len() {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial wire BY index is invalid".to_string(),
                ));
            }
            validate_direct_projection_column(
                &projection.expr[*index],
                projection_child_schema,
                input_schema,
                *index,
                "BY",
            )?;
            by.push(column_expr(input_schema, *index));
        }
        let by_fields = exprlist_to_fields(&by, &input)?;
        let mut raw_aggregates = Vec::with_capacity(wire.functions.len());
        for (index, function) in wire.functions.iter().enumerate() {
            validate_wire_duration(function.range, "range")?;
            let expected_index = 1 + wire.by_indices.len() + index;
            if function.argument_index != expected_index {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial wire argument index is invalid".to_string(),
                ));
            }
            let Some(argument_name) = projection_column_name(input_schema, function.argument_index)
            else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial wire argument column is invalid".to_string(),
                ));
            };
            if argument_name != format!("__range_arg_{index}") {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial wire argument alias is invalid".to_string(),
                ));
            }
            validate_materialized_projection_argument(
                &projection.expr[function.argument_index],
                projection_child_schema,
                input_schema,
                function.argument_index,
                &argument_name,
                function.kind,
            )?;
            raw_aggregates.push(aggregate_expr_for_wire_kind(
                function.kind,
                Expr::Column(Column::new_unqualified(argument_name)),
            ));
        }
        // ProjectRel decode may erase Expr::Alias while retaining the output
        // DFSchema field name. Normalize only the Aggregate construction input
        // back to the alias form so DataFusion can resolve `__range_arg_i`.
        // The returned Partial still retains the decoded Projection input.
        let normalized_projection_expr = projection
            .expr
            .iter()
            .enumerate()
            .map(|(slot, expr)| {
                let argument_index = slot.checked_sub(1 + wire.by_indices.len());
                match argument_index {
                    Some(index)
                        if index < wire.functions.len() && !matches!(expr, Expr::Alias(_)) =>
                    {
                        expr.clone().alias(format!("__range_arg_{index}"))
                    }
                    _ => expr.clone(),
                }
            })
            .collect::<Vec<_>>();
        let aggregate_input = Arc::new(LogicalPlan::Projection(Projection::try_new(
            normalized_projection_expr,
            projection.input.clone(),
        )?));
        let aggregate = StateMergeHelper::coerce_aggr_node(Aggregate::try_new(
            aggregate_input.clone(),
            vec![],
            raw_aggregates,
        )?)?;
        if aggregate.aggr_expr.len() != wire.functions.len() {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire aggregate count is invalid".to_string(),
            ));
        }

        let mut state_columns = Vec::with_capacity(wire.functions.len());
        let mut state_types = Vec::with_capacity(wire.functions.len());
        let mut state_fields = Vec::with_capacity(wire.functions.len() + 1 + by.len());
        let mut range_expr = Vec::with_capacity(wire.functions.len());
        for (index, (function, aggregate_expr)) in
            wire.functions.iter().zip(&aggregate.aggr_expr).enumerate()
        {
            if canonical_aggregate_kind(aggregate_expr) != Some(function.kind) {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial wire aggregate identity is invalid".to_string(),
                ));
            }
            let contract = StateMergeHelper::split_coerced_aggregate_expr(
                aggregate_expr,
                aggregate.input.as_ref(),
            )?;
            let state_column = format!("__range_state_{index}");
            let state_expr = materialize_state_argument(contract.partial_state_expr, index)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "RangeSelect Partial wire state aggregate is invalid".to_string(),
                    )
                })?
                .alias(state_column.clone());
            let (_, state_field) = state_expr.to_field(input_schema.as_ref())?;
            if !matches!(state_field.data_type(), DataType::Struct(_)) {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial wire state must be a Struct".to_string(),
                ));
            }
            let state_type = state_field.data_type().clone();
            let mut state_field = state_field.as_ref().clone();
            state_field.set_nullable(false);
            state_fields.push((None, Arc::new(state_field)));
            state_columns.push(state_column.clone());
            state_types.push(state_type.clone());
            range_expr.push(RangeFn {
                name: state_column,
                data_type: state_type,
                expr: state_expr,
                range: function.range,
                fill: None,
                need_cast: false,
            });
        }
        state_fields.push((
            None,
            Arc::new(Field::new(
                "__range_bucket_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                time_field.is_nullable(),
            )),
        ));
        state_fields.extend(by_fields.clone());
        let metadata = input_schema.metadata().clone();
        let schema = Arc::new(DFSchema::new_with_metadata(state_fields, metadata.clone())?);
        let by_schema = Arc::new(DFSchema::new_with_metadata(by_fields, metadata)?);
        let partial = Self {
            input: input.clone(),
            range_expr,
            align: wire.align,
            align_to: wire.align_to,
            time_index: time_field.name().clone(),
            time_expr,
            by,
            schema: schema.clone(),
            by_schema: by_schema.clone(),
            schema_project: None,
            schema_before_project: schema,
            mode: RangeSelectMode::Partial(PartialRangeSelectSpec {
                state_columns,
                state_types,
                by_fields: (0..by_schema.fields().len())
                    .map(|index| FieldContract::from_schema(&by_schema, index))
                    .collect(),
                wire,
            }),
        };
        let rebuilt = partial.with_exprs_and_inputs(
            partial.expressions(),
            vec![aggregate_input.as_ref().clone()],
        )?;
        if rebuilt.schema != partial.schema || rebuilt.by_schema != partial.by_schema {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire reconstruction changed its output contract".to_string(),
            ));
        }
        Ok(partial)
    }

    /// Builds the only valid local two-stage RangeSelect shape. Any unsupported
    /// expression is reported as `None` so the caller can preserve Complete
    /// semantics rather than turning an optimization into a query error.
    pub(crate) fn try_split_for_pushdown(&self) -> Option<LogicalPlan> {
        if !matches!(self.mode, RangeSelectMode::Complete)
            || !matches!(self.time_expr, Expr::Column(_))
            || self.by.iter().any(|expr| !matches!(expr, Expr::Column(_)))
            || !is_pushdown_safe_input(self.input.as_ref())
        {
            return None;
        }

        let aggregate = Aggregate::try_new(
            self.input.clone(),
            vec![],
            self.range_expr
                .iter()
                .map(|range| range.expr.clone())
                .collect(),
        )
        .ok()?;
        let aggregate = StateMergeHelper::coerce_aggr_node(aggregate).ok()?;
        let mut partial_ranges = Vec::with_capacity(self.range_expr.len());
        let mut final_ranges = Vec::with_capacity(self.range_expr.len());
        let mut state_fields = Vec::with_capacity(self.range_expr.len());
        let mut state_columns = Vec::with_capacity(self.range_expr.len());
        let mut state_types = Vec::with_capacity(self.range_expr.len());
        let mut raw_merge_result_fields = Vec::with_capacity(self.range_expr.len());
        let mut partial_state_exprs = Vec::with_capacity(self.range_expr.len());
        let mut materialized_arguments = Vec::with_capacity(self.range_expr.len());
        let mut wire_functions = Vec::with_capacity(self.range_expr.len());

        for (index, (range, expr)) in self.range_expr.iter().zip(&aggregate.aggr_expr).enumerate() {
            let Some(aggregate_kind) = canonical_aggregate_kind(expr) else {
                return None;
            };
            let Expr::AggregateFunction(aggregate_expr) = expr else {
                return None;
            };
            let [argument] = aggregate_expr.params.args.as_slice() else {
                return None;
            };
            let argument_column = format!("__range_arg_{index}");
            if aggregate
                .input
                .schema()
                .index_of_column_by_name(None, &argument_column)
                .is_some()
            {
                return None;
            }
            let contract =
                StateMergeHelper::split_coerced_aggregate_expr(expr, aggregate.input.as_ref())
                    .ok()?;
            let state_column = format!("__range_state_{index}");
            let final_expr = contract.merge_expr(Column::new_unqualified(state_column.clone()));
            let Expr::Alias(final_alias) = final_expr else {
                return None;
            };
            let final_expr = final_alias.expr.as_ref().clone().alias(range.name.clone());
            let final_result_field = contract.final_result_field.as_ref();
            raw_merge_result_fields.push(FieldContract::from_qualified_field(
                None,
                &Field::new(
                    range.name.clone(),
                    final_result_field.data_type().clone(),
                    final_result_field.is_nullable(),
                )
                .with_metadata(final_result_field.metadata().clone()),
            ));
            state_columns.push(state_column.clone());
            partial_state_exprs.push(contract.partial_state_expr);
            materialized_arguments.push(argument.clone());
            wire_functions.push(PartialRangeWireFunction {
                kind: aggregate_kind,
                argument_index: 1 + self.by.len() + index,
                range: range.range,
            });
            final_ranges.push(RangeFn {
                name: range.name.clone(),
                data_type: range.data_type.clone(),
                expr: final_expr,
                range: range.range,
                fill: range.fill.clone(),
                need_cast: range.need_cast,
            });
        }

        let time_index = 0;
        let by_indices = (1..=self.by.len()).collect::<Vec<_>>();
        let projection_expr = std::iter::once(self.time_expr.clone())
            .chain(self.by.clone())
            .chain(
                materialized_arguments
                    .into_iter()
                    .enumerate()
                    .map(|(index, expr)| expr.alias(format!("__range_arg_{index}"))),
            )
            .collect::<Vec<_>>();
        let projection = Projection::try_new(projection_expr, aggregate.input).ok()?;
        let projection_input = Arc::new(LogicalPlan::Projection(projection));
        let projection_schema = projection_input.schema();
        let partial_time_expr = column_expr(projection_schema, time_index);
        let partial_by = by_indices
            .iter()
            .map(|index| column_expr(projection_schema, *index))
            .collect::<Vec<_>>();
        for (index, ((state_expr, state_column), range)) in partial_state_exprs
            .into_iter()
            .zip(state_columns.iter())
            .zip(self.range_expr.iter())
            .enumerate()
        {
            let partial_expr = materialize_state_argument(state_expr, index)?.alias(state_column);
            let (_, state_field) = partial_expr.to_field(projection_schema).ok()?;
            if !matches!(state_field.data_type(), DataType::Struct(_)) {
                return None;
            }
            let state_data_type = state_field.data_type().clone();
            let mut state_field = state_field.as_ref().clone();
            state_field.set_nullable(false);
            state_fields.push((None, Arc::new(state_field)));
            state_types.push(state_data_type.clone());
            partial_ranges.push(RangeFn {
                name: state_column.clone(),
                data_type: state_data_type,
                expr: partial_expr,
                range: range.range,
                fill: None,
                need_cast: false,
            });
        }

        let bucket_column = "__range_bucket_ms".to_string();
        let time_field = partial_time_expr.to_field(projection_schema).ok()?;
        state_fields.push((
            None,
            Arc::new(Field::new(
                bucket_column.clone(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                time_field.1.is_nullable(),
            )),
        ));
        let by_fields = exprlist_to_fields(&partial_by, &projection_input).ok()?;
        state_fields.extend(by_fields.clone());
        let partial_schema = Arc::new(
            DFSchema::new_with_metadata(state_fields, projection_schema.metadata().clone()).ok()?,
        );
        let partial_by_schema = Arc::new(
            DFSchema::new_with_metadata(by_fields, projection_schema.metadata().clone()).ok()?,
        );
        let partial = Self {
            input: projection_input,
            range_expr: partial_ranges,
            align: self.align,
            align_to: self.align_to,
            // Partial consumes the raw time index but publishes aligned bucket
            // values under `__range_bucket_ms` in its state schema.
            time_index: time_field.1.name().clone(),
            time_expr: partial_time_expr,
            by: partial_by,
            schema: partial_schema.clone(),
            by_schema: partial_by_schema.clone(),
            schema_project: None,
            schema_before_project: partial_schema,
            mode: RangeSelectMode::Partial(PartialRangeSelectSpec {
                state_columns: state_columns.clone(),
                state_types: state_types.clone(),
                by_fields: partial_by_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, _)| FieldContract::from_schema(&partial_by_schema, index))
                    .collect(),
                wire: PartialRangeWireMetadata::try_new(
                    self.align,
                    self.align_to,
                    time_index,
                    by_indices,
                    wire_functions,
                )
                .ok()?,
            }),
        };
        let partial_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(partial),
        });
        let final_by = self
            .by_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let (qualifier, field) = self.by_schema.qualified_field(index);
                Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect();
        let final_plan = Self {
            input: Arc::new(partial_plan),
            range_expr: final_ranges,
            align: self.align,
            align_to: self.align_to,
            time_index: bucket_column.clone(),
            time_expr: Expr::Column(Column::new_unqualified(bucket_column.clone())),
            by: final_by,
            schema: self.schema.clone(),
            by_schema: self.by_schema.clone(),
            schema_project: self.schema_project.clone(),
            schema_before_project: self.schema_before_project.clone(),
            mode: RangeSelectMode::Final(FinalRangeSelectSpec {
                state_columns,
                bucket_column,
                state_types,
                by_input_fields: self
                    .by_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, _)| FieldContract::from_schema(&self.by_schema, index))
                    .collect(),
                raw_merge_result_fields,
                legacy_range_fields: (0..self.range_expr.len())
                    .map(|index| FieldContract::from_schema(&self.schema_before_project, index))
                    .collect(),
                legacy_time_field: FieldContract::from_schema(
                    &self.schema_before_project,
                    self.range_expr.len(),
                ),
                legacy_by_fields: self
                    .by_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, _)| FieldContract::from_schema(&self.by_schema, index))
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
        Some(LogicalPlan::Extension(Extension {
            node: Arc::new(final_plan),
        }))
    }

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
}

/// Test-only real Partial plans for query-side serializer tests.
#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeSelectPartialFixture {
    CompoundAvg,
    Builtins,
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) struct RangeSelectSplitFixture {
    pub complete: LogicalPlan,
    pub partial: LogicalPlan,
    pub final_plan: LogicalPlan,
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) struct RangeSelectPartialExecFixture {
    pub logical_schema: DFSchemaRef,
    pub physical_schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

#[cfg(test)]
fn range_select_test_scan(fields: Vec<Arc<Field>>) -> LogicalPlan {
    let schema = Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let source = Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
        datafusion::datasource::empty::EmptyTable::new(schema),
    )));
    datafusion_expr::LogicalPlanBuilder::scan("range_select_fixture", source, None)
        .expect("test fixture scan")
        .build()
        .expect("test fixture plan")
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn range_select_split_fixture(
    variant: RangeSelectPartialFixture,
) -> RangeSelectSplitFixture {
    use datafusion::prelude::col;

    let timestamp = Expr::Column(Column::new_unqualified("timestamp"));
    let by = Expr::Column(Column::new_unqualified("group"));
    let (input, range_expr) = match variant {
        RangeSelectPartialFixture::CompoundAvg => (
            range_select_test_scan(vec![
                Arc::new(Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )),
                Arc::new(Field::new("value_a", DataType::Float64, true)),
                Arc::new(Field::new("value_b", DataType::Float64, true)),
                Arc::new(Field::new("group", DataType::Utf8, true)),
            ]),
            vec![RangeFn {
                name: "avg".to_string(),
                data_type: DataType::Float64,
                expr: avg(col("value_a") + col("value_b")),
                range: Duration::from_secs(10),
                fill: None,
                need_cast: false,
            }],
        ),
        RangeSelectPartialFixture::Builtins => (
            range_select_test_scan(vec![
                Arc::new(Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )),
                Arc::new(Field::new("value", DataType::Float64, true)),
                Arc::new(Field::new("group", DataType::Utf8, true)),
            ]),
            vec![
                RangeFn {
                    name: "min".to_string(),
                    data_type: DataType::Float64,
                    expr: min(col("value")),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
                RangeFn {
                    name: "max".to_string(),
                    data_type: DataType::Float64,
                    expr: max(col("value")),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
                RangeFn {
                    name: "sum".to_string(),
                    data_type: DataType::Float64,
                    expr: sum(col("value")),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
                RangeFn {
                    name: "count".to_string(),
                    data_type: DataType::Int64,
                    expr: count(col("value")),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
                RangeFn {
                    name: "avg".to_string(),
                    data_type: DataType::Float64,
                    expr: avg(col("value")),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
            ],
        ),
    };
    let projection_expr = range_expr
        .iter()
        .map(|range| range.expr.clone())
        .chain([timestamp.clone(), by.clone()])
        .collect::<Vec<_>>();
    let complete = RangeSelect::try_new(
        Arc::new(input),
        range_expr,
        Duration::from_secs(5),
        0,
        timestamp,
        vec![by],
        &projection_expr,
    )
    .expect("test fixture Complete RangeSelect");
    let LogicalPlan::Extension(final_extension) = complete
        .try_split_for_pushdown()
        .expect("test fixture split")
    else {
        unreachable!();
    };
    let partial = final_extension
        .node
        .as_any()
        .downcast_ref::<RangeSelect>()
        .expect("test fixture Final RangeSelect")
        .input
        .as_ref()
        .clone();
    RangeSelectSplitFixture {
        complete: LogicalPlan::Extension(Extension {
            node: Arc::new(complete),
        }),
        partial,
        final_plan: LogicalPlan::Extension(final_extension),
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn range_select_partial_fixture(variant: RangeSelectPartialFixture) -> LogicalPlan {
    range_select_split_fixture(variant).partial
}

/// Plans and collects a fixture Partial through its real ProjectionExec child.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) async fn collect_range_select_partial_fixture(
    partial_plan: &LogicalPlan,
    variant: RangeSelectPartialFixture,
) -> DataFusionResult<RangeSelectPartialExecFixture> {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;

    let LogicalPlan::Extension(extension) = partial_plan else {
        return Err(DataFusionError::Plan(
            "RangeSelect fixture Partial must be an Extension plan".to_string(),
        ));
    };
    let partial = extension
        .node
        .as_any()
        .downcast_ref::<RangeSelect>()
        .ok_or_else(|| DataFusionError::Plan("RangeSelect fixture node is invalid".to_string()))?;
    let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
        return Err(DataFusionError::Plan(
            "RangeSelect fixture Partial input must be a Projection".to_string(),
        ));
    };
    let (raw_schema, batch) = match variant {
        RangeSelectPartialFixture::CompoundAvg => {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("value_a", DataType::Float64, true),
                Field::new("value_b", DataType::Float64, true),
                Field::new("group", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        0, 5_000,
                    ])),
                    Arc::new(arrow::array::Float64Array::from(vec![1.0, 3.0])),
                    Arc::new(arrow::array::Float64Array::from(vec![2.0, 4.0])),
                    Arc::new(arrow::array::StringArray::from(vec!["g", "g"])),
                ],
            )?;
            (schema, batch)
        }
        RangeSelectPartialFixture::Builtins => {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("value", DataType::Float64, true),
                Field::new("group", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        0, 5_000,
                    ])),
                    Arc::new(arrow::array::Float64Array::from(vec![1.0, 3.0])),
                    Arc::new(arrow::array::StringArray::from(vec!["g", "g"])),
                ],
            )?;
            (schema, batch)
        }
    };
    let raw_exec: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![batch]], raw_schema, None)?,
    )));
    let session = SessionContext::new();
    let state = session.state();
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
        .collect::<DfResult<Vec<_>>>()?;
    let projected_exec: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::projection::ProjectionExec::try_new(expressions, raw_exec)?,
    );
    let partial_exec = partial.to_execution_plan(partial.input.as_ref(), projected_exec, &state)?;
    let physical_schema = partial_exec.schema();
    let batches = datafusion::physical_plan::collect(partial_exec, session.task_ctx()).await?;
    Ok(RangeSelectPartialExecFixture {
        logical_schema: partial.schema.clone(),
        physical_schema,
        batches,
    })
}

fn is_pushdown_safe_input(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => is_pushdown_safe_input(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => is_pushdown_safe_input(filter.input.as_ref()),
        LogicalPlan::TableScan(_) => true,
        _ => false,
    }
}

fn canonical_aggregate_kind(expr: &Expr) -> Option<RangeAggregateKind> {
    let Expr::AggregateFunction(function) = expr else {
        return None;
    };
    if function.params.distinct
        || function.params.filter.is_some()
        || !function.params.order_by.is_empty()
        || function.params.null_treatment.is_some()
    {
        return None;
    }
    let kind = canonical_aggregate_udf_kind(&function.func)?;
    if kind == RangeAggregateKind::Count
        && !matches!(function.params.args.as_slice(), [Expr::Column(_)])
    {
        return None;
    }
    Some(kind)
}

fn canonical_aggregate_udf_kind(
    function: &datafusion_expr::AggregateUDF,
) -> Option<RangeAggregateKind> {
    let inner = function.inner().as_any();
    if inner.is::<datafusion::functions_aggregate::min_max::Min>() {
        Some(RangeAggregateKind::Min)
    } else if inner.is::<datafusion::functions_aggregate::min_max::Max>() {
        Some(RangeAggregateKind::Max)
    } else if inner.is::<datafusion::functions_aggregate::sum::Sum>() {
        Some(RangeAggregateKind::Sum)
    } else if inner.is::<datafusion::functions_aggregate::average::Avg>() {
        Some(RangeAggregateKind::Avg)
    } else if inner.is::<datafusion::functions_aggregate::count::Count>() {
        Some(RangeAggregateKind::Count)
    } else {
        None
    }
}

#[allow(dead_code)]
fn aggregate_expr_for_wire_kind(kind: RangeAggregateKind, argument: Expr) -> Expr {
    match kind {
        RangeAggregateKind::Min => min(argument),
        RangeAggregateKind::Max => max(argument),
        RangeAggregateKind::Sum => sum(argument),
        RangeAggregateKind::Count => count(argument),
        RangeAggregateKind::Avg => avg(argument),
    }
}

fn column_expr(schema: &DFSchema, index: usize) -> Expr {
    let (qualifier, field) = schema.qualified_field(index);
    Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
}

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

fn expr_is_projection_column(expr: &Expr, schema: &DFSchema, index: usize) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };
    if index >= schema.fields().len() {
        return false;
    }
    let (qualifier, field) = schema.qualified_field(index);
    column.relation.as_ref() == qualifier && column.name == *field.name()
}

fn projection_column_name(schema: &DFSchema, index: usize) -> Option<String> {
    if index >= schema.fields().len() {
        return None;
    }
    let (qualifier, field) = schema.qualified_field(index);
    if qualifier.is_some() {
        return None;
    }
    Some(field.name().clone())
}

fn validate_direct_projection_column(
    expr: &Expr,
    child_schema: &DFSchema,
    output_schema: &DFSchema,
    slot: usize,
    kind: &str,
) -> DataFusionResult<()> {
    if slot >= output_schema.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "RangeSelect Partial wire {kind} Projection slot must be a direct column"
        )));
    }
    let (output_qualifier, output_field) = output_schema.qualified_field(slot);
    if let Expr::Alias(alias) = expr {
        let Expr::Column(column) = alias.expr.as_ref() else {
            return Err(DataFusionError::Plan(format!(
                "RangeSelect Partial wire {kind} Projection alias must wrap a direct column"
            )));
        };
        let (_, field) = Expr::Column(column.clone()).to_field(child_schema)?;
        if alias.relation.is_some()
            || alias.name != *field.name()
            || output_qualifier.is_some()
            || *output_field.name() != alias.name
            || output_field.data_type() != field.data_type()
            || output_field.is_nullable() != field.is_nullable()
            || output_field.metadata() != field.metadata()
        {
            return Err(DataFusionError::Plan(format!(
                "RangeSelect Partial wire {kind} Projection alias does not preserve an unqualified field contract"
            )));
        }
        return Ok(());
    }
    if !matches!(expr, Expr::Column(_)) {
        return Err(DataFusionError::Plan(format!(
            "RangeSelect Partial wire {kind} Projection slot must be a direct column"
        )));
    }
    let (qualifier, field) = expr.to_field(child_schema)?;
    if !FieldContract::from_qualified_field(qualifier.as_ref(), &field)
        .matches_qualified_field(output_qualifier, output_field)
    {
        return Err(DataFusionError::Plan(format!(
            "RangeSelect Partial wire {kind} Projection slot does not preserve its field contract"
        )));
    }
    Ok(())
}

fn validate_materialized_projection_argument(
    expr: &Expr,
    child_schema: &DFSchema,
    output_schema: &DFSchema,
    slot: usize,
    expected_name: &str,
    kind: RangeAggregateKind,
) -> DataFusionResult<()> {
    let (inner, is_local_alias) = match expr {
        Expr::Alias(alias) if alias.name == expected_name => (alias.expr.as_ref(), true),
        Expr::Alias(_) => {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial wire argument Projection alias is invalid".to_string(),
            ));
        }
        other => (other, false),
    };
    if kind == RangeAggregateKind::Count && !matches!(inner, Expr::Column(_)) {
        return Err(DataFusionError::Plan(
            "RangeSelect Partial wire COUNT argument must be a direct column".to_string(),
        ));
    }
    if slot >= output_schema.fields().len() {
        return Err(DataFusionError::Plan(
            "RangeSelect Partial wire argument Projection slot is invalid".to_string(),
        ));
    }
    let (qualifier, field) = expr.to_field(child_schema)?;
    let (output_qualifier, output_field) = output_schema.qualified_field(slot);
    let output_is_expected = output_qualifier.is_none() && output_field.name() == expected_name;
    let field_contract_matches = if is_local_alias {
        qualifier.is_none()
            && field.name() == expected_name
            && FieldContract::from_qualified_field(qualifier.as_ref(), &field)
                .matches_qualified_field(output_qualifier, output_field)
    } else {
        field.data_type() == output_field.data_type()
            && field.is_nullable() == output_field.is_nullable()
            && field.metadata() == output_field.metadata()
    };
    if !output_is_expected || !field_contract_matches {
        return Err(DataFusionError::Plan(
            "RangeSelect Partial wire argument Projection slot does not preserve its field contract"
                .to_string(),
        ));
    }
    Ok(())
}

impl UserDefinedLogicalNodeCore for RangeSelect {
    fn name(&self) -> &str {
        if self.partial_wire_metadata().is_some() {
            crate::range_select::serializer::RANGE_SELECT_PARTIAL_NODE_NAME
        } else {
            "RangeSelect"
        }
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
        let (range_exprs, time_and_by) = exprs.split_at(self.range_expr.len());
        let (time_expr, by) = time_and_by.split_first().ok_or_else(|| {
            DataFusionError::Plan("RangeSelect: missing time expression".to_string())
        })?;
        let range_expr = range_exprs
            .iter()
            .zip(&self.range_expr)
            .map(|(e, range)| RangeFn {
                name: range.name.clone(),
                data_type: range.data_type.clone(),
                expr: e.clone(),
                range: range.range,
                fill: range.fill.clone(),
                need_cast: range.need_cast,
            })
            .collect();
        let input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Plan("RangeSelect expects exactly one input, got 0".to_string())
        })?;
        let input = Arc::new(input);
        match &self.mode {
            RangeSelectMode::Complete => {
                self.rebuild_complete(input, range_expr, time_expr.clone(), by.to_vec())
            }
            RangeSelectMode::Partial(spec) => {
                self.rebuild_partial(input, range_expr, time_expr.clone(), by.to_vec(), spec)
            }
            RangeSelectMode::Final(spec) => {
                self.rebuild_final(input, range_expr, time_expr.clone(), by.to_vec(), spec)
            }
        }
    }
}

impl RangeSelect {
    fn rebuild_complete(
        &self,
        input: Arc<LogicalPlan>,
        range_expr: Vec<RangeFn>,
        time_expr: Expr,
        by: Vec<Expr>,
    ) -> DataFusionResult<Self> {
        let mut fields = range_expr
            .iter()
            .map(|range| {
                Ok((
                    None,
                    Arc::new(Field::new(
                        range.name.clone(),
                        range.data_type.clone(),
                        !matches!(range.fill, Some(Fill::Const(..))),
                    )),
                ))
            })
            .collect::<DfResult<Vec<_>>>()?;
        let time_field = time_expr.to_field(input.schema().as_ref())?;
        let time_index = time_field.1.name().clone();
        fields.push(time_field);
        let by_fields = exprlist_to_fields(&by, &input)?;
        fields.extend(by_fields.clone());
        let metadata = input.schema().metadata().clone();
        let schema_before_project =
            Arc::new(DFSchema::new_with_metadata(fields, metadata.clone())?);
        let by_schema = Arc::new(DFSchema::new_with_metadata(by_fields, metadata.clone())?);
        let schema = schema_from_projection(
            &schema_before_project,
            self.schema_project.as_deref(),
            &metadata,
        )?;

        Ok(Self {
            input,
            range_expr,
            align: self.align,
            align_to: self.align_to,
            time_index,
            time_expr,
            by,
            schema,
            by_schema,
            schema_project: self.schema_project.clone(),
            schema_before_project,
            mode: RangeSelectMode::Complete,
        })
    }

    fn rebuild_partial(
        &self,
        input: Arc<LogicalPlan>,
        range_expr: Vec<RangeFn>,
        time_expr: Expr,
        by: Vec<Expr>,
        spec: &PartialRangeSelectSpec,
    ) -> DataFusionResult<Self> {
        if range_expr.len() != spec.state_columns.len()
            || range_expr.len() != spec.state_types.len()
            || by.len() != spec.by_fields.len()
            || spec.wire.functions.is_empty()
            || spec.wire.functions.len() != range_expr.len()
            || spec.wire.by_indices.len() != by.len()
            || self.align != spec.wire.align
            || self.align_to != spec.wire.align_to
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial expressions do not match its contract".to_string(),
            ));
        }

        if !matches!(input.as_ref(), LogicalPlan::Projection(_)) {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial requires a materialization Projection input".to_string(),
            ));
        }
        if !expr_is_projection_column(&time_expr, input.schema(), spec.wire.time_index) {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial time expression does not match its wire index".to_string(),
            ));
        }
        if by
            .iter()
            .zip(&spec.wire.by_indices)
            .any(|(expr, index)| !expr_is_projection_column(expr, input.schema(), *index))
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial BY expressions do not match their wire indices".to_string(),
            ));
        }

        let time_field = time_expr.to_field(input.schema().as_ref())?;
        let by_fields = exprlist_to_fields(&by, &input)?;
        let actual_by_fields = qualified_field_contracts(&by_fields);
        if actual_by_fields != spec.by_fields {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial BY expressions do not match its contract".to_string(),
            ));
        }

        let mut fields = Vec::with_capacity(range_expr.len() + 1 + by_fields.len());
        for (range_index, (range, (state_column, state_type))) in range_expr
            .iter()
            .zip(spec.state_columns.iter().zip(&spec.state_types))
            .enumerate()
        {
            let wire_function = &spec.wire.functions[range_index];
            if range.range != wire_function.range {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial range does not match its wire contract".to_string(),
                ));
            }
            let Expr::Alias(alias) = &range.expr else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state expressions must be aliases".to_string(),
                ));
            };
            let Expr::AggregateFunction(aggregate) = alias.expr.as_ref() else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state aliases must wrap aggregate functions".to_string(),
                ));
            };
            let Some(state_wrapper) = aggregate
                .func
                .inner()
                .as_any()
                .downcast_ref::<common_function::aggrs::aggr_wrapper::StateWrapper>(
            ) else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state aggregate must be a StateWrapper".to_string(),
                ));
            };
            if canonical_aggregate_udf_kind(state_wrapper.inner()) != Some(wire_function.kind) {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial StateWrapper identity does not match its wire contract"
                        .to_string(),
                ));
            }
            if aggregate.params.distinct
                || aggregate.params.filter.is_some()
                || !aggregate.params.order_by.is_empty()
                || aggregate.params.null_treatment.is_some()
            {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state aggregate has unsupported parameters".to_string(),
                ));
            }
            let argument_index = wire_function.argument_index;
            let Some(argument_name) = projection_column_name(input.schema(), argument_index) else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state argument wire index is invalid".to_string(),
                ));
            };
            if !matches!(
                aggregate.params.args.as_slice(),
                [Expr::Column(column)] if column.relation.is_none() && column.name == argument_name
            ) {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state aggregate does not consume its materialized argument"
                        .to_string(),
                ));
            }
            let (qualifier, field) = range.expr.to_field(input.schema().as_ref())?;
            if alias.name != *state_column
                || qualifier.is_some()
                || field.name() != state_column
                || !matches!(field.data_type(), DataType::Struct(_))
                || field.data_type() != state_type
            {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial state expression does not match its contract".to_string(),
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
                time_field.1.is_nullable(),
            )),
        ));
        fields.extend(by_fields.clone());
        let metadata = input.schema().metadata().clone();
        let schema = Arc::new(DFSchema::new_with_metadata(fields, metadata.clone())?);
        let by_schema = Arc::new(DFSchema::new_with_metadata(by_fields, metadata)?);

        Ok(Self {
            input,
            range_expr,
            align: self.align,
            align_to: self.align_to,
            time_index: time_field.1.name().clone(),
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
        &self,
        input: Arc<LogicalPlan>,
        range_expr: Vec<RangeFn>,
        time_expr: Expr,
        by: Vec<Expr>,
        spec: &FinalRangeSelectSpec,
    ) -> DataFusionResult<Self> {
        if range_expr.len() != spec.state_columns.len()
            || range_expr.len() != spec.state_types.len()
            || range_expr.len() != spec.raw_merge_result_fields.len()
            || range_expr.len() != spec.legacy_range_fields.len()
            || by.len() != spec.by_input_fields.len()
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Final expressions do not match its contract".to_string(),
            ));
        }

        let input_schema = input.schema();
        for (state_column, state_type) in spec.state_columns.iter().zip(&spec.state_types) {
            let index = input_schema
                .index_of_column_by_name(None, state_column)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "RangeSelect Final is missing state column {state_column}"
                    ))
                })?;
            let (qualifier, field) = input_schema.qualified_field(index);
            if qualifier.is_some() || field.data_type() != state_type || field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "RangeSelect Final state column {state_column} has an invalid contract"
                )));
            }
        }
        let bucket_index = input_schema
            .index_of_column_by_name(None, &spec.bucket_column)
            .ok_or_else(|| {
                DataFusionError::Plan("RangeSelect Final is missing bucket column".to_string())
            })?;
        let (bucket_qualifier, bucket_field) = input_schema.qualified_field(bucket_index);
        if bucket_qualifier.is_some()
            || bucket_field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Final bucket must be an unqualified TimestampMillisecond".to_string(),
            ));
        }

        for (range, (state_column, raw_merge_result_field)) in range_expr
            .iter()
            .zip(spec.state_columns.iter().zip(&spec.raw_merge_result_fields))
        {
            let Expr::Alias(alias) = &range.expr else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final merge expressions must be aliases".to_string(),
                ));
            };
            let Expr::AggregateFunction(aggregate) = alias.expr.as_ref() else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final merge aliases must wrap aggregate functions".to_string(),
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
                || aggregate.params.args.len() != 1
            {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final merge expression does not match its contract".to_string(),
                ));
            }
            let Expr::Column(column) = &aggregate.params.args[0] else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final merge aggregate must consume a state column".to_string(),
                ));
            };
            if column.relation.is_some() || column.name != *state_column {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final merge aggregate references an invalid state column"
                        .to_string(),
                ));
            }
            let (qualifier, field) = range.expr.to_field(input_schema.as_ref())?;
            if !raw_merge_result_field.matches_qualified_field(qualifier.as_ref(), &field) {
                return Err(DataFusionError::Plan(format!(
                    "RangeSelect Final merge expression for {state_column} does not match the raw output contract"
                )));
            }
        }
        let Expr::Column(time_column) = &time_expr else {
            return Err(DataFusionError::Plan(
                "RangeSelect Final time expression must be a bucket column".to_string(),
            ));
        };
        if time_column.relation.is_some() || time_column.name != spec.bucket_column {
            return Err(DataFusionError::Plan(
                "RangeSelect Final time expression references an invalid bucket column".to_string(),
            ));
        }

        for (expr, expected) in by.iter().zip(&spec.by_input_fields) {
            let Expr::Column(column) = expr else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final BY expressions must be direct column references".to_string(),
                ));
            };
            let index = input_schema
                .index_of_column_by_name(column.relation.as_ref(), &column.name)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "RangeSelect Final is missing BY column {}",
                        column.flat_name()
                    ))
                })?;
            let (qualifier, field) = input_schema.qualified_field(index);
            if !expected.matches_qualified_field(qualifier, field) {
                return Err(DataFusionError::Plan(
                    "RangeSelect Final BY expression does not match its contract".to_string(),
                ));
            }
        }

        let mut fields = spec
            .legacy_range_fields
            .iter()
            .map(FieldContract::to_qualified_field)
            .collect::<Vec<_>>();
        fields.push(spec.legacy_time_field.to_qualified_field());
        fields.extend(
            spec.legacy_by_fields
                .iter()
                .map(FieldContract::to_qualified_field),
        );
        let metadata: HashMap<_, _> = spec.legacy_metadata.clone().into_iter().collect();
        let schema_before_project =
            Arc::new(DFSchema::new_with_metadata(fields, metadata.clone())?);
        let by_schema = Arc::new(DFSchema::new_with_metadata(
            spec.legacy_by_fields
                .iter()
                .map(FieldContract::to_qualified_field)
                .collect(),
            metadata.clone(),
        )?);
        let schema = schema_from_projection(
            &schema_before_project,
            spec.schema_project.as_deref(),
            &metadata,
        )?;

        Ok(Self {
            input,
            range_expr,
            align: self.align,
            align_to: self.align_to,
            time_index: self.time_index.clone(),
            time_expr,
            by,
            schema,
            by_schema,
            schema_project: spec.schema_project.clone(),
            schema_before_project,
            mode: RangeSelectMode::Final(spec.clone()),
        })
    }
}

fn qualified_field_contracts(
    fields: &[(Option<datafusion_common::TableReference>, Arc<Field>)],
) -> Vec<FieldContract> {
    fields
        .iter()
        .map(|(qualifier, field)| FieldContract::from_qualified_field(qualifier.as_ref(), field))
        .collect()
}

fn schema_from_projection(
    schema_before_project: &DFSchemaRef,
    schema_project: Option<&[usize]>,
    metadata: &HashMap<String, String>,
) -> DataFusionResult<DFSchemaRef> {
    let Some(schema_project) = schema_project else {
        return Ok(schema_before_project.clone());
    };
    let fields = schema_project
        .iter()
        .map(|index| {
            if *index >= schema_before_project.fields().len() {
                return Err(DataFusionError::Plan(format!(
                    "RangeSelect schema_project index {index} is out of bounds"
                )));
            }
            let (qualifier, field) = schema_before_project.qualified_field(*index);
            Ok((qualifier.cloned(), field.clone()))
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        metadata.clone(),
    )?))
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
                "RangeSelect Partial must not apply a schema projection".to_string(),
            ));
        }
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
        let by = self.create_physical_expr_list(false, &self.by, input_dfschema, session_state)?;
        let schema = physical_schema(&self.schema);
        let schema_before_project = physical_schema(&self.schema_before_project);
        let by_schema = physical_schema(&self.by_schema);
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
            by_schema,
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

/// Physical RangeSelect role. `Partial` exposes one non-null `Struct` state
/// column per range function; `Final` consumes the same layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeSelectExecMode {
    Complete,
    Partial,
    Final,
}

fn physical_schema(schema: &DFSchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
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

#[cfg(test)]
impl RangeSelectExec {
    pub(crate) fn mode(&self) -> RangeSelectExecMode {
        self.mode
    }
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
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Plan("RangeSelectExec expects exactly one child, got 0".to_string())
        })?;
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
                .map(|f| SortField::new(f.data_type().clone()))
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
            series_hashes: HashMap::new(),
            next_series_id: 0,
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
    /// Hashes only select a collision bucket. Every candidate's actual encoded
    /// BY row is compared before it is reused.
    series_hashes: HashMap<u64, Vec<u64>>,
    next_series_id: u64,
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
    fn resolve_series_id(&mut self, hash: u64, row: OwnedRow) -> u64 {
        if let Some(ids) = self.series_hashes.get(&hash) {
            if let Some(id) = ids.iter().find(|id| {
                self.series_map
                    .get(id)
                    .is_some_and(|series| series.row == row)
            }) {
                return *id;
            }
        }
        let id = self.next_series_id;
        self.next_series_id += 1;
        self.series_hashes.entry(hash).or_default().push(id);
        self.series_map.insert(
            id,
            SeriesState {
                row,
                align_ts_accumulator: BTreeMap::new(),
            },
        );
        id
    }
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
        let metric = self.metric.clone();
        let _timer = metric.elapsed_compute().timer();
        if self.mode == RangeSelectExecMode::Final {
            return self.merge_partial_context(batch);
        }
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
        let series_ids = (0..num_rows)
            .map(|row| self.resolve_series_id(hashes[row], by_rows.row(row).owned()))
            .collect::<Vec<_>>();
        for i in 0..self.range_exec.len() {
            let args = self.evaluate_many(&batch, &self.range_exec[i].expressions())?;
            // use self.modify_map record (hash, align_ts) => [row_nums]
            produce_align_time(
                self.align_to,
                self.range_exec[i].range,
                self.align,
                ts_column_ref,
                &series_ids,
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
                |((hash, ts, _row), offset)| {
                    let (offset, length) = (offset[0], offset[1] - offset[0]);
                    let sliced_arrays: Vec<ArrayRef> = args
                        .iter()
                        .map(|array| array.slice(offset, length))
                        .collect();
                    let accumulators_map = self.series_map.get_mut(hash).ok_or_else(|| {
                        DataFusionError::Internal(
                            "RangeSelect lost a resolved collision-safe series id".into(),
                        )
                    })?;
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
        let by_arrays = self.evaluate_many(&batch, &self.by)?;
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

        for row in 0..num_rows {
            let series_id = self.resolve_series_id(hashes[row], by_rows.row(row).owned());
            let bucket = timestamp.value(row);
            let series = self.series_map.get_mut(&series_id).ok_or_else(|| {
                DataFusionError::Internal(
                    "RangeSelect lost a resolved collision-safe series id".into(),
                )
            })?;
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
                let state = &states[0];
                let state = state
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
        columns.extend(self.row_converter.convert_rows(by_rows)?);
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
        let timestamp = compute::cast(
            &timestamps.finish(),
            self.schema.field(columns.len()).data_type(),
        )?;
        columns.push(timestamp);
        columns.extend(self.row_converter.convert_rows(by_rows)?);
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

    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use arrow_schema::SortOptions;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
    use datafusion::functions_aggregate::min_max;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::{SessionContext, col};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::Column;
    use datatypes::arrow::array::{
        Float64Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use datatypes::arrow_array::StringArray;

    use super::*;

    #[test]
    fn range_equal_align_bucket_starts_match_date_bin_floor() {
        let cases = [
            (0, vec![-1, 0, 1, 4_999, 5_000], vec![-5_000, 0, 5_000]),
            (
                2_000,
                vec![1_999, 2_000, 6_999, 7_000],
                vec![-3_000, 2_000, 7_000],
            ),
        ];

        for (origin, timestamps, expected) in cases {
            let timestamps = TimestampMillisecondArray::from(timestamps);
            let mut modify_map = HashMap::new();
            produce_align_time(
                origin,
                5_000,
                5_000,
                &timestamps,
                &vec![0; timestamps.len()],
                &mut modify_map,
            );
            let actual = modify_map
                .keys()
                .map(|(_, align_ts)| *align_ts)
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, expected.into_iter().collect());
        }
    }

    #[tokio::test]
    async fn range_select_serializer_fixture_has_real_stages_and_partial_execution() {
        let fixture = range_select_split_fixture(RangeSelectPartialFixture::CompoundAvg);
        let LogicalPlan::Extension(complete_extension) = &fixture.complete else {
            unreachable!();
        };
        let LogicalPlan::Extension(partial_extension) = &fixture.partial else {
            unreachable!();
        };
        let LogicalPlan::Extension(final_extension) = &fixture.final_plan else {
            unreachable!();
        };
        let complete = complete_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap();
        let partial = partial_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap();
        let final_node = final_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap();
        assert!(matches!(complete.mode, RangeSelectMode::Complete));
        assert!(matches!(partial.mode, RangeSelectMode::Partial(_)));
        assert!(matches!(final_node.mode, RangeSelectMode::Final(_)));

        let collected = collect_range_select_partial_fixture(
            &fixture.partial,
            RangeSelectPartialFixture::CompoundAvg,
        )
        .await
        .unwrap();
        assert_eq!(
            collected.physical_schema,
            physical_schema(&collected.logical_schema)
        );
        let batch = collected.batches.first().unwrap();
        assert!(batch.num_rows() > 0);
        assert!(matches!(
            batch.schema().field(0).data_type(),
            DataType::Struct(_)
        ));
        assert!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .is_some_and(|state| state.null_count() == 0)
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(batch.schema().field(2).name(), "group");
    }

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
            EmissionType::Incremental,
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
            EmissionType::Incremental,
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

    fn empty_input(
        fields: Vec<(Option<datafusion_common::TableReference>, Arc<Field>)>,
    ) -> LogicalPlan {
        assert!(fields.iter().all(|(qualifier, _)| qualifier.is_none()));
        range_select_test_scan(fields.into_iter().map(|(_, field)| field).collect())
    }

    fn input_field(name: &str, data_type: DataType, nullable: bool) -> Arc<Field> {
        Arc::new(Field::new(name, data_type, nullable))
    }

    fn state_type() -> DataType {
        DataType::Struct(vec![input_field("value", DataType::Int64, true)].into())
    }

    fn complete_node(input: LogicalPlan) -> RangeSelect {
        RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "result".to_string(),
                data_type: DataType::Int64,
                expr: Expr::Column(datafusion_common::Column::new_unqualified("value")),
                range: Duration::from_secs(1),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(1),
            0,
            Expr::Column(datafusion_common::Column::new_unqualified("timestamp")),
            vec![Expr::Column(datafusion_common::Column::new_unqualified(
                "host",
            ))],
            &[],
        )
        .unwrap()
    }

    fn split_range_node(
        aggregate: Expr,
        data_type: DataType,
        fill: Option<Fill>,
        need_cast: bool,
    ) -> (RangeSelect, RangeSelect) {
        let input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Int64, true)),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        let complete = RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "result".to_string(),
                data_type,
                expr: aggregate,
                range: Duration::from_secs(1),
                fill,
                need_cast,
            }],
            Duration::from_secs(1),
            0,
            Expr::Column(datafusion_common::Column::new_unqualified("timestamp")),
            vec![Expr::Column(datafusion_common::Column::new_unqualified(
                "host",
            ))],
            &[],
        )
        .unwrap();
        let LogicalPlan::Extension(final_extension) = complete.try_split_for_pushdown().unwrap()
        else {
            unreachable!();
        };
        let final_node = final_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        let LogicalPlan::Extension(partial_extension) = final_node.input.as_ref() else {
            unreachable!();
        };
        let partial_node = partial_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        (partial_node, final_node)
    }

    fn overlap_avg_split_nodes() -> (RangeSelect, RangeSelect, RangeSelect) {
        let input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value_a", DataType::Float64, true)),
            (None, input_field("value_b", DataType::Float64, true)),
            (None, input_field("group", DataType::Utf8, true)),
        ]);
        let range_expr = avg(col("value_a") + col("value_b"));
        let time_expr = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"));
        let by_expr = Expr::Column(datafusion_common::Column::new_unqualified("group"));
        let complete = RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "result".to_string(),
                data_type: DataType::Float64,
                expr: range_expr.clone(),
                range: Duration::from_secs(10),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            time_expr.clone(),
            vec![by_expr.clone()],
            &[range_expr, time_expr, by_expr],
        )
        .unwrap();
        let LogicalPlan::Extension(final_extension) = complete.try_split_for_pushdown().unwrap()
        else {
            unreachable!();
        };
        let final_node = final_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        let LogicalPlan::Extension(partial_extension) = final_node.input.as_ref() else {
            unreachable!();
        };
        let partial_node = partial_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        (complete, partial_node, final_node)
    }

    fn overlap_avg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("value_a", DataType::Float64, true),
            Field::new("value_b", DataType::Float64, true),
            Field::new("group", DataType::Utf8, true),
        ]))
    }

    fn overlap_avg_batch(rows: &[(i64, f64, f64)]) -> RecordBatch {
        let timestamps = TimestampMillisecondArray::from(
            rows.iter()
                .map(|(timestamp, _, _)| *timestamp)
                .collect::<Vec<_>>(),
        );
        let value_a = Float64Array::from(
            rows.iter()
                .map(|(_, value_a, _)| *value_a)
                .collect::<Vec<_>>(),
        );
        let value_b = Float64Array::from(
            rows.iter()
                .map(|(_, _, value_b)| *value_b)
                .collect::<Vec<_>>(),
        );
        let groups = StringArray::from(vec!["g"; rows.len()]);
        RecordBatch::try_new(
            overlap_avg_schema(),
            vec![
                Arc::new(timestamps),
                Arc::new(value_a),
                Arc::new(value_b),
                Arc::new(groups),
            ],
        )
        .unwrap()
    }

    fn memory_exec(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], schema, None).unwrap(),
        )))
    }

    fn materialized_partial_input_exec(
        partial: &RangeSelect,
        raw_input: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
    ) -> Arc<dyn ExecutionPlan> {
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            panic!("Partial input must be a materialization Projection");
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
                        session_state.execution_props(),
                    )?,
                    projection.schema.field(index).name().clone(),
                ))
            })
            .collect::<DfResult<Vec<_>>>()
            .unwrap();
        Arc::new(
            datafusion::physical_plan::projection::ProjectionExec::try_new(expressions, raw_input)
                .unwrap(),
        )
    }

    fn sorted_range_rows(batches: &[RecordBatch]) -> Vec<(String, i64, f64)> {
        let mut rows = batches
            .iter()
            .filter(|batch| batch.num_rows() != 0)
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let timestamps = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let groups = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|index| {
                        (
                            groups.value(index).to_string(),
                            timestamps.value(index),
                            values.value(index),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        rows
    }

    fn split_builtin_nodes(
        range: Duration,
        align: Duration,
    ) -> (RangeSelect, RangeSelect, RangeSelect) {
        let input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Float64, true)),
            (None, input_field("group", DataType::Utf8, true)),
        ]);
        let range_expr = vec![
            ("min", min(col("value")), DataType::Float64),
            ("max", max(col("value")), DataType::Float64),
            ("sum", sum(col("value")), DataType::Float64),
            ("count", count(col("value")), DataType::Int64),
            ("avg", avg(col("value")), DataType::Float64),
        ];
        let time_expr = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"));
        let by_expr = Expr::Column(datafusion_common::Column::new_unqualified("group"));
        let projection_expr = range_expr
            .iter()
            .map(|(_, expr, _)| expr.clone())
            .chain([time_expr.clone(), by_expr.clone()])
            .collect::<Vec<_>>();
        let complete = RangeSelect::try_new(
            Arc::new(input),
            range_expr
                .into_iter()
                .map(|(name, expr, data_type)| RangeFn {
                    name: name.to_string(),
                    data_type,
                    expr,
                    range,
                    fill: None,
                    need_cast: false,
                })
                .collect(),
            align,
            0,
            time_expr,
            vec![by_expr],
            &projection_expr,
        )
        .unwrap();
        let LogicalPlan::Extension(final_extension) = complete.try_split_for_pushdown().unwrap()
        else {
            unreachable!();
        };
        let final_node = final_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        let LogicalPlan::Extension(partial_extension) = final_node.input.as_ref() else {
            unreachable!();
        };
        let partial_node = partial_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        (complete, partial_node, final_node)
    }

    fn builtin_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("group", DataType::Utf8, true),
        ]))
    }

    fn builtin_batch(rows: &[(i64, Option<f64>)]) -> RecordBatch {
        let timestamps = TimestampMillisecondArray::from(
            rows.iter()
                .map(|(timestamp, _)| *timestamp)
                .collect::<Vec<_>>(),
        );
        let values = Float64Array::from(rows.iter().map(|(_, value)| *value).collect::<Vec<_>>());
        let groups = StringArray::from(vec!["g"; rows.len()]);
        RecordBatch::try_new(
            builtin_schema(),
            vec![Arc::new(timestamps), Arc::new(values), Arc::new(groups)],
        )
        .unwrap()
    }

    fn sorted_builtin_rows(batches: &[RecordBatch]) -> Vec<(String, i64, Vec<ScalarValue>)> {
        let mut rows = batches
            .iter()
            .filter(|batch| batch.num_rows() != 0)
            .flat_map(|batch| {
                let timestamps = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let groups = batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|index| {
                        (
                            groups.value(index).to_string(),
                            timestamps.value(index),
                            (0..5)
                                .map(|column| {
                                    ScalarValue::try_from_array(batch.column(column), index)
                                })
                                .collect::<DfResult<Vec<_>>>()
                                .unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        rows
    }

    async fn execute_builtin_split(
        complete: &RangeSelect,
        partial: &RangeSelect,
        final_node: &RangeSelect,
        partial_inputs: Vec<RecordBatch>,
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>) {
        execute_split(
            complete,
            partial,
            final_node,
            builtin_schema(),
            partial_inputs,
            &["value"],
        )
        .await
    }

    async fn execute_split(
        complete: &RangeSelect,
        partial: &RangeSelect,
        final_node: &RangeSelect,
        input_schema: SchemaRef,
        partial_inputs: Vec<RecordBatch>,
        raw_value_columns: &[&str],
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>) {
        let session = SessionContext::new();
        let state = session.state();
        let complete_exec = complete
            .to_execution_plan(
                complete.input.as_ref(),
                memory_exec(input_schema.clone(), partial_inputs.clone()),
                &state,
            )
            .unwrap();
        let complete_output = datafusion::physical_plan::collect(complete_exec, session.task_ctx())
            .await
            .unwrap();
        let mut partial_output = Vec::new();
        for input in partial_inputs {
            let raw_input = memory_exec(input_schema.clone(), vec![input]);
            let partial_exec = partial
                .to_execution_plan(
                    partial.input.as_ref(),
                    materialized_partial_input_exec(partial, raw_input, &state),
                    &state,
                )
                .unwrap();
            assert_eq!(partial_exec.schema(), physical_schema(&partial.schema));
            partial_output.extend(
                datafusion::physical_plan::collect(partial_exec, session.task_ctx())
                    .await
                    .unwrap(),
            );
        }
        assert!(
            final_node
                .input
                .schema()
                .fields()
                .iter()
                .all(|field| raw_value_columns.iter().all(|name| field.name() != *name))
        );
        let final_exec = final_node
            .to_execution_plan(
                final_node.input.as_ref(),
                memory_exec(physical_schema(&partial.schema), partial_output.clone()),
                &state,
            )
            .unwrap();
        assert_eq!(final_exec.schema(), physical_schema(&final_node.schema));
        let final_output = datafusion::physical_plan::collect(final_exec, session.task_ctx())
            .await
            .unwrap();
        (complete_output, partial_output, final_output)
    }

    #[test]
    fn split_materializes_compound_avg_argument_with_stable_wire_index() {
        let (_, partial, _) = overlap_avg_split_nodes();
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            panic!("Partial input must be a materialization Projection");
        };
        assert_eq!(projection.schema.fields().len(), 3);
        assert_eq!(projection.schema.field(0).name(), "timestamp");
        assert_eq!(projection.schema.field(1).name(), "group");
        assert_eq!(projection.schema.field(2).name(), "__range_arg_0");
        assert!(matches!(projection.expr[2], Expr::Alias(_)));
        let Expr::Alias(state_alias) = &partial.range_expr[0].expr else {
            panic!("Partial state must be aliased");
        };
        let Expr::AggregateFunction(state) = state_alias.expr.as_ref() else {
            panic!("Partial state must wrap an aggregate");
        };
        assert!(matches!(
            state.params.args.as_slice(),
            [Expr::Column(column)] if column.relation.is_none() && column.name == "__range_arg_0"
        ));
        let wire = partial.partial_wire_metadata().unwrap();
        assert_eq!(wire.time_index(), 0);
        assert_eq!(wire.by_indices(), &[1]);
        assert_eq!(wire.align(), Duration::from_secs(5));
        assert_eq!(wire.align_to(), 0);
        assert_eq!(wire.functions().len(), 1);
        assert_eq!(wire.functions()[0].argument_index(), 2);
        assert_eq!(wire.functions()[0].kind(), RangeAggregateKind::Avg);
        assert_eq!(wire.functions()[0].range(), Duration::from_secs(10));
    }

    #[test]
    fn split_materializes_each_builtin_argument_without_deduplication() {
        let (_, partial, _) = split_builtin_nodes(Duration::from_secs(10), Duration::from_secs(5));
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            panic!("Partial input must be a materialization Projection");
        };
        assert_eq!(
            projection
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "timestamp",
                "group",
                "__range_arg_0",
                "__range_arg_1",
                "__range_arg_2",
                "__range_arg_3",
                "__range_arg_4",
            ]
        );
        let wire = partial.partial_wire_metadata().unwrap();
        assert_eq!(wire.time_index(), 0);
        assert_eq!(wire.by_indices(), &[1]);
        assert_eq!(
            wire.functions()
                .iter()
                .map(|function| (function.argument_index(), function.kind(), function.range()))
                .collect::<Vec<_>>(),
            vec![
                (2, RangeAggregateKind::Min, Duration::from_secs(10)),
                (3, RangeAggregateKind::Max, Duration::from_secs(10)),
                (4, RangeAggregateKind::Sum, Duration::from_secs(10)),
                (5, RangeAggregateKind::Count, Duration::from_secs(10)),
                (6, RangeAggregateKind::Avg, Duration::from_secs(10)),
            ]
        );
        for (index, range) in partial.range_expr.iter().enumerate() {
            let Expr::Alias(alias) = &range.expr else {
                panic!("Partial state must be aliased");
            };
            let Expr::AggregateFunction(state) = alias.expr.as_ref() else {
                panic!("Partial state must wrap an aggregate");
            };
            assert!(matches!(
                state.params.args.as_slice(),
                [Expr::Column(column)] if column.relation.is_none() && column.name == format!("__range_arg_{index}")
            ));
        }
    }

    #[test]
    fn split_rejects_materialized_argument_name_collision() {
        let input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Float64, true)),
            (None, input_field("group", DataType::Utf8, true)),
            (None, input_field("__range_arg_0", DataType::Float64, true)),
        ]);
        let range_expr = sum(col("value"));
        let time_expr = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"));
        let by_expr = Expr::Column(datafusion_common::Column::new_unqualified("group"));
        let complete = RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "sum".to_string(),
                data_type: DataType::Float64,
                expr: range_expr.clone(),
                range: Duration::from_secs(5),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            time_expr.clone(),
            vec![by_expr.clone()],
            &[range_expr, time_expr, by_expr],
        )
        .unwrap();
        assert!(complete.try_split_for_pushdown().is_none());
    }

    #[test]
    fn rebuild_partial_rejects_stale_wire_metadata() {
        let (_, partial, _) = split_builtin_nodes(Duration::from_secs(10), Duration::from_secs(5));
        let assert_rejected = |node: RangeSelect| {
            assert!(
                node.with_exprs_and_inputs(node.expressions(), vec![node.input.as_ref().clone()])
                    .is_err()
            );
        };

        let mut wrong_kind = partial.clone();
        let RangeSelectMode::Partial(spec) = &mut wrong_kind.mode else {
            unreachable!();
        };
        spec.wire.functions[0].kind = RangeAggregateKind::Max;
        assert_rejected(wrong_kind);

        let mut wrong_argument = partial.clone();
        let RangeSelectMode::Partial(spec) = &mut wrong_argument.mode else {
            unreachable!();
        };
        spec.wire.functions[0].argument_index = 0;
        assert_rejected(wrong_argument);

        let mut wrong_range = partial.clone();
        let RangeSelectMode::Partial(spec) = &mut wrong_range.mode else {
            unreachable!();
        };
        spec.wire.functions[0].range = Duration::from_secs(1);
        assert_rejected(wrong_range);

        let mut wrong_align = partial.clone();
        let RangeSelectMode::Partial(spec) = &mut wrong_align.mode else {
            unreachable!();
        };
        spec.wire.align = Duration::from_secs(1);
        assert_rejected(wrong_align);

        let mut wrong_align_to = partial.clone();
        let RangeSelectMode::Partial(spec) = &mut wrong_align_to.mode else {
            unreachable!();
        };
        spec.wire.align_to = 1;
        assert_rejected(wrong_align_to);

        let mut empty_functions = partial;
        let RangeSelectMode::Partial(spec) = &mut empty_functions.mode else {
            unreachable!();
        };
        spec.wire.functions.clear();
        assert_rejected(empty_functions);
    }

    #[test]
    fn partial_from_wire_reconstructs_split_builtin_and_compound_contracts() {
        for (_, partial, _) in [
            split_builtin_nodes(Duration::from_secs(10), Duration::from_secs(5)),
            overlap_avg_split_nodes(),
        ] {
            let rebuilt = RangeSelect::try_new_partial_from_wire(
                partial.input.clone(),
                partial.partial_wire_metadata().unwrap().clone(),
            )
            .unwrap();
            assert_eq!(rebuilt.expressions(), partial.expressions());
            assert_eq!(rebuilt.schema, partial.schema);
            assert_eq!(rebuilt.schema_before_project, partial.schema_before_project);
            assert_eq!(rebuilt.by_schema, partial.by_schema);
            assert_eq!(
                rebuilt.partial_wire_metadata(),
                partial.partial_wire_metadata()
            );
        }
    }

    #[test]
    fn partial_from_wire_accepts_substrait_decoded_bare_argument_shape() {
        let (_, partial, _) = overlap_avg_split_nodes();
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!();
        };
        // DataFusion Substrait ProjectRel decode preserves the output schema but
        // drops Expr::Alias, yielding this canonical decoded shape.
        let mut decoded_projection = projection.clone();
        let Expr::Alias(alias) = &decoded_projection.expr[2] else {
            unreachable!();
        };
        decoded_projection.expr[2] = alias.expr.as_ref().clone();
        let decoded_input = Arc::new(LogicalPlan::Projection(decoded_projection));
        let rebuilt = RangeSelect::try_new_partial_from_wire(
            decoded_input,
            partial.partial_wire_metadata().unwrap().clone(),
        )
        .unwrap();
        assert_eq!(rebuilt.schema, partial.schema);
        assert_eq!(
            rebuilt.partial_wire_metadata(),
            partial.partial_wire_metadata()
        );
    }

    #[test]
    fn partial_from_wire_rejects_malformed_input_and_metadata() {
        let (_, partial, _) = overlap_avg_split_nodes();
        let wire = partial.partial_wire_metadata().unwrap().clone();
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!();
        };
        assert!(
            RangeSelect::try_new_partial_from_wire(projection.input.clone(), wire.clone()).is_err()
        );

        let mut out_of_bounds = wire.clone();
        out_of_bounds.functions[0].argument_index = usize::MAX;
        assert!(
            RangeSelect::try_new_partial_from_wire(partial.input.clone(), out_of_bounds).is_err()
        );

        let mut missing_alias = wire.clone();
        missing_alias.functions[0].argument_index = 1;
        assert!(
            RangeSelect::try_new_partial_from_wire(partial.input.clone(), missing_alias).is_err()
        );

        let mut empty_functions = wire.clone();
        empty_functions.functions.clear();
        assert!(
            RangeSelect::try_new_partial_from_wire(partial.input.clone(), empty_functions).is_err()
        );

        let wrong_time_projection = Arc::new(LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    Expr::Column(datafusion_common::Column::new_unqualified("value_a")),
                    Expr::Column(datafusion_common::Column::new_unqualified("group")),
                    (col("value_a") + col("value_b")).alias("__range_arg_0"),
                ],
                projection.input.clone(),
            )
            .unwrap(),
        ));
        assert!(
            RangeSelect::try_new_partial_from_wire(wrong_time_projection, wire.clone()).is_err()
        );

        let wrong_argument_projection = Arc::new(LogicalPlan::Projection(
            Projection::try_new(
                vec![
                    Expr::Column(datafusion_common::Column::new_unqualified("timestamp")),
                    Expr::Column(datafusion_common::Column::new_unqualified("group")),
                    Expr::Column(datafusion_common::Column::new_unqualified("group"))
                        .alias("__range_arg_0"),
                ],
                projection.input.clone(),
            )
            .unwrap(),
        ));
        assert!(RangeSelect::try_new_partial_from_wire(wrong_argument_projection, wire).is_err());

        let reject_projection = |expr: Vec<Expr>, wire: PartialRangeWireMetadata| {
            let projection = Arc::new(LogicalPlan::Projection(
                Projection::try_new(expr, projection.input.clone()).unwrap(),
            ));
            assert!(RangeSelect::try_new_partial_from_wire(projection, wire).is_err());
        };
        let Expr::Alias(argument_alias) = &projection.expr[2] else {
            unreachable!();
        };
        let mut bare_wrong_output_name = projection.expr.clone();
        bare_wrong_output_name[2] = argument_alias.expr.as_ref().clone();
        reject_projection(
            bare_wrong_output_name,
            partial.partial_wire_metadata().unwrap().clone(),
        );

        let mut wrong_alias = projection.expr.clone();
        wrong_alias[2] =
            Expr::Column(datafusion_common::Column::new_unqualified("value_a")).alias("wrong_arg");
        reject_projection(
            wrong_alias,
            partial.partial_wire_metadata().unwrap().clone(),
        );

        let mut extra_slot = projection.expr.clone();
        extra_slot.push(
            Expr::Column(datafusion_common::Column::new_unqualified("value_a")).alias("__extra"),
        );
        reject_projection(extra_slot, partial.partial_wire_metadata().unwrap().clone());

        let mut time_alias = projection.expr.clone();
        time_alias[0] = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"))
            .alias("timestamp");
        let canonical_time_projection = Arc::new(LogicalPlan::Projection(
            Projection::try_new(time_alias, projection.input.clone()).unwrap(),
        ));
        assert!(
            RangeSelect::try_new_partial_from_wire(
                canonical_time_projection,
                partial.partial_wire_metadata().unwrap().clone(),
            )
            .is_ok()
        );

        let mut by_compound = projection.expr.clone();
        by_compound[1] = (col("value_a") + col("value_b")).alias("group");
        reject_projection(
            by_compound,
            partial.partial_wire_metadata().unwrap().clone(),
        );

        let (_, builtin_partial, _) =
            split_builtin_nodes(Duration::from_secs(10), Duration::from_secs(5));
        let builtin_wire = builtin_partial.partial_wire_metadata().unwrap().clone();
        let LogicalPlan::Projection(builtin_projection) = builtin_partial.input.as_ref() else {
            unreachable!();
        };
        let reject_count_projection = |expr: Vec<Expr>| {
            let projection = Arc::new(LogicalPlan::Projection(
                Projection::try_new(expr, builtin_projection.input.clone()).unwrap(),
            ));
            assert!(
                RangeSelect::try_new_partial_from_wire(projection, builtin_wire.clone()).is_err()
            );
        };
        let mut count_literal = builtin_projection.expr.clone();
        count_literal[5] = lit(1_i64).alias("__range_arg_3");
        reject_count_projection(count_literal);
        let mut count_compound = builtin_projection.expr.clone();
        count_compound[5] = (col("value") + lit(1_i64)).alias("__range_arg_3");
        reject_count_projection(count_compound);

        let mut decoded_count_literal = builtin_projection.clone();
        decoded_count_literal.expr[5] = lit(1_i64);
        assert!(
            RangeSelect::try_new_partial_from_wire(
                Arc::new(LogicalPlan::Projection(decoded_count_literal)),
                builtin_wire.clone(),
            )
            .is_err()
        );
        let mut decoded_count_compound = builtin_projection.clone();
        decoded_count_compound.expr[5] = col("value") + lit(1_i64);
        assert!(
            RangeSelect::try_new_partial_from_wire(
                Arc::new(LogicalPlan::Projection(decoded_count_compound)),
                builtin_wire,
            )
            .is_err()
        );

        assert!(
            PartialRangeWireMetadata::try_new(
                Duration::ZERO,
                0,
                0,
                vec![],
                vec![
                    PartialRangeWireFunction::try_new(
                        RangeAggregateKind::Avg,
                        1,
                        Duration::from_secs(1),
                    )
                    .unwrap()
                ],
            )
            .is_err()
        );
        assert!(
            PartialRangeWireFunction::try_new(RangeAggregateKind::Avg, 1, Duration::ZERO,).is_err()
        );
    }

    #[tokio::test]
    async fn split_materializes_post_coercion_cast_before_state_wrapper() {
        let input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("int_value", DataType::Int64, true)),
            (None, input_field("float_value", DataType::Float64, true)),
            (None, input_field("group", DataType::Utf8, true)),
        ]);
        let range_expr = avg(col("int_value") + col("float_value"));
        let time_expr = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"));
        let by_expr = Expr::Column(datafusion_common::Column::new_unqualified("group"));
        let split_source = RangeSelect::try_new(
            Arc::new(input.clone()),
            vec![RangeFn {
                name: "avg".to_string(),
                data_type: DataType::Float64,
                expr: range_expr.clone(),
                range: Duration::from_secs(10),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            time_expr.clone(),
            vec![by_expr.clone()],
            &[range_expr.clone(), time_expr.clone(), by_expr.clone()],
        )
        .unwrap();
        // This must split the original uncoerced RangeFn. The Cast assertion
        // below therefore fails if try_split_for_pushdown stops coercing it.
        let LogicalPlan::Extension(final_extension) =
            split_source.try_split_for_pushdown().unwrap()
        else {
            unreachable!();
        };
        let final_node = final_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        let LogicalPlan::Extension(partial_extension) = final_node.input.as_ref() else {
            unreachable!();
        };
        let partial = partial_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        // RangeSelect is an extension node, so ordinary TypeCoercion does not
        // descend into RangeFn expressions. Use the same production aggregate
        // coercion boundary solely for the executable Complete baseline.
        let coerced = StateMergeHelper::coerce_aggr_node(
            Aggregate::try_new(Arc::new(input.clone()), vec![], vec![range_expr]).unwrap(),
        )
        .unwrap();
        let coerced_range_expr = coerced.aggr_expr[0].clone();
        let complete = RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "avg".to_string(),
                data_type: DataType::Float64,
                expr: coerced_range_expr.clone(),
                range: Duration::from_secs(10),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            time_expr.clone(),
            vec![by_expr.clone()],
            &[coerced_range_expr, time_expr, by_expr],
        )
        .unwrap();
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!();
        };
        let Expr::Alias(argument) = &projection.expr[2] else {
            unreachable!();
        };
        assert!(
            matches!(argument.expr.as_ref(), Expr::BinaryExpr(binary) if matches!(binary.left.as_ref(), Expr::Cast(_)))
        );
        let mut decoded_projection = projection.clone();
        decoded_projection.expr[2] = argument.expr.as_ref().clone();
        let decoded = RangeSelect::try_new_partial_from_wire(
            Arc::new(LogicalPlan::Projection(decoded_projection)),
            partial.partial_wire_metadata().unwrap().clone(),
        )
        .unwrap();
        assert_eq!(decoded.schema, partial.schema);
        let rebuilt = partial
            .with_exprs_and_inputs(partial.expressions(), vec![partial.input.as_ref().clone()])
            .unwrap();
        assert_eq!(rebuilt.schema, partial.schema);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("int_value", DataType::Int64, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("group", DataType::Utf8, true),
        ]));
        let batch = |rows: &[(i64, i64, f64)]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(
                        rows.iter().map(|(time, _, _)| *time).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, value, _)| *value).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(
                        rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(vec!["g"; rows.len()])),
                ],
            )
            .unwrap()
        };
        let first = batch(&[(0, 1, 2.0)]);
        let second = batch(&[(0, 3, 4.0)]);
        let session = SessionContext::new();
        let state = session.state();
        let complete_output = datafusion::physical_plan::collect(
            complete
                .to_execution_plan(
                    complete.input.as_ref(),
                    memory_exec(schema.clone(), vec![first.clone(), second.clone()]),
                    &state,
                )
                .unwrap(),
            session.task_ctx(),
        )
        .await
        .unwrap();
        let mut partial_output = Vec::new();
        for raw in [first, second] {
            let partial_exec = partial
                .to_execution_plan(
                    partial.input.as_ref(),
                    materialized_partial_input_exec(
                        &partial,
                        memory_exec(schema.clone(), vec![raw]),
                        &state,
                    ),
                    &state,
                )
                .unwrap();
            partial_output.extend(
                datafusion::physical_plan::collect(partial_exec, session.task_ctx())
                    .await
                    .unwrap(),
            );
        }
        let final_output = datafusion::physical_plan::collect(
            final_node
                .to_execution_plan(
                    final_node.input.as_ref(),
                    memory_exec(physical_schema(&partial.schema), partial_output),
                    &state,
                )
                .unwrap(),
            session.task_ctx(),
        )
        .await
        .unwrap();
        assert_eq!(
            sorted_range_rows(&complete_output),
            sorted_range_rows(&final_output)
        );
    }

    fn split_single_sum_nodes(
        time_type: DataType,
        fill: Option<Fill>,
        range: Duration,
        align: Duration,
    ) -> (RangeSelect, RangeSelect, RangeSelect) {
        let input = empty_input(vec![
            (None, input_field("timestamp", time_type, true)),
            (None, input_field("value", DataType::Float64, true)),
            (None, input_field("group", DataType::Utf8, true)),
        ]);
        let range_expr = sum(col("value"));
        let time_expr = Expr::Column(datafusion_common::Column::new_unqualified("timestamp"));
        let by_expr = Expr::Column(datafusion_common::Column::new_unqualified("group"));
        let complete = RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "sum".to_string(),
                data_type: DataType::Float64,
                expr: range_expr.clone(),
                range,
                fill,
                need_cast: false,
            }],
            align,
            0,
            time_expr.clone(),
            vec![by_expr.clone()],
            &[range_expr, time_expr, by_expr],
        )
        .unwrap();
        let LogicalPlan::Extension(final_extension) = complete.try_split_for_pushdown().unwrap()
        else {
            unreachable!();
        };
        let final_node = final_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        let LogicalPlan::Extension(partial_extension) = final_node.input.as_ref() else {
            unreachable!();
        };
        let partial_node = partial_extension
            .node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        (complete, partial_node, final_node)
    }

    fn sum_schema(time_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", time_type, true),
            Field::new("value", DataType::Float64, true),
            Field::new("group", DataType::Utf8, true),
        ]))
    }

    fn sum_batch(time_type: DataType, rows: &[(i64, Option<f64>)]) -> RecordBatch {
        let timestamps = rows
            .iter()
            .map(|(timestamp, _)| *timestamp)
            .collect::<Vec<_>>();
        let timestamp: ArrayRef = match time_type {
            DataType::Timestamp(TimeUnit::Second, _) => {
                Arc::new(TimestampSecondArray::from(timestamps))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Arc::new(TimestampMillisecondArray::from(timestamps))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Arc::new(TimestampMicrosecondArray::from(timestamps))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Arc::new(TimestampNanosecondArray::from(timestamps))
            }
            other => unreachable!("unsupported test timestamp type {other:?}"),
        };
        let values = Float64Array::from(rows.iter().map(|(_, value)| *value).collect::<Vec<_>>());
        let groups = StringArray::from(vec!["g"; rows.len()]);
        RecordBatch::try_new(
            sum_schema(timestamp.data_type().clone()),
            vec![timestamp, Arc::new(values), Arc::new(groups)],
        )
        .unwrap()
    }

    fn sorted_sum_rows(batches: &[RecordBatch]) -> Vec<(String, ScalarValue, ScalarValue)> {
        let mut rows = batches
            .iter()
            .filter(|batch| batch.num_rows() != 0)
            .flat_map(|batch| {
                let groups = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|index| {
                        (
                            groups.value(index).to_string(),
                            ScalarValue::try_from_array(batch.column(1), index).unwrap(),
                            ScalarValue::try_from_array(batch.column(0), index).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then(timestamp_scalar_value(&left.1).cmp(&timestamp_scalar_value(&right.1)))
        });
        rows
    }

    fn timestamp_scalar_value(value: &ScalarValue) -> i64 {
        match value {
            ScalarValue::TimestampSecond(Some(value), _)
            | ScalarValue::TimestampMillisecond(Some(value), _)
            | ScalarValue::TimestampMicrosecond(Some(value), _)
            | ScalarValue::TimestampNanosecond(Some(value), _) => *value,
            other => unreachable!("unexpected timestamp scalar {other:?}"),
        }
    }

    fn memory_exec_partitions(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, None).unwrap(),
        )))
    }

    #[tokio::test]
    async fn range_select_partial_final_matches_complete_for_overlapping_avg() {
        let (complete, partial, final_node) = overlap_avg_split_nodes();
        let first_partial_batch = overlap_avg_batch(&[(0, 1.0, 2.0), (5_000, 3.0, 4.0)]);
        let second_partial_batch = overlap_avg_batch(&[(5_000, 5.0, 6.0), (10_000, 7.0, 8.0)]);
        let session = SessionContext::new();
        let state = session.state();

        let complete_exec = complete
            .to_execution_plan(
                complete.input.as_ref(),
                memory_exec(
                    overlap_avg_schema(),
                    vec![first_partial_batch.clone(), second_partial_batch.clone()],
                ),
                &state,
            )
            .unwrap();
        let complete_output = datafusion::physical_plan::collect(complete_exec, session.task_ctx())
            .await
            .unwrap();

        let partial_exec = partial
            .to_execution_plan(
                partial.input.as_ref(),
                materialized_partial_input_exec(
                    &partial,
                    memory_exec(overlap_avg_schema(), vec![first_partial_batch]),
                    &state,
                ),
                &state,
            )
            .unwrap();
        assert_eq!(partial_exec.schema(), physical_schema(&partial.schema));
        assert_eq!(partial_exec.schema().fields().len(), 3);
        assert!(matches!(
            partial_exec.schema().field(0).data_type(),
            DataType::Struct(fields) if !fields.iter().any(|field| matches!(field.data_type(), DataType::Struct(_)))
        ));
        assert_eq!(
            partial_exec.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(partial_exec.schema().field(2).name(), "group");
        let first_partial_output =
            datafusion::physical_plan::collect(partial_exec, session.task_ctx())
                .await
                .unwrap();

        let second_partial_exec = partial
            .to_execution_plan(
                partial.input.as_ref(),
                materialized_partial_input_exec(
                    &partial,
                    memory_exec(overlap_avg_schema(), vec![second_partial_batch]),
                    &state,
                ),
                &state,
            )
            .unwrap();
        let second_partial_output =
            datafusion::physical_plan::collect(second_partial_exec, session.task_ctx())
                .await
                .unwrap();
        let partial_output = first_partial_output
            .into_iter()
            .chain(second_partial_output)
            .collect::<Vec<_>>();

        assert!(
            final_node
                .input
                .schema()
                .fields()
                .iter()
                .all(|field| field.name() != "value_a" && field.name() != "value_b")
        );
        let final_exec = final_node
            .to_execution_plan(
                final_node.input.as_ref(),
                memory_exec(physical_schema(&partial.schema), partial_output),
                &state,
            )
            .unwrap();
        assert_eq!(final_exec.schema(), physical_schema(&final_node.schema));
        let final_output = datafusion::physical_plan::collect(final_exec, session.task_ctx())
            .await
            .unwrap();

        let complete_rows = sorted_range_rows(&complete_output);
        let final_rows = sorted_range_rows(&final_output);
        assert!(!complete_rows.is_empty());
        assert_eq!(complete_rows.len(), final_rows.len());
        for (complete_row, final_row) in complete_rows.iter().zip(final_rows) {
            assert_eq!(complete_row.0, final_row.0);
            assert_eq!(complete_row.1, final_row.1);
            assert!((complete_row.2 - final_row.2).abs() < f64::EPSILON);
        }
    }

    #[tokio::test]
    async fn range_select_partial_final_matches_complete_for_builtin_overlap_matrix() {
        let partial_inputs = vec![
            builtin_batch(&[(0, Some(1.0)), (5_000, Some(3.0))]),
            builtin_batch(&[(5_000, Some(5.0)), (10_000, Some(7.0))]),
        ];
        for (range, align, expected_count_at_zero) in [
            (Duration::from_secs(10), Duration::from_secs(5), 3),
            (Duration::from_secs(15), Duration::from_secs(5), 4),
        ] {
            let (complete, partial, final_node) = split_builtin_nodes(range, align);
            let (complete_output, partial_output, final_output) =
                execute_builtin_split(&complete, &partial, &final_node, partial_inputs.clone())
                    .await;
            assert!(partial_output.iter().all(|batch| {
                batch.num_rows() == 0
                    || matches!(batch.schema().field(0).data_type(), DataType::Struct(_))
            }));
            assert_eq!(complete_output[0].schema(), final_output[0].schema());
            let complete_rows = sorted_builtin_rows(&complete_output);
            let final_rows = sorted_builtin_rows(&final_output);
            assert!(!complete_rows.is_empty());
            assert_eq!(complete_rows, final_rows);
            assert!(complete_rows.iter().any(|(_, timestamp, values)| {
                *timestamp == 0 && values[3] == ScalarValue::Int64(Some(expected_count_at_zero))
            }));
        }
    }

    #[tokio::test]
    async fn range_select_partial_final_preserves_empty_builtin_states() {
        let (complete, partial, final_node) =
            split_builtin_nodes(Duration::from_secs(10), Duration::from_secs(5));
        let (complete_output, _, final_output) = execute_builtin_split(
            &complete,
            &partial,
            &final_node,
            vec![builtin_batch(&[(0, None)]), builtin_batch(&[(5_000, None)])],
        )
        .await;
        let complete_rows = sorted_builtin_rows(&complete_output);
        let final_rows = sorted_builtin_rows(&final_output);
        assert!(!complete_rows.is_empty());
        assert_eq!(complete_rows, final_rows);
        for (_, _, values) in complete_rows {
            assert_eq!(values[0], ScalarValue::Float64(None));
            assert_eq!(values[1], ScalarValue::Float64(None));
            assert_eq!(values[2], ScalarValue::Float64(None));
            assert_eq!(values[3], ScalarValue::Int64(Some(0)));
            assert_eq!(values[4], ScalarValue::Float64(None));
        }
    }

    #[tokio::test]
    async fn range_select_partial_final_matches_complete_for_fill_matrix() {
        let input_type = DataType::Timestamp(TimeUnit::Millisecond, None);
        let partial_inputs = vec![
            sum_batch(input_type.clone(), &[(0, Some(2.0)), (10_000, None)]),
            sum_batch(input_type.clone(), &[(0, Some(4.0)), (15_000, Some(8.0))]),
        ];
        for (fill, expected) in [
            (Fill::Null, vec![Some(6.0), None, None, Some(8.0)]),
            (
                Fill::Const(ScalarValue::Float64(Some(42.0))),
                vec![Some(6.0), Some(42.0), Some(42.0), Some(8.0)],
            ),
            (Fill::Prev, vec![Some(6.0), Some(6.0), Some(6.0), Some(8.0)]),
            (
                Fill::Linear,
                vec![Some(6.0), Some(20.0 / 3.0), Some(22.0 / 3.0), Some(8.0)],
            ),
        ] {
            let (complete, partial, final_node) = split_single_sum_nodes(
                input_type.clone(),
                Some(fill),
                Duration::from_secs(5),
                Duration::from_secs(5),
            );
            let (complete_output, _, final_output) = execute_split(
                &complete,
                &partial,
                &final_node,
                sum_schema(input_type.clone()),
                partial_inputs.clone(),
                &["value"],
            )
            .await;
            assert_eq!(complete_output[0].schema(), final_output[0].schema());
            let complete_rows = sorted_sum_rows(&complete_output);
            let final_rows = sorted_sum_rows(&final_output);
            assert_eq!(complete_rows, final_rows);
            assert_eq!(complete_rows.len(), expected.len());
            for ((_, _, value), expected) in complete_rows.into_iter().zip(expected) {
                match (value, expected) {
                    (ScalarValue::Float64(Some(actual)), Some(expected)) => {
                        assert!((actual - expected).abs() < 1e-12);
                    }
                    (actual, expected) => assert_eq!(actual, ScalarValue::Float64(expected)),
                }
            }
        }
    }

    #[tokio::test]
    async fn range_select_partial_final_preserves_public_timestamp_units() {
        for (time_type, first_timestamp, second_timestamp) in [
            (DataType::Timestamp(TimeUnit::Second, None), 0, 5),
            (DataType::Timestamp(TimeUnit::Millisecond, None), 0, 5_000),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                0,
                5_000_000,
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                0,
                5_000_000_000,
            ),
        ] {
            let (complete, partial, final_node) = split_single_sum_nodes(
                time_type.clone(),
                None,
                Duration::from_secs(10),
                Duration::from_secs(5),
            );
            let (complete_output, partial_output, final_output) = execute_split(
                &complete,
                &partial,
                &final_node,
                sum_schema(time_type.clone()),
                vec![
                    sum_batch(time_type.clone(), &[(first_timestamp, Some(2.0))]),
                    sum_batch(time_type.clone(), &[(second_timestamp, Some(4.0))]),
                ],
                &["value"],
            )
            .await;
            assert_eq!(
                partial.schema.field(1).data_type(),
                &DataType::Timestamp(TimeUnit::Millisecond, None)
            );
            assert!(partial_output.iter().all(|batch| {
                batch.num_rows() == 0
                    || batch.schema().field(1).data_type()
                        == &DataType::Timestamp(TimeUnit::Millisecond, None)
            }));
            assert_eq!(complete.schema.field(1).data_type(), &time_type);
            assert_eq!(final_node.schema.field(1).data_type(), &time_type);
            assert_eq!(complete_output[0].schema(), final_output[0].schema());
            assert_eq!(
                sorted_sum_rows(&complete_output),
                sorted_sum_rows(&final_output)
            );
        }
    }

    #[test]
    fn range_select_mode_properties_match_declared_partitioning() {
        let (complete, partial, final_node) = split_single_sum_nodes(
            DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
            Duration::from_secs(10),
            Duration::from_secs(5),
        );
        let session = SessionContext::new();
        let state = session.state();
        let input = memory_exec_partitions(
            sum_schema(DataType::Timestamp(TimeUnit::Millisecond, None)),
            vec![
                vec![sum_batch(
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    &[(0, Some(1.0))],
                )],
                vec![sum_batch(
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    &[(5_000, Some(2.0))],
                )],
            ],
        );
        let partial_exec = partial
            .to_execution_plan(
                partial.input.as_ref(),
                materialized_partial_input_exec(&partial, input.clone(), &state),
                &state,
            )
            .unwrap();
        let complete_exec = complete
            .to_execution_plan(complete.input.as_ref(), input, &state)
            .unwrap();
        let final_exec = final_node
            .to_execution_plan(
                final_node.input.as_ref(),
                memory_exec(physical_schema(&partial.schema), vec![]),
                &state,
            )
            .unwrap();
        assert_eq!(
            partial_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );
        assert!(matches!(
            partial_exec.required_input_distribution().as_slice(),
            [Distribution::UnspecifiedDistribution]
        ));
        for exec in [&complete_exec, &final_exec] {
            assert!(matches!(
                exec.required_input_distribution().as_slice(),
                [Distribution::SinglePartition]
            ));
            assert_eq!(exec.properties().output_partitioning().partition_count(), 1);
            assert_eq!(exec.properties().emission_type, EmissionType::Final);
            assert_eq!(exec.properties().boundedness, Boundedness::Bounded);
        }
        assert_eq!(partial_exec.properties().emission_type, EmissionType::Final);
        assert_eq!(partial_exec.properties().boundedness, Boundedness::Bounded);
    }

    #[test]
    fn rebuild_complete_rederives_schema_and_rejects_invalid_arity() {
        let original_input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Int64, true)),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        let node = complete_node(original_input.clone());
        let replacement = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
            ),
            (None, input_field("value", DataType::Int64, true)),
            (None, input_field("host", DataType::Utf8, false)),
        ]);
        let rebuilt = node
            .with_exprs_and_inputs(node.expressions(), vec![replacement])
            .unwrap();
        assert_eq!(
            rebuilt.schema_before_project.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Second, None)
        );
        assert!(!rebuilt.by_schema.field(0).is_nullable());
        assert_ne!(rebuilt.schema_before_project, node.schema_before_project);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![])
                .is_err()
        );
        assert!(
            node.with_exprs_and_inputs(
                node.expressions(),
                vec![original_input.clone(), original_input]
            )
            .is_err()
        );
        assert!(
            node.with_exprs_and_inputs(
                vec![Expr::Column(datafusion_common::Column::new_unqualified(
                    "value"
                ))],
                vec![node.input.as_ref().clone()]
            )
            .is_err()
        );
    }

    #[test]
    fn rebuild_partial_reconstructs_state_schema_and_rejects_bad_states() {
        let (node, _) = split_range_node(sum(col("value")), DataType::Int64, None, false);
        let input = node.input.as_ref().clone();
        let rebuilt = node
            .with_exprs_and_inputs(node.expressions(), vec![input.clone()])
            .unwrap();
        assert!(!rebuilt.schema.field(0).is_nullable());
        assert_eq!(rebuilt.schema, node.schema);
        let mut wrong_alias = node.expressions();
        wrong_alias[0] =
            Expr::Column(datafusion_common::Column::new_unqualified("value")).alias("wrong");
        assert!(
            node.with_exprs_and_inputs(wrong_alias, vec![input.clone()])
                .is_err()
        );
        let mut non_struct = node.expressions();
        non_struct[0] = Expr::Column(datafusion_common::Column::new_unqualified("host"))
            .alias("__range_state_0");
        assert!(
            node.with_exprs_and_inputs(non_struct, vec![input.clone()])
                .is_err()
        );
        let wrong_type = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Utf8, true)),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![wrong_type])
                .is_err()
        );
    }

    #[test]
    fn rebuild_final_reconstructs_legacy_schema_and_validates_input_contract() {
        let (_, node) = split_range_node(sum(col("value")), DataType::Int64, None, false);
        let input = node.input.as_ref().clone();
        let rebuilt = node
            .with_exprs_and_inputs(node.expressions(), vec![input.clone()])
            .unwrap();
        assert_eq!(rebuilt.schema, node.schema);
        assert_eq!(rebuilt.by_schema, node.by_schema);

        let missing_state = empty_input(vec![
            (
                None,
                input_field(
                    "__range_bucket_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![missing_state])
                .is_err()
        );
        let wrong_state_type = empty_input(vec![
            (None, input_field("__range_state_0", DataType::Int64, false)),
            (
                None,
                input_field(
                    "__range_bucket_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![wrong_state_type])
                .is_err()
        );
        let nullable_state = empty_input(vec![
            (None, input_field("__range_state_0", state_type(), true)),
            (
                None,
                input_field(
                    "__range_bucket_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![nullable_state])
                .is_err()
        );
        let missing_bucket = empty_input(vec![
            (None, input_field("__range_state_0", state_type(), false)),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![missing_bucket])
                .is_err()
        );
        let wrong_bucket = empty_input(vec![
            (None, input_field("__range_state_0", state_type(), false)),
            (
                None,
                input_field("__range_bucket_ms", DataType::Int64, true),
            ),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![wrong_bucket])
                .is_err()
        );
        let mut wrong_merge = node.expressions();
        wrong_merge[0] =
            Expr::Column(datafusion_common::Column::new_unqualified("wrong_state")).alias("result");
        assert!(
            node.with_exprs_and_inputs(wrong_merge, vec![input.clone()])
                .is_err()
        );

        let mut null_treatment = node.expressions();
        let Expr::Alias(alias) = &mut null_treatment[0] else {
            unreachable!();
        };
        let Expr::AggregateFunction(aggregate) = alias.expr.as_mut() else {
            unreachable!();
        };
        aggregate.params.null_treatment = Some(datafusion_expr::expr::NullTreatment::IgnoreNulls);
        assert!(
            node.with_exprs_and_inputs(null_treatment, vec![input])
                .is_err()
        );
    }

    #[test]
    fn rebuild_split_preserves_raw_and_published_aggregate_contracts() {
        let cases = [
            (
                count(col("value")),
                DataType::Int64,
                None,
                false,
                false,
                DataType::Int64,
                true,
            ),
            (
                sum(col("value")),
                DataType::Int64,
                Some(Fill::Const(ScalarValue::Int64(Some(0)))),
                false,
                true,
                DataType::Int64,
                false,
            ),
            (
                sum(col("value")),
                DataType::Float64,
                Some(Fill::Linear),
                true,
                true,
                DataType::Float64,
                true,
            ),
        ];

        for (
            aggregate,
            data_type,
            fill,
            need_cast,
            raw_nullable,
            published_type,
            published_nullable,
        ) in cases
        {
            let (partial, final_node) = split_range_node(aggregate, data_type, fill, need_cast);
            let rebuilt_partial = partial
                .with_exprs_and_inputs(partial.expressions(), vec![partial.input.as_ref().clone()])
                .unwrap();
            let rebuilt_final = final_node
                .with_exprs_and_inputs(
                    final_node.expressions(),
                    vec![final_node.input.as_ref().clone()],
                )
                .unwrap();
            assert_eq!(rebuilt_partial.schema, partial.schema);
            assert_eq!(rebuilt_final.schema, final_node.schema);

            let RangeSelectMode::Final(spec) = &final_node.mode else {
                unreachable!();
            };
            assert_eq!(spec.raw_merge_result_fields[0].nullable, raw_nullable);
            assert_eq!(spec.legacy_range_fields[0].data_type, published_type);
            assert_eq!(spec.legacy_range_fields[0].nullable, published_nullable);
        }
    }

    #[test]
    fn rebuild_split_rejects_fake_wrappers_and_non_direct_final_time() {
        let (partial, final_node) =
            split_range_node(sum(col("value")), DataType::Int64, None, false);
        let state_name = partial.range_expr[0].name.clone();
        let fake_input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Int64, true)),
            (None, input_field("host", DataType::Utf8, true)),
            (
                None,
                input_field(
                    "fake_state",
                    partial.schema.field(0).data_type().clone(),
                    true,
                ),
            ),
        ]);
        let mut fake_partial_exprs = partial.expressions();
        fake_partial_exprs[0] =
            Expr::Column(datafusion_common::Column::new_unqualified("fake_state"))
                .alias(state_name);
        assert!(
            partial
                .with_exprs_and_inputs(fake_partial_exprs, vec![fake_input])
                .is_err()
        );

        let mut fake_final_exprs = final_node.expressions();
        fake_final_exprs[0] = Expr::Column(datafusion_common::Column::new_unqualified(
            "__range_state_0",
        ))
        .alias("result");
        assert!(
            final_node
                .with_exprs_and_inputs(fake_final_exprs, vec![final_node.input.as_ref().clone()],)
                .is_err()
        );

        let bucket = Expr::Column(datafusion_common::Column::new_unqualified(
            "__range_bucket_ms",
        ));
        let mut cast_time_exprs = final_node.expressions();
        cast_time_exprs[1] = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(bucket.clone()),
            data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
        });
        assert!(
            final_node
                .with_exprs_and_inputs(cast_time_exprs, vec![final_node.input.as_ref().clone()])
                .is_err()
        );

        let mut arithmetic_time_exprs = final_node.expressions();
        arithmetic_time_exprs[1] = Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
            left: Box::new(bucket),
            op: datafusion_expr::Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
        });
        assert!(
            final_node
                .with_exprs_and_inputs(
                    arithmetic_time_exprs,
                    vec![final_node.input.as_ref().clone()],
                )
                .is_err()
        );
    }

    #[test]
    fn rebuild_rejects_invalid_schema_project() {
        let input = empty_input(vec![
            (
                None,
                input_field(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ),
            (None, input_field("value", DataType::Int64, true)),
            (None, input_field("host", DataType::Utf8, true)),
        ]);
        let mut node = complete_node(input.clone());
        node.schema_project = Some(vec![99]);
        assert!(
            node.with_exprs_and_inputs(node.expressions(), vec![input])
                .is_err()
        );
    }
}
