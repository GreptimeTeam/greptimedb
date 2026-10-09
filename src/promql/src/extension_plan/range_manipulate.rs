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

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_telemetry::{debug, warn};
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, TimestampMillisecondArray,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef, TableReference};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    InputDistributionRequirements, PhysicalExpr, PlanProperties, RecordBatchStream,
    ReplaceChildrenOptions, SendableRecordBatchStream, Statistics, StatisticsArgs,
};
use datafusion_expr::ident;
use datatypes::timestamp::timestamp_array_to_primitive;
use futures::{Stream, StreamExt, ready};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::{
    METRIC_NUM_SERIES, Millisecond, local_offset, nanoseconds_per_native_tick, resolve_column_name,
    serialize_column_index, timestamp_unit,
};
use crate::metrics::PROMQL_SERIES_COUNT;
use crate::range_array::RangeArray;

/// Time series manipulator for range function.
///
/// This plan will "fold" time index and value columns into [RangeArray]s, and truncate
/// other columns to the same length with the "folded" [RangeArray] column.
///
/// To pass runtime information to the execution plan (or the range function), This plan
/// will add those extra columns:
/// - timestamp range with type [RangeArray], which is the folded timestamp column.
/// - end of current range with the same type as the timestamp column. (todo)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RangeManipulate {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    offset: Millisecond,
    time_index: String,
    field_columns: Vec<String>,
    input: LogicalPlan,
    output_schema: DFSchemaRef,
    unfix: Option<UnfixIndices>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UnfixIndices {
    pub time_index_idx: u64,
    pub tag_column_indices: Vec<u64>,
}

impl RangeManipulate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        offset: Millisecond,
        range: Millisecond,
        time_index: String,
        field_columns: Vec<String>,
        input: LogicalPlan,
    ) -> DataFusionResult<Self> {
        let output_schema =
            Self::calculate_output_schema(input.schema(), &time_index, &field_columns)?;
        Ok(Self {
            start,
            end,
            interval,
            range,
            offset,
            time_index,
            field_columns,
            input,
            output_schema,
            unfix: None,
        })
    }

    pub const fn name() -> &'static str {
        "RangeManipulate"
    }

    pub fn build_timestamp_range_name(time_index: &str) -> String {
        format!("{time_index}_range")
    }

    pub fn internal_range_end_col_name() -> String {
        "__internal_range_end".to_string()
    }

    fn range_timestamp_name(&self) -> String {
        Self::build_timestamp_range_name(&self.time_index)
    }

    fn calculate_output_schema(
        input_schema: &DFSchemaRef,
        time_index: &str,
        field_columns: &[String],
    ) -> DataFusionResult<DFSchemaRef> {
        let columns = input_schema.fields();
        let mut new_columns = Vec::with_capacity(columns.len() + 1);
        for i in 0..columns.len() {
            let x = input_schema.qualified_field(i);
            new_columns.push((x.0.cloned(), x.1.clone()));
        }

        // process time index column
        // the raw timestamp field is preserved. And a new timestamp_range field is appended to the last.
        let Some(ts_col_index) = input_schema.index_of_column_by_name(None, time_index) else {
            return Err(datafusion::common::field_not_found(
                None::<TableReference>,
                time_index,
                input_schema.as_ref(),
            ));
        };
        let ts_col_field = &columns[ts_col_index];
        let output_time_field = Arc::new(
            ts_col_field
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
        );
        new_columns[ts_col_index] = (
            input_schema.qualified_field(ts_col_index).0.cloned(),
            output_time_field.clone(),
        );
        let timestamp_range_field = Field::new(
            Self::build_timestamp_range_name(time_index),
            RangeArray::convert_field(output_time_field.as_ref())
                .data_type()
                .clone(),
            ts_col_field.is_nullable(),
        );
        new_columns.push((None, Arc::new(timestamp_range_field)));

        // process value columns
        for name in field_columns {
            let Some(index) = input_schema.index_of_column_by_name(None, name) else {
                return Err(datafusion::common::field_not_found(
                    None::<TableReference>,
                    name,
                    input_schema.as_ref(),
                ));
            };
            new_columns[index] = (None, Arc::new(RangeArray::convert_field(&columns[index])));
        }

        Ok(Arc::new(DFSchema::new_with_metadata(
            new_columns,
            input_schema.metadata().clone(),
        )?))
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        let properties = exec_input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            properties.partitioning.clone(),
            properties.emission_type,
            properties.boundedness,
        ));
        Arc::new(RangeManipulateExec {
            offset: self.offset,
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index_column: self.time_index.clone(),
            time_range_column: self.range_timestamp_name(),
            field_columns: self.field_columns.clone(),
            input: exec_input,
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            fused_rate: None,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let time_index_idx = serialize_column_index(self.input.schema(), &self.time_index);

        let tag_column_indices = self
            .field_columns
            .iter()
            .map(|name| serialize_column_index(self.input.schema(), name))
            .collect::<Vec<u64>>();

        pb::RangeManipulate {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index_idx,
            tag_column_indices,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_range_manipulate = pb::RangeManipulate::decode(bytes).context(DeserializeSnafu)?;
        let empty_schema = Arc::new(DFSchema::empty());
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: empty_schema.clone(),
        });

        let unfix = UnfixIndices {
            time_index_idx: pb_range_manipulate.time_index_idx,
            tag_column_indices: pb_range_manipulate.tag_column_indices.clone(),
        };
        debug!("RangeManipulate deserialize unfix: {:?}", unfix);

        // Unlike `Self::new()`, this method doesn't check the input schema as it will fail
        // because the input schema is empty.
        // But this is Ok since datafusion guarantees to call `with_exprs_and_inputs` for the
        // deserialized plan.
        Ok(Self {
            start: pb_range_manipulate.start,
            end: pb_range_manipulate.end,
            interval: pb_range_manipulate.interval,
            range: pb_range_manipulate.range,
            offset: 0,
            time_index: String::new(),
            field_columns: Vec::new(),
            input: placeholder_plan,
            output_schema: empty_schema,
            unfix: Some(unfix),
        })
    }
}

impl PartialOrd for RangeManipulate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare fields in order excluding output_schema
        match self.start.partial_cmp(&other.start) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.end.partial_cmp(&other.end) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.interval.partial_cmp(&other.interval) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.range.partial_cmp(&other.range) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.offset.partial_cmp(&other.offset) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_index.partial_cmp(&other.time_index) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.field_columns.partial_cmp(&other.field_columns) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for RangeManipulate {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        if self.unfix.is_some() {
            return vec![];
        }

        let mut exprs = Vec::with_capacity(1 + self.field_columns.len());
        exprs.push(ident(&self.time_index));
        exprs.extend(self.field_columns.iter().map(ident));
        exprs
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        if self.unfix.is_some() {
            return None;
        }

        let input_schema = self.input.schema();
        let input_len = input_schema.fields().len();
        let time_index_idx = input_schema.index_of_column_by_name(None, &self.time_index)?;

        if output_columns.is_empty() {
            let indices = (0..input_len).collect::<Vec<_>>();
            return Some(vec![indices]);
        }

        let mut required = Vec::with_capacity(output_columns.len() + 1 + self.field_columns.len());
        required.push(time_index_idx);
        for value_column in &self.field_columns {
            required.push(input_schema.index_of_column_by_name(None, value_column)?);
        }
        for &idx in output_columns {
            if idx < input_len {
                required.push(idx);
            } else if idx == input_len {
                // Derived timestamp range column.
                required.push(time_index_idx);
            } else {
                warn!(
                    "Output column index {} is out of bounds for input schema with length {}",
                    idx, input_len
                );
                return None;
            }
        }

        required.sort_unstable();
        required.dedup();
        Some(vec![required])
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromRangeManipulate: req range=[{}..{}], interval=[{}], eval range=[{}], time index=[{}], values={:?}",
            self.start, self.end, self.interval, self.range, self.time_index, self.field_columns
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "RangeManipulate should have at exact one input".to_string(),
            ));
        }

        let input: LogicalPlan = inputs.pop().unwrap();
        let input_schema = input.schema();

        if let Some(unfix) = &self.unfix {
            // transform indices to names
            let time_index = resolve_column_name(
                unfix.time_index_idx,
                input_schema,
                "RangeManipulate",
                "time index",
            )?;

            let field_columns = unfix
                .tag_column_indices
                .iter()
                .map(|idx| resolve_column_name(*idx, input_schema, "RangeManipulate", "tag"))
                .collect::<DataFusionResult<Vec<String>>>()?;

            let output_schema =
                Self::calculate_output_schema(input_schema, &time_index, &field_columns)?;

            Ok(Self {
                start: self.start,
                end: self.end,
                interval: self.interval,
                range: self.range,
                offset: local_offset(&input, &time_index),
                time_index,
                field_columns,
                input,
                output_schema,
                unfix: None,
            })
        } else {
            let output_schema =
                Self::calculate_output_schema(input_schema, &self.time_index, &self.field_columns)?;

            Ok(Self {
                start: self.start,
                end: self.end,
                interval: self.interval,
                range: self.range,
                offset: self.offset,
                time_index: self.time_index.clone(),
                field_columns: self.field_columns.clone(),
                input,
                output_schema,
                unfix: None,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct RangeManipulateExec {
    offset: Millisecond,
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    time_index_column: String,
    time_range_column: String,
    field_columns: Vec<String>,

    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
    fused_rate: Option<FusedRate>,
}

#[derive(Debug, Clone)]
struct FusedRate {
    rate_source: usize,
    rate_position: usize,
    timestamp_position: usize,
    projections: Vec<ProjectionExpr>,
    folded_schema: SchemaRef,
}

impl RangeManipulateExec {
    /// Fuse a projection containing one supported PromQL rate expression into this range operator.
    ///
    /// Returns `None` when the raw columns or projection shape do not match the rate kernel ABI.
    pub fn try_fuse_rate_projection(
        &self,
        projection: &ProjectionExec,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.offset != 0 {
            return Ok(None);
        }
        let raw_input_schema = self.input.schema();
        let Some((time_index, _)) = raw_input_schema.column_with_name(&self.time_index_column)
        else {
            return Ok(None);
        };
        if raw_input_schema
            .fields()
            .get(time_index)
            .map(|f| f.data_type())
            != Some(&DataType::Timestamp(TimeUnit::Millisecond, None))
        {
            return Ok(None);
        }
        let Some((time_range_index, _)) =
            self.output_schema.column_with_name(&self.time_range_column)
        else {
            return Ok(None);
        };
        let Some((rate_field_name, rate_field_index)) =
            self.field_columns.iter().find_map(|name| {
                self.output_schema
                    .column_with_name(name)
                    .map(|(index, _)| (name, index))
            })
        else {
            return Ok(None);
        };
        let Some((raw_rate_index, raw_rate_field)) =
            raw_input_schema.column_with_name(rate_field_name)
        else {
            return Ok(None);
        };
        if raw_rate_field.data_type() != &DataType::Float64 {
            return Ok(None);
        }
        if raw_input_schema.field(time_index).is_nullable() {
            return Ok(None);
        }
        let projections = projection.expr().to_vec();
        let mut rate_position = None;
        let mut timestamp_position = None;
        for (position, item) in projections.iter().enumerate() {
            if let Some(function) = item.expr.downcast_ref::<ScalarFunctionExpr>() {
                if rate_position.is_some()
                    || crate::functions::Rate::scalar_udf() != *function.fun()
                    || function.args().len() != 4
                {
                    return Ok(None);
                }
                let Some(ts_arg) = function.args()[0].downcast_ref::<Column>() else {
                    return Ok(None);
                };
                let Some(value_arg) = function.args()[1].downcast_ref::<Column>() else {
                    return Ok(None);
                };
                let Some(eval_arg) = function.args()[2].downcast_ref::<Column>() else {
                    return Ok(None);
                };
                let Some(range_arg) = function.args()[3].downcast_ref::<Literal>() else {
                    return Ok(None);
                };
                if ts_arg.index() != time_range_index
                    || eval_arg.index() != time_index
                    || value_arg.index() != rate_field_index
                {
                    return Ok(None);
                }
                let datafusion::common::ScalarValue::Int64(Some(duration)) = range_arg.value()
                else {
                    return Ok(None);
                };
                if *duration != self.range {
                    return Ok(None);
                }
                rate_position = Some(position);
            } else {
                let Some(column) = item.expr.downcast_ref::<Column>() else {
                    return Ok(None);
                };
                if column.index() == time_range_index
                    || self.field_columns.iter().any(|name| {
                        self.output_schema
                            .column_with_name(name)
                            .is_some_and(|(i, _)| i == column.index())
                    })
                {
                    return Ok(None);
                }
                if column.index() == time_index && timestamp_position.replace(position).is_some() {
                    return Ok(None);
                }
            }
        }
        let Some(rate_position) = rate_position else {
            return Ok(None);
        };
        let Some(timestamp_position) = timestamp_position else {
            return Ok(None);
        };
        if self.output_schema.fields().get(time_range_index).is_none()
            || raw_input_schema.fields().get(raw_rate_index).is_none()
            || projections.iter().any(|item| {
                item.expr
                    .downcast_ref::<Column>()
                    .is_some_and(|column| self.output_schema.fields().get(column.index()).is_none())
            })
        {
            return Ok(None);
        }
        let base = Arc::new(Self {
            fused_rate: None,
            ..self.clone()
        }) as Arc<dyn ExecutionPlan>;
        let rebuilt = ProjectionExec::try_new_with_schema_metadata(
            projections.clone(),
            base,
            projection.schema().as_ref(),
        )?;
        let fused = Self {
            fused_rate: Some(FusedRate {
                rate_source: raw_rate_index,
                rate_position,
                timestamp_position,
                projections,
                folded_schema: self.output_schema.clone(),
            }),
            output_schema: projection.schema(),
            properties: rebuilt.properties().clone(),
            ..self.clone()
        };
        Ok(Some(Arc::new(fused)))
    }
}

impl ExecutionPlan for RangeManipulateExec {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        let input_requirement = self
            .input
            .input_distribution_requirements()
            .into_per_child();
        if input_requirement.is_empty() {
            // if the input is EmptyMetric, its required_input_distribution() is empty so we can't
            // use its input distribution.
            InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
        } else {
            InputDistributionRequirements::new(input_requirement)
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        let exec_input = children[0].clone();
        let properties = if let Some(fused) = &self.fused_rate {
            let ordinary = Arc::new(Self {
                fused_rate: None,
                output_schema: fused.folded_schema.clone(),
                ..self.as_ref().clone()
            })
            .replace_children(
                vec![exec_input.clone()],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
            ProjectionExec::try_new_with_schema_metadata(
                fused.projections.clone(),
                ordinary,
                self.output_schema.as_ref(),
            )?
            .properties()
            .clone()
        } else {
            let properties = exec_input.properties();
            Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.output_schema.clone()),
                properties.partitioning.clone(),
                properties.emission_type,
                properties.boundedness,
            ))
        };
        Ok(Arc::new(Self {
            offset: self.offset,
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index_column: self.time_index_column.clone(),
            time_range_column: self.time_range_column.clone(),
            field_columns: self.field_columns.clone(),
            output_schema: self.output_schema.clone(),
            input: children[0].clone(),
            metric: self.metric.clone(),
            properties,
            fused_rate: self.fused_rate.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        let metrics_builder = MetricBuilder::new(&self.metric);
        let num_series = Count::new();
        metrics_builder
            .with_partition(partition)
            .build(MetricValue::Count {
                name: METRIC_NUM_SERIES.into(),
                count: num_series.clone(),
            });

        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let time_index = schema
            .column_with_name(&self.time_index_column)
            .unwrap_or_else(|| panic!("time index column {} not found", self.time_index_column))
            .0;
        let field_columns = self
            .field_columns
            .iter()
            .map(|value_col| {
                schema
                    .column_with_name(value_col)
                    .unwrap_or_else(|| panic!("value column {value_col} not found",))
                    .0
            })
            .collect();
        let time_unit = timestamp_unit(schema.field(time_index).data_type())?;
        let aligned_ts_array =
            RangeManipulateStream::build_aligned_ts_array(self.start, self.end, self.interval);
        Ok(Box::pin(RangeManipulateStream {
            offset: self.offset,
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index,
            time_unit,
            field_columns,
            aligned_ts_array,
            output_schema: self.output_schema.clone(),
            input,
            metric: baseline_metric,
            num_series,
            fused_rate: self.fused_rate.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DataFusionResult<Arc<Statistics>> {
        let input_stats = &input_stats[0];

        let estimated_row_num = (self.end - self.start) as f64 / self.interval as f64;
        let estimated_total_bytes = input_stats
            .total_byte_size
            .get_value()
            .zip(input_stats.num_rows.get_value())
            .map(|(size, rows)| {
                Precision::Inexact(((*size as f64 / *rows as f64) * estimated_row_num).floor() as _)
            })
            .unwrap_or_default();

        Ok(Arc::new(Statistics {
            num_rows: Precision::Inexact(estimated_row_num as _),
            total_byte_size: estimated_total_bytes,
            // TODO(ruihang): support this column statistics
            column_statistics: Statistics::unknown_column(&self.schema()),
        }))
    }

    fn name(&self) -> &str {
        if self.fused_rate.is_some() {
            "PromRangeRateExec"
        } else {
            "RangeManipulateExec"
        }
    }
}

impl DisplayAs for RangeManipulateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "{}: req range=[{}..{}], interval=[{}], eval range=[{}], time index=[{}]",
                    if self.fused_rate.is_some() {
                        "PromRangeRateExec"
                    } else {
                        "PromRangeManipulateExec"
                    },
                    self.start,
                    self.end,
                    self.interval,
                    self.range,
                    self.time_index_column
                )
            }
        }
    }
}

pub struct RangeManipulateStream {
    offset: Millisecond,
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    time_index: usize,
    time_unit: TimeUnit,
    field_columns: Vec<usize>,
    aligned_ts_array: ArrayRef,

    output_schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    /// Number of series processed.
    num_series: Count,
    fused_rate: Option<FusedRate>,
}

impl RecordBatchStream for RangeManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for RangeManipulateStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = std::time::Instant::now();
                    let result = self.manipulate(batch);
                    if let Ok(None) = result {
                        self.metric.elapsed_compute().add_elapsed(timer);
                        continue;
                    } else {
                        self.num_series.add(1);
                        self.metric.elapsed_compute().add_elapsed(timer);
                        break Poll::Ready(result.transpose());
                    }
                }
                None => {
                    PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
                    break Poll::Ready(None);
                }
                Some(Err(e)) => break Poll::Ready(Some(Err(e))),
            }
        };
        self.metric.record_poll(poll)
    }
}

impl RangeManipulateStream {
    // Prometheus: https://github.com/prometheus/prometheus/blob/e934d0f01158a1d55fa0ebb035346b195fcc1260/promql/engine.go#L1113-L1198
    // But they are not exactly the same, because we don't eager-evaluate on the data in this plan.
    // And the generated timestamp is not aligned to the step. It's expected to do later.
    pub fn manipulate(&self, input: RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        // calculate the range
        let (ranges, (start, end)) = self.calculate_range(&input)?;
        // ignore this if all ranges are empty
        if ranges.iter().all(|(_, len)| *len == 0) {
            return Ok(None);
        }

        if let Some(fused) = &self.fused_rate {
            let timestamps = input
                .column(self.time_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Fused rate requires millisecond timestamps".into())
                })?;
            let values = input
                .column(fused.rate_source)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Fused rate requires Float64 values".into())
                })?;
            let aligned = if start != self.start || end != self.end {
                Self::build_aligned_ts_array(start, end, self.interval)
            } else {
                self.aligned_ts_array.clone()
            };
            let eval_ts = aligned
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Fused rate requires millisecond evaluation timestamps".into(),
                    )
                })?;
            let rates = crate::functions::Rate::calculate_ranges(
                timestamps, values, &ranges, eval_ts, self.range,
            )?;
            let take_indices = Int64Array::from(vec![0; ranges.len()]);
            let mut columns = Vec::with_capacity(fused.projections.len());
            for (position, projection) in fused.projections.iter().enumerate() {
                if position == fused.rate_position {
                    columns.push(Arc::new(rates.clone()) as ArrayRef);
                } else if position == fused.timestamp_position {
                    columns.push(aligned.clone());
                } else {
                    let Some(column) = projection.expr.downcast_ref::<Column>() else {
                        return Err(DataFusionError::Execution(
                            "Invalid fused rate projection mapping".into(),
                        ));
                    };
                    columns.push(compute::take(
                        input.column(column.index()),
                        &take_indices,
                        None,
                    )?);
                }
            }
            return RecordBatch::try_new(self.output_schema.clone(), columns)
                .map(Some)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }

        // transform columns
        let mut other_columns = (0..input.columns().len()).collect::<HashSet<_>>();
        let mut new_columns = input.columns().to_vec();
        for index in self.field_columns.iter() {
            let _ = other_columns.remove(index);
            let column = input.column(*index);
            let new_column = Arc::new(
                RangeArray::from_ranges(column.clone(), ranges.clone())
                    .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?
                    .into_dict(),
            );
            new_columns[*index] = new_column;
        }

        // The timestamp range payload is always millisecond ABI. Shift in wide
        // native precision before truncating toward zero, preserving null validity.
        let scale = nanoseconds_per_native_tick(self.time_unit);
        let (timestamps, _) = timestamp_array_to_primitive(input.column(self.time_index))
            .ok_or_else(|| {
                DataFusionError::Execution("Time index column is not a timestamp".into())
            })?;
        let timestamp_values = timestamps
            .values()
            .iter()
            .enumerate()
            .map(|(index, timestamp)| {
                if !input.column(self.time_index).is_valid(index) {
                    return Ok(None);
                }
                let shifted_ns = (*timestamp as i128) * scale + (self.offset as i128) * 1_000_000;
                i64::try_from(shifted_ns / 1_000_000)
                    .map(Some)
                    .map_err(|_| {
                        ArrowError::ComputeError(
                            "RangeManipulate timestamp payload overflow".into(),
                        )
                    })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let timestamp_values = TimestampMillisecondArray::from(timestamp_values);
        let ts_range_column = RangeArray::from_ranges(Arc::new(timestamp_values), ranges.clone())
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?
            .into_dict();
        new_columns.push(Arc::new(ts_range_column));

        // truncate other columns
        let take_indices = Int64Array::from(vec![0; ranges.len()]);
        for index in other_columns.into_iter() {
            new_columns[index] = compute::take(&input.column(index), &take_indices, None)?;
        }
        // replace timestamp with the aligned one
        let new_time_index = if ranges.len() != self.aligned_ts_array.len() {
            Self::build_aligned_ts_array(start, end, self.interval)
        } else {
            self.aligned_ts_array.clone()
        };
        new_columns[self.time_index] = new_time_index;

        RecordBatch::try_new(self.output_schema.clone(), new_columns)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn build_aligned_ts_array(start: i64, end: i64, interval: i64) -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from_iter_values(
            (start..=end).step_by(interval as _),
        ))
    }

    /// Return values:
    /// - A vector of tuples where each tuple contains the start index and length of the range.
    /// - A tuple of the actual start/end timestamp used to calculate the range.
    #[allow(clippy::type_complexity)]
    fn calculate_range(
        &self,
        input: &RecordBatch,
    ) -> DataFusionResult<(Vec<(u32, u32)>, (i64, i64))> {
        let ts_column = input.column(self.time_index);
        let scale = nanoseconds_per_native_tick(self.time_unit);
        let (timestamps, _) = timestamp_array_to_primitive(ts_column).ok_or_else(|| {
            DataFusionError::Execution("Time index column is not a timestamp".into())
        })?;
        let timestamps = timestamps.values();
        let timestamp =
            |index| (timestamps[index] as i128) * scale + (self.offset as i128) * 1_000_000;
        let len = timestamps.len();
        if len == 0 {
            return Ok((vec![], (self.start, self.end)));
        }

        // Shorten the range using wide arithmetic so timestamps near the native
        // type limits retain every query-aligned evaluation point.
        let query_start = self.start as i128;
        let query_end = self.end as i128;
        let interval = self.interval as i128;
        let first_ts = timestamp(0).div_euclid(1_000_000);
        // Preserve the query's alignment pattern when optimizing start time.
        let remainder = (first_ts - query_start).rem_euclid(interval);
        let first_ts_aligned = first_ts + (interval - remainder).rem_euclid(interval);
        let last_ts_with_range =
            (timestamp(len - 1) + (self.range as i128) * 1_000_000).div_euclid(1_000_000);
        let remainder = (last_ts_with_range - query_start).rem_euclid(interval);
        let last_ts_aligned = last_ts_with_range - remainder;
        let start = query_start.max(first_ts_aligned);
        let end = query_end.min(last_ts_aligned);
        if start > end {
            return Ok((vec![], (self.start, self.end)));
        }
        // The intersection is within the declared i64 query bounds.
        let start = start as i64;
        let end = end as i64;
        let mut ranges = Vec::new();

        // Range membership is decided on shifted native ticks, before the
        // timestamp-range payload is converted to its millisecond ABI. This
        // keeps sub-millisecond samples distinct in a range; equal millisecond
        // payload values are not a reason to deduplicate input samples.
        //
        // Calculate for every aligned timestamp (`curr_ts`), assuming ordered timestamps.
        let mut left = 0usize;
        let mut right = 0usize;
        for curr_ts in (start..=end).step_by(self.interval as _) {
            let start_ts = (curr_ts as i128) * 1_000_000 - (self.range as i128) * 1_000_000;

            while left < len && timestamp(left) <= start_ts {
                left += 1;
            }
            right = right.max(left);
            while right < len && timestamp(right) <= (curr_ts as i128) * 1_000_000 {
                right += 1;
            }

            if left == right {
                ranges.push((0, 0));
            } else {
                ranges.push((left as _, (right - left) as _));
            }
        }

        Ok((ranges, (start, end)))
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampMicrosecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Int64Type, Schema, TimestampMillisecondType,
    };
    use datafusion::common::ScalarValue;
    use datafusion::common::ToDFSchema;
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{
        EmptyRelation, Extension, LogicalPlan, UserDefinedLogicalNodeCore,
    };
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::expressions::Literal as PhysicalLiteral;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use futures::FutureExt;

    use super::*;

    fn rate_projection(schema: &SchemaRef, duration: i64) -> ProjectionExec {
        rate_projection_with_udf(schema, duration, crate::functions::Rate::scalar_udf())
    }

    fn rate_projection_with_udf(
        schema: &SchemaRef,
        duration: i64,
        udf: datafusion::logical_expr::ScalarUDF,
    ) -> ProjectionExec {
        let config = Arc::new(ConfigOptions::default());
        let args = vec![
            Arc::new(Column::new("timestamp_range", 5)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("value", 3)),
            Arc::new(Column::new("timestamp", 4)),
            Arc::new(PhysicalLiteral::new(ScalarValue::Int64(Some(duration)))),
        ];
        let rate_field = Arc::new(Field::new("rate", DataType::Float64, true));
        let rate = Arc::new(ScalarFunctionExpr::new(
            "prom_rate",
            Arc::new(udf),
            args,
            rate_field,
            config.clone(),
        ));
        ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("timestamp", 4)) as Arc<dyn PhysicalExpr>,
                    "timestamp".to_string(),
                ),
                (Arc::new(Column::new("dc", 1)), "dc".to_string()),
                (rate as Arc<dyn PhysicalExpr>, "rate".to_string()),
            ],
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
        )
        .unwrap()
    }

    async fn fusion_fixture(
        start: i64,
        end: i64,
        interval: i64,
        range: i64,
    ) -> (
        RangeManipulateExec,
        Arc<dyn ExecutionPlan>,
        ProjectionExec,
        RecordBatch,
    ) {
        let raw_schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("dc", DataType::Utf8, true),
            Field::new("unused", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            raw_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("h"); 7])),
                Arc::new(StringArray::from(vec![Some("west"); 7])),
                Arc::new(Int64Array::from(vec![Some(1); 7])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(3.0),
                    Some(0.5),
                    None,
                    Some(2.0),
                    Some(4.0),
                    Some(7.0),
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    0, 10, 20, 30, 40, 50, 60,
                ])),
            ],
        )
        .unwrap();
        let source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch.clone()]], raw_schema.clone(), None).unwrap(),
        ))) as Arc<dyn ExecutionPlan>;
        let logical = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: raw_schema.to_dfschema_ref().unwrap(),
        });
        let logical = RangeManipulate::new(
            start,
            end,
            interval,
            0,
            range,
            "timestamp".into(),
            vec!["value".into()],
            logical,
        )
        .unwrap();
        let range_exec = logical.to_execution_plan(source.clone());
        let range_exec = range_exec
            .downcast_ref::<RangeManipulateExec>()
            .unwrap()
            .clone();
        let folded_schema = range_exec.schema();
        let projection = rate_projection(&folded_schema, range);
        (range_exec, source, projection, batch)
    }

    #[tokio::test]
    async fn fused_rate_projection_matches_range_projection_for_clipped_windows() {
        for (start, end, interval, duration) in
            [(15, 45, 10, 30), (20, 20, 10, 20), (-15, 105, 10, 30)]
        {
            let (range, source, projection, _) =
                fusion_fixture(start, end, interval, duration).await;
            let fused = range
                .try_fuse_rate_projection(&projection)
                .unwrap()
                .expect("supported rate projection");
            assert_eq!(fused.name(), "PromRangeRateExec");
            let fused = fused.downcast_ref::<RangeManipulateExec>().unwrap().clone();
            assert_eq!(fused.schema(), projection.schema());
            let replacement = range.input.clone();
            let fused = Arc::new(fused)
                .replace_children(
                    vec![replacement],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
                .unwrap();
            assert_eq!(fused.schema(), projection.schema());
            let ordinary_range = range.input.clone();
            let ordinary = ProjectionExec::try_new_with_schema_metadata(
                projection.expr().to_vec(),
                range_to_exec(&range, ordinary_range),
                projection.schema().as_ref(),
            )
            .unwrap();
            let expected = datafusion::physical_plan::collect(
                Arc::new(ordinary),
                SessionContext::default().task_ctx(),
            )
            .await
            .unwrap();
            let actual =
                datafusion::physical_plan::collect(fused, SessionContext::default().task_ctx())
                    .await
                    .unwrap();
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(expected.iter()) {
                assert_eq!(actual.schema(), expected.schema());
                assert_eq!(actual.column(0), expected.column(0));
                assert_eq!(actual.column(1), expected.column(1));
                let a = actual
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let e = expected
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(a.len(), e.len());
                for (a, e) in a.iter().zip(e.iter()) {
                    match (a, e) {
                        (None, None) => {}
                        (Some(a), Some(e)) => assert_eq!(a.to_bits(), e.to_bits()),
                        _ => panic!("rate null mismatch"),
                    }
                }
            }
            let _ = source;
        }
    }

    #[tokio::test]
    async fn fused_replacement_recomputes_projection_properties() {
        let (range, _, projection, _) = fusion_fixture(15, 45, 10, 30).await;
        let fused = range
            .try_fuse_rate_projection(&projection)
            .unwrap()
            .unwrap();
        let replacement = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![], vec![]], range.input.schema(), None).unwrap(),
        ))) as Arc<dyn ExecutionPlan>;
        let fused = fused
            .replace_children(
                vec![replacement.clone()],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        let ordinary = Arc::new(range.clone())
            .replace_children(
                vec![replacement],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        let ordinary = ProjectionExec::try_new_with_schema_metadata(
            projection.expr().to_vec(),
            ordinary,
            projection.schema().as_ref(),
        )
        .unwrap();
        assert!(matches!(
            fused.properties().partitioning,
            Partitioning::UnknownPartitioning(2)
        ));
        assert_eq!(
            fused.properties().partitioning.partition_count(),
            ordinary.properties().partitioning.partition_count()
        );
        assert_eq!(
            fused.properties().emission_type,
            ordinary.properties().emission_type
        );
        assert_eq!(
            fused.properties().boundedness,
            ordinary.properties().boundedness
        );
    }

    #[tokio::test]
    async fn same_name_custom_rate_udf_is_not_fused() {
        let (range, _, projection, _) = fusion_fixture(15, 45, 10, 30).await;
        let custom_udf = datafusion_expr::create_udf(
            "prom_rate",
            vec![
                RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
                RangeArray::convert_data_type(DataType::Float64),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Int64,
            ],
            DataType::Float64,
            datafusion::logical_expr::Volatility::Volatile,
            Arc::new(|_| {
                Ok(datafusion::physical_plan::ColumnarValue::Scalar(
                    ScalarValue::Float64(Some(1.0)),
                ))
            }),
        );
        let custom_projection = rate_projection_with_udf(&range.schema(), 30, custom_udf);
        assert_eq!(
            custom_projection.expr()[2]
                .expr
                .downcast_ref::<ScalarFunctionExpr>()
                .unwrap()
                .name(),
            projection.expr()[2]
                .expr
                .downcast_ref::<ScalarFunctionExpr>()
                .unwrap()
                .name()
        );
        assert!(
            range
                .try_fuse_rate_projection(&custom_projection)
                .unwrap()
                .is_none()
        );
    }

    fn range_to_exec(
        range: &RangeManipulateExec,
        source: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(RangeManipulateExec {
            input: source,
            fused_rate: None,
            ..range.clone()
        })
    }

    const TIME_INDEX_COLUMN: &str = "timestamp";

    fn project_batch(batch: &RecordBatch, indices: &[usize]) -> RecordBatch {
        let fields = indices
            .iter()
            .map(|&idx| batch.schema().field(idx).clone())
            .collect::<Vec<_>>();
        let columns = indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn prepare_test_data() -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("value_1", DataType::Float64, true),
            Field::new("value_2", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
            0, 30_000, 60_000, 90_000, 120_000, // every 30s
            180_000, 240_000, // every 60s
            241_000, 271_000, 291_000, // others
        ])) as _;
        let field_column: ArrayRef = Arc::new(Float64Array::from(vec![1.0; 10])) as _;
        let path_column = Arc::new(StringArray::from(vec!["foo"; 10])) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                timestamp_column,
                field_column.clone(),
                field_column,
                path_column,
            ],
        )
        .unwrap();

        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
        ))
    }

    async fn do_normalize_test(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        expected: String,
    ) {
        let memory_exec = Arc::new(prepare_test_data());
        let time_index = TIME_INDEX_COLUMN.to_string();
        let field_columns = vec!["value_1".to_string(), "value_2".to_string()];
        let manipulate_output_schema = SchemaRef::new(
            RangeManipulate::calculate_output_schema(
                &memory_exec.schema().to_dfschema_ref().unwrap(),
                &time_index,
                &field_columns,
            )
            .unwrap()
            .as_arrow()
            .clone(),
        );
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(manipulate_output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let normalize_exec = Arc::new(RangeManipulateExec {
            offset: 0,
            start,
            end,
            interval,
            range,
            field_columns,
            output_schema: manipulate_output_schema,
            time_range_column: RangeManipulate::build_timestamp_range_name(&time_index),
            time_index_column: time_index,
            input: memory_exec,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            fused_rate: None,
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        // DirectoryArray from RangeArray cannot be print as normal arrays.
        let result_literal: String = result
            .into_iter()
            .filter_map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        if matches!(array.data_type(), &DataType::Dictionary(..)) {
                            let dict_array = array
                                .as_any()
                                .downcast_ref::<DictionaryArray<Int64Type>>()
                                .unwrap()
                                .clone();
                            format!("{:?}", RangeArray::try_new(dict_array).unwrap())
                        } else {
                            format!("{array:?}")
                        }
                    })
                    .reduce(|lhs, rhs| lhs + "\n" + &rhs)
            })
            .reduce(|lhs, rhs| lhs + "\n\n" + &rhs)
            .unwrap();

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn native_timestamps_preserve_range_membership_and_ms_payload() {
        for (unit, ticks_per_ms) in [
            (TimeUnit::Microsecond, 1_000_i64),
            (TimeUnit::Nanosecond, 1_000_000_i64),
        ] {
            let lower = 1_000 * ticks_per_ms;
            let upper = 1_001 * ticks_per_ms;
            // Exclude the lower boundary and future sample; retain both native
            // samples in the same millisecond bucket and the exact upper sample.
            let timestamps = vec![lower, lower + 1, lower + 2, upper, upper + 1];
            let time: ArrayRef = match unit {
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(timestamps)),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(timestamps)),
                _ => unreachable!(),
            };
            let schema = Arc::new(Schema::new(vec![
                Field::new(TIME_INDEX_COLUMN, DataType::Timestamp(unit, None), false),
                Field::new("value", DataType::Float64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    time,
                    Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
                ],
            )
            .unwrap();
            let logical_input = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone().to_dfschema_ref().unwrap(),
            });
            let plan = RangeManipulate::new(
                1_001,
                1_001,
                1,
                0,
                1,
                TIME_INDEX_COLUMN.to_string(),
                vec!["value".to_string()],
                logical_input.clone(),
            )
            .unwrap();
            let output_time = Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            );
            let output_schema = Arc::new(Schema::new(vec![
                output_time.clone(),
                RangeArray::convert_field(&Field::new("value", DataType::Float64, true)),
                Field::new(
                    RangeManipulate::build_timestamp_range_name(TIME_INDEX_COLUMN),
                    RangeArray::convert_field(&output_time).data_type().clone(),
                    false,
                ),
            ]));
            assert_eq!(plan.schema().as_arrow(), output_schema.as_ref());

            let rebuilt = RangeManipulate::deserialize(&plan.serialize())
                .unwrap()
                .with_exprs_and_inputs(vec![], vec![logical_input])
                .unwrap();
            assert_eq!(rebuilt.schema(), plan.schema());
            assert_eq!(rebuilt.input.schema().as_arrow(), schema.as_ref());

            let input = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap(),
            )));
            let exec = rebuilt.to_execution_plan(input);
            assert_eq!(exec.schema(), output_schema);
            assert_eq!(exec.children()[0].schema(), schema);

            let batches =
                datafusion::physical_plan::collect(exec, SessionContext::default().task_ctx())
                    .await
                    .unwrap();
            assert_eq!(batches.len(), 1, "{unit:?}");
            let output = &batches[0];
            assert_eq!(output.schema(), output_schema);
            assert_eq!(output.num_rows(), 1);
            assert_eq!(
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[1_001]
            );

            // RangeArray packs offset/length into dictionary keys; Arrow dictionary
            // equality treats those packed keys as indices and cannot compare them.
            let values = RangeArray::try_new(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int64Type>>()
                    .unwrap()
                    .clone(),
            )
            .unwrap();
            assert_eq!(values.get_offset_length(0), Some((1, 3)));
            assert_eq!(
                values.get(0).unwrap().to_data(),
                Float64Array::from(vec![20.0, 30.0, 40.0]).to_data()
            );
            let timestamps = RangeArray::try_new(
                output
                    .column(2)
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int64Type>>()
                    .unwrap()
                    .clone(),
            )
            .unwrap();
            assert_eq!(timestamps.get_offset_length(0), Some((1, 3)));
            assert_eq!(
                timestamps.get(0).unwrap().to_data(),
                TimestampMillisecondArray::from(vec![1_000, 1_000, 1_001]).to_data()
            );
        }
    }

    #[test]
    fn logical_offset_participates_in_ordering() {
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: prepare_test_data().schema().to_dfschema_ref().unwrap(),
        });
        let first = RangeManipulate::new(
            0,
            0,
            0,
            0,
            0,
            TIME_INDEX_COLUMN.to_string(),
            vec!["value_1".to_string()],
            input.clone(),
        )
        .unwrap();
        let second = RangeManipulate::new(
            0,
            0,
            0,
            1,
            0,
            TIME_INDEX_COLUMN.to_string(),
            vec!["value_1".to_string()],
            input,
        )
        .unwrap();
        assert_ne!(first, second);
        assert_eq!(first.partial_cmp(&second), Some(std::cmp::Ordering::Less));
    }

    #[tokio::test]
    async fn logical_normalize_offset_survives_rebuild_and_executes() {
        for (name, time_unit, raw, offset, start, range, expected_payload) in [
            (
                "millisecond offset",
                TimeUnit::Millisecond,
                0,
                1_000,
                1_000,
                1_000,
                1_000,
            ),
            (
                "negative native lower limit with positive window",
                TimeUnit::Nanosecond,
                -9_223_112_837_000_000_000,
                -259_200_000,
                -9_223_372_037_000,
                300_000,
                -9_223_372_037_000,
            ),
            (
                "second timestamp with negative fractional offset",
                TimeUnit::Second,
                1,
                -500,
                1_000,
                1_000,
                500,
            ),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    TIME_INDEX_COLUMN,
                    DataType::Timestamp(time_unit, None),
                    false,
                ),
                Field::new("value", DataType::Float64, true),
            ]));
            let input = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone().to_dfschema_ref().unwrap(),
            });
            let normalize = crate::extension_plan::SeriesNormalize::new(
                offset,
                TIME_INDEX_COLUMN,
                false,
                Vec::new(),
                input.clone(),
            );
            let normalize =
                crate::extension_plan::SeriesNormalize::deserialize(&normalize.serialize())
                    .unwrap()
                    .with_exprs_and_inputs(vec![], vec![input.clone()])
                    .unwrap();
            let normalized = LogicalPlan::Extension(Extension {
                node: Arc::new(normalize),
            });
            let fresh = RangeManipulate::new(
                start,
                start,
                1,
                offset,
                range,
                TIME_INDEX_COLUMN.to_string(),
                vec!["value".to_string()],
                input.clone(),
            )
            .unwrap()
            .with_exprs_and_inputs(vec![], vec![input.clone()])
            .unwrap();
            let serialized = RangeManipulate::new(
                start,
                start,
                1,
                offset,
                range,
                TIME_INDEX_COLUMN.to_string(),
                vec!["value".to_string()],
                normalized.clone(),
            )
            .unwrap();
            let decoded = RangeManipulate::deserialize(&serialized.serialize())
                .unwrap()
                .with_exprs_and_inputs(vec![], vec![normalized])
                .unwrap();
            let timestamp: ArrayRef = match time_unit {
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(vec![raw])),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(vec![raw])),
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(vec![raw])),
                _ => unreachable!(),
            };
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![timestamp, Arc::new(Float64Array::from(vec![7.0]))],
            )
            .unwrap();
            for (mode, rebuilt) in [("fresh", fresh), ("decoded", decoded)] {
                let rebuilt = rebuilt
                    .with_exprs_and_inputs(vec![], vec![input.clone()])
                    .unwrap();
                assert_eq!(rebuilt.offset, offset, "{name}: {mode}");
                assert_eq!(rebuilt.input.schema(), input.schema(), "{name}: {mode}");

                let empty_exec_input = Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(&[vec![]], schema.clone(), None).unwrap(),
                )));
                let exec_input = Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(&[vec![batch.clone()]], schema.clone(), None)
                        .unwrap(),
                )));
                let exec = rebuilt
                    .to_execution_plan(empty_exec_input)
                    .replace_children(
                        vec![exec_input],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )
                    .unwrap();
                let output =
                    datafusion::physical_plan::collect(exec, SessionContext::default().task_ctx())
                        .await
                        .unwrap();
                assert_eq!(output.len(), 1, "{name}: {mode}");
                let output = &output[0];
                assert_eq!(output.num_rows(), 1, "{name}: {mode}");
                assert_eq!(
                    output
                        .column(0)
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(0),
                    start,
                    "{name}: {mode}"
                );
                let values = RangeArray::try_new(
                    output
                        .column(1)
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int64Type>>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                assert_eq!(values.get_offset_length(0), Some((0, 1)), "{name}: {mode}");
                assert_eq!(
                    values.get(0).unwrap().to_data(),
                    Float64Array::from(vec![7.0]).to_data(),
                    "{name}: {mode}"
                );
                let timestamps = RangeArray::try_new(
                    output
                        .column(2)
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int64Type>>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                assert_eq!(
                    timestamps.get_offset_length(0),
                    Some((0, 1)),
                    "{name}: {mode}"
                );
                assert_eq!(
                    timestamps.get(0).unwrap().to_data(),
                    TimestampMillisecondArray::from(vec![expected_payload]).to_data(),
                    "{name}: {mode}"
                );
            }
        }
    }

    #[tokio::test]
    async fn range_payload_preserves_null_timestamp_and_rejects_offset_overflow() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let null_timestamp =
            TimestampMillisecondArray::new(vec![1_000].into(), Some(NullBuffer::from(vec![false])));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(null_timestamp),
                Arc::new(Float64Array::from(vec![7.0])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap(),
        )));
        let plan = RangeManipulate::new(
            1_000,
            1_000,
            1,
            0,
            1,
            TIME_INDEX_COLUMN.to_string(),
            vec!["value".to_string()],
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone().to_dfschema_ref().unwrap(),
            }),
        )
        .unwrap();
        let output = datafusion::physical_plan::collect(
            plan.to_execution_plan(input),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap();
        let timestamps = RangeArray::try_new(
            output[0]
                .column(2)
                .as_any()
                .downcast_ref::<DictionaryArray<Int64Type>>()
                .unwrap()
                .clone(),
        )
        .unwrap();
        let payload = timestamps.get(0).unwrap();
        let payload = payload
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(payload.len(), 1);
        assert!(!payload.is_valid(0));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, i64::MAX])),
                Arc::new(Float64Array::from(vec![7.0, 8.0])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap(),
        )));
        let normalized = crate::extension_plan::SeriesNormalize::new(
            1,
            TIME_INDEX_COLUMN,
            false,
            Vec::new(),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.to_dfschema_ref().unwrap(),
            }),
        );
        let plan = RangeManipulate::new(
            1,
            1,
            1,
            1,
            1,
            TIME_INDEX_COLUMN.to_string(),
            vec!["value".to_string()],
            LogicalPlan::Extension(Extension {
                node: Arc::new(normalized),
            }),
        )
        .unwrap();
        let error = datafusion::physical_plan::collect(
            plan.to_execution_plan(input),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timestamp payload overflow"));
    }

    #[tokio::test]
    async fn pruning_should_keep_time_and_value_columns_for_exec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("value_1", DataType::Float64, true),
            Field::new("value_2", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let df_schema = schema.clone().to_dfschema_ref().unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        });
        let plan = RangeManipulate::new(
            0,
            310_000,
            30_000,
            0,
            90_000,
            TIME_INDEX_COLUMN.to_string(),
            vec!["value_1".to_string(), "value_2".to_string()],
            input,
        )
        .unwrap();

        // Simulate a parent projection requesting only the `path` column.
        let output_columns = [3usize];
        let required = plan.necessary_children_exprs(&output_columns).unwrap();
        let required = &required[0];
        assert_eq!(required.as_slice(), &[0, 1, 2, 3]);

        let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
            0, 30_000, 60_000, 90_000, 120_000, // every 30s
            180_000, 240_000, // every 60s
            241_000, 271_000, 291_000, // others
        ])) as _;
        let field_column: ArrayRef = Arc::new(Float64Array::from(vec![1.0; 10])) as _;
        let path_column = Arc::new(StringArray::from(vec!["foo"; 10])) as _;
        let input_batch = RecordBatch::try_new(
            schema,
            vec![
                timestamp_column,
                field_column.clone(),
                field_column,
                path_column,
            ],
        )
        .unwrap();

        let projected = project_batch(&input_batch, required);
        let projected_schema = projected.schema();
        let memory_exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![projected]], projected_schema, None).unwrap(),
        )));
        let range_exec = plan.to_execution_plan(memory_exec);
        let session_context = SessionContext::default();
        let output_batches =
            datafusion::physical_plan::collect(range_exec, session_context.task_ctx())
                .await
                .unwrap();
        assert_eq!(output_batches.len(), 1);

        let output_batch = &output_batches[0];
        let path = output_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(path.iter().all(|v| v == Some("foo")));

        // Simulate the pre-fix pruning behavior: omit the timestamp/value columns from the child.
        let broken_required = [3usize];
        let broken = project_batch(&input_batch, &broken_required);
        let broken_schema = broken.schema();
        let broken_exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![broken]], broken_schema, None).unwrap(),
        )));
        let broken_range_exec = plan.to_execution_plan(broken_exec);
        let session_context = SessionContext::default();
        let broken_result = std::panic::AssertUnwindSafe(async {
            datafusion::physical_plan::collect(broken_range_exec, session_context.task_ctx()).await
        })
        .catch_unwind()
        .await;
        assert!(broken_result.is_err());
    }

    #[tokio::test]
    async fn interval_30s_range_90s() {
        let expected = String::from(
            "PrimitiveArray<Timestamp(ms)>\n[\n  \
                1970-01-01T00:00:00,\n  \
                1970-01-01T00:00:30,\n  \
                1970-01-01T00:01:00,\n  \
                1970-01-01T00:01:30,\n  \
                1970-01-01T00:02:00,\n  \
                1970-01-01T00:02:30,\n  \
                1970-01-01T00:03:00,\n  \
                1970-01-01T00:03:30,\n  \
                1970-01-01T00:04:00,\n  \
                1970-01-01T00:04:30,\n  \
                1970-01-01T00:05:00,\n\
            ]\nRangeArray { \
                base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
                ranges: [Some(0..1), Some(0..2), Some(0..3), Some(1..4), Some(2..5), Some(3..5), Some(4..6), Some(5..6), Some(5..7), Some(6..8), Some(6..10)] \
            }\nRangeArray { \
                base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
                ranges: [Some(0..1), Some(0..2), Some(0..3), Some(1..4), Some(2..5), Some(3..5), Some(4..6), Some(5..6), Some(5..7), Some(6..8), Some(6..10)] \
            }\nStringArray\n[\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n]\n\
            RangeArray { \
                base array: PrimitiveArray<Timestamp(ms)>\n[\n  1970-01-01T00:00:00,\n  1970-01-01T00:00:30,\n  1970-01-01T00:01:00,\n  1970-01-01T00:01:30,\n  1970-01-01T00:02:00,\n  1970-01-01T00:03:00,\n  1970-01-01T00:04:00,\n  1970-01-01T00:04:01,\n  1970-01-01T00:04:31,\n  1970-01-01T00:04:51,\n], \
                ranges: [Some(0..1), Some(0..2), Some(0..3), Some(1..4), Some(2..5), Some(3..5), Some(4..6), Some(5..6), Some(5..7), Some(6..8), Some(6..10)] \
            }",
        );
        do_normalize_test(0, 310_000, 30_000, 90_000, expected.clone()).await;

        // dump large range
        do_normalize_test(-300000, 310_000, 30_000, 90_000, expected).await;
    }

    #[tokio::test]
    async fn small_empty_range() {
        let expected = String::from(
            "PrimitiveArray<Timestamp(ms)>\n[\n  \
            1970-01-01T00:00:00.001,\n  \
            1970-01-01T00:00:03.001,\n  \
            1970-01-01T00:00:06.001,\n  \
            1970-01-01T00:00:09.001,\n\
        ]\nRangeArray { \
            base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
            ranges: [Some(0..1), Some(0..0), Some(0..0), Some(0..0)] \
        }\nRangeArray { \
            base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
            ranges: [Some(0..1), Some(0..0), Some(0..0), Some(0..0)] \
        }\nStringArray\n[\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n]\n\
        RangeArray { \
            base array: PrimitiveArray<Timestamp(ms)>\n[\n  1970-01-01T00:00:00,\n  1970-01-01T00:00:30,\n  1970-01-01T00:01:00,\n  1970-01-01T00:01:30,\n  1970-01-01T00:02:00,\n  1970-01-01T00:03:00,\n  1970-01-01T00:04:00,\n  1970-01-01T00:04:01,\n  1970-01-01T00:04:31,\n  1970-01-01T00:04:51,\n], \
            ranges: [Some(0..1), Some(0..0), Some(0..0), Some(0..0)] \
        }",
        );
        do_normalize_test(1, 10_001, 3_000, 1_000, expected).await;
    }

    #[test]
    fn test_calculate_range_preserves_alignment() {
        // Test case: query starts at timestamp ending in 4000, step is 30s
        // Data starts at different alignment - should preserve query's 4000 pattern
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            TimestampMillisecondType::DATA_TYPE,
            false,
        )]));
        let empty_stream = MemoryStream::try_new(vec![], schema.clone(), None).unwrap();

        let stream = RangeManipulateStream {
            offset: 0,
            start: 1758093274000, // ends in 4000
            end: 1758093334000,   // ends in 4000
            interval: 30000,      // 30s step
            range: 60000,         // 60s lookback
            time_index: 0,
            time_unit: TimeUnit::Millisecond,
            field_columns: vec![],
            aligned_ts_array: Arc::new(TimestampMillisecondArray::from(vec![0i64; 0])),
            output_schema: schema.clone(),
            input: Box::pin(empty_stream),
            metric: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            num_series: Count::new(),
            fused_rate: None,
        };

        // Create test data with timestamps not aligned to query pattern
        let test_timestamps = vec![
            1758093260000, // ends in 0000 (different alignment)
            1758093290000, // ends in 0000
            1758093320000, // ends in 0000
        ];
        let ts_array = TimestampMillisecondArray::from(test_timestamps);
        let test_schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            TimestampMillisecondType::DATA_TYPE,
            false,
        )]));
        let batch = RecordBatch::try_new(test_schema, vec![Arc::new(ts_array)]).unwrap();

        let (ranges, (start, end)) = stream.calculate_range(&batch).unwrap();

        // Verify the optimized start preserves query alignment (should end in 4000)
        assert_eq!(
            start % 30000,
            1758093274000 % 30000,
            "Optimized start should preserve query alignment pattern"
        );

        // Verify we generate correct number of ranges for the alignment
        let expected_timestamps: Vec<i64> = (start..=end).step_by(30000).collect();
        assert_eq!(ranges.len(), expected_timestamps.len());

        // Verify all generated timestamps maintain the same alignment pattern
        for ts in expected_timestamps {
            assert_eq!(
                ts % 30000,
                1758093274000 % 30000,
                "All timestamps should maintain query alignment pattern"
            );
        }
    }

    #[tokio::test]
    async fn no_intersection_batch_is_skipped_and_stream_continues() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                TimestampMillisecondType::DATA_TYPE,
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: schema.clone().to_dfschema_ref().unwrap(),
        });
        let plan = RangeManipulate::new(
            0,
            50,
            10,
            0,
            1,
            TIME_INDEX_COLUMN.to_string(),
            vec!["value".to_string()],
            input,
        )
        .unwrap();
        let no_intersection = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![100])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let intersection = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![20])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![no_intersection, intersection]], schema, None)
                .unwrap(),
        )));

        let batches = datafusion::physical_plan::collect(
            plan.to_execution_plan(input),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let timestamps = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(0), 20);
    }

    fn calculate_range_for_test(
        query_start: i64,
        query_end: i64,
        interval: i64,
        range: i64,
        timestamps: &[i64],
    ) -> (Vec<(u32, u32)>, (i64, i64)) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            TIME_INDEX_COLUMN,
            TimestampMillisecondType::DATA_TYPE,
            false,
        )]));
        let empty_stream = MemoryStream::try_new(vec![], schema.clone(), None).unwrap();
        let stream = RangeManipulateStream {
            offset: 0,
            start: query_start,
            end: query_end,
            interval,
            range,
            time_index: 0,
            time_unit: TimeUnit::Millisecond,
            field_columns: vec![],
            aligned_ts_array: Arc::new(TimestampMillisecondArray::from(vec![0i64; 0])),
            output_schema: schema.clone(),
            input: Box::pin(empty_stream),
            metric: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            num_series: Count::new(),
            fused_rate: None,
        };
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(
                timestamps.to_vec(),
            ))],
        )
        .unwrap();

        stream.calculate_range(&batch).unwrap()
    }

    #[test]
    fn calculate_range_keeps_query_aligned_tail() {
        let (ranges, bounds) = calculate_range_for_test(4, 94, 30, 15, &[20, 50, 80]);

        assert_eq!(bounds, (34, 94));
        assert_eq!(ranges, vec![(0, 1), (1, 1), (2, 1)]);
    }

    /// Calculates exact offsets directly from the range predicate used by PromQL.
    ///
    /// Input timestamps are sorted and non-null. Interval is positive, range is
    /// nonnegative, and test values are chosen to avoid `i64` overflow.
    fn calculate_range_oracle(
        timestamps: &[i64],
        start: i64,
        end: i64,
        interval: i64,
        range: i64,
    ) -> Vec<(u32, u32)> {
        // Match `calculate_range`'s explicit empty-input early return.
        if timestamps.is_empty() || start > end {
            return vec![];
        }

        (start..=end)
            .step_by(interval as usize)
            .map(|curr| {
                let mut offset = None;
                let mut length = 0;
                for (index, &ts) in timestamps.iter().enumerate() {
                    if ts > curr - range && ts <= curr {
                        offset.get_or_insert(index);
                        length += 1;
                    }
                }
                (offset.unwrap_or(0) as u32, length)
            })
            .collect()
    }

    #[test]
    fn calculate_range_characterizes_returned_bounds() {
        let cases = [
            (
                "positive non-aligned last timestamp plus range",
                0,
                100,
                10,
                9,
                vec![13, 26],
                (20, 30),
            ),
            (
                "negative non-aligned last timestamp plus range",
                -50,
                50,
                10,
                9,
                vec![-37, -26],
                (-30, -20),
            ),
            (
                "query alignment not based on epoch",
                4,
                94,
                30,
                15,
                vec![20, 50, 80],
                (34, 94),
            ),
            ("leading data", 0, 100, 10, 10, vec![-10, 15], (0, 20)),
            (
                "trailing data past query end",
                0,
                100,
                10,
                10,
                vec![35, 45, 110],
                (40, 100),
            ),
            (
                "optimized start after query end",
                0,
                50,
                10,
                0,
                vec![100],
                (0, 50),
            ),
        ];

        for (name, query_start, query_end, interval, range, timestamps, expected_bounds) in cases {
            let (_, bounds) =
                calculate_range_for_test(query_start, query_end, interval, range, &timestamps);
            assert_eq!(bounds, expected_bounds, "{name}");
        }
    }

    #[test]
    fn calculate_range_keeps_extreme_range_tail() {
        let (ranges, bounds) =
            calculate_range_for_test(i64::MAX - 1, i64::MAX, 1, i64::MAX, &[i64::MAX]);

        assert_eq!(bounds, (i64::MAX, i64::MAX));
        assert_eq!(ranges, vec![(0, 1)]);
    }

    #[test]
    fn calculate_range_matches_bruteforce_oracle_for_deterministic_cases() {
        let cases = vec![
            (
                "duplicate lower and upper bounds",
                vec![0, 10, 10, 20, 20, 30],
                10,
                20,
                10,
                10,
            ),
            (
                "zero range excludes duplicates at current timestamp",
                vec![10, 10, 10],
                10,
                10,
                1,
                0,
            ),
            (
                "consecutive nonempty empty nonempty ranges",
                vec![10, 30],
                10,
                30,
                10,
                5,
            ),
            ("step smaller than range", vec![0, 4, 8, 12], 0, 12, 3, 5),
            ("step equal to range", vec![0, 5, 10, 15], 0, 15, 5, 5),
            ("step greater than range", vec![0, 7, 14, 21], 0, 21, 7, 3),
            (
                "negative sparse/tail timestamps",
                vec![-30, -20, -10, 0],
                -25,
                5,
                5,
                7,
            ),
            ("one sample", vec![42], 0, 100, 10, 15),
            ("empty input", vec![], -20, 20, 5, 10),
        ];

        for (name, timestamps, query_start, query_end, interval, range) in cases {
            let (actual, (start, end)) =
                calculate_range_for_test(query_start, query_end, interval, range, &timestamps);
            let expected = calculate_range_oracle(&timestamps, start, end, interval, range);
            assert_eq!(actual, expected, "{name}");
        }
    }

    #[test]
    fn calculate_range_positive_time_translated_regression() {
        let timestamps = [0, 10, 20, 30];
        let expected = vec![(0, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1), (3, 1)];
        let (actual, bounds) = calculate_range_for_test(5, 35, 5, 7, &timestamps);

        assert_eq!(bounds, (5, 35));
        assert_eq!(
            calculate_range_oracle(&timestamps, bounds.0, bounds.1, 5, 7),
            expected
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn calculate_range_matches_oracle_for_dense_positive_time_windows() {
        let timestamps = (0..=3_600).step_by(15).collect::<Vec<i64>>();

        for (name, range) in [
            ("one minute", 60),
            ("five minutes", 300),
            ("one hour", 3_600),
        ] {
            let (actual, (start, end)) = calculate_range_for_test(0, 3_600, 15, range, &timestamps);
            assert_eq!((start, end), (0, 3_600), "{name} bounds");
            assert_eq!(
                actual,
                calculate_range_oracle(&timestamps, start, end, 15, range),
                "{name} window"
            );
        }
    }

    #[derive(Clone, Copy)]
    struct TinyPrng(u64);

    impl TinyPrng {
        fn next_u64(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn next_i64(&mut self, min: i64, max: i64) -> i64 {
            min + (self.next_u64() % (max - min + 1) as u64) as i64
        }
    }

    #[test]
    fn calculate_range_matches_bruteforce_oracle_for_seeded_matrix() {
        let mut prng = TinyPrng(0x5eed_cafe_f00d_baad);

        for case in 0..512 {
            let interval = prng.next_i64(1, 11);
            let range = prng.next_i64(0, 25);
            let query_start = prng.next_i64(-200, 200);
            let query_end = query_start + interval * prng.next_i64(0, 20);
            let mut timestamps = Vec::new();
            let mut timestamp = prng.next_i64(-250, 250);
            for _ in 0..prng.next_i64(0, 20) {
                timestamp += prng.next_i64(0, 7);
                timestamps.push(timestamp);
            }

            let (actual, (start, end)) =
                calculate_range_for_test(query_start, query_end, interval, range, &timestamps);
            let expected = calculate_range_oracle(&timestamps, start, end, interval, range);
            let expected = if actual.is_empty() && !expected.is_empty() {
                assert!(
                    expected.iter().all(|(_, len)| *len == 0),
                    "case={case}, timestamps={timestamps:?}, query=({query_start}, {query_end}), \
                     interval={interval}, range={range}, bounds=({start}, {end}): \
                     no-intersection output must have no selected samples"
                );
                vec![]
            } else {
                expected
            };
            assert_eq!(
                actual, expected,
                "case={case}, timestamps={timestamps:?}, query=({query_start}, {query_end}), \
                 interval={interval}, range={range}, bounds=({start}, {end})"
            );
        }
    }
}
