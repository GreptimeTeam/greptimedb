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
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, ExprSchema, Result, ScalarValue, TableReference,
};
use datafusion_expr::{EmptyRelation, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_optimizer::optimize_projections::OptimizeProjections;
use datafusion_optimizer::{OptimizerContext, OptimizerRule};
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::{
    EquivalenceProperties, LexRequirement, OrderingRequirements, Partitioning, PhysicalExpr,
    PhysicalSortRequirement,
};
use futures::{StreamExt, TryStreamExt, stream};
use promql::extension_plan::{InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize};
use serde::{Deserialize, Serialize};
use store_api::region_engine::ScanProcessor;

use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::scan_range::{
    RangeFunction, RangeOutput, RangeProcessor, RangeSpec,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct Window {
    #[serde(default)]
    function: RangeFunction,
    start: i64,
    end: i64,
    step: i64,
    width: i64,
    offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct Output {
    source: usize,
    name: String,
    qualifier: Option<String>,
    nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SeriesRange {
    input: LogicalPlan,
    window: Window,
    // Timestamp, sample value, TSID, then projected labels.
    columns: Vec<Column>,
    output: Vec<Output>,
    schema: DFSchemaRef,
    unresolved: Option<Vec<usize>>,
}

#[derive(Serialize, Deserialize)]
struct WireRange {
    version: u8,
    window: Window,
    columns: Vec<usize>,
    output: Vec<Output>,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

fn bare(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => bare(&alias.expr),
        _ => expr,
    }
}

impl SeriesRange {
    pub(crate) const NAME: &str = "ResearchSeriesRange";

    pub(crate) fn shift_time(&self, delta: i64) -> Result<Self> {
        let mut rebound = self.clone();
        rebound.window.start =
            crate::research_series_grid::plan_template::shift(self.window.start, delta)?;
        rebound.window.end =
            crate::research_series_grid::plan_template::shift(self.window.end, delta)?;
        Ok(rebound)
    }

    pub(crate) fn recognize_instant(
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
                    .ok_or_else(|| invalid("Missing instant selector output"))?;
                let (qualifier, field) = plan.schema().qualified_field(index);
                Ok(Expr::Column(Column::new(qualifier.cloned(), field.name())))
            })
            .collect::<Result<Vec<_>>>()?;
        let input = super::series_fragment::peel_projections(plan, &mut expressions)?;
        let LogicalPlan::Extension(extension) = input else {
            return Ok(None);
        };
        let Some(instant) = extension.node.as_any().downcast_ref::<InstantManipulate>() else {
            return Ok(None);
        };
        let Some(((start, end, step, lookback, offset), instant_time)) =
            instant.research_series_instant()
        else {
            return Ok(None);
        };
        if lookback < 0 || step <= 0 || end < start {
            return Ok(None);
        }
        let mut input = instant.inputs()[0];
        if let LogicalPlan::Extension(extension) = input
            && let Some(normalize) = extension.node.as_any().downcast_ref::<SeriesNormalize>()
        {
            if normalize.research_series_normalization() != (offset, instant_time, false) {
                return Ok(None);
            }
            input = normalize.inputs()[0];
        }
        let LogicalPlan::Extension(extension) = input else {
            return Ok(None);
        };
        let Some(divide) = extension.node.as_any().downcast_ref::<SeriesDivide>() else {
            return Ok(None);
        };
        if divide.tags() != ["__tsid"] {
            return Ok(None);
        }
        let input = divide.inputs()[0];
        if input
            .schema()
            .index_of_column_by_name(None, "greptime_native_histogram")
            .is_some()
        {
            return Ok(None);
        }
        let Some(tsid) = input.schema().index_of_column_by_name(None, "__tsid") else {
            return Ok(None);
        };
        let (qualifier, field) = input.schema().qualified_field(tsid);
        let source_columns = expressions
            .iter()
            .map(|expr| match bare(expr) {
                Expr::Column(column) => Some(column.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        let Some(source_columns) = source_columns else {
            return Ok(None);
        };
        if source_columns[tags.len()].name != instant_time {
            return Ok(None);
        }
        let columns = vec![
            source_columns[tags.len()].clone(),
            source_columns[tags.len() + 1].clone(),
            Column::new(qualifier.cloned(), field.name()),
        ]
        .into_iter()
        .chain(source_columns[..tags.len()].iter().cloned())
        .collect();
        let names = tags.iter().map(String::as_str).chain([time, value]);
        let sources = (3..3 + tags.len()).chain([0, 1]);
        let output = names
            .zip(sources)
            .map(|(name, source)| {
                let index = plan.schema().index_of_column_by_name(None, name).unwrap();
                let (qualifier, field) = plan.schema().qualified_field(index);
                Output {
                    source,
                    name: field.name().clone(),
                    qualifier: qualifier.map(TableReference::to_quoted_string),
                    nullable: field.is_nullable(),
                }
            })
            .collect();
        Self::new(
            input.clone(),
            Window {
                function: RangeFunction::Instant,
                start,
                end,
                step,
                width: lookback,
                offset,
            },
            columns,
            output,
        )
        .map(Some)
    }

    pub(crate) fn plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub(crate) fn rewrite(plan: LogicalPlan) -> Result<LogicalPlan> {
        // PromPlanner carries TSID through function projections until the outer
        // projection drops it. Prune that output before recognizing the window.
        let plan = OptimizeProjections::new()
            .rewrite(plan, &OptimizerContext::new())?
            .data;
        plan.transform_up(|plan| {
            Ok(match Self::recognize(&plan)? {
                Some(node) => Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                })),
                None => Transformed::no(plan),
            })
        })
        .map(|result| result.data)
    }

    fn recognize(plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(None);
        };
        let Expr::IsNotNull(filtered) = &filter.predicate else {
            return Ok(None);
        };
        let Expr::Column(filtered) = filtered.as_ref() else {
            return Ok(None);
        };
        let LogicalPlan::Projection(projection) = filter.input.as_ref() else {
            return Ok(None);
        };
        let LogicalPlan::Extension(extension) = projection.input.as_ref() else {
            return Ok(None);
        };
        let Some(range) = extension.node.as_any().downcast_ref::<RangeManipulate>() else {
            return Ok(None);
        };
        let Some((start, end, step, width, offset, time, value)) = range.research_series_window()
        else {
            return Ok(None);
        };
        if step <= 0
            || width <= 0
            || width > step
            || end < start
            || (i128::from(end) - i128::from(start)) / i128::from(step) + 1 > 1_000_000
        {
            return Ok(None);
        }
        let input = range.inputs()[0];
        let LogicalPlan::Extension(normalize) = input else {
            return Ok(None);
        };
        let Some(normalize) = normalize.node.as_any().downcast_ref::<SeriesNormalize>() else {
            return Ok(None);
        };
        if normalize.research_series_normalization() != (offset, time, true) {
            return Ok(None);
        }
        let LogicalPlan::Extension(divide) = normalize.inputs()[0] else {
            return Ok(None);
        };
        let Some(divide) = divide.node.as_any().downcast_ref::<SeriesDivide>() else {
            return Ok(None);
        };
        if divide.tags() != ["__tsid"] {
            return Ok(None);
        }
        let schema = input.schema();
        let column = |name: &str| -> Option<Column> {
            let index = schema.index_of_column_by_name(None, name)?;
            let (qualifier, field) = schema.qualified_field(index);
            Some(Column::new(qualifier.cloned(), field.name()))
        };
        let (Some(time_column), Some(value_column), Some(tsid_column)) =
            (column(time), column(value), column("__tsid"))
        else {
            return Ok(None);
        };
        let mut columns = vec![time_column, value_column, tsid_column];
        let expected = [
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Float64,
            DataType::UInt64,
        ];
        for (column, datatype) in columns.iter().zip(&expected) {
            let field = schema.field_from_column(column)?;
            if field.data_type() != datatype
                || (datatype != &DataType::Float64 && field.is_nullable())
            {
                return Ok(None);
            }
        }
        let time_range = RangeManipulate::build_timestamp_range_name(time);
        let range_schema = range.schema();
        let matches_column = |expr: &Expr, name: &str| matches!(expr, Expr::Column(column) if range_schema.index_of_column(column).ok() == range_schema.index_of_column_by_name(None, name));
        let mut output = Vec::new();
        let mut rate_index = None;
        let mut counter_function = RangeFunction::Rate;
        let mut has_time = false;
        for (index, expr) in projection.expr.iter().enumerate() {
            let source = match bare(expr) {
                Expr::Column(column) if column.name == time => {
                    has_time = true;
                    0
                }
                Expr::Column(column)
                    if column.name != value
                        && column.name != time_range
                        && column.name != "__tsid" =>
                {
                    let field = schema.field_from_column(column)?;
                    let label_type = match field.data_type() {
                        DataType::Dictionary(_, value) => value.as_ref(),
                        datatype => datatype,
                    };
                    if !label_type.is_string() {
                        return Ok(None);
                    }
                    columns.push(column.clone());
                    columns.len() - 1
                }
                Expr::ScalarFunction(function)
                    if matches!(
                        function.name(),
                        "prom_rate" | "prom_increase" | "prom_irate"
                    ) && rate_index.is_none() =>
                {
                    counter_function = match function.name() {
                        "prom_increase" => RangeFunction::Increase,
                        "prom_irate" => RangeFunction::Irate,
                        _ => RangeFunction::Rate,
                    };
                    let args = function.args.as_slice();
                    let [times, values, ..] = args else {
                        return Ok(None);
                    };
                    if !matches_column(times, &time_range) || !matches_column(values, value) {
                        return Ok(None);
                    }
                    if counter_function == RangeFunction::Irate {
                        if args.len() != 2 {
                            return Ok(None);
                        }
                    } else if !matches!(args, [_, _, instant, length]
                        if matches_column(instant, time)
                        && matches!(length, Expr::Literal(ScalarValue::Int64(Some(length)), _) if *length == width))
                    {
                        return Ok(None);
                    }
                    rate_index = Some(index);
                    1
                }
                _ => return Ok(None),
            };
            let (qualifier, field) = projection.schema.qualified_field(index);
            output.push(Output {
                source,
                name: field.name().clone(),
                qualifier: qualifier.map(TableReference::to_quoted_string),
                nullable: field.is_nullable(),
            });
        }
        if !has_time
            || rate_index.is_none()
            || projection.schema.index_of_column(filtered).ok() != rate_index
        {
            return Ok(None);
        }
        Self::new(
            divide.inputs()[0].clone(),
            Window {
                function: counter_function,
                start,
                end,
                step,
                width,
                offset,
            },
            columns,
            output,
        )
        .map(Some)
    }

    fn new(
        input: LogicalPlan,
        window: Window,
        columns: Vec<Column>,
        output: Vec<Output>,
    ) -> Result<Self> {
        if window.function == RangeFunction::Max
            || columns.len() < 3
            || window.step <= 0
            || (window.function == RangeFunction::Instant && window.width < 0)
            || (window.function != RangeFunction::Instant
                && (window.width <= 0 || window.width > window.step))
            || window.end < window.start
            || (i128::from(window.end) - i128::from(window.start)) / i128::from(window.step) + 1
                > 1_000_000
        {
            return Err(invalid("Invalid series range descriptor"));
        }
        if output.iter().filter(|field| field.source == 0).count() != 1
            || output.iter().filter(|field| field.source == 1).count() != 1
        {
            return Err(invalid(
                "Series range requires one timestamp and one value output",
            ));
        }
        for (index, datatype) in [
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Float64,
            DataType::UInt64,
        ]
        .iter()
        .enumerate()
        {
            if input
                .schema()
                .field_from_column(&columns[index])?
                .data_type()
                != datatype
            {
                return Err(invalid("Series range input type changed"));
            }
        }
        for column in &columns[3..] {
            let datatype = input.schema().field_from_column(column)?.data_type();
            let datatype = match datatype {
                DataType::Dictionary(_, value) => value.as_ref(),
                datatype => datatype,
            };
            if !datatype.is_string() {
                return Err(invalid("Series range label type changed"));
            }
        }
        let fields = output
            .iter()
            .map(|output| {
                if output.source == 2 {
                    return Err(invalid("Series range cannot project its internal identity"));
                }
                let column = columns
                    .get(output.source)
                    .ok_or_else(|| invalid("Series range output index out of bounds"))?;
                let field = if output.source == 1 {
                    Field::new(&output.name, DataType::Float64, output.nullable)
                } else {
                    input
                        .schema()
                        .field_from_column(column)?
                        .as_ref()
                        .clone()
                        .with_name(&output.name)
                        .with_nullable(output.nullable)
                };
                Ok((
                    output.qualifier.as_deref().map(TableReference::from),
                    Arc::new(field),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().as_ref().metadata().clone(),
        )?);
        Ok(Self {
            input,
            window,
            columns,
            output,
            schema,
            unresolved: None,
        })
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let columns = self
            .columns
            .iter()
            .map(|column| self.input.schema().index_of_column(column))
            .collect::<Result<_>>()?;
        serde_json::to_vec(&WireRange {
            version: 4,
            window: self.window.clone(),
            columns,
            output: self.output.clone(),
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        let wire: WireRange = serde_json::from_slice(bytes)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if !matches!(wire.version, 2 | 3 | 4)
            || wire.columns.len() < 3
            || (wire.version == 2 && wire.window.function != RangeFunction::Rate)
            || wire.window.function == RangeFunction::Max
            || (wire.version < 4 && wire.window.function == RangeFunction::Instant)
        {
            return Err(invalid("Invalid series range wire descriptor"));
        }
        let schema = Arc::new(DFSchema::empty());
        Ok(Self {
            input: LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone(),
            }),
            window: wire.window,
            columns: Vec::new(),
            output: wire.output,
            schema,
            unresolved: Some(wire.columns),
        })
    }

    pub(crate) fn specification(
        &self,
        input_schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<RangeSpec> {
        if self.unresolved.is_some() {
            return Err(invalid("Unresolved series range input"));
        }
        let indices = self
            .columns
            .iter()
            .map(|column| self.input.schema().index_of_column(column))
            .collect::<Result<Vec<_>>>()?;
        let output = self
            .output
            .iter()
            .map(|output| match output.source {
                0 => RangeOutput::Timestamp,
                1 => RangeOutput::Value,
                index => RangeOutput::Tag(indices[index]),
            })
            .collect();
        let schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(self.schema.as_arrow().clone()))
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
        Ok(RangeSpec {
            aggregate: None,
            packed: None,
            function: self.window.function,
            schema,
            input_schema,
            output,
            timestamp: indices[0],
            value: indices[1],
            tsid: indices[2],
            grid: Arc::new(
                TimeGrid::new(self.window.start, self.window.end, self.window.step)
                    .map_err(|_| invalid("Invalid range grid"))?,
            ),
            window: self.window.width,
            // SeriesNormalize leaves native sample timestamps raw.
            offset: self.window.offset,
        })
    }

    pub(crate) fn physical(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let spec = Arc::new(self.specification(input.schema())?);
        Ok(Arc::new(SeriesRangeExec::new(
            input,
            spec,
            self.window.offset,
        )))
    }
}

impl PartialOrd for SeriesRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (
            &self.window,
            &self.columns,
            &self.output,
            &self.unresolved,
            &self.input,
        )
            .partial_cmp(&(
                &other.window,
                &other.columns,
                &other.output,
                &other.unresolved,
                &other.input,
            ))
    }
}

impl UserDefinedLogicalNodeCore for SeriesRange {
    fn name(&self) -> &str {
        Self::NAME
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        // Labels are pass-through outputs. Making them required expressions would
        // prevent projection pruning below aggregates that discard those labels.
        self.columns
            .iter()
            .take(3)
            .cloned()
            .map(Expr::Column)
            .collect()
    }
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        if self.unresolved.is_some() {
            return None;
        }
        let schema = self.input.schema();
        let mut required = self.columns[..3]
            .iter()
            .map(|column| schema.index_of_column(column).ok())
            .collect::<Option<Vec<_>>>()?;
        for &index in output_columns {
            let column = self.columns.get(self.output.get(index)?.source)?;
            required.push(schema.index_of_column(column).ok()?);
        }
        required.sort_unstable();
        required.dedup();
        Some(vec![required])
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}, window={}ms",
            Self::NAME,
            self.window.function.name(),
            self.window.width
        )
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let [input] = inputs.as_slice() else {
            return Err(invalid("Series range requires one input"));
        };
        if let Some(indices) = &self.unresolved {
            let columns = indices
                .iter()
                .map(|&index| {
                    if index >= input.schema().fields().len() {
                        return Err(invalid("Series range input index out of bounds"));
                    }
                    let (qualifier, field) = input.schema().qualified_field(index);
                    Ok(Column::new(qualifier.cloned(), field.name()))
                })
                .collect::<Result<Vec<_>>>()?;
            return Self::new(
                input.clone(),
                self.window.clone(),
                columns,
                self.output.clone(),
            );
        }
        if expressions == self.expressions() && input.schema() == self.input.schema() {
            let mut node = self.clone();
            node.input = input.clone();
            return Ok(node);
        }
        if expressions.len() != 3 {
            return Err(invalid("Series range column count changed"));
        }
        let mut columns = expressions
            .into_iter()
            .map(|expr| match expr {
                Expr::Column(column) => Ok(column),
                _ => Err(invalid("Series range requires columns")),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut remap = vec![Some(0), Some(1), Some(2)];
        for column in &self.columns[3..] {
            if input.schema().index_of_column(column).is_ok() {
                remap.push(Some(columns.len()));
                columns.push(column.clone());
            } else {
                remap.push(None);
            }
        }
        let output = self
            .output
            .iter()
            .filter_map(|output| {
                remap[output.source].map(|source| Output {
                    source,
                    ..output.clone()
                })
            })
            .collect();
        Self::new(input.clone(), self.window.clone(), columns, output)
    }
}

#[derive(Debug)]
pub(crate) struct SeriesRangeExec {
    input: Arc<dyn ExecutionPlan>,
    pub(crate) spec: Arc<RangeSpec>,
    pub(crate) raw_offset: i64,
    properties: Arc<PlanProperties>,
}

impl SeriesRangeExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        spec: Arc<RangeSpec>,
        raw_offset: i64,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(spec.packed.as_ref().map_or_else(
                || {
                    spec.aggregate.as_ref().map_or_else(
                        || spec.schema.arrow_schema().clone(),
                        |a| a.schema.arrow_schema().clone(),
                    )
                },
                |p| p.schema.arrow_schema().clone(),
            )),
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            spec,
            raw_offset,
            properties,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SeriesProgramExec {
    scan: table::table::scan::RegionScanExec,
    fallback: Arc<SeriesRangeExec>,
}

impl SeriesProgramExec {
    pub(crate) fn new(scan: table::table::scan::RegionScanExec, spec: Arc<RangeSpec>) -> Self {
        let offset = spec.offset;
        let fallback = Arc::new(SeriesRangeExec::new(Arc::new(scan.clone()), spec, offset));
        Self { scan, fallback }
    }
}

impl DisplayAs for SeriesProgramExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResearchSeriesProgram: {}",
            self.fallback.spec.function.name()
        )
    }
}

impl ExecutionPlan for SeriesProgramExec {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn name(&self) -> &str {
        "ResearchSeriesProgram"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.fallback.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(invalid("Series program is a leaf"));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(invalid("Series program has one output partition"));
        }
        let scan = self.scan.clone();
        let fallback = self.fallback.clone();
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let cancelled = guard.0.clone();
        let processor_context = context.clone();
        let spec = fallback.spec.clone();
        let factory: store_api::region_engine::ScanProcessorFactory = Arc::new(move || {
            let memory = MemoryConsumer::new("ResearchSeriesProgram")
                .register(processor_context.memory_pool());
            RangeProcessor::new(spec.clone(), memory, cancelled.clone())
                .map(|processor| Box::new(processor) as Box<dyn ScanProcessor>)
                .map_err(|error| {
                    common_error::ext::BoxedError::new(crate::error::Error::from(error))
                })
        });
        let work = stream::once(async move {
            let output = scan
                .execute_series_program(context.clone(), factory.clone())
                .await?;
            let output = if let Some(output) = output {
                output
            } else {
                let processor = factory().map_err(|e| DataFusionError::External(Box::new(e)))?;
                match scan.execute_with_processor(0, context.clone(), processor)? {
                    Some(output) => output,
                    None => fallback.execute(0, context)?,
                }
            };
            // Dropping the response cancels processors, including detached blocking tasks.
            Ok::<_, DataFusionError>(output.map(move |batch| {
                let _ = &guard;
                batch
            }))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), work)))
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, AtomicOrdering::Relaxed);
    }
}

impl DisplayAs for SeriesRangeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResearchSeriesRange: {}", self.spec.function.name())
    }
}

impl ExecutionPlan for SeriesRangeExec {
    fn name(&self) -> &str {
        "ResearchSeriesRange"
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(series_input_ordering(
            &self.spec.input_schema,
            self.spec.tsid,
            self.spec.timestamp,
        ))]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(invalid("Series range requires one physical input"));
        };
        if input.schema() != self.spec.input_schema {
            return Err(invalid("Series range physical input schema changed"));
        }
        Ok(Arc::new(Self::new(
            input.clone(),
            self.spec.clone(),
            self.raw_offset,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let spec = self.spec.clone();
        let normalized = normalize_batches(
            self.input.clone(),
            spec.timestamp,
            spec.tsid,
            self.raw_offset,
            spec.function != RangeFunction::Instant,
        )?;
        let mut input = normalized.execute(partition, context.clone())?;
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let memory = MemoryConsumer::new(self.name()).register(context.memory_pool());
        let mut processor = RangeProcessor::new(spec.clone(), memory, guard.0.clone())?;
        let input_schema = Arc::new(
            datatypes::schema::Schema::try_from(spec.input_schema.clone())
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
        let work = stream::once(async move {
            while let Some(batch) = input.try_next().await? {
                let schema = input_schema.clone();
                processor = common_runtime::spawn_blocking_query(move || {
                    processor
                        .process(&common_recordbatch::RecordBatch::from_df_record_batch(
                            schema, batch,
                        ))
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    Ok::<_, DataFusionError>(processor)
                })
                .await
                .map_err(|error| DataFusionError::Execution(error.to_string()))??;
            }
            let result = common_runtime::spawn_blocking_query(move || {
                let batches = processor
                    .finish()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                Ok::<_, DataFusionError>((batches, processor))
            })
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            drop(guard);
            result
        })
        .map_ok(|(batches, processor)| {
            stream::unfold(
                (batches.into_iter(), processor),
                |(mut batches, processor)| async move {
                    batches
                        .next()
                        .map(|batch| (Ok(batch.into_df_record_batch()), (batches, processor)))
                },
            )
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), work)))
    }
}

pub(crate) fn series_input_ordering(
    schema: &Schema,
    tsid: usize,
    timestamp: usize,
) -> OrderingRequirements {
    let requirements = [tsid, timestamp].map(|index| PhysicalSortRequirement {
        expr: Arc::new(PhysicalColumn::new(schema.field(index).name(), index)),
        options: Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
    });
    // Both keys are always present; their indices are validated during binding.
    OrderingRequirements::Hard(vec![LexRequirement::new(requirements).unwrap()])
}

pub(crate) fn normalize_batches(
    input: Arc<dyn ExecutionPlan>,
    timestamp: usize,
    tsid: usize,
    offset: i64,
    filter_stale: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let time = schema.field(timestamp).name().clone();
    let identity = schema.field(tsid).name().clone();
    let logical = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::try_from(schema.as_ref().clone())?),
    });
    // Built only for batch execution. The native scan already preserves visibility,
    // complete series, and timestamp order without constructing these operators.
    let divided = SeriesDivide::new(vec![identity.clone()], time.clone(), logical.clone())
        .to_execution_plan(input);
    Ok(
        SeriesNormalize::new(offset, time, filter_stale, vec![identity], logical)
            .to_execution_plan(divided),
    )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        Array, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use promql::functions::{IDelta, Increase, Rate};

    use super::*;

    fn input_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("__tsid", DataType::UInt64, false),
            Field::new("host", DataType::Utf8, true),
        ]))
    }

    fn plan(offset: i64, width: i64) -> LogicalPlan {
        counter_plan(offset, width, RangeFunction::Rate)
    }

    fn counter_plan(offset: i64, width: i64, function: RangeFunction) -> LogicalPlan {
        let input = LogicalPlanBuilder::scan(
            "devices",
            Arc::new(datafusion::logical_expr::LogicalTableSource::new(
                input_schema(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(vec!["__tsid".into()], "ts".into(), input)),
        });
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                offset,
                "ts",
                true,
                vec!["__tsid".into()],
                divide,
            )),
        });
        let range = RangeManipulate::new(
            20_000,
            30_000,
            10_000,
            offset,
            width,
            "ts".into(),
            vec!["value".into()],
            normalize,
        )
        .unwrap();
        LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(range),
        }))
        .project(vec![
            col("host"),
            col("ts"),
            match function {
                RangeFunction::Rate => Rate::scalar_udf().call(vec![
                    col("ts_range"),
                    col("value"),
                    col("ts"),
                    lit(width),
                ]),
                RangeFunction::Increase => Increase::scalar_udf().call(vec![
                    col("ts_range"),
                    col("value"),
                    col("ts"),
                    lit(width),
                ]),
                RangeFunction::Irate => {
                    IDelta::<true>::scalar_udf().call(vec![col("ts_range"), col("value")])
                }
                RangeFunction::Max | RangeFunction::Instant => unreachable!(),
            }
            .alias("rate"),
        ])
        .unwrap()
        .filter(col("rate").is_not_null())
        .unwrap()
        .build()
        .unwrap()
    }

    #[tokio::test]
    async fn packed_instant_matches_original_selection_and_wire_roundtrip() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        use datatypes::value::Value;
        use store_api::region_engine::{SeriesBlockProcessor, VisibleSeriesBlock};

        fn points(batches: Vec<RecordBatch>) -> Vec<(i64, u64)> {
            batches
                .into_iter()
                .flat_map(|batch| {
                    let times = batch
                        .column_by_name("ts")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let values = batch
                        .column_by_name("greptime_value")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    (0..batch.num_rows())
                        .filter_map(|row| {
                            values
                                .is_valid(row)
                                .then(|| (times.value(row), values.value(row).to_bits()))
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        }
        let schema = Arc::new(Schema::new(
            input_schema()
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == "value" {
                        Arc::new(field.as_ref().clone().with_name("greptime_value"))
                    } else {
                        field.clone()
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let values = vec![
            Some(-0.0),
            Some(2.0),
            None,
            Some(9.0),
            Some(f64::from_bits(0x7ff0_0000_0000_0002)),
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(7.0),
        ];
        let times = (-1..7).map(|index| index * 100).collect::<Vec<i64>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(times.clone())),
                Arc::new(Float64Array::from(values.clone())),
                Arc::new(UInt64Array::from(vec![1; times.len()])),
                Arc::new(StringArray::from(vec!["a"; times.len()])),
            ],
        )
        .unwrap();
        for lookback in [0, 100, 500] {
            for offset in [-70, 0, 90] {
                let logical = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: schema.clone().to_dfschema_ref().unwrap(),
                });
                let divide = LogicalPlan::Extension(Extension {
                    node: Arc::new(SeriesDivide::new(
                        vec!["__tsid".into()],
                        "ts".into(),
                        logical,
                    )),
                });
                let normalize = LogicalPlan::Extension(Extension {
                    node: Arc::new(SeriesNormalize::new(
                        offset,
                        "ts",
                        false,
                        vec!["__tsid".into()],
                        divide,
                    )),
                });
                let instant = Arc::new(InstantManipulate::new(
                    -100,
                    900,
                    lookback,
                    100,
                    offset,
                    "ts".into(),
                    vec!["host".into()],
                    Some("greptime_value".into()),
                    normalize,
                ));
                let plan = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
                    node: instant.clone(),
                }))
                .project(vec![col("greptime_value"), col("host"), col("ts")])
                .unwrap()
                .build()
                .unwrap();
                let node =
                    SeriesRange::recognize_instant(&plan, &["host".into()], "ts", "greptime_value")
                        .unwrap()
                        .unwrap();
                let node = SeriesRange::deserialize(&node.serialize().unwrap())
                    .unwrap()
                    .with_exprs_and_inputs(vec![], vec![node.input.clone()])
                    .unwrap();
                let mut old_wire: serde_json::Value =
                    serde_json::from_slice(&node.serialize().unwrap()).unwrap();
                old_wire["version"] = 3.into();
                assert!(SeriesRange::deserialize(&serde_json::to_vec(&old_wire).unwrap()).is_err());
                let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(
                        &[vec![batch.slice(0, 3), batch.slice(3, 5)]],
                        schema.clone(),
                        None,
                    )
                    .unwrap(),
                )));
                let context = SessionContext::new().task_ctx();
                let original = instant.to_execution_plan(
                    normalize_batches(source.clone(), 0, 2, offset, false).unwrap(),
                );
                let expected = points(
                    original
                        .execute(0, context.clone())
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap(),
                );
                let actual = points(
                    node.physical(source)
                        .unwrap()
                        .execute(0, context)
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap(),
                );
                assert_eq!(
                    actual, expected,
                    "fallback offset={offset} lookback={lookback}"
                );

                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
                let mut processor = RangeProcessor::new(
                    Arc::new(node.specification(schema.clone()).unwrap()),
                    MemoryConsumer::new("instant-test").register(&pool),
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
                // The immutable native format contains no nulls. A stale sample
                // has the same suppressing effect as null in packed selectors.
                let mut blocks = times.iter().zip(&values).map(|(&timestamp, value)| {
                    Ok(VisibleSeriesBlock {
                        first_timestamp: timestamp,
                        timestamp_step: 0,
                        values: Float64Array::from(vec![
                            value.unwrap_or(f64::from_bits(0x7ff0_0000_0000_0002)),
                        ])
                        .into(),
                    })
                });
                processor
                    .process_series(&[("host".into(), Value::from("a"))], &mut blocks)
                    .unwrap();
                let output = processor
                    .finish()
                    .unwrap()
                    .into_iter()
                    .map(|batch| batch.into_df_record_batch())
                    .collect();
                assert_eq!(
                    points(output),
                    expected,
                    "native offset={offset} lookback={lookback}"
                );
                drop(processor);
                assert_eq!(pool.reserved(), 0);
            }
        }
    }

    #[test]
    fn series_range_rebinds_remote_columns_and_rejects_overlapping_windows() {
        assert!(
            SeriesRange::recognize(&plan(3_000, 20_000))
                .unwrap()
                .is_none()
        );
        let node = SeriesRange::recognize(&plan(3_000, 10_000))
            .unwrap()
            .unwrap();
        let original_schema = node.schema.clone();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Schema::new(vec![
                Field::new(
                    "remote_ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("remote_value", DataType::Float64, true),
                Field::new("remote_id", DataType::UInt64, false),
                Field::new("remote_host", DataType::Utf8, true),
            ])
            .to_dfschema_ref()
            .unwrap(),
        });
        let restored = SeriesRange::deserialize(&node.serialize().unwrap())
            .unwrap()
            .with_exprs_and_inputs(vec![], vec![input.clone()])
            .unwrap();
        assert_eq!(restored.schema, original_schema);
        assert_eq!(restored.columns[0].name, "remote_ts");
        let mut wire: WireRange = serde_json::from_slice(&node.serialize().unwrap()).unwrap();
        wire.columns[0] = 9;
        let invalid = SeriesRange::deserialize(&serde_json::to_vec(&wire).unwrap()).unwrap();
        assert!(invalid.with_exprs_and_inputs(vec![], vec![input]).is_err());
    }

    #[test]
    fn template_shift_preserves_window_semantics_and_rejects_overflow() {
        let node = SeriesRange::recognize(&plan(3_000, 10_000))
            .unwrap()
            .unwrap();
        let extension = Extension {
            node: Arc::new(node.clone()),
        };
        let rebound =
            crate::research_series_grid::plan_template::shift_extension(&extension, -25_000)
                .unwrap()
                .unwrap();
        let LogicalPlan::Extension(rebound) = rebound else {
            unreachable!()
        };
        let rebound = rebound.node.as_any().downcast_ref::<SeriesRange>().unwrap();
        let wire: WireRange = serde_json::from_slice(&rebound.serialize().unwrap()).unwrap();
        assert_eq!((wire.window.start, wire.window.end), (-5_000, 5_000));
        assert_eq!(
            (wire.window.step, wire.window.width, wire.window.offset),
            (10_000, 10_000, 3_000)
        );
        assert_eq!(rebound.shift_time(25_000).unwrap(), node);
        assert!(node.shift_time(i64::MAX - 25_000).is_err());
        assert_eq!((node.window.start, node.window.end), (20_000, 30_000));
    }

    #[test]
    fn series_range_reads_rate_only_v2_wire() {
        let node = SeriesRange::recognize(&plan(0, 10_000)).unwrap().unwrap();
        let mut wire: serde_json::Value =
            serde_json::from_slice(&node.serialize().unwrap()).unwrap();
        wire["version"] = 2.into();
        wire["window"].as_object_mut().unwrap().remove("function");
        let restored = SeriesRange::deserialize(&serde_json::to_vec(&wire).unwrap())
            .unwrap()
            .with_exprs_and_inputs(vec![], vec![node.input.clone()])
            .unwrap();
        assert_eq!(
            restored.specification(input_schema()).unwrap().function,
            RangeFunction::Rate
        );
        wire["window"]["function"] = "Increase".into();
        assert!(SeriesRange::deserialize(&serde_json::to_vec(&wire).unwrap()).is_err());
    }

    #[tokio::test]
    async fn series_range_fallback_applies_offset_once_and_preserves_counter_values() {
        for function in [
            RangeFunction::Rate,
            RangeFunction::Increase,
            RangeFunction::Irate,
        ] {
            for offset in [-3_000, 0, 3_000] {
                let node = SeriesRange::recognize(&counter_plan(offset, 10_000, function))
                    .unwrap()
                    .unwrap();
                let node = SeriesRange::deserialize(&node.serialize().unwrap())
                    .unwrap()
                    .with_exprs_and_inputs(vec![], vec![node.input.clone()])
                    .unwrap();
                let times = vec![7_000, 12_000, 17_000, 22_000, 27_000, 32_000];
                let values = vec![Some(0.0), Some(5.0), Some(10.0), Some(2.0), Some(7.0), None];
                let batch = RecordBatch::try_new(
                    input_schema(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(times.clone())),
                        Arc::new(Float64Array::from(values.clone())),
                        Arc::new(UInt64Array::from(vec![1; times.len()])),
                        Arc::new(StringArray::from(vec![Some("a"); times.len()])),
                    ],
                )
                .unwrap();
                for mode in 0..3 {
                    let physical = if mode == 2 {
                        use api::v1::SemanticType;
                        use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
                        use store_api::region_engine::SinglePartitionScanner;
                        use store_api::storage::{RegionId, ScanRequest};

                        let mut metadata = RegionMetadataBuilder::new(RegionId::new(1, 1));
                        for (index, field) in input_schema().fields().iter().enumerate() {
                            metadata.push_column_metadata(ColumnMetadata {
                                column_schema: datatypes::schema::ColumnSchema::try_from(
                                    field.as_ref(),
                                )
                                .unwrap(),
                                column_id: index as u32,
                                semantic_type: match index {
                                    0 => SemanticType::Timestamp,
                                    1 => SemanticType::Field,
                                    _ => SemanticType::Tag,
                                },
                            });
                        }
                        metadata.primary_key(vec![2, 3]);
                        let schema =
                            Arc::new(datatypes::schema::Schema::try_from(input_schema()).unwrap());
                        let source = common_recordbatch::RecordBatch::from_df_record_batch(
                            schema.clone(),
                            batch.clone(),
                        );
                        let batches =
                            common_recordbatch::RecordBatches::try_new(schema, vec![source])
                                .unwrap();
                        let scanner = SinglePartitionScanner::new(
                            batches.as_stream(),
                            false,
                            Arc::new(metadata.build().unwrap()),
                            None,
                        );
                        let scan = table::table::scan::RegionScanExec::new(
                            Box::new(scanner),
                            ScanRequest::default(),
                            None,
                        )
                        .unwrap();
                        Arc::new(SeriesProgramExec::new(
                            scan,
                            Arc::new(node.specification(input_schema()).unwrap()),
                        )) as Arc<dyn ExecutionPlan>
                    } else if mode == 1 {
                        let reordered = batch.project(&[3, 2, 0, 1]).unwrap();
                        let fields = reordered
                            .schema()
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, field)| {
                                field.as_ref().clone().with_name(format!("source_{i}"))
                            })
                            .collect::<Vec<_>>();
                        let schema = Arc::new(Schema::new(fields));
                        let reordered =
                            RecordBatch::try_new(schema.clone(), reordered.columns().to_vec())
                                .unwrap();
                        let spec = super::super::direct_binding::remap_range_spec(
                            node.specification(input_schema()).unwrap(),
                            schema.clone(),
                            &[2, 3, 1, 0],
                        )
                        .unwrap();
                        let memory: Arc<dyn ExecutionPlan> =
                            Arc::new(DataSourceExec::new(Arc::new(
                                MemorySourceConfig::try_new(&[vec![reordered]], schema, None)
                                    .unwrap(),
                            )));
                        Arc::new(SeriesRangeExec::new(memory, Arc::new(spec), offset))
                            as Arc<dyn ExecutionPlan>
                    } else {
                        let memory: Arc<dyn ExecutionPlan> =
                            Arc::new(DataSourceExec::new(Arc::new(
                                MemorySourceConfig::try_new(
                                    &[vec![batch.clone()]],
                                    input_schema(),
                                    None,
                                )
                                .unwrap(),
                            )));
                        node.physical(memory).unwrap()
                    };
                    let output = physical
                        .execute(0, SessionContext::new().task_ctx())
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    for batch in &output {
                        let labels = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        assert!(labels.iter().all(|label| label == Some("a")));
                    }
                    let (times, values): (Vec<_>, Vec<_>) = times
                        .iter()
                        .copied()
                        .zip(values.iter().copied())
                        .filter_map(|(time, value)| value.map(|value| (time, value)))
                        .unzip();
                    let grid = TimeGrid::new(20_000, 30_000, 10_000).unwrap();
                    let expected = crate::research_series_grid::range::evaluate(
                        function.name(),
                        &grid,
                        times,
                        values,
                        10_000,
                        offset,
                    )
                    .unwrap();
                    let actual = output
                        .iter()
                        .flat_map(|batch| {
                            let times = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap();
                            let values = batch
                                .column(2)
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap();
                            assert_eq!(values.null_count(), 0);
                            (0..batch.num_rows())
                                .map(|row| (times.value(row), values.value(row).to_bits()))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    let expected = grid
                        .instants
                        .iter()
                        .zip(expected)
                        .filter_map(|(&time, value)| value.map(|value| (time, value.to_bits())))
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected, "offset {offset}");
                }
            }
        }
    }

    #[tokio::test]
    async fn series_range_survives_remote_plan_roundtrip() {
        use crate::QueryEngineFactory;
        use crate::dummy_catalog::DummyCatalogList;
        use crate::options::QueryOptions;
        use crate::query_engine::DefaultSerializer;
        use datafusion::datasource::MemTable;
        use session::context::QueryContext;
        use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

        let original = plan(3_000, 10_000);
        let rewritten = SeriesRange::rewrite(original.clone()).unwrap();
        assert_eq!(rewritten.schema(), original.schema());
        let bytes = DFLogicalSubstraitConvertor
            .encode(&rewritten, DefaultSerializer)
            .unwrap();
        let factory = QueryEngineFactory::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        let decoder = factory
            .query_engine()
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();
        let table = Arc::new(MemTable::try_new(input_schema(), vec![vec![]]).unwrap());
        let restored = decoder
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(table)),
                false,
            )
            .await
            .unwrap();
        assert_eq!(rewritten.schema(), restored.schema());
        let LogicalPlan::Extension(extension) = restored else {
            panic!("Range node was lost")
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<SeriesRange>()
            .unwrap();
        assert_eq!(node.window.offset, 3_000);
        assert_eq!(node.window.width, 10_000);
        assert!(node.unresolved.is_none());
    }

    #[test]
    fn series_range_prunes_internal_identity_before_recognizing_window() {
        let LogicalPlan::Filter(filter) = plan(0, 10_000) else {
            unreachable!()
        };
        let LogicalPlan::Projection(projection) = filter.input.as_ref() else {
            unreachable!()
        };
        let mut expressions = projection.expr.clone();
        expressions.push(col("__tsid"));
        let input = LogicalPlanBuilder::from(projection.input.as_ref().clone())
            .project(expressions)
            .unwrap()
            .filter(filter.predicate)
            .unwrap()
            .project(vec![col("host"), col("ts"), col("rate")])
            .unwrap()
            .build()
            .unwrap();
        let schema = input.schema().clone();
        let rewritten = SeriesRange::rewrite(input).unwrap();
        assert_eq!(rewritten.schema(), &schema);
        let mut windows = 0;
        rewritten
            .apply(|plan| {
                if let LogicalPlan::Extension(extension) = plan {
                    assert!(!extension.node.as_any().is::<RangeManipulate>());
                    if let Some(range) = extension.node.as_any().downcast_ref::<SeriesRange>() {
                        windows += 1;
                        assert!(
                            range
                                .schema
                                .index_of_column_by_name(None, "__tsid")
                                .is_none()
                        );
                        assert!(
                            range
                                .input
                                .schema()
                                .index_of_column_by_name(None, "__tsid")
                                .is_some()
                        );
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert_eq!(windows, 1);
    }

    #[test]
    fn series_range_allows_aggregate_to_prune_unused_labels() {
        let input = SeriesRange::rewrite(plan(0, 10_000)).unwrap();
        let aggregate = LogicalPlanBuilder::from(input)
            .aggregate(
                vec![col("ts")],
                vec![datafusion::functions_aggregate::sum::sum(col("rate"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let optimized = OptimizeProjections::new()
            .rewrite(aggregate, &OptimizerContext::new())
            .unwrap()
            .data;
        let mut saw_range = false;
        let mut saw_scan = false;
        optimized
            .apply(|plan| {
                if let LogicalPlan::Extension(extension) = plan
                    && let Some(range) = extension.node.as_any().downcast_ref::<SeriesRange>()
                {
                    saw_range = true;
                    assert!(range.schema.index_of_column_by_name(None, "host").is_none());
                    assert!(
                        range
                            .input
                            .schema()
                            .index_of_column_by_name(None, "host")
                            .is_none()
                    );
                    assert!(
                        range
                            .input
                            .schema()
                            .index_of_column_by_name(None, "__tsid")
                            .is_some()
                    );
                    assert_eq!(
                        range.serialize().unwrap(),
                        SeriesRange::deserialize(&range.serialize().unwrap())
                            .unwrap()
                            .with_exprs_and_inputs(vec![], vec![range.input.clone()])
                            .unwrap()
                            .serialize()
                            .unwrap()
                    );
                }
                if let LogicalPlan::TableScan(scan) = plan {
                    saw_scan = true;
                    assert_eq!(scan.projection.as_ref().unwrap(), &vec![0, 1, 2]);
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert!(saw_range && saw_scan);
    }

    fn fragment_plan(offset: i64, sum: bool) -> crate::research_series_grid::SeriesFragment {
        let input = LogicalPlanBuilder::from(SeriesRange::rewrite(plan(offset, 10_000)).unwrap())
            .project(vec![
                col("host"),
                col("ts"),
                (lit(8.0)
                    * Expr::Cast(datafusion_expr::expr::Cast::new(
                        Box::new(col("rate")),
                        DataType::Float64,
                    )))
                .alias("scaled"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let input = if sum {
            LogicalPlanBuilder::from(input)
                .aggregate(
                    vec![col("host"), col("ts")],
                    vec![datafusion::functions_aggregate::sum::sum(col("scaled")).alias("scaled")],
                )
                .unwrap()
                .build()
                .unwrap()
        } else {
            input
        };
        crate::research_series_grid::SeriesFragment::recognize(
            &input,
            &["host".into()],
            "ts",
            "scaled",
        )
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn series_fragment_fuses_scalar_sum_and_preserves_offset_and_empty_windows() {
        use datafusion::arrow::array::ListArray;
        for offset in [-3_000, 0, 3_000] {
            for sum in [false, true] {
                let node = fragment_plan(offset, sum);
                let times = vec![7_000, 12_000, 17_000, 22_000, 27_000, 32_000];
                let values = vec![Some(0.0), Some(5.0), Some(10.0), Some(2.0), None, None];
                let batch = RecordBatch::try_new(
                    input_schema(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(
                            times.iter().copied().cycle().take(12).collect::<Vec<_>>(),
                        )),
                        Arc::new(Float64Array::from(
                            values
                                .iter()
                                .copied()
                                .chain(values.iter().map(|v| v.map(|v| v * 2.0)))
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(UInt64Array::from(vec![1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2])),
                        Arc::new(StringArray::from(vec![Some("a"); 12])),
                    ],
                )
                .unwrap();
                for remap in [false, true] {
                    let physical = if remap {
                        let reordered = batch.project(&[3, 2, 0, 1]).unwrap();
                        let fields = reordered
                            .schema()
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, field)| {
                                field.as_ref().clone().with_name(format!("source_{i}"))
                            })
                            .collect::<Vec<_>>();
                        let schema = Arc::new(Schema::new(fields));
                        let reordered =
                            RecordBatch::try_new(schema.clone(), reordered.columns().to_vec())
                                .unwrap();
                        let spec = super::super::direct_binding::remap_range_spec(
                            node.specification(input_schema()).unwrap(),
                            schema.clone(),
                            &[2, 3, 1, 0],
                        )
                        .unwrap();
                        let memory: Arc<dyn ExecutionPlan> =
                            Arc::new(DataSourceExec::new(Arc::new(
                                MemorySourceConfig::try_new(&[vec![reordered]], schema, None)
                                    .unwrap(),
                            )));
                        Arc::new(SeriesRangeExec::new(memory, Arc::new(spec), offset))
                            as Arc<dyn ExecutionPlan>
                    } else {
                        let memory: Arc<dyn ExecutionPlan> =
                            Arc::new(DataSourceExec::new(Arc::new(
                                MemorySourceConfig::try_new(
                                    &[vec![batch.clone()]],
                                    input_schema(),
                                    None,
                                )
                                .unwrap(),
                            )));
                        node.physical(memory).unwrap()
                    };
                    let output = physical
                        .execute(0, SessionContext::new().task_ctx())
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    for batch in &output {
                        let labels = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        assert!(labels.iter().all(|label| label == Some("a")));
                    }
                    let (times, values): (Vec<_>, Vec<_>) = times
                        .iter()
                        .copied()
                        .zip(values.iter().copied())
                        .filter_map(|(t, v)| v.map(|v| (t, v)))
                        .unzip();
                    let expected = crate::research_series_grid::range::evaluate(
                        "rate",
                        &TimeGrid::new(20_000, 30_000, 10_000).unwrap(),
                        times,
                        values,
                        10_000,
                        offset,
                    )
                    .unwrap();
                    let mut actual = Vec::new();
                    for batch in output {
                        let list = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap();
                        for row in 0..batch.num_rows() {
                            let values = list.value(row);
                            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                            actual.push(values.iter().collect::<Vec<_>>());
                        }
                    }
                    let expected = if expected.iter().all(Option::is_none) {
                        vec![]
                    } else if sum {
                        vec![
                            expected
                                .iter()
                                .map(|v| v.map(|v| v * 8.0 + v * 16.0))
                                .collect::<Vec<_>>(),
                        ]
                    } else {
                        vec![
                            expected
                                .iter()
                                .map(|v| v.map(|v| v * 8.0))
                                .collect::<Vec<_>>(),
                            expected
                                .iter()
                                .map(|v| v.map(|v| v * 16.0))
                                .collect::<Vec<_>>(),
                        ]
                    };
                    assert_eq!(actual, expected, "offset={offset}, sum={sum}");
                }
            }
        }
    }

    #[tokio::test]
    async fn series_fragment_survives_projection_optimization_and_remote_roundtrip() {
        use crate::QueryEngineFactory;
        use crate::dummy_catalog::DummyCatalogList;
        use crate::options::QueryOptions;
        use crate::query_engine::DefaultSerializer;
        use datafusion::datasource::MemTable;
        use session::context::QueryContext;
        use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
        for sum in [false, true] {
            let plan = fragment_plan(3_000, sum).plan();
            let optimized = OptimizeProjections::new()
                .rewrite(plan.clone(), &OptimizerContext::new())
                .unwrap()
                .data;
            assert_eq!(optimized.schema(), plan.schema());
            let bytes = DFLogicalSubstraitConvertor
                .encode(&optimized, DefaultSerializer)
                .unwrap();
            let factory = QueryEngineFactory::new(
                catalog::memory::new_memory_catalog_manager().unwrap(),
                None,
                None,
                None,
                None,
                false,
                QueryOptions::default(),
            );
            let decoder = factory
                .query_engine()
                .engine_context(QueryContext::arc())
                .new_plan_decoder()
                .unwrap();
            let table = Arc::new(MemTable::try_new(input_schema(), vec![vec![]]).unwrap());
            let restored = decoder
                .decode(
                    bytes,
                    Arc::new(DummyCatalogList::with_table_provider(table)),
                    false,
                )
                .await
                .unwrap();
            assert_eq!(restored.schema(), plan.schema());
            let LogicalPlan::Extension(extension) = restored else {
                panic!("Fragment lost");
            };
            let fragment = extension
                .node
                .as_any()
                .downcast_ref::<crate::research_series_grid::SeriesFragment>()
                .unwrap();
            assert_eq!(fragment.partial_sum(), sum);
            assert!(fragment.serialize().is_ok());
        }
    }

    #[test]
    fn series_fragment_rejects_value_changing_cast() {
        let input = LogicalPlanBuilder::from(SeriesRange::rewrite(plan(0, 10_000)).unwrap())
            .project(vec![
                col("host"),
                col("ts"),
                Expr::Cast(datafusion_expr::expr::Cast::new(
                    Box::new(Expr::Cast(datafusion_expr::expr::Cast::new(
                        Box::new(col("rate")),
                        DataType::Int64,
                    ))),
                    DataType::Float64,
                ))
                .alias("scaled"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(
            crate::research_series_grid::SeriesFragment::recognize(
                &input,
                &["host".into()],
                "ts",
                "scaled"
            )
            .unwrap()
            .is_none()
        );
    }
}
