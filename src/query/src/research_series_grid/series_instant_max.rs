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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, ExprSchema, Result};
use datafusion_expr::{EmptyRelation, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::{
    EquivalenceProperties, OrderingRequirements, Partitioning, PhysicalExpr,
};
use futures::{TryStreamExt, stream};
use promql::extension_plan::{InstantManipulate, SeriesDivide, SeriesNormalize};
use serde::{Deserialize, Serialize};
use store_api::region_engine::ScanProcessor;
use table::table::scan::RegionScanExec;

use super::grid::TimeGrid;
use super::online_frontier::PACKED_POINTS;
use super::scan_max::{InstantMaxProcessor, InstantMaxSpec};
use super::series_fragment::{bare, column_index, peel_projections};
use super::series_range::{normalize_batches, series_input_ordering};

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SeriesInstantMax {
    input: LogicalPlan,
    // Timestamp, sample value, TSID, then grouping labels.
    columns: Vec<Column>,
    names: Vec<String>,
    grid: [i64; 5],
    schema: DFSchemaRef,
    unresolved: Option<Vec<usize>>,
}

#[derive(Serialize, Deserialize)]
struct Wire {
    version: u8,
    columns: Vec<usize>,
    names: Vec<String>,
    grid: [i64; 5],
}

impl SeriesInstantMax {
    pub(crate) fn shift_time(&self, delta: i64) -> Result<Self> {
        let mut rebound = self.clone();
        rebound.grid[0] = crate::research_series_grid::plan_template::shift(self.grid[0], delta)?;
        rebound.grid[1] = crate::research_series_grid::plan_template::shift(self.grid[1], delta)?;
        Ok(rebound)
    }

    pub(crate) const NAME: &str = "ResearchSeriesInstantMax";

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
                    .ok_or_else(|| invalid("Missing instant max output"))?;
                let (qualifier, field) = plan.schema().qualified_field(index);
                Ok(Expr::Column(Column::new(qualifier.cloned(), field.name())))
            })
            .collect::<Result<Vec<_>>>()?;
        let input = peel_projections(plan, &mut expressions)?;
        let LogicalPlan::Aggregate(aggregate) = input else {
            return Ok(None);
        };
        if aggregate.aggr_expr.len() != 1
            || aggregate.group_expr.len() != tags.len() + 1
            || column_index(&expressions[tags.len() + 1], input) != Some(aggregate.group_expr.len())
        {
            return Ok(None);
        }
        let Expr::AggregateFunction(function) = bare(&aggregate.aggr_expr[0]) else {
            return Ok(None);
        };
        if function.func.name() != "max"
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
        let input = peel_projections(aggregate.input.as_ref(), &mut expressions)?;
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
        if lookback < 0
            || step <= 0
            || end < start
            || (i128::from(end) - i128::from(start)) / i128::from(step) + 1 > 1_000_000
        {
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
        let as_column = |expr: &Expr| match bare(expr) {
            Expr::Column(column) => Some(column.clone()),
            _ => None,
        };
        let Some(timestamp) = as_column(&expressions[tags.len()]) else {
            return Ok(None);
        };
        let Some(value) = as_column(&expressions[tags.len() + 1]) else {
            return Ok(None);
        };
        if timestamp.name != instant_time
            || value.name != "greptime_value"
            || input
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
        let mut columns = vec![
            timestamp,
            value,
            Column::new(qualifier.cloned(), field.name()),
        ];
        for expr in &expressions[..tags.len()] {
            let Some(column) = as_column(expr) else {
                return Ok(None);
            };
            columns.push(column);
        }
        if columns.iter().enumerate().any(|(i, column)| {
            !input
                .schema()
                .field_from_column(column)
                .is_ok_and(|field| valid_type(i, field))
        }) {
            return Ok(None);
        }
        Self::new(
            input.clone(),
            columns,
            tags.to_vec(),
            [start, end, step, lookback, offset],
        )
        .map(Some)
    }

    fn new(
        input: LogicalPlan,
        columns: Vec<Column>,
        names: Vec<String>,
        grid: [i64; 5],
    ) -> Result<Self> {
        let [start, end, step, lookback, _] = grid;
        if columns.len() != names.len() + 3
            || lookback < 0
            || step <= 0
            || end < start
            || (i128::from(end) - i128::from(start)) / i128::from(step) + 1 > 1_000_000
        {
            return Err(invalid("Invalid instant max descriptor"));
        }
        for (index, column) in columns.iter().enumerate() {
            if !valid_type(index, input.schema().field_from_column(column)?) {
                return Err(invalid("Invalid instant max source type"));
            }
        }
        let mut fields = columns[3..]
            .iter()
            .zip(&names)
            .map(|(column, name)| {
                if name == PACKED_POINTS {
                    return Err(invalid("Instant max label collision"));
                }
                Ok((
                    None,
                    Arc::new(
                        input
                            .schema()
                            .field_from_column(column)?
                            .as_ref()
                            .clone()
                            .with_name(name),
                    ),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        fields.push((
            None,
            Arc::new(Field::new(
                PACKED_POINTS,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new(
                                "time",
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                            Field::new("value", DataType::Float64, true),
                        ]
                        .into(),
                    ),
                    false,
                ))),
                false,
            )),
        ));
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().as_ref().metadata().clone(),
        )?);
        Ok(Self {
            input,
            columns,
            names,
            grid,
            schema,
            unresolved: None,
        })
    }

    pub(crate) fn evaluation_grid(&self) -> [i64; 5] {
        self.grid
    }

    pub(crate) fn plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let columns = self
            .columns
            .iter()
            .map(|c| self.input.schema().index_of_column(c))
            .collect::<Result<Vec<_>>>()?;
        serde_json::to_vec(&Wire {
            version: 1,
            columns,
            names: self.names.clone(),
            grid: self.grid,
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        let wire: Wire =
            serde_json::from_slice(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
        if wire.version != 1 || wire.columns.len() != wire.names.len() + 3 {
            return Err(invalid("Invalid instant max wire descriptor"));
        }
        let schema = Arc::new(DFSchema::empty());
        Ok(Self {
            input: LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone(),
            }),
            columns: Vec::new(),
            names: wire.names,
            grid: wire.grid,
            schema,
            unresolved: Some(wire.columns),
        })
    }

    pub(crate) fn specification(
        &self,
        input_schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<InstantMaxSpec> {
        if self.unresolved.is_some() {
            return Err(invalid("Unresolved instant max input"));
        }
        let indices = self
            .columns
            .iter()
            .map(|c| self.input.schema().index_of_column(c))
            .collect::<Result<Vec<_>>>()?;
        let mut fields = self.schema.fields()[..self.names.len()].to_vec();
        fields.push(Arc::new(Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )));
        fields.push(Arc::new(Field::new("value", DataType::Float64, true)));
        let schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(Schema::new(fields)))
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let [start, end, step, lookback, offset] = self.grid;
        let spec = InstantMaxSpec {
            schema,
            input_schema,
            groups: indices[3..].iter().copied().chain([indices[0]]).collect(),
            timestamp: indices[0],
            value: indices[1],
            tsid: indices[2],
            grid: Arc::new(TimeGrid::new(start, end, step).map_err(invalid)?),
            lookback,
            offset,
            sum: false,
        };
        Ok(spec)
    }

    pub(crate) fn physical(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let spec = Arc::new(self.specification(input.schema())?);
        let packed = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(self.schema.as_arrow().clone()))
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        Ok(Arc::new(InstantMaxExec::new(input, spec, packed)))
    }
}

fn valid_type(index: usize, field: &Field) -> bool {
    match index {
        0 => {
            field.data_type() == &DataType::Timestamp(TimeUnit::Millisecond, None)
                && !field.is_nullable()
        }
        1 => field.data_type() == &DataType::Float64,
        2 => field.data_type() == &DataType::UInt64 && !field.is_nullable(),
        _ => match field.data_type() {
            DataType::Dictionary(_, value) => value.is_string(),
            value => value.is_string(),
        },
    }
}

impl PartialOrd for SeriesInstantMax {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            &self.grid,
            &self.columns,
            &self.names,
            &self.unresolved,
            &self.input,
        )
            .partial_cmp(&(
                &other.grid,
                &other.columns,
                &other.names,
                &other.unresolved,
                &other.input,
            ))
    }
}

impl UserDefinedLogicalNodeCore for SeriesInstantMax {
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
        self.columns.iter().cloned().map(Expr::Column).collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: partial_max=true", Self::NAME)
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let [input] = inputs.as_slice() else {
            return Err(invalid("Instant max requires one input"));
        };
        let columns = if let Some(indices) = &self.unresolved {
            indices
                .iter()
                .map(|&index| {
                    if index >= input.schema().fields().len() {
                        return Err(invalid("Instant max input index outside schema"));
                    }
                    let (qualifier, field) = input.schema().qualified_field(index);
                    Ok(Column::new(qualifier.cloned(), field.name()))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            expressions
                .into_iter()
                .map(|expr| match expr {
                    Expr::Column(column) => Ok(column),
                    _ => Err(invalid("Instant max requires source columns")),
                })
                .collect::<Result<Vec<_>>>()?
        };
        if self.unresolved.is_none()
            && columns == self.columns
            && input.schema() == self.input.schema()
        {
            let mut node = self.clone();
            node.input = input.clone();
            return Ok(node);
        }
        Self::new(input.clone(), columns, self.names.clone(), self.grid)
    }
}

fn bind_source(
    mut input: Arc<dyn ExecutionPlan>,
    spec: &InstantMaxSpec,
) -> Option<(RegionScanExec, Arc<InstantMaxSpec>)> {
    let mut indices = vec![spec.timestamp, spec.value, spec.tsid];
    indices.extend_from_slice(&spec.groups);
    loop {
        if let Some(cooperative) = input.downcast_ref::<CooperativeExec>() {
            input = cooperative.input().clone();
        } else if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
            indices = indices
                .iter()
                .map(|&index| {
                    projection
                        .expr()
                        .get(index)?
                        .expr
                        .downcast_ref::<PhysicalColumn>()
                        .map(PhysicalColumn::index)
                })
                .collect::<Option<Vec<_>>>()?;
            input = projection.input().clone();
        } else {
            break;
        }
    }
    let scan = input.downcast_ref::<RegionScanExec>()?;
    let schema = scan.schema();
    if scan.scanner_type() != "SeriesScan"
        || schema.index_of(&scan.time_index()).ok() != Some(indices[0])
        || schema.field(indices[1]).name() != "greptime_value"
        || schema.field(indices[2]).name() != "__tsid"
        || schema.index_of("greptime_native_histogram").is_ok()
        || indices[..3]
            .iter()
            .enumerate()
            .any(|(i, &index)| !valid_type(i, schema.field(index)))
        || indices[3..].iter().any(|&index| {
            index != indices[0] && !scan.tag_columns().contains(schema.field(index).name())
        })
    {
        return None;
    }
    Some((
        scan.clone(),
        Arc::new(InstantMaxSpec {
            schema: spec.schema.clone(),
            input_schema: schema,
            timestamp: indices[0],
            value: indices[1],
            tsid: indices[2],
            groups: indices[3..].to_vec(),
            grid: spec.grid.clone(),
            lookback: spec.lookback,
            offset: spec.offset,
            sum: false,
        }),
    ))
}

#[derive(Debug)]
struct InstantMaxExec {
    input: Arc<dyn ExecutionPlan>,
    spec: Arc<InstantMaxSpec>,
    packed: datatypes::schema::SchemaRef,
    native: Option<(RegionScanExec, Arc<InstantMaxSpec>)>,
    properties: Arc<PlanProperties>,
}

impl InstantMaxExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        spec: Arc<InstantMaxSpec>,
        packed: datatypes::schema::SchemaRef,
    ) -> Self {
        let native = bind_source(input.clone(), &spec);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(packed.arrow_schema().clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            spec,
            packed,
            native,
            properties,
        }
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
impl DisplayAs for InstantMaxExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResearchSeriesInstantMax: native={}",
            self.native.is_some()
        )
    }
}

impl ExecutionPlan for InstantMaxExec {
    fn name(&self) -> &str {
        SeriesInstantMax::NAME
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
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(invalid("Instant max requires one physical input"));
        };
        if input.schema() != self.spec.input_schema {
            return Err(invalid("Instant max input schema changed"));
        }
        Ok(Arc::new(Self::new(
            input.clone(),
            self.spec.clone(),
            self.packed.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(invalid("Instant max has one output partition"));
        }
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let mut streams = Vec::new();
        let mut native_partitions = 0;
        for partition in 0..self.input.properties().partitioning.partition_count() {
            if let Some((scan, spec)) = &self.native {
                let memory =
                    MemoryConsumer::new("SeriesInstantMaxLocal").register(context.memory_pool());
                let processor = InstantMaxProcessor::new(spec.clone(), memory, guard.0.clone())?
                    .with_packed_schema(self.packed.clone());
                if let Some(stream) =
                    scan.execute_with_processor(partition, context.clone(), Box::new(processor))?
                {
                    streams.push(stream);
                    native_partitions += 1;
                    continue;
                }
            }
            let normalized = normalize_batches(
                self.input.clone(),
                self.spec.timestamp,
                self.spec.tsid,
                self.spec.offset,
                false,
            )?;
            let mut stream = normalized.execute(partition, context.clone())?;
            let memory =
                MemoryConsumer::new("SeriesInstantMaxBatch").register(context.memory_pool());
            let mut processor =
                InstantMaxProcessor::new(self.spec.clone(), memory, guard.0.clone())?
                    .with_packed_schema(self.packed.clone());
            let schema = Arc::new(
                datatypes::schema::Schema::try_from(self.spec.input_schema.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );
            let work = stream::once(async move {
                while let Some(batch) = stream.try_next().await? {
                    let schema = schema.clone();
                    processor = common_runtime::spawn_blocking_query(move || {
                        processor
                            .process(&common_recordbatch::RecordBatch::from_df_record_batch(
                                schema, batch,
                            ))
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        Ok::<_, DataFusionError>(processor)
                    })
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))??;
                }
                common_runtime::spawn_blocking_query(move || {
                    let output = processor
                        .finish()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Ok::<_, DataFusionError>((output, processor))
                })
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
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
            streams.push(Box::pin(RecordBatchStreamAdapter::new(
                self.packed.arrow_schema().clone(),
                work,
            )));
        }
        common_telemetry::debug!(
            native_partitions,
            "Research direct instant max fragment activated"
        );
        let memory = MemoryConsumer::new("SeriesInstantMaxMerge").register(context.memory_pool());
        let retained = memory.new_empty();
        let spec = self.spec.clone();
        let packed = self.packed.clone();
        let work = stream::once(async move {
            let mut input = stream::select_all(streams);
            let mut batches = Vec::new();
            while let Some(batch) = input.try_next().await? {
                retained.try_grow(batch.get_array_memory_size().saturating_add(256))?;
                batches.push(batch);
            }
            let cancelled = guard.0.clone();
            let output = common_runtime::spawn_blocking_query(move || {
                let _retained = retained;
                let mut processor =
                    InstantMaxProcessor::new(spec, memory, cancelled)?.with_packed_schema(packed);
                for batch in batches {
                    processor.merge_packed(&batch)?;
                }
                let output = processor
                    .finish()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok::<_, DataFusionError>((output, processor))
            })
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))??;
            drop(guard);
            Ok::<_, DataFusionError>(output)
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, Float64Array, ListArray, StringArray, StructArray, TimestampMillisecondArray,
        UInt64Array,
    };
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{LogicalPlanBuilder, col};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, true),
            Field::new("__tsid", DataType::UInt64, false),
            Field::new("host", DataType::Utf8, true),
        ]))
    }

    fn plan(offset: i64) -> LogicalPlan {
        let input = LogicalPlanBuilder::scan(
            "devices",
            Arc::new(datafusion_expr::LogicalTableSource::new(schema())),
            None,
        )
        .unwrap()
        .sort(vec![
            col("__tsid").sort(true, true),
            col("ts").sort(true, true),
        ])
        .unwrap()
        .build()
        .unwrap();
        let mut input = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(vec!["__tsid".into()], "ts".into(), input)),
        });
        if offset != 0 {
            input = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesNormalize::new(
                    offset,
                    "ts",
                    false,
                    vec!["__tsid".into()],
                    input,
                )),
            });
        }
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                10_000,
                30_000,
                10_000,
                10_000,
                offset,
                "ts".into(),
                vec!["host".into()],
                Some("greptime_value".into()),
                input,
            )),
        });
        LogicalPlanBuilder::from(input)
            .project(vec![col("host"), col("ts"), col("greptime_value")])
            .unwrap()
            .aggregate(
                vec![col("host"), col("ts")],
                vec![
                    datafusion::functions_aggregate::min_max::max_udaf()
                        .call(vec![col("greptime_value")])
                        .alias("maximum"),
                ],
            )
            .unwrap()
            .project(vec![
                col("host").alias("node"),
                col("ts"),
                col("maximum").alias("load"),
            ])
            .unwrap()
            .build()
            .unwrap()
    }

    fn node(offset: i64) -> SeriesInstantMax {
        SeriesInstantMax::recognize(&plan(offset), &["node".into()], "ts", "load")
            .unwrap()
            .unwrap()
    }

    fn batch(tsid: u64, times: Vec<i64>, values: Vec<Option<f64>>) -> RecordBatch {
        let count = times.len();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(TimestampMillisecondArray::from(times)),
                Arc::new(Float64Array::from(values)),
                Arc::new(UInt64Array::from(vec![tsid; count])),
                Arc::new(StringArray::from(vec![Some("a"); count])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn instant_fragment_optimizer_orders_raw_samples() {
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
        use datafusion_common::config::ConfigOptions;

        let partitions = vec![vec![batch(
            1,
            vec![30_000, 0, 20_000, 10_000],
            vec![Some(7.0), Some(2.0), Some(6.0), Some(4.0)],
        )]];
        let source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema(), None).unwrap(),
        )));
        let physical = node(0).physical(source).unwrap();
        let optimized = EnsureRequirements::new()
            .optimize(physical, &ConfigOptions::default())
            .unwrap();
        let output = optimized
            .execute(0, SessionContext::new().task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(output.len(), 1);
        let points = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let points = points.as_any().downcast_ref::<StructArray>().unwrap();
        let times = points
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let values = points
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(times.values().as_ref(), &[10_000, 20_000, 30_000]);
        assert_eq!(values.values().as_ref(), &[4.0, 6.0, 7.0]);
    }

    #[tokio::test]
    async fn instant_fragment_preserves_offsets_stale_nulls_duplicates_and_partition_max() {
        for offset in [-3_000, 0, 3_000] {
            let partitions = vec![
                vec![batch(
                    1,
                    vec![0, 10_000, 20_000, 20_000, 30_000],
                    vec![
                        Some(2.0),
                        Some(f64::from_bits(0x7ff0000000000002)),
                        Some(99.0),
                        None,
                        Some(7.0),
                    ],
                )],
                vec![batch(
                    2,
                    vec![0, 10_000, 20_000, 30_000],
                    vec![Some(3.0), Some(4.0), Some(6.0), Some(5.0)],
                )],
                vec![],
            ];
            let source = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&partitions, schema(), None).unwrap(),
            )));
            let node = node(offset);
            let physical = node.physical(source).unwrap();
            let output = physical
                .execute(0, SessionContext::new().task_ctx())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(
                output[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "a"
            );
            let points = output[0]
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0);
            let points = points.as_any().downcast_ref::<StructArray>().unwrap();
            let times = points
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = points
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(times.values().as_ref(), &[10_000, 20_000, 30_000]);
            assert_eq!(values.null_count(), 0);
            assert_eq!(
                values.values().as_ref(),
                if offset > 0 {
                    &[3.0, 4.0, 6.0]
                } else {
                    &[4.0, 6.0, 7.0]
                }
            );
        }
    }

    #[tokio::test]
    async fn instant_fragment_remote_roundtrip_rebinds_columns_and_rejects_bad_indices() {
        use crate::QueryEngineFactory;
        use crate::dummy_catalog::DummyCatalogList;
        use crate::options::QueryOptions;
        use crate::query_engine::DefaultSerializer;
        use datafusion::datasource::MemTable;
        use session::context::QueryContext;
        use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
        let original = node(3_000);
        let plan = original.clone().plan();
        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
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
        let table = Arc::new(MemTable::try_new(schema(), vec![vec![]]).unwrap());
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
            panic!("lost instant fragment")
        };
        let restored = extension
            .node
            .as_any()
            .downcast_ref::<SeriesInstantMax>()
            .unwrap();
        assert_eq!(restored.grid, [10_000, 30_000, 10_000, 10_000, 3_000]);
        assert!(restored.unresolved.is_none());
        let mut wire: Wire = serde_json::from_slice(&original.serialize().unwrap()).unwrap();
        wire.columns[0] = usize::MAX;
        let unresolved =
            SeriesInstantMax::deserialize(&serde_json::to_vec(&wire).unwrap()).unwrap();
        assert!(
            unresolved
                .with_exprs_and_inputs(vec![], vec![original.input.clone()])
                .is_err()
        );
        wire.columns[0] = 0;
        wire.grid[2] = 0;
        let unresolved =
            SeriesInstantMax::deserialize(&serde_json::to_vec(&wire).unwrap()).unwrap();
        assert!(
            unresolved
                .with_exprs_and_inputs(vec![], vec![original.input])
                .is_err()
        );
    }
}
