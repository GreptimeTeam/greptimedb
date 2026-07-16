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

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Rows, SemanticType};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::{
    ArrowComputeSnafu, CreateRecordBatchesSnafu, NewDfRecordBatchSnafu, Result as RecordBatchResult,
};
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_recordbatch::{
    DfRecordBatch, OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Column, Result as DataFusionResult};
use datafusion::functions::expr_fn::coalesce;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array};
use datatypes::arrow::compute::kernels::zip::zip;
use datatypes::arrow::compute::{
    cast as cast_array, filter_record_batch as filter_df_record_batch, is_not_null,
};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures_util::Stream;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    DATA_SCHEMA_VALUE_INT_COLUMN_PREFIX,
};
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, RegionScannerRef, ScannerProperties,
};
use store_api::region_request::AlterKind;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, RegionId, ScanRequest, SequenceNumber};

use crate::error::{InvalidMetadataSnafu, InvalidRequestSnafu, Result};

const INTERNAL_COLUMN_ID_MASK: ColumnId = 1 << (ColumnId::BITS - 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetricValueColumn {
    /// Index of the Float64 value in `RegionMetadata::column_metadatas`.
    pub(crate) value_index: usize,
    /// Index of its Int64 companion in `RegionMetadata::column_metadatas`.
    pub(crate) int_index: usize,
}

/// Derives the internal companion ID without consuming an ID from the user space.
pub(crate) const fn metric_value_int_column_id(value_column_id: ColumnId) -> ColumnId {
    value_column_id | INTERNAL_COLUMN_ID_MASK
}

pub(crate) fn metric_value_int_column_name(value_column_name: &str) -> String {
    format!("{DATA_SCHEMA_VALUE_INT_COLUMN_PREFIX}{value_column_name}")
}

pub(crate) fn is_metric_value_int_column(name: &str) -> bool {
    name.starts_with(DATA_SCHEMA_VALUE_INT_COLUMN_PREFIX)
}

pub(crate) fn metric_value_columns(metadata: &RegionMetadata) -> Vec<MetricValueColumn> {
    if !is_metric_engine_data_region(metadata) {
        return Vec::new();
    }

    metadata
        .column_metadatas
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            column.semantic_type == SemanticType::Field
                && column.column_schema.data_type == ConcreteDataType::float64_datatype()
        })
        .filter_map(|(value_index, value_column)| {
            let int_name = metric_value_int_column_name(&value_column.column_schema.name);
            let int_index = metadata.column_index_by_name(&int_name)?;
            let int_column = &metadata.column_metadatas[int_index];
            if int_column.column_id != metric_value_int_column_id(value_column.column_id)
                || int_column.semantic_type != SemanticType::Field
                || int_column.column_schema.data_type != ConcreteDataType::int64_datatype()
            {
                return None;
            }

            Some(MetricValueColumn {
                value_index,
                int_index,
            })
        })
        .collect()
}

pub(crate) fn visible_region_metadata(metadata: &RegionMetadataRef) -> Result<RegionMetadataRef> {
    let split_columns = metric_value_columns(metadata);
    if split_columns.is_empty() {
        return Ok(metadata.clone());
    }

    let names = split_columns
        .iter()
        .map(|column| {
            metadata.column_metadatas[column.int_index]
                .column_schema
                .name
                .clone()
        })
        .collect::<Vec<_>>();

    let mut builder = RegionMetadataBuilder::from_existing((**metadata).clone());
    builder
        .alter(AlterKind::DropColumns { names })
        .context(InvalidMetadataSnafu)?;
    builder.build().map(Arc::new).context(InvalidMetadataSnafu)
}

pub(crate) fn visible_column_metadatas(
    columns: &[store_api::metadata::ColumnMetadata],
) -> Vec<store_api::metadata::ColumnMetadata> {
    columns
        .iter()
        .filter(|column| !is_metric_value_int_column(&column.column_schema.name))
        .cloned()
        .collect()
}

fn is_metric_engine_data_region(metadata: &RegionMetadata) -> bool {
    let has_internal_tag = |name, column_id| {
        metadata.column_by_name(name).is_some_and(|column| {
            column.semantic_type == SemanticType::Tag
                && column.column_id == column_id
                && metadata.primary_key.contains(&column_id)
        })
    };

    has_internal_tag(
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
        ReservedColumnId::table_id(),
    ) && has_internal_tag(DATA_SCHEMA_TSID_COLUMN_NAME, ReservedColumnId::tsid())
}

/// Moves exact integral Float64 values into their Int64 companion columns.
pub(crate) fn split_metric_values(rows: &mut Rows) {
    let split_columns = rows
        .schema
        .iter()
        .enumerate()
        .filter(|(_, column)| column.datatype == ColumnDataType::Float64 as i32)
        .filter_map(|(value_index, value_column)| {
            let int_name = metric_value_int_column_name(&value_column.column_name);
            rows.schema
                .iter()
                .position(|column| {
                    column.column_name == int_name
                        && column.datatype == ColumnDataType::Int64 as i32
                })
                .map(|int_index| (value_index, int_index))
        })
        .collect::<Vec<_>>();

    for row in &mut rows.rows {
        for &(value_index, int_index) in &split_columns {
            if matches!(
                row.values
                    .get(int_index)
                    .and_then(|value| value.value_data.as_ref()),
                Some(ValueData::I64Value(_))
            ) {
                if let Some(value) = row.values.get_mut(value_index) {
                    value.value_data = None;
                }
                continue;
            }

            let Some(ValueData::F64Value(value)) = row
                .values
                .get(value_index)
                .and_then(|value| value.value_data.as_ref())
            else {
                continue;
            };
            let Some(int_value) = integer_value(*value) else {
                continue;
            };

            row.values[value_index].value_data = None;
            row.values[int_index].value_data = Some(ValueData::I64Value(int_value));
        }
    }
}

fn integer_value(value: f64) -> Option<i64> {
    if !value.is_finite() || value < i64::MIN as f64 || value >= i64::MAX as f64 {
        return None;
    }

    let int_value = value as i64;
    ((int_value as f64) == value).then_some(int_value)
}

pub(crate) fn prepare_value_split_scan(
    region_id: RegionId,
    request: &mut ScanRequest,
    physical_metadata: &RegionMetadataRef,
    visible_metadata: &RegionMetadataRef,
) -> Result<Option<ValueSplitProjectionMapper>> {
    let split_columns = metric_value_columns(physical_metadata);
    if split_columns.is_empty() {
        return Ok(None);
    }

    let split_value_columns = split_columns
        .iter()
        .map(|column| {
            let name = physical_metadata.column_metadatas[column.value_index]
                .column_schema
                .name
                .as_str();
            (name, *column)
        })
        .collect::<HashMap<_, _>>();

    let mut simple_filters = HashMap::<String, Vec<_>>::new();
    request.filters = request
        .filters
        .drain(..)
        .map(|filter| {
            if let Some(evaluator) = SimpleFilterEvaluator::try_new(&filter)
                && split_value_columns.contains_key(evaluator.column_name())
            {
                simple_filters
                    .entry(evaluator.column_name().to_string())
                    .or_default()
                    .push(evaluator);
            }
            rewrite_metric_value_filter(region_id, filter, &split_value_columns)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut visible_projection = match request.projection_input.as_ref() {
        Some(projection_input) => projection_input.projection.clone(),
        None => (0..visible_metadata.column_metadatas.len()).collect(),
    };
    let visible_columns = visible_projection.len();
    for column_name in simple_filters.keys() {
        if let Some(index) = visible_metadata.column_index_by_name(column_name)
            && !visible_projection.contains(&index)
        {
            visible_projection.push(index);
        }
    }

    let mut physical_projection = Vec::with_capacity(visible_projection.len());
    let mut output_columns = Vec::with_capacity(visible_projection.len());
    for visible_index in visible_projection {
        let visible_column = visible_metadata
            .column_metadatas
            .get(visible_index)
            .with_context(|| InvalidRequestSnafu {
                region_id,
                reason: format!("projection index {visible_index} is out of bound"),
            })?;
        let value_name = &visible_column.column_schema.name;
        let split_column = split_value_columns.get(value_name.as_str());
        let physical_index = match split_column {
            Some(column) => column.value_index,
            None => physical_metadata
                .column_index_by_name(value_name)
                .with_context(|| InvalidRequestSnafu {
                    region_id,
                    reason: format!("column {value_name} is missing from physical metadata"),
                })?,
        };
        let input_value_index = physical_projection.len();
        physical_projection.push(physical_index);

        let input_int_index = if let Some(split_column) = split_column {
            let input_int_index = physical_projection.len();
            physical_projection.push(split_column.int_index);
            Some(input_int_index)
        } else {
            None
        };
        output_columns.push(ValueColumnProjection {
            input_index: input_value_index,
            int_index: input_int_index,
            output_schema: visible_column.column_schema.clone(),
            simple_filters: simple_filters.remove(value_name).unwrap_or_default(),
        });
    }

    request.projection_input.get_or_insert_default().projection = physical_projection;

    let mapper = ValueSplitProjectionMapper::new(output_columns, visible_columns);
    Ok(Some(mapper))
}

fn rewrite_metric_value_filter(
    region_id: RegionId,
    filter: Expr,
    split_value_columns: &HashMap<&str, MetricValueColumn>,
) -> Result<Expr> {
    let filter_display = filter.to_string();
    let mut rewriter = MetricValueFilterRewriter {
        split_value_columns,
    };
    filter
        .rewrite(&mut rewriter)
        .map(|rewritten| rewritten.data)
        .map_err(|err| {
            InvalidRequestSnafu {
                region_id,
                reason: format!("failed to rewrite metric value filter {filter_display}: {err}"),
            }
            .build()
        })
}

struct MetricValueFilterRewriter<'a> {
    split_value_columns: &'a HashMap<&'a str, MetricValueColumn>,
}

impl TreeNodeRewriter for MetricValueFilterRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> DataFusionResult<Transformed<Expr>> {
        let recursion = if matches!(
            expr,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ) {
            TreeNodeRecursion::Jump
        } else {
            TreeNodeRecursion::Continue
        };

        Ok(Transformed::new(expr, false, recursion))
    }

    fn f_up(&mut self, expr: Expr) -> DataFusionResult<Transformed<Expr>> {
        let Expr::Column(column) = expr else {
            return Ok(Transformed::no(expr));
        };

        if !self.split_value_columns.contains_key(column.name.as_str()) {
            return Ok(Transformed::no(Expr::Column(column)));
        }

        let int_column = Column {
            relation: column.relation.clone(),
            name: metric_value_int_column_name(&column.name),
            spans: column.spans.clone(),
        };
        let float_expr = Expr::Column(column);
        let int_expr = cast(Expr::Column(int_column), ArrowDataType::Float64);
        Ok(Transformed::yes(coalesce(vec![int_expr, float_expr])))
    }
}

#[derive(Clone)]
struct ValueColumnProjection {
    input_index: usize,
    int_index: Option<usize>,
    output_schema: ColumnSchema,
    simple_filters: Vec<SimpleFilterEvaluator>,
}

#[derive(Clone)]
pub(crate) struct ValueSplitProjectionMapper {
    output_schema: SchemaRef,
    working_schema: SchemaRef,
    columns: Vec<ValueColumnProjection>,
    has_split: bool,
}

impl ValueSplitProjectionMapper {
    fn new(columns: Vec<ValueColumnProjection>, visible_columns: usize) -> Self {
        let has_split = columns.iter().any(|column| column.int_index.is_some());
        let working_columns = columns
            .iter()
            .map(|column| column.output_schema.clone())
            .collect::<Vec<_>>();
        let working_schema = Arc::new(Schema::new(working_columns));
        let output_schema = if visible_columns == columns.len() {
            working_schema.clone()
        } else {
            Arc::new(Schema::new(
                columns
                    .iter()
                    .take(visible_columns)
                    .map(|column| column.output_schema.clone())
                    .collect(),
            ))
        };

        Self {
            output_schema,
            working_schema,
            columns,
            has_split,
        }
    }

    fn convert_batch(&self, batch: RecordBatch) -> RecordBatchResult<RecordBatch> {
        if !self.has_split {
            let projection = self
                .columns
                .iter()
                .map(|column| column.input_index)
                .collect::<Vec<_>>();
            return batch.try_project(&projection);
        }

        let arrays = self
            .columns
            .iter()
            .map(|column| {
                if let Some(int_index) = column.int_index {
                    coalesce_value_columns(
                        batch.column(column.input_index),
                        batch.column(int_index),
                    )
                } else {
                    Ok(batch.column(column.input_index).clone())
                }
            })
            .collect::<RecordBatchResult<Vec<_>>>()?;

        let mut df_record_batch =
            DfRecordBatch::try_new(self.working_schema.arrow_schema().clone(), arrays)
                .context(NewDfRecordBatchSnafu)?;
        for (column_index, column) in self.columns.iter().enumerate() {
            for evaluator in &column.simple_filters {
                df_record_batch = apply_value_filter(df_record_batch, column_index, evaluator)?;
            }
        }
        let batch = RecordBatch::from_df_record_batch(self.working_schema.clone(), df_record_batch);

        let output_columns = self.output_schema.num_columns();
        if output_columns == self.columns.len() {
            Ok(batch)
        } else {
            batch.try_project(&(0..output_columns).collect::<Vec<_>>())
        }
    }
}

fn apply_value_filter(
    batch: DfRecordBatch,
    column_index: usize,
    evaluator: &SimpleFilterEvaluator,
) -> RecordBatchResult<DfRecordBatch> {
    let values = batch.column(column_index);
    let matches = evaluator.evaluate_array(values)?;
    let predicate = BooleanArray::new(matches, values.nulls().cloned());
    filter_df_record_batch(&batch, &predicate).context(ArrowComputeSnafu)
}

fn coalesce_value_columns(float_col: &ArrayRef, int_col: &ArrayRef) -> RecordBatchResult<ArrayRef> {
    let float_array = float_col
        .as_any()
        .downcast_ref::<Float64Array>()
        .with_context(|| CreateRecordBatchesSnafu {
            reason: format!("expected Float64 metric value column, got {float_col:?}"),
        })?;
    let int_array = int_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| CreateRecordBatchesSnafu {
            reason: format!("expected Int64 metric value column, got {int_col:?}"),
        })?;

    let int_as_float = cast_array(int_array, &ArrowDataType::Float64).context(ArrowComputeSnafu)?;
    let use_int = is_not_null(int_array).context(ArrowComputeSnafu)?;
    zip(&use_int, &int_as_float, float_array).context(ArrowComputeSnafu)
}

pub(crate) struct MetricRegionScanner {
    inner: RegionScannerRef,
    mapper: Option<ValueSplitProjectionMapper>,
    metadata: RegionMetadataRef,
}

impl MetricRegionScanner {
    pub(crate) fn new(
        inner: RegionScannerRef,
        mapper: Option<ValueSplitProjectionMapper>,
        metadata: RegionMetadataRef,
    ) -> Self {
        Self {
            inner,
            mapper,
            metadata,
        }
    }
}

impl fmt::Debug for MetricRegionScanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricRegionScanner")
            .field("inner", &self.inner)
            .field(
                "has_split",
                &self.mapper.as_ref().is_some_and(|mapper| mapper.has_split),
            )
            .finish()
    }
}

impl DisplayAs for MetricRegionScanner {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl RegionScanner for MetricRegionScanner {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &ScannerProperties {
        self.inner.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.mapper.as_ref().map_or_else(
            || self.inner.schema(),
            |mapper| mapper.output_schema.clone(),
        )
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.inner.prepare(request)
    }

    fn scan_partition(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        let stream = self.inner.scan_partition(ctx, metrics_set, partition)?;
        match &self.mapper {
            Some(mapper) => Ok(Box::pin(ValueSplitRecordBatchStream {
                inner: stream,
                mapper: mapper.clone(),
            })),
            None => Ok(stream),
        }
    }

    fn has_predicate_without_region(&self) -> bool {
        self.inner.has_predicate_without_region()
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        if self.mapper.as_ref().is_some_and(|mapper| mapper.has_split) {
            return vec![false; filter_exprs.len()];
        }

        self.inner.add_dyn_filter_to_predicate(filter_exprs)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.inner.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: RegionId) {
        self.inner.set_query_load_region_id(region_id);
    }

    fn snapshot_sequence(&self) -> Option<SequenceNumber> {
        self.inner.snapshot_sequence()
    }
}

struct ValueSplitRecordBatchStream {
    inner: SendableRecordBatchStream,
    mapper: ValueSplitProjectionMapper,
}

impl RecordBatchStream for ValueSplitRecordBatchStream {
    fn name(&self) -> &str {
        "ValueSplitRecordBatchStream"
    }

    fn schema(&self) -> SchemaRef {
        self.mapper.output_schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

impl Stream for ValueSplitRecordBatchStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|opt| opt.map(|result| result.and_then(|batch| self.mapper.convert_batch(batch))))
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion::logical_expr::{col, lit};
    use datatypes::arrow::array::{TimestampMillisecondArray, UInt32Array, UInt64Array};
    use datatypes::prelude::ConcreteDataType;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::{
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    };
    use store_api::storage::consts::ReservedColumnId;

    use super::*;

    const VALUE_COLUMN: &str = "value";
    const ROW_ID_COLUMN: &str = "row_id";

    fn column(
        column_id: u32,
        semantic_type: SemanticType,
        name: &str,
        data_type: ConcreteDataType,
        nullable: bool,
    ) -> ColumnMetadata {
        ColumnMetadata {
            column_id,
            semantic_type,
            column_schema: ColumnSchema::new(name, data_type, nullable),
        }
    }

    fn test_metadata() -> RegionMetadataRef {
        let int_column = metric_value_int_column_name(VALUE_COLUMN);
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(column(
                ReservedColumnId::table_id(),
                SemanticType::Tag,
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
                false,
            ))
            .push_column_metadata(column(
                ReservedColumnId::tsid(),
                SemanticType::Tag,
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ))
            .push_column_metadata(column(
                0,
                SemanticType::Field,
                VALUE_COLUMN,
                ConcreteDataType::float64_datatype(),
                true,
            ))
            .push_column_metadata(column(
                metric_value_int_column_id(0),
                SemanticType::Field,
                &int_column,
                ConcreteDataType::int64_datatype(),
                true,
            ))
            .push_column_metadata(column(
                2,
                SemanticType::Field,
                ROW_ID_COLUMN,
                ConcreteDataType::int64_datatype(),
                false,
            ))
            .push_column_metadata(column(
                3,
                SemanticType::Timestamp,
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ))
            .primary_key(vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()]);
        Arc::new(builder.build().unwrap())
    }

    fn test_array(name: &str) -> ArrayRef {
        match name {
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME => Arc::new(UInt32Array::from(vec![1; 7])),
            DATA_SCHEMA_TSID_COLUMN_NAME => Arc::new(UInt64Array::from(vec![1; 7])),
            VALUE_COLUMN => Arc::new(Float64Array::from(vec![
                None,
                Some(-1.5),
                None,
                Some(0.5),
                None,
                Some(f64::NAN),
                None,
            ])),
            name if name == metric_value_int_column_name(VALUE_COLUMN) => {
                Arc::new(Int64Array::from(vec![
                    Some(-2),
                    None,
                    Some(0),
                    None,
                    Some(2),
                    None,
                    None,
                ]))
            }
            ROW_ID_COLUMN => Arc::new(Int64Array::from_iter_values(0..7)),
            "ts" => Arc::new(TimestampMillisecondArray::from_iter_values(0..7)),
            _ => unreachable!("unknown test column {name}"),
        }
    }

    fn projected_batch(metadata: &RegionMetadataRef, projection: &[usize]) -> RecordBatch {
        let columns = projection
            .iter()
            .map(|index| {
                let column = &metadata.column_metadatas[*index];
                test_array(&column.column_schema.name)
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(
            projection
                .iter()
                .map(|index| metadata.column_metadatas[*index].column_schema.clone())
                .collect(),
        ));
        let batch = DfRecordBatch::try_new(schema.arrow_schema().clone(), columns).unwrap();
        RecordBatch::from_df_record_batch(schema, batch)
    }

    fn convert(filters: Vec<Expr>, projected_names: &[&str]) -> (ScanRequest, RecordBatch) {
        let metadata = test_metadata();
        let visible_metadata = visible_region_metadata(&metadata).unwrap();
        let projection = projected_names
            .iter()
            .map(|name| visible_metadata.column_index_by_name(name).unwrap())
            .collect::<Vec<_>>();
        let mut request = ScanRequest {
            projection_input: Some(projection.into()),
            filters,
            ..Default::default()
        };
        let mapper = prepare_value_split_scan(
            metadata.region_id,
            &mut request,
            &metadata,
            &visible_metadata,
        )
        .unwrap()
        .unwrap();
        let input = projected_batch(&metadata, request.projection_indices().unwrap());
        let output = mapper.convert_batch(input).unwrap();
        (request, output)
    }

    fn row_ids(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column_by_name(ROW_ID_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn test_normal_coalesce_preserves_values_nulls_and_nan() {
        let (_, batch) = convert(Vec::new(), &[VALUE_COLUMN]);
        let values = batch
            .column_by_name(VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(values.len(), 7);
        assert_eq!(values.value(0), -2.0);
        assert_eq!(values.value(1), -1.5);
        assert_eq!(values.value(2), 0.0);
        assert_eq!(values.value(3), 0.5);
        assert_eq!(values.value(4), 2.0);
        assert!(values.value(5).is_nan());
        assert!(values.is_null(6));
    }

    #[test]
    fn test_simple_value_filter_operators_and_projection() {
        let cases = [
            (col(VALUE_COLUMN).eq(lit(0.5_f64)), vec![3]),
            (col(VALUE_COLUMN).not_eq(lit(0.5_f64)), vec![0, 1, 2, 4, 5]),
            (col(VALUE_COLUMN).lt(lit(0.5_f64)), vec![0, 1, 2]),
            (col(VALUE_COLUMN).lt_eq(lit(0.5_f64)), vec![0, 1, 2, 3]),
            (col(VALUE_COLUMN).gt(lit(0.5_f64)), vec![4, 5]),
            (col(VALUE_COLUMN).gt_eq(lit(0.5_f64)), vec![3, 4, 5]),
            (lit(0.5_f64).lt(col(VALUE_COLUMN)), vec![4, 5]),
            (
                col(VALUE_COLUMN)
                    .eq(lit(-2.0_f64))
                    .or(col(VALUE_COLUMN).eq(lit(0.5_f64))),
                vec![0, 3],
            ),
        ];

        for (filter, expected) in cases {
            let (request, batch) = convert(vec![filter.clone()], &[ROW_ID_COLUMN]);
            assert_eq!(row_ids(&batch), expected, "filter: {filter}");
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema.column_schemas()[0].name, ROW_ID_COLUMN);
            assert_eq!(request.projection_indices().unwrap().len(), 3);
        }
    }

    #[test]
    fn test_multiple_simple_value_filters_are_anded() {
        let (_, batch) = convert(
            vec![
                col(VALUE_COLUMN).gt(lit(-2.0_f64)),
                col(VALUE_COLUMN).lt(lit(2.0_f64)),
            ],
            &[ROW_ID_COLUMN],
        );

        assert_eq!(row_ids(&batch), vec![1, 2, 3]);
    }

    #[test]
    fn test_non_simple_value_filter_is_left_for_the_upper_filter() {
        let (request, batch) = convert(vec![col(VALUE_COLUMN).is_not_null()], &[ROW_ID_COLUMN]);

        assert_eq!(row_ids(&batch), (0..7).collect::<Vec<_>>());
        assert_eq!(request.projection_indices().unwrap().len(), 1);
        assert!(request.filters[0].to_string().contains("coalesce"));
    }

    #[test]
    fn test_split_metric_values() {
        let int_column = metric_value_int_column_name(VALUE_COLUMN);
        let mut rows = Rows {
            schema: vec![
                api::v1::ColumnSchema {
                    column_name: VALUE_COLUMN.to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
                api::v1::ColumnSchema {
                    column_name: int_column,
                    datatype: ColumnDataType::Int64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
            ],
            rows: [
                (Some(ValueData::F64Value(1.0)), None),
                (Some(ValueData::F64Value(1.5)), None),
                (Some(ValueData::F64Value(f64::NAN)), None),
                (None, None),
                (Some(ValueData::F64Value(2.0)), Some(ValueData::I64Value(3))),
            ]
            .into_iter()
            .map(|(float, int)| api::v1::Row {
                values: vec![
                    api::v1::Value { value_data: float },
                    api::v1::Value { value_data: int },
                ],
            })
            .collect(),
        };

        split_metric_values(&mut rows);

        let values = rows
            .rows
            .iter()
            .map(|row| {
                (
                    row.values[0].value_data.clone(),
                    row.values[1].value_data.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(values[0], (None, Some(ValueData::I64Value(1))));
        assert_eq!(values[1], (Some(ValueData::F64Value(1.5)), None));
        let Some(ValueData::F64Value(value)) = &values[2].0 else {
            panic!("expected NaN Float64 value");
        };
        assert!(value.is_nan());
        assert!(values[2].1.is_none());
        assert_eq!(values[3], (None, None));
        assert_eq!(values[4], (None, Some(ValueData::I64Value(3))));
    }

    #[test]
    fn test_integer_value_classification() {
        assert_eq!(integer_value(1.0), Some(1));
        assert_eq!(integer_value(-1.0), Some(-1));
        assert_eq!(integer_value(-0.0), Some(0));
        assert_eq!(integer_value(i64::MIN as f64), Some(i64::MIN));
        assert_eq!(integer_value(1.5), None);
        assert_eq!(integer_value(f64::NAN), None);
        assert_eq!(integer_value(f64::INFINITY), None);
        assert_eq!(integer_value(i64::MAX as f64), None);
    }
}
