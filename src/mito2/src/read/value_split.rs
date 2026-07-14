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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::{
    ArrowComputeSnafu, CreateRecordBatchesSnafu, NewDfRecordBatchSnafu, PhysicalExprSnafu,
    Result as RecordBatchResult,
};
use common_recordbatch::filter::batch_filter;
use common_recordbatch::{
    DfRecordBatch, OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Column, Result as DataFusionResult, ToDFSchema};
use datafusion::execution::context::ExecutionProps;
use datafusion::functions::expr_fn::coalesce;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datatypes::arrow::compute::kernels::zip::zip;
use datatypes::arrow::compute::{cast as cast_array, is_not_null};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures::Stream;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::metric_engine_value_int_column_name;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, RegionScannerRef, ScannerProperties,
};
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};

use crate::error::{InvalidRequestSnafu, RecordBatchSnafu, Result};
use crate::metric_value::{MetricValueColumn, metric_value_columns, visible_region_metadata};

pub(crate) fn prepare_value_split_scan(
    region_id: RegionId,
    request: &mut ScanRequest,
    physical_metadata: &RegionMetadataRef,
) -> Result<Option<ValueSplitProjectionMapper>> {
    let split_columns = metric_value_columns(physical_metadata);
    if split_columns.is_empty() {
        return Ok(None);
    }

    let visible_metadata = visible_region_metadata(physical_metadata, &split_columns)?;
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

    let mut residual_column_names = HashSet::new();
    let mut residual_filters = Vec::new();
    request.filters = request
        .filters
        .drain(..)
        .map(|filter| {
            let residual_filter = filter.clone();
            let (filter, columns) =
                rewrite_metric_value_filter(region_id, filter, &split_value_columns)?;
            if let Some(columns) = columns {
                residual_column_names.extend(columns);
                residual_filters.push(residual_filter);
            }
            Ok(filter)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut visible_projection = match request.projection_input.as_ref() {
        Some(projection_input) => projection_input.projection.clone(),
        None => (0..visible_metadata.column_metadatas.len()).collect(),
    };
    let visible_columns = visible_projection.len();
    let mut projected = visible_projection.iter().copied().collect::<HashSet<_>>();
    for (index, column) in visible_metadata.column_metadatas.iter().enumerate() {
        if residual_column_names.contains(&column.column_schema.name) && projected.insert(index) {
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
        });
    }

    request.projection_input.get_or_insert_default().projection = physical_projection;

    let mapper = ValueSplitProjectionMapper::try_new(
        visible_metadata,
        output_columns,
        visible_columns,
        residual_filters,
    )
    .context(RecordBatchSnafu)?;
    Ok(Some(mapper))
}

fn rewrite_metric_value_filter(
    region_id: RegionId,
    filter: Expr,
    split_value_columns: &HashMap<&str, MetricValueColumn>,
) -> Result<(Expr, Option<HashSet<String>>)> {
    let filter_display = filter.to_string();
    let mut rewriter = MetricValueFilterRewriter {
        split_value_columns,
        referenced_columns: HashSet::new(),
    };
    filter
        .rewrite(&mut rewriter)
        .map(|rewritten| {
            let residual_columns = rewritten.transformed.then_some(rewriter.referenced_columns);
            (rewritten.data, residual_columns)
        })
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
    referenced_columns: HashSet<String>,
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
        self.referenced_columns.insert(column.name.clone());

        if !self.split_value_columns.contains_key(column.name.as_str()) {
            return Ok(Transformed::no(Expr::Column(column)));
        }

        let int_column = Column {
            relation: column.relation.clone(),
            name: metric_engine_value_int_column_name(&column.name),
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
}

#[derive(Clone)]
pub(crate) struct ValueSplitProjectionMapper {
    metadata: RegionMetadataRef,
    output_schema: SchemaRef,
    working_schema: SchemaRef,
    columns: Vec<ValueColumnProjection>,
    has_split: bool,
    residual_filter: Option<Arc<dyn PhysicalExpr>>,
}

impl ValueSplitProjectionMapper {
    fn try_new(
        metadata: RegionMetadataRef,
        columns: Vec<ValueColumnProjection>,
        visible_columns: usize,
        residual_filters: Vec<Expr>,
    ) -> RecordBatchResult<Self> {
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
        let residual_filter = if let Some(filter) = conjunction(residual_filters) {
            let df_schema = working_schema
                .arrow_schema()
                .clone()
                .to_dfschema_ref()
                .context(PhysicalExprSnafu)?;
            Some(
                create_physical_expr(&filter, &df_schema, &ExecutionProps::new())
                    .context(PhysicalExprSnafu)?,
            )
        } else {
            None
        };

        Ok(Self {
            metadata,
            output_schema,
            working_schema,
            columns,
            has_split,
            residual_filter,
        })
    }

    fn convert_batch(&self, batch: RecordBatch) -> RecordBatchResult<RecordBatch> {
        if !self.has_split && self.residual_filter.is_none() {
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

        let df_record_batch =
            DfRecordBatch::try_new(self.working_schema.arrow_schema().clone(), arrays)
                .context(NewDfRecordBatchSnafu)?;
        let batch = RecordBatch::from_df_record_batch(self.working_schema.clone(), df_record_batch);
        let batch = if let Some(predicate) = &self.residual_filter {
            let df_record_batch =
                batch_filter(batch.df_record_batch(), predicate).context(PhysicalExprSnafu)?;
            RecordBatch::from_df_record_batch(self.working_schema.clone(), df_record_batch)
        } else {
            batch
        };

        let output_columns = self.output_schema.num_columns();
        if output_columns == self.columns.len() {
            Ok(batch)
        } else {
            batch.try_project(&(0..output_columns).collect::<Vec<_>>())
        }
    }
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

pub(crate) struct ValueSplitScanner {
    inner: RegionScannerRef,
    mapper: ValueSplitProjectionMapper,
}

impl ValueSplitScanner {
    pub(crate) fn new(inner: RegionScannerRef, mapper: ValueSplitProjectionMapper) -> Self {
        Self { inner, mapper }
    }
}

impl fmt::Debug for ValueSplitScanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueSplitScanner")
            .field("inner", &self.inner)
            .field("has_split", &self.mapper.has_split)
            .finish()
    }
}

impl DisplayAs for ValueSplitScanner {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl RegionScanner for ValueSplitScanner {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &ScannerProperties {
        self.inner.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.mapper.output_schema.clone()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.mapper.metadata.clone()
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
        Ok(Box::pin(ValueSplitRecordBatchStream {
            inner: stream,
            mapper: self.mapper.clone(),
        }))
    }

    fn has_predicate_without_region(&self) -> bool {
        self.inner.has_predicate_without_region()
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        if self.mapper.has_split {
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
