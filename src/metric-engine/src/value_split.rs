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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::{
    CreateRecordBatchesSnafu, NewDfRecordBatchSnafu, PhysicalExprSnafu, Result as RecordBatchResult,
};
use common_recordbatch::filter::batch_filter;
use common_recordbatch::{
    DfRecordBatch, OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::common::ToDFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion_physical_expr::PhysicalExpr;
use datatypes::arrow::array::{Array, ArrayRef, Float64Array, Float64Builder, Int64Array};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures_util::Stream;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, RegionScannerRef, ScannerProperties,
};
use store_api::storage::{RegionId, SequenceNumber};

#[derive(Clone)]
pub(crate) enum ValueColumnProjection {
    Direct {
        input_index: usize,
        output_schema: ColumnSchema,
    },
    Split {
        float_index: usize,
        int_index: usize,
        output_schema: ColumnSchema,
    },
}

#[derive(Clone)]
pub(crate) struct ValueSplitProjectionMapper {
    output_schema: SchemaRef,
    working_schema: SchemaRef,
    columns: Vec<ValueColumnProjection>,
    visible_columns: usize,
    has_split: bool,
    residual_filters: Vec<Expr>,
}

impl ValueSplitProjectionMapper {
    pub(crate) fn new(
        columns: Vec<ValueColumnProjection>,
        visible_columns: usize,
        residual_filters: Vec<Expr>,
    ) -> Self {
        let has_split = columns
            .iter()
            .any(|column| matches!(column, ValueColumnProjection::Split { .. }));
        let working_columns = columns
            .iter()
            .map(|column| match column {
                ValueColumnProjection::Direct { output_schema, .. }
                | ValueColumnProjection::Split { output_schema, .. } => output_schema.clone(),
            })
            .collect::<Vec<_>>();
        let output_columns = working_columns
            .iter()
            .take(visible_columns)
            .cloned()
            .collect::<Vec<_>>();

        Self {
            output_schema: Arc::new(Schema::new(output_columns)),
            working_schema: Arc::new(Schema::new(working_columns)),
            columns,
            visible_columns,
            has_split,
            residual_filters,
        }
    }

    fn convert_batch(&self, batch: RecordBatch) -> RecordBatchResult<RecordBatch> {
        if !self.has_split && self.residual_filters.is_empty() {
            let projection = self
                .columns
                .iter()
                .map(|column| match column {
                    ValueColumnProjection::Direct { input_index, .. } => *input_index,
                    ValueColumnProjection::Split { .. } => unreachable!(),
                })
                .collect::<Vec<_>>();
            return batch.try_project(&projection);
        }

        let arrays = self
            .columns
            .iter()
            .map(|column| match column {
                ValueColumnProjection::Direct { input_index, .. } => {
                    Ok(batch.column(*input_index).clone())
                }
                ValueColumnProjection::Split {
                    float_index,
                    int_index,
                    ..
                } => coalesce_value_columns(batch.column(*float_index), batch.column(*int_index)),
            })
            .collect::<RecordBatchResult<Vec<_>>>()?;

        let df_record_batch =
            DfRecordBatch::try_new(self.working_schema.arrow_schema().clone(), arrays)
                .context(NewDfRecordBatchSnafu)?;
        let mut batch =
            RecordBatch::from_df_record_batch(self.working_schema.clone(), df_record_batch);
        batch = self.apply_residual_filters(batch)?;

        if self.visible_columns == self.columns.len() {
            Ok(RecordBatch::from_df_record_batch(
                self.output_schema.clone(),
                batch.into_df_record_batch(),
            ))
        } else {
            let projection = (0..self.visible_columns).collect::<Vec<_>>();
            batch.try_project(&projection)
        }
    }

    fn apply_residual_filters(&self, batch: RecordBatch) -> RecordBatchResult<RecordBatch> {
        let Some(filter) = conjunction(self.residual_filters.clone()) else {
            return Ok(batch);
        };
        let df_schema = self
            .working_schema
            .arrow_schema()
            .clone()
            .to_dfschema_ref()
            .context(PhysicalExprSnafu)?;
        let predicate = create_physical_expr(&filter, &df_schema, &ExecutionProps::new())
            .context(PhysicalExprSnafu)?;
        let df_record_batch =
            batch_filter(batch.df_record_batch(), &predicate).context(PhysicalExprSnafu)?;
        Ok(RecordBatch::from_df_record_batch(
            self.working_schema.clone(),
            df_record_batch,
        ))
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

    let mut builder = Float64Builder::with_capacity(float_array.len());
    for row in 0..float_array.len() {
        if !int_array.is_null(row) {
            builder.append_value(int_array.value(row) as f64);
        } else if !float_array.is_null(row) {
            builder.append_value(float_array.value(row));
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

pub(crate) struct ValueSplitScanner {
    inner: RegionScannerRef,
    logical_metadata: RegionMetadataRef,
    mapper: ValueSplitProjectionMapper,
}

impl ValueSplitScanner {
    pub(crate) fn new(
        inner: RegionScannerRef,
        logical_metadata: RegionMetadataRef,
        mapper: ValueSplitProjectionMapper,
    ) -> Self {
        Self {
            inner,
            logical_metadata,
            mapper,
        }
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
        self.logical_metadata.clone()
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
