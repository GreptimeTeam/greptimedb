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

use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_schema::{Field, Schema, SchemaRef};
use common_query::DfPhysicalPlan;
use datafusion::common::{Result as DataFusionResult, Statistics};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_common::{DFField, DFSchema, DFSchemaRef};
use datafusion_expr::utils::exprlist_to_fields;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use snafu::ResultExt;

use crate::error::{DataFusionSnafu, Result};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct RangeFn {
    pub expr: Expr,
    pub range: Duration,
    pub fill: String,
}

impl Display for RangeFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RangeFn {{ expr:{} range:{}s fill:{} }}",
            self.expr.display_name().unwrap_or("?".into()),
            self.range.as_secs(),
            self.fill,
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RangeSelect {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// all range expressions
    pub range_expr: Vec<RangeFn>,
    pub algin: Duration,
    pub time_index: String,
    pub by: Vec<Expr>,
    pub schema: DFSchemaRef,
}

impl RangeSelect {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        range_expr: Vec<RangeFn>,
        algin: Duration,
        time_index: Expr,
        by: Vec<Expr>,
    ) -> Result<Self> {
        let mut fields = range_expr
            .iter()
            .map(|RangeFn { expr, .. }| {
                Ok(DFField::new_unqualified(
                    &expr.display_name()?,
                    expr.get_type(input.schema())?,
                    expr.nullable(input.schema())?,
                ))
            })
            .collect::<DfResult<Vec<_>>>()
            .context(DataFusionSnafu)?;
        // add align_ts
        let ts_field = time_index
            .to_field(input.schema().as_ref())
            .context(DataFusionSnafu)?;
        let time_index_name = ts_field.name().clone();
        fields.push(ts_field);
        // add by
        fields.extend(
            exprlist_to_fields(by.iter().collect::<Vec<_>>().into_iter(), &input)
                .context(DataFusionSnafu)?,
        );
        let schema = DFSchema::new_with_metadata(fields, input.schema().metadata().clone())
            .context(DataFusionSnafu)?;
        Ok(Self {
            input,
            range_expr,
            algin,
            time_index: time_index_name,
            schema: Arc::new(schema),
            by,
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
            "RangeSelect: range_exprs=[{}], algin={}s time_index={}",
            self.range_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.algin.as_secs(),
            self.time_index
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert!(!inputs.is_empty());

        Self {
            algin: self.algin,
            range_expr: self.range_expr.clone(),
            input: Arc::new(inputs[0].clone()),
            time_index: self.time_index.clone(),
            schema: self.schema.clone(),
            by: self.by.clone(),
        }
    }
}

impl RangeSelect {
    pub fn to_execution_plan(
        &self,
        _logical_input: &LogicalPlan,
        exec_input: Arc<dyn ExecutionPlan>,
        _session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
            .collect();
        Ok(Arc::new(RangeSelectExec {
            input: exec_input,
            schema: Arc::new(Schema::new(fields)),
        }))
    }
}

#[derive(Debug)]
pub struct RangeSelectExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl DisplayAs for RangeSelectExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RangeSelectExec: ")
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
            schema: self.schema.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<common_query::physical_plan::TaskContext>,
    ) -> datafusion_common::Result<common_recordbatch::DfSendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(RangeSelectStream {
            schema: self.schema.clone(),
            input,
        }))
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

pub struct RangeSelectStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
}

impl RecordBatchStream for RangeSelectStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RangeSelectStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(_batch))) => {
                Poll::Ready(Some(Ok(RecordBatch::new_empty(self.schema.clone()))))
            }
            Poll::Ready(other) => Poll::Ready(other),
            Poll::Pending => Poll::Pending,
        }
    }
}
