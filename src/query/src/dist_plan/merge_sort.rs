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

//! Merge sort logical plan for distributed query execution, roughly corresponding to the
//! `SortPreservingMergeExec` operator in datafusion
//!

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_stream::stream;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::BoxedError;
use common_plugins::GREPTIME_EXEC_READ_COST;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::{DfRecordBatchStreamAdapter, RecordBatchMetrics};
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{
    DfSendableRecordBatchStream, RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream,
};
use common_telemetry::tracing_context::TracingContext;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::EquivalenceProperties;
use datatypes::schema::{Schema, SchemaRef};
use futures_util::StreamExt;
use greptime_proto::v1::region::RegionRequestHeader;
use meter_core::data::ReadItem;
use meter_macros::read_meter;
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::table_name::TableName;

use crate::region_query::RegionQueryHandlerRef;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct MergeSortLogicalPlan {
    pub input: LogicalPlan,
    pub expr: Vec<Expr>,
    pub fetch: Option<usize>,
}

impl MergeSortLogicalPlan {
    pub fn new(input: LogicalPlan, expr: Vec<Expr>, fetch: Option<usize>) -> Self {
        Self { input, expr, fetch }
    }

    pub fn name() -> &'static str {
        "MergeSort"
    }

    /// Create a [LogicalPlan::Extension] node from this merge sort plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNodeCore for MergeSortLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    // Prevent further optimization.
    // The input can be retrieved by `self.input()`
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    // Prevent further optimization
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeSort: ")?;
        for (i, expr_item) in self.expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expr_item}")?;
        }
        if let Some(a) = self.fetch {
            write!(f, ", fetch={a}")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion::prelude::Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let mut zelf = self.clone();
        zelf.expr = exprs;
        zelf.input = inputs.pop().ok_or_else(|| {
            DataFusionError::Internal("Expected exactly one input with MergeSort".to_string())
        })?;
        Ok(self.clone())
    }
}
