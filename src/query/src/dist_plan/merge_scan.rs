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

use std::any::Any;
use std::sync::Arc;

use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_stream::try_stream;
use client::client_manager::DatanodeClients;
use client::Database;
use common_base::bytes::Bytes;
use common_error::ext::BoxedError;
use common_meta::peer::Peer;
use common_meta::table_name::TableName;
use common_query::physical_plan::TaskContext;
use common_query::Output;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{
    DfSendableRecordBatchStream, RecordBatch, RecordBatchStreamAdaptor, SendableRecordBatchStream,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::PhysicalSortExpr;
use datatypes::schema::Schema;
use futures_util::StreamExt;
use snafu::ResultExt;

use crate::error::{ConvertSchemaSnafu, RemoteRequestSnafu, UnexpectedOutputKindSnafu};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct MergeScanLogicalPlan {
    /// In logical plan phase it only contains one input
    input: LogicalPlan,
    /// If this plan is a placeholder
    is_placeholder: bool,
}

impl UserDefinedLogicalNodeCore for MergeScanLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        self.input.expressions()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeScan [is_placeholder={}]", self.is_placeholder)
    }

    // todo: maybe contains exprs will be useful
    // todo: add check for inputs' length
    fn from_template(&self, _exprs: &[datafusion_expr::Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            input: inputs[0].clone(),
            is_placeholder: self.is_placeholder,
        }
    }
}

impl MergeScanLogicalPlan {
    pub fn new(input: LogicalPlan, is_placeholder: bool) -> Self {
        Self {
            input,
            is_placeholder,
        }
    }

    pub fn name() -> &'static str {
        "MergeScan"
    }

    /// Create a [LogicalPlan::Extension] node from this merge scan plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn is_placeholder(&self) -> bool {
        self.is_placeholder
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
}

#[derive(Debug)]
pub struct MergeScanExec {
    table: TableName,
    peers: Vec<Peer>,
    substrait_plan: Bytes,
    arrow_schema: ArrowSchemaRef,
    clients: Arc<DatanodeClients>,
}

impl MergeScanExec {
    pub fn new(
        table: TableName,
        peers: Vec<Peer>,
        substrait_plan: Bytes,
        arrow_schema: ArrowSchemaRef,
        clients: Arc<DatanodeClients>,
    ) -> Self {
        // remove all metadata
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_schema.fields().to_vec()));
        Self {
            table,
            peers,
            substrait_plan,
            arrow_schema,
            clients,
        }
    }

    pub fn to_stream(&self, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let substrait_plan = self.substrait_plan.to_vec();
        let peers = self.peers.clone();
        let clients = self.clients.clone();
        let table = self.table.clone();
        let trace_id = context.task_id().and_then(|id| id.parse().ok());

        let stream = try_stream! {
            for peer in peers {
                let client = clients.get_client(&peer).await;
                let database = Database::new(&table.catalog_name, &table.schema_name, client);
                let output: Output = database
                    .logical_plan(substrait_plan.clone(), trace_id)
                    .await
                    .context(RemoteRequestSnafu)
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;

                match output {
                    Output::AffectedRows(_) => {
                        Err(BoxedError::new(
                            UnexpectedOutputKindSnafu {
                                expected: "RecordBatches or Stream",
                                got: "AffectedRows",
                            }
                            .build(),
                        ))
                        .context(ExternalSnafu)?;
                    }
                    Output::RecordBatches(record_batches) => {
                        for batch in record_batches.into_iter() {
                            yield Self::remove_metadata_from_record_batch(batch);
                        }
                    }
                    Output::Stream(mut stream) => {
                        while let Some(batch) = stream.next().await {
                            yield Self::remove_metadata_from_record_batch(batch?);
                        }
                    }
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdaptor {
            schema: Arc::new(
                self.arrow_schema
                    .clone()
                    .try_into()
                    .context(ConvertSchemaSnafu)?,
            ),
            stream: Box::pin(stream),
            output_ordering: None,
        }))
    }

    fn remove_metadata_from_record_batch(batch: RecordBatch) -> RecordBatch {
        let schema = ArrowSchema::new(batch.schema.arrow_schema().fields().to_vec());
        RecordBatch::new(
            Arc::new(Schema::try_from(schema).unwrap()),
            batch.columns().iter().cloned(),
        )
        .unwrap()
    }
}

impl ExecutionPlan for MergeScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "should not call `with_new_children` on MergeScanExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<DfSendableRecordBatchStream> {
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(
            self.to_stream(context)?,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for MergeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeScanExec: peers=[")?;
        for peer in self.peers.iter() {
            write!(f, "{}, ", peer)?;
        }
        write!(f, "]")
    }
}
