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

use std::sync::Arc;

use api::v1::meta::ProcedureStatus;
use common_error::ext::BoxedError;
use common_meta::cluster::{ClusterInfo, NodeInfo, Role};
use common_meta::datanode::RegionStat;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::node_manager::DatanodeManagerRef;
use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutor};
use common_meta::rpc::procedure;
use common_procedure::{ProcedureInfo, ProcedureState};
use common_query::request::QueryRequest;
use common_recordbatch::adapter::{AsyncRecordBatchStreamAdapter, DfRecordBatchStreamAdapter};
use common_recordbatch::util::{ChainedRecordBatchStream, LimitedRecordBatchStream};
use common_recordbatch::{DfSendableRecordBatchStream, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::schema::SchemaRef;
use futures_util::future::try_join_all;
use meta_client::MetaClientRef;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error;
use crate::information_schema::{DatanodeInspectRequest, InformationExtension};

pub struct DistributedInformationExtension {
    meta_client: MetaClientRef,
    datanode_manager: DatanodeManagerRef,
}

impl DistributedInformationExtension {
    pub fn new(meta_client: MetaClientRef, datanode_manager: DatanodeManagerRef) -> Self {
        Self {
            meta_client,
            datanode_manager,
        }
    }

    async fn inspect_datanode_stream(
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
        request: DatanodeInspectRequest,
    ) -> std::result::Result<SendableRecordBatchStream, crate::error::Error> {
        let nodes = meta_client
            .list_nodes(Some(Role::Datanode))
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListNodesSnafu)?;

        let limit = request.scan.limit;
        let plan = request
            .build_plan()
            .context(crate::error::DatafusionSnafu)?;

        let streams = try_join_all(nodes.into_iter().map(|node| {
            let datanode_manager = datanode_manager.clone();
            let plan = plan.clone();
            async move {
                let client = datanode_manager.datanode(&node.peer).await;
                client
                    .handle_query(QueryRequest {
                        plan,
                        region_id: RegionId::default(),
                        header: None,
                    })
                    .await
                    .context(crate::error::HandleQuerySnafu)
            }
        }))
        .await?;

        let chained =
            ChainedRecordBatchStream::new(streams).context(crate::error::CreateRecordBatchSnafu)?;
        match limit {
            Some(limit) => Ok(Box::pin(LimitedRecordBatchStream::new(
                Box::pin(chained),
                limit,
            ))),
            None => Ok(Box::pin(chained)),
        }
    }
}

struct DistributedInspectExec {
    request: DatanodeInspectRequest,
    meta_client: MetaClientRef,
    datanode_manager: DatanodeManagerRef,
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for DistributedInspectExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedInspectExec")
            .field("request", &self.request)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DistributedInspectExec {
    fn new(
        request: DatanodeInspectRequest,
        schema: SchemaRef,
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
    ) -> Self {
        let arrow_schema = schema.arrow_schema().clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            request,
            meta_client,
            datanode_manager,
            schema,
            arrow_schema,
            properties,
        }
    }
}

impl DisplayAs for DistributedInspectExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DistributedInspectExec: kind={:?}, scan={}, schema={:?}",
            self.request.kind, self.request.scan, self.arrow_schema
        )
    }
}

impl ExecutionPlan for DistributedInspectExec {
    fn name(&self) -> &str {
        "DistributedInspectExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "DistributedInspectExec is a leaf execution plan and cannot accept children"
                    .to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<DfSendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "DistributedInspectExec invalid partition. Expected 0, got {partition}"
            )));
        }

        let meta_client = self.meta_client.clone();
        let datanode_manager = self.datanode_manager.clone();
        let request = self.request.clone();
        let schema = self.schema.clone();
        let future = async move {
            DistributedInformationExtension::inspect_datanode_stream(
                meta_client,
                datanode_manager,
                request,
            )
            .await
            .map_err(BoxedError::new)
            .context(common_recordbatch::error::ExternalSnafu)
        };
        let stream = AsyncRecordBatchStreamAdapter::new(schema, Box::pin(future));
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(Box::pin(stream))))
    }
}

#[async_trait::async_trait]
impl InformationExtension for DistributedInformationExtension {
    type Error = crate::error::Error;

    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        self.meta_client
            .list_nodes(None)
            .await
            .map_err(BoxedError::new)
            .context(error::ListNodesSnafu)
    }

    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
        let procedures = self
            .meta_client
            .list_procedures(&ExecutorContext::default())
            .await
            .map_err(BoxedError::new)
            .context(error::ListProceduresSnafu)?
            .procedures;
        let mut result = Vec::with_capacity(procedures.len());
        for procedure in procedures {
            let pid = match procedure.id {
                Some(pid) => pid,
                None => return error::ProcedureIdNotFoundSnafu {}.fail(),
            };
            let pid = procedure::pb_pid_to_pid(&pid)
                .map_err(BoxedError::new)
                .context(error::ConvertProtoDataSnafu)?;
            let status = ProcedureStatus::try_from(procedure.status)
                .map(|v| v.as_str_name())
                .unwrap_or("Unknown")
                .to_string();
            let procedure_info = ProcedureInfo {
                id: pid,
                type_name: procedure.type_name,
                start_time_ms: procedure.start_time_ms,
                end_time_ms: procedure.end_time_ms,
                state: ProcedureState::Running,
                lock_keys: procedure.lock_keys,
            };
            result.push((status, procedure_info));
        }

        Ok(result)
    }

    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
        self.meta_client
            .list_region_stats()
            .await
            .map_err(BoxedError::new)
            .context(error::ListRegionStatsSnafu)
    }

    async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
        self.meta_client
            .list_flow_stats()
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListFlowStatsSnafu)
    }

    async fn inspect_datanode(
        &self,
        request: DatanodeInspectRequest,
    ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
        Self::inspect_datanode_stream(
            self.meta_client.clone(),
            self.datanode_manager.clone(),
            request,
        )
        .await
    }

    fn inspect_datanode_plan(
        &self,
        request: DatanodeInspectRequest,
        schema: SchemaRef,
    ) -> std::result::Result<Option<Arc<dyn ExecutionPlan>>, Self::Error> {
        Ok(Some(Arc::new(DistributedInspectExec::new(
            request,
            schema,
            self.meta_client.clone(),
            self.datanode_manager.clone(),
        ))))
    }
}
