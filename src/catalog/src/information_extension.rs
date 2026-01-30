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

use api::v1::meta::ProcedureStatus;
use common_error::ext::BoxedError;
use common_meta::cluster::{ClusterInfo, NodeInfo, Role};
use common_meta::datanode::RegionStat;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutor};
use common_meta::region_rpc::RegionRpcRef;
use common_meta::rpc::procedure;
use common_procedure::{ProcedureInfo, ProcedureState};
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::util::ChainedRecordBatchStream;
use meta_client::MetaClientRef;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error;
use crate::information_schema::{DatanodeInspectRequest, InformationExtension};

pub struct DistributedInformationExtension {
    meta_client: MetaClientRef,
    region_rpc: RegionRpcRef,
}

impl DistributedInformationExtension {
    pub fn new(meta_client: MetaClientRef, region_rpc: RegionRpcRef) -> Self {
        Self {
            meta_client,
            region_rpc,
        }
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
        // Aggregate results from all datanodes
        let nodes = self
            .meta_client
            .list_nodes(Some(Role::Datanode))
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListNodesSnafu)?;

        let plan = request
            .build_plan()
            .context(crate::error::DatafusionSnafu)?;

        let mut streams = Vec::with_capacity(nodes.len());
        for node in nodes {
            let stream = self
                .region_rpc
                .handle_query(
                    &node.peer,
                    QueryRequest {
                        plan: plan.clone(),
                        region_id: RegionId::default(),
                        header: None,
                    },
                )
                .await
                .context(crate::error::HandleQuerySnafu)?;
            streams.push(stream);
        }

        let chained =
            ChainedRecordBatchStream::new(streams).context(crate::error::CreateRecordBatchSnafu)?;
        Ok(Box::pin(chained))
    }
}
