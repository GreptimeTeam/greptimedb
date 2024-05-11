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
use std::time::Duration;

use api::v1::meta::{
    procedure_service_server, DdlTaskRequest as PbDdlTaskRequest,
    DdlTaskResponse as PbDdlTaskResponse, MigrateRegionRequest, MigrateRegionResponse,
    ProcedureStateResponse, QueryProcedureRequest,
};
use common_meta::ddl::ExecutorContext;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest};
use common_meta::rpc::procedure;
use snafu::{ensure, OptionExt, ResultExt};
use tonic::{Request, Response};

use super::GrpcResult;
use crate::error;
use crate::metasrv::Metasrv;
use crate::procedure::region_migration::manager::RegionMigrationProcedureTask;

#[async_trait::async_trait]
impl procedure_service_server::ProcedureService for Metasrv {
    async fn query(
        &self,
        request: Request<QueryProcedureRequest>,
    ) -> GrpcResult<ProcedureStateResponse> {
        let QueryProcedureRequest { header, pid, .. } = request.into_inner();
        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let pid = pid.context(error::MissingRequiredParameterSnafu { param: "pid" })?;
        let pid = procedure::pb_pid_to_pid(&pid).context(error::ConvertProtoDataSnafu)?;

        let state = self
            .procedure_manager()
            .procedure_state(pid)
            .await
            .context(error::QueryProcedureSnafu)?
            .context(error::ProcedureNotFoundSnafu {
                pid: pid.to_string(),
            })?;

        Ok(Response::new(procedure::procedure_state_to_pb_response(
            &state,
        )))
    }

    async fn ddl(&self, request: Request<PbDdlTaskRequest>) -> GrpcResult<PbDdlTaskResponse> {
        let PbDdlTaskRequest {
            header,
            query_context,
            task,
            ..
        } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
        let cluster_id = header.cluster_id;
        let query_context = query_context
            .context(error::MissingRequiredParameterSnafu {
                param: "query_context",
            })?
            .into();
        let task: DdlTask = task
            .context(error::MissingRequiredParameterSnafu { param: "task" })?
            .try_into()
            .context(error::ConvertProtoDataSnafu)?;

        let resp = self
            .procedure_executor()
            .submit_ddl_task(
                &ExecutorContext {
                    cluster_id: Some(cluster_id),
                    tracing_context: Some(header.tracing_context),
                },
                SubmitDdlTaskRequest {
                    query_context: Arc::new(query_context),
                    task,
                },
            )
            .await
            .context(error::SubmitDdlTaskSnafu)?
            .into();

        Ok(Response::new(resp))
    }

    async fn migrate(
        &self,
        request: Request<MigrateRegionRequest>,
    ) -> GrpcResult<MigrateRegionResponse> {
        ensure!(
            self.meta_peer_client().is_leader(),
            error::UnexpectedSnafu {
                violated: "Trying to submit a region migration procedure to non-leader meta server"
            }
        );

        let MigrateRegionRequest {
            header,
            region_id,
            from_peer,
            to_peer,
            replay_timeout_secs,
            ..
        } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
        let cluster_id = header.cluster_id;

        let from_peer = self
            .lookup_peer(cluster_id, from_peer)
            .await?
            .context(error::PeerUnavailableSnafu { peer_id: from_peer })?;
        let to_peer = self
            .lookup_peer(cluster_id, to_peer)
            .await?
            .context(error::PeerUnavailableSnafu { peer_id: to_peer })?;

        let pid = self
            .region_migration_manager()
            .submit_procedure(RegionMigrationProcedureTask {
                cluster_id,
                region_id: region_id.into(),
                from_peer,
                to_peer,
                replay_timeout: Duration::from_secs(replay_timeout_secs.into()),
            })
            .await?
            .map(procedure::pid_to_pb_pid);

        let resp = MigrateRegionResponse {
            pid,
            ..Default::default()
        };

        Ok(Response::new(resp))
    }
}
