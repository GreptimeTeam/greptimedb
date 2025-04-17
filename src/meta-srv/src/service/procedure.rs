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
    DdlTaskResponse as PbDdlTaskResponse, Error, MigrateRegionRequest, MigrateRegionResponse,
    ProcedureDetailRequest, ProcedureDetailResponse, ProcedureStateResponse, QueryProcedureRequest,
    ResponseHeader,
};
use common_meta::ddl::ExecutorContext;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest};
use common_meta::rpc::procedure;
use common_telemetry::warn;
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response};

use crate::error;
use crate::metasrv::Metasrv;
use crate::procedure::region_migration::manager::RegionMigrationProcedureTask;
use crate::service::GrpcResult;

#[async_trait::async_trait]
impl procedure_service_server::ProcedureService for Metasrv {
    async fn query(
        &self,
        request: Request<QueryProcedureRequest>,
    ) -> GrpcResult<ProcedureStateResponse> {
        if !self.is_leader() {
            let resp = ProcedureStateResponse {
                header: Some(ResponseHeader::failed(Error::is_not_leader())),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `query procedure state` request have reached the meta. Detail: {:?}.", request);
            return Ok(Response::new(resp));
        }

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
        if !self.is_leader() {
            let resp = PbDdlTaskResponse {
                header: Some(ResponseHeader::failed(Error::is_not_leader())),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `ddl` request have reached the meta. Detail: {:?}.", request);
            return Ok(Response::new(resp));
        }

        let PbDdlTaskRequest {
            header,
            query_context,
            task,
            ..
        } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
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
        if !self.is_leader() {
            let resp = MigrateRegionResponse {
                header: Some(ResponseHeader::failed(Error::is_not_leader())),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `migrate` request have reached the meta. Detail: {:?}.", request);
            return Ok(Response::new(resp));
        }

        let MigrateRegionRequest {
            header,
            region_id,
            from_peer,
            to_peer,
            timeout_secs,
        } = request.into_inner();

        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let from_peer = self
            .lookup_peer(from_peer)
            .await?
            .context(error::PeerUnavailableSnafu { peer_id: from_peer })?;
        let to_peer = self
            .lookup_peer(to_peer)
            .await?
            .context(error::PeerUnavailableSnafu { peer_id: to_peer })?;

        let pid = self
            .region_migration_manager()
            .submit_procedure(RegionMigrationProcedureTask {
                region_id: region_id.into(),
                from_peer,
                to_peer,
                timeout: Duration::from_secs(timeout_secs.into()),
            })
            .await?
            .map(procedure::pid_to_pb_pid);

        let resp = MigrateRegionResponse {
            pid,
            ..Default::default()
        };

        Ok(Response::new(resp))
    }

    async fn details(
        &self,
        request: Request<ProcedureDetailRequest>,
    ) -> GrpcResult<ProcedureDetailResponse> {
        if !self.is_leader() {
            let resp = ProcedureDetailResponse {
                header: Some(ResponseHeader::failed(Error::is_not_leader())),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `procedure details` request have reached the meta. Detail: {:?}.", request);
            return Ok(Response::new(resp));
        }

        let ProcedureDetailRequest { header } = request.into_inner();
        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let metas = self
            .procedure_manager()
            .list_procedures()
            .await
            .context(error::QueryProcedureSnafu)?;
        Ok(Response::new(procedure::procedure_details_to_pb_response(
            metas,
        )))
    }
}
