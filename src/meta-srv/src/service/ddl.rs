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

use api::v1::meta::{
    procedure_server, DdlTaskRequest as PbDdlTaskRequest, DdlTaskResponse as PbDdlTaskResponse,
    MigrateRegionRequest, MigrateRegionResponse, ProcedureId, ProcedureStateResponse,
    QueryProcedureRequest,
};
use common_meta::ddl::ExecutorContext;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest};
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response};

use super::GrpcResult;
use crate::error;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl procedure_server::Procedure for MetaSrv {
    async fn query(
        &self,
        pid: Request<QueryProcedureRequest>,
    ) -> GrpcResult<ProcedureStateResponse> {
        todo!();
    }

    async fn ddl(&self, request: Request<PbDdlTaskRequest>) -> GrpcResult<PbDdlTaskResponse> {
        let PbDdlTaskRequest { header, task, .. } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
        let cluster_id = header.cluster_id;
        let task: DdlTask = task
            .context(error::MissingRequiredParameterSnafu { param: "task" })?
            .try_into()
            .context(error::ConvertProtoDataSnafu)?;

        let resp = self
            .ddl_executor()
            .submit_ddl_task(
                &ExecutorContext {
                    cluster_id: Some(cluster_id),
                    tracing_context: Some(header.tracing_context),
                },
                SubmitDdlTaskRequest { task },
            )
            .await
            .context(error::SubmitDdlTaskSnafu)?
            .into();

        Ok(Response::new(resp))
    }

    async fn migrate(
        &self,
        pid: Request<MigrateRegionRequest>,
    ) -> GrpcResult<MigrateRegionResponse> {
        todo!();
    }
}
