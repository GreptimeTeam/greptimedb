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

use api::v1::meta::{ddl_task_server, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use api::v1::TableId;
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response};

use super::GrpcResult;
use crate::ddl::ProcedureStatus;
use crate::error;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl ddl_task_server::DdlTask for MetaSrv {
    async fn submit_ddl_task(
        &self,
        request: Request<SubmitDdlTaskRequest>,
    ) -> GrpcResult<SubmitDdlTaskResponse> {
        let SubmitDdlTaskRequest { header, task, .. } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
        let task = task
            .context(error::MissingRequiredParameterSnafu { param: "task" })?
            .try_into()
            .context(error::ConvertProtoDataSnafu)?;
        let (id, status) = self
            .ddl_manager()
            .execute_procedure_task(header.cluster_id, task)
            .await?;

        let mut resp = SubmitDdlTaskResponse {
            key: id.to_string().into(),
            ..Default::default()
        };

        let ProcedureStatus::CreateTable(status) = status;
        resp.table_id = Some(TableId {
            id: status.table_id,
        });

        Ok(Response::new(resp))
    }
}
