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

use api::v1::frontend::frontend_server::Frontend;
use api::v1::frontend::{
    KillProcessRequest, KillProcessResponse, ListProcessRequest, ListProcessResponse,
};
use catalog::process_manager::ProcessManagerRef;
use common_telemetry::error;
use tonic::{Code, Request, Response, Status};

#[derive(Clone)]
pub struct FrontendGrpcHandler {
    process_manager: ProcessManagerRef,
}

impl FrontendGrpcHandler {
    pub fn new(process_manager: ProcessManagerRef) -> Self {
        Self { process_manager }
    }
}

#[async_trait::async_trait]
impl Frontend for FrontendGrpcHandler {
    async fn list_process(
        &self,
        request: Request<ListProcessRequest>,
    ) -> Result<Response<ListProcessResponse>, Status> {
        let list_process_request = request.into_inner();
        let catalog = if list_process_request.catalog.is_empty() {
            None
        } else {
            Some(list_process_request.catalog.as_str())
        };
        let processes = self.process_manager.local_processes(catalog).map_err(|e| {
            error!(e; "Failed to handle list process request");
            Status::new(Code::Internal, e.to_string())
        })?;
        Ok(Response::new(ListProcessResponse { processes }))
    }

    async fn kill_process(
        &self,
        request: Request<KillProcessRequest>,
    ) -> Result<Response<KillProcessResponse>, Status> {
        let req = request.into_inner();
        let found = self
            .process_manager
            .kill_process(req.server_addr, req.catalog, req.process_id)
            .await
            .map_err(|e| {
                error!(e; "Failed to handle kill process request");
                Status::new(Code::Internal, e.to_string())
            })?;

        Ok(Response::new(KillProcessResponse { found }))
    }
}
