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
use api::v1::frontend::{ListProcessRequest, ListProcessResponse};
use common_frontend::ProcessManagerRef;
use tonic::{Request, Response, Status};

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
        _request: Request<ListProcessRequest>,
    ) -> Result<Response<ListProcessResponse>, Status> {
        let processes = self.process_manager.local_processes(None).unwrap();
        Ok(Response::new(ListProcessResponse { processes }))
    }
}
