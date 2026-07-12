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

use api::v1::meta::{PullConfigRequest, PullConfigResponse, ResponseHeader, config_server};
use common_options::plugin_options::PluginOptionsSerializerRef;
use common_telemetry::{info, warn};
use snafu::ResultExt;
use tonic::{Request, Response};

use crate::error::SerializeConfigSnafu;
use crate::metasrv::Metasrv;
use crate::service::GrpcResult;

#[async_trait::async_trait]
impl config_server::Config for Metasrv {
    async fn pull_config(&self, req: Request<PullConfigRequest>) -> GrpcResult<PullConfigResponse> {
        let payload = match self.plugins().get::<PluginOptionsSerializerRef>() {
            Some(p) => p
                .serialize()
                .inspect_err(|e| warn!(e; "Failed to serialize plugin options"))
                .context(SerializeConfigSnafu)?,
            None => String::new(),
        };

        let res = PullConfigResponse {
            header: Some(ResponseHeader::success()),
            payload,
        };

        let member_id = req.into_inner().header.as_ref().map(|h| h.member_id);
        info!("Sending meta config to member: {member_id:?}");

        Ok(Response::new(res))
    }
}
