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
    BatchGetRequest as PbBatchGetRequest, BatchGetResponse as PbBatchGetResponse, MetasrvNodeInfo,
    MetasrvPeersRequest, MetasrvPeersResponse, RangeRequest as PbRangeRequest,
    RangeResponse as PbRangeResponse, cluster_server,
};
use snafu::ResultExt;
use tonic::Request;

use crate::metasrv::Metasrv;
use crate::service::GrpcResult;
use crate::{check_leader, error, metasrv};

#[async_trait::async_trait]
impl cluster_server::Cluster for Metasrv {
    async fn batch_get(&self, req: Request<PbBatchGetRequest>) -> GrpcResult<PbBatchGetResponse> {
        check_leader!(self, req, PbBatchGetResponse, "`batch_get`");

        let req = req.into_inner().into();
        let resp = self
            .in_memory()
            .batch_get(req)
            .await
            .context(error::KvBackendSnafu)?;

        let resp = resp.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(resp))
    }

    async fn range(&self, req: Request<PbRangeRequest>) -> GrpcResult<PbRangeResponse> {
        check_leader!(self, req, PbRangeResponse, "`range`");

        let req = req.into_inner().into();
        let res = self
            .in_memory()
            .range(req)
            .await
            .context(error::KvBackendSnafu)?;

        let resp = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(resp))
    }

    async fn metasrv_peers(
        &self,
        req: Request<MetasrvPeersRequest>,
    ) -> GrpcResult<MetasrvPeersResponse> {
        check_leader!(self, req, MetasrvPeersResponse, "`metasrv_peers`");

        let leader_addr = &self.options().grpc.server_addr;
        let (leader, followers) = match self.election() {
            Some(election) => {
                let nodes = election.all_candidates().await?;
                let followers = nodes
                    .into_iter()
                    .filter(|node_info| &node_info.addr != leader_addr)
                    .map(api::v1::meta::MetasrvNodeInfo::from)
                    .collect();
                (self.node_info().into(), followers)
            }
            None => (self.make_node_info(leader_addr), vec![]),
        };

        let resp = MetasrvPeersResponse {
            header: Some(ResponseHeader::success()),
            leader: Some(leader),
            followers,
        };

        Ok(Response::new(resp))
    }
}

impl Metasrv {
    pub fn is_leader(&self) -> bool {
        // Returns true when there is no `election`, indicating the presence of only one `Metasrv` node, which is the leader.
        self.election().map(|x| x.is_leader()).unwrap_or(true)
    }

    fn make_node_info(&self, addr: &str) -> MetasrvNodeInfo {
        let build_info = common_version::build_info();
        metasrv::MetasrvNodeInfo {
            addr: addr.to_string(),
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            start_time_ms: self.start_time_ms(),
            total_cpu_millicores: self.resource_stat().get_total_cpu_millicores(),
            total_memory_bytes: self.resource_stat().get_total_memory_bytes(),
            cpu_usage_millicores: self.resource_stat().get_cpu_usage_millicores(),
            memory_usage_bytes: self.resource_stat().get_memory_usage_bytes(),
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        }
        .into()
    }
}
