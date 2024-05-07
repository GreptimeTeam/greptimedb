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
    cluster_server, BatchGetRequest as PbBatchGetRequest, BatchGetResponse as PbBatchGetResponse,
    Error, MetasrvNodeInfo, MetasrvPeersRequest, MetasrvPeersResponse,
    RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse, ResponseHeader,
};
use common_telemetry::warn;
use snafu::ResultExt;
use tonic::{Request, Response};

use crate::metasrv::Metasrv;
use crate::service::GrpcResult;
use crate::{error, metasrv};

#[async_trait::async_trait]
impl cluster_server::Cluster for Metasrv {
    async fn batch_get(&self, req: Request<PbBatchGetRequest>) -> GrpcResult<PbBatchGetResponse> {
        if !self.is_leader() {
            let is_not_leader = ResponseHeader::failed(0, Error::is_not_leader());
            let resp = PbBatchGetResponse {
                header: Some(is_not_leader),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `batch_get` request have reached the meta. Detail: {:?}.", req);
            return Ok(Response::new(resp));
        }

        let req = req.into_inner().into();
        let resp = self
            .in_memory()
            .batch_get(req)
            .await
            .context(error::KvBackendSnafu)?;

        let resp = resp.to_proto_resp(ResponseHeader::success(0));
        Ok(Response::new(resp))
    }

    async fn range(&self, req: Request<PbRangeRequest>) -> GrpcResult<PbRangeResponse> {
        if !self.is_leader() {
            let is_not_leader = ResponseHeader::failed(0, Error::is_not_leader());
            let resp = PbRangeResponse {
                header: Some(is_not_leader),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `range` request have reached the meta. Detail: {:?}.", req);
            return Ok(Response::new(resp));
        }

        let req = req.into_inner().into();
        let res = self
            .in_memory()
            .range(req)
            .await
            .context(error::KvBackendSnafu)?;

        let resp = res.to_proto_resp(ResponseHeader::success(0));
        Ok(Response::new(resp))
    }

    async fn metasrv_peers(
        &self,
        req: Request<MetasrvPeersRequest>,
    ) -> GrpcResult<MetasrvPeersResponse> {
        if !self.is_leader() {
            let is_not_leader = ResponseHeader::failed(0, Error::is_not_leader());
            let resp = MetasrvPeersResponse {
                header: Some(is_not_leader),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a `metasrv_peers` request have reached the meta. Detail: {:?}.", req);
            return Ok(Response::new(resp));
        }

        fn make_node_info(addr: String) -> Option<MetasrvNodeInfo> {
            let build_info = common_version::build_info();
            Some(
                metasrv::MetasrvNodeInfo {
                    addr,
                    version: build_info.version.to_string(),
                    git_commit: build_info.commit_short.to_string(),
                }
                .into(),
            )
        }

        let (leader, followers) = match self.election() {
            Some(election) => {
                let leader = election.leader().await?;
                let nodes = election.all_candidates().await?;
                let followers = nodes
                    .into_iter()
                    .filter(|node_info| node_info.addr != leader.0)
                    .map(api::v1::meta::MetasrvNodeInfo::from)
                    .collect();
                (make_node_info(leader.0.clone()), followers)
            }
            None => (make_node_info(self.options().server_addr.clone()), vec![]),
        };

        let resp = MetasrvPeersResponse {
            header: Some(ResponseHeader::success(0)),
            leader,
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
}
