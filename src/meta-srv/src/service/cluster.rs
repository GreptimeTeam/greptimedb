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
    cluster_server, Error, GetKvRequest, GetKvResponse, RangeRequest, RangeResponse, ResponseHeader,
};
use tonic::{Request, Response};

use super::store::ext::KvStoreExt;
use super::GrpcResult;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl cluster_server::Cluster for MetaSrv {
    async fn batch_get(&self, req: Request<GetKvRequest>) -> GrpcResult<GetKvResponse> {
        if !self.is_leader() {
            let is_not_leader = ResponseHeader::failed(0, Error::is_not_leader());
            let resp = GetKvResponse {
                header: Some(is_not_leader),
                ..Default::default()
            };
            return Ok(Response::new(resp));
        }

        let req = req.into_inner();
        let kvs = self.in_memory().batch_get(req.keys).await?;
        let success = ResponseHeader::success(0);

        let get_resp = GetKvResponse {
            kvs,
            header: Some(success),
        };

        Ok(Response::new(get_resp))
    }

    async fn range(&self, req: Request<RangeRequest>) -> GrpcResult<RangeResponse> {
        if !self.is_leader() {
            let is_not_leader = ResponseHeader::failed(0, Error::is_not_leader());
            let resp = RangeResponse {
                header: Some(is_not_leader),
                ..Default::default()
            };
            return Ok(Response::new(resp));
        }

        let req = req.into_inner();
        let res = self.in_memory().range(req).await?;

        Ok(Response::new(res))
    }
}

impl MetaSrv {
    pub fn is_leader(&self) -> bool {
        if let Some(election) = self.election() {
            if election.is_leader() {
                return true;
            }
        }
        false
    }
}
