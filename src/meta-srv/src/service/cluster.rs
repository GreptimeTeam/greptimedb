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
    Error, RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse, ResponseHeader,
};
use common_telemetry::warn;
use snafu::ResultExt;
use tonic::{Request, Response};

use crate::error;
use crate::metasrv::MetaSrv;
use crate::service::GrpcResult;

#[async_trait::async_trait]
impl cluster_server::Cluster for MetaSrv {
    async fn batch_get(&self, req: Request<PbBatchGetRequest>) -> GrpcResult<PbBatchGetResponse> {
        if !self.is_leader() {
            let is_not_leader = ResponseHeader::failed(0, Error::is_not_leader());
            let resp = PbBatchGetResponse {
                header: Some(is_not_leader),
                ..Default::default()
            };

            warn!("The current meta is not leader, but a batch_get request have reached the meta. Detail: {:?}.", req);
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

            warn!("The current meta is not leader, but a range request have reached the meta. Detail: {:?}.", req);
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
}

impl MetaSrv {
    pub fn is_leader(&self) -> bool {
        self.election().map(|x| x.is_leader()).unwrap_or(false)
    }
}
