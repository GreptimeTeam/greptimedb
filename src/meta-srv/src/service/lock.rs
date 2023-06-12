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

use api::v1::meta::{lock_server, LockRequest, LockResponse, UnlockRequest, UnlockResponse};
use tonic::{Request, Response};

use super::GrpcResult;
use crate::lock::Opts;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl lock_server::Lock for MetaSrv {
    async fn lock(&self, request: Request<LockRequest>) -> GrpcResult<LockResponse> {
        let LockRequest {
            name, expire_secs, ..
        } = request.into_inner();
        let expire_secs = Some(expire_secs as u64);

        let key = self.lock().lock(name, Opts { expire_secs }).await?;

        let resp = LockResponse {
            key,
            ..Default::default()
        };

        Ok(Response::new(resp))
    }

    async fn unlock(&self, request: Request<UnlockRequest>) -> GrpcResult<UnlockResponse> {
        let UnlockRequest { key, .. } = request.into_inner();

        let _ = self.lock().unlock(key).await?;

        let resp = UnlockResponse {
            ..Default::default()
        };

        Ok(Response::new(resp))
    }
}
