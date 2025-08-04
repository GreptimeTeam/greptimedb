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

#[macro_export]
macro_rules! check_leader {
    ($self:expr, $request:expr, $resp_ty:ty, $msg:expr) => {
        use common_telemetry::warn;
        use api::v1::meta::{ResponseHeader, Error};
        use tonic::Response;

        if !$self.is_leader() {
            warn!(
                "The current metasrv is not the leader, but a {} request has reached the meta. Detail: {:?}.",
                $msg, $request
            );
            let mut resp: $resp_ty = Default::default();
            resp.header = Some(ResponseHeader::failed(
                Error::is_not_leader(),
            ));
            return Ok(Response::new(resp));
        }
    };
}
