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
    LockRequest as PbLockRequest, LockResponse as PbLockResponse, UnlockRequest as PbUnlockRequest,
};

#[derive(Debug)]
pub struct LockRequest {
    pub name: Vec<u8>,
    pub expire_secs: i64,
}

impl From<LockRequest> for PbLockRequest {
    fn from(req: LockRequest) -> Self {
        Self {
            header: None,
            name: req.name,
            expire_secs: req.expire_secs,
        }
    }
}

#[derive(Debug)]
pub struct LockResponse {
    pub key: Vec<u8>,
}

impl From<PbLockResponse> for LockResponse {
    fn from(resp: PbLockResponse) -> Self {
        Self { key: resp.key }
    }
}

#[derive(Debug)]
pub struct UnlockRequest {
    pub key: Vec<u8>,
}

impl From<UnlockRequest> for PbUnlockRequest {
    fn from(req: UnlockRequest) -> Self {
        Self {
            header: None,
            key: req.key,
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{
        LockRequest as PbLockRequest, LockResponse as PbLockResponse,
        UnlockRequest as PbUnlockRequest,
    };

    use super::LockRequest;
    use crate::rpc::lock::{LockResponse, UnlockRequest};

    #[test]
    fn test_convert_lock_req() {
        let lock_req = LockRequest {
            name: "lock_1".as_bytes().to_vec(),
            expire_secs: 1,
        };
        let pb_lock_req: PbLockRequest = lock_req.into();

        let expected = PbLockRequest {
            header: None,
            name: "lock_1".as_bytes().to_vec(),
            expire_secs: 1,
        };

        assert_eq!(expected, pb_lock_req);
    }

    #[test]
    fn test_convert_unlock_req() {
        let unlock_req = UnlockRequest {
            key: "lock_1_12378123".as_bytes().to_vec(),
        };
        let pb_unlock_req: PbUnlockRequest = unlock_req.into();

        let expected = PbUnlockRequest {
            header: None,
            key: "lock_1_12378123".as_bytes().to_vec(),
        };

        assert_eq!(expected, pb_unlock_req);
    }

    #[test]
    fn test_convert_lock_response() {
        let pb_lock_resp = PbLockResponse {
            header: None,
            key: "lock_1_12378123".as_bytes().to_vec(),
        };

        let lock_resp: LockResponse = pb_lock_resp.into();

        let expected_key = "lock_1_12378123".as_bytes().to_vec();

        assert_eq!(expected_key, lock_resp.key);
    }
}
