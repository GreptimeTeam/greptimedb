// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::Result;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, PutRequest};

pub(crate) async fn put_conditionally(
    kv_backend: &KvBackendRef,
    key: Vec<u8>,
    value: Vec<u8>,
    if_not_exists: bool,
) -> Result<bool> {
    let success = if if_not_exists {
        let req = CompareAndPutRequest::new()
            .with_key(key)
            .with_expect(vec![])
            .with_value(value);
        let res = kv_backend.compare_and_put(req).await?;
        res.success
    } else {
        let req = PutRequest::new().with_key(key).with_value(value);
        kv_backend.put(req).await?;
        true
    };

    Ok(success)
}
