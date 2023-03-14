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

use std::sync::Arc;

use api::v1::meta::{
    BatchGetRequest, BatchGetResponse, BatchPutRequest, BatchPutResponse, CompareAndPutRequest,
    CompareAndPutResponse, DeleteRangeRequest, DeleteRangeResponse, MoveValueRequest,
    MoveValueResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};

use crate::error::Result;

pub type KvStoreRef = Arc<dyn KvStore>;
pub type ResettableKvStoreRef = Arc<dyn ResettableKvStore>;

#[async_trait::async_trait]
pub trait KvStore: Send + Sync {
    async fn range(&self, req: RangeRequest) -> Result<RangeResponse>;

    async fn put(&self, req: PutRequest) -> Result<PutResponse>;

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse>;

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse>;

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse>;

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse>;

    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse>;
}

pub trait ResettableKvStore: KvStore {
    fn reset(&self);
}
