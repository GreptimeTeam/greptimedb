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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::error::ExternalSnafu;
use common_meta::kv_backend::txn::{Txn, TxnResponse};
use common_meta::kv_backend::{KvBackend, KvBackendRef, TxnService};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use snafu::ResultExt;

use crate::error::Error;

pub type KvStoreRef = Arc<dyn KvBackend<Error = Error>>;
pub type ResettableKvStoreRef = Arc<dyn ResettableKvStore>;

pub trait ResettableKvStore: KvBackend<Error = Error> {
    fn reset(&self);
}

/// An adaptor to bridge [KvStoreRef] and [KvBackendRef].
pub struct KvBackendAdapter(KvStoreRef);

impl KvBackendAdapter {
    pub fn wrap(kv_store: KvStoreRef) -> KvBackendRef {
        Arc::new(Self(kv_store))
    }
}

#[async_trait]
impl TxnService for KvBackendAdapter {
    type Error = common_meta::error::Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
        self.0
            .txn(txn)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}

#[async_trait]
impl KvBackend for KvBackendAdapter {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn as_any(&self) -> &dyn Any {
        self.0.as_any()
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Self::Error> {
        self.0
            .range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error> {
        self.0
            .put(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
        self.0
            .batch_put(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        self.0
            .batch_get(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn compare_and_put(
        &self,
        req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error> {
        self.0
            .compare_and_put(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn delete_range(
        &self,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error> {
        self.0
            .delete_range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn batch_delete(
        &self,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error> {
        self.0
            .batch_delete(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}
