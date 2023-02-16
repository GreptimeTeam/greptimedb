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

use object_store::ObjectStore;
use tokio::sync::Notify;

use crate::scheduler::rate_limit::{BoxedRateLimitToken, RateLimitToken};
use crate::scheduler::{Handler, LocalScheduler, Request};
use crate::sst::AccessLayerRef;

pub struct FilePurgeRequest {
    sst_layer: AccessLayerRef,
    file_path: String,
}

impl Request for FilePurgeRequest {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.file_path.clone()
    }
}

struct FilePurgeHandler {}

#[async_trait::async_trait]
impl Handler for FilePurgeHandler {
    type Request = FilePurgeRequest;

    async fn handle_request(
        &self,
        req: Self::Request,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> crate::error::Result<()> {
        req.sst_layer.delete_sst(&req.file_path).await?;
        token.try_release();
        finish_notifier.notify_one();
        Ok(())
    }
}

pub type FilePurgerRef = Arc<LocalScheduler<FilePurgeRequest>>;

#[cfg(test)]
mod tests {
    use object_store::backend::fs::Builder;
    use store_api::storage::OpType;
    use tempdir::TempDir;

    use super::*;
    use crate::memtable::tests::{schema_for_test, write_kvs};
    use crate::memtable::{DefaultMemtableBuilder, IterContext, MemtableBuilder};
    use crate::sst::parquet::ParquetWriter;
    use crate::sst::{AccessLayer, FsAccessLayer, Source, WriteOptions};

    struct MockRateLimitToken;

    impl RateLimitToken for MockRateLimitToken {
        fn try_release(&self) {}
    }

    #[tokio::test]
    async fn test_file_purger() {
        let schema = schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        write_kvs(
            &*memtable,
            10,
            OpType::Put,
            &[(1, 1), (2, 2)],
            &[(Some(1), Some(1)), (Some(2), Some(2))],
        );

        let dir = TempDir::new("write_parquet").unwrap();
        let object_store = ObjectStore::new(
            Builder::default()
                .root(dir.path().to_str().unwrap())
                .build()
                .unwrap(),
        );

        let sst_file_name = "test-read-large.parquet";
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let layer = Arc::new(FsAccessLayer::new("sst", object_store.clone()));
        let sst_info = layer
            .write_sst(sst_file_name, Source::Iter(iter), &WriteOptions {})
            .await
            .unwrap();

        let request = FilePurgeRequest {
            sst_layer: layer.clone(),
            file_path: sst_file_name.to_string(),
        };

        let handler = FilePurgeHandler {};

        let notify = Arc::new(Notify::new());
        handler
            .handle_request(request, Box::new(MockRateLimitToken {}), notify)
            .await
            .unwrap();
    }
}
