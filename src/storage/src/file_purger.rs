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

struct FilePurgeRequest {
    file_path: String,
}

impl Request for FilePurgeRequest {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.file_path.clone()
    }
}

struct FilePurgeHandler {
    object_store: ObjectStore,
}

impl Handler for FilePurgeHandler {
    type Request = FilePurgeRequest;

    async fn handle_request(
        &self,
        req: Self::Request,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> crate::error::Result<()> {
        let object = self.object_store.object(&req.file_path);
        object.delete().await?;
        token.try_release();
        finish_notifier.notify_one();
        Ok(())
    }
}

pub type FilePurgerRef = Arc<LocalScheduler<FilePurgeRequest, String>>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use object_store::backend::fs::Builder;
    use store_api::storage::OpType;
    use tempdir::TempDir;

    use super::*;
    use crate::memtable::tests::schema_for_test;
    use crate::memtable::{DefaultMemtableBuilder, IterContext, MemtableBuilder};
    use crate::sst::parquet::ParquetWriter;
    use crate::sst::{Source, WriteOptions};

    #[tokio::test]
    async fn test_file_purger() {
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        let rows_total = 100;
        let mut keys_vec = Vec::with_capacity(rows_total);
        let mut values_vec = Vec::with_capacity(rows_total);

        for i in 0..rows_total {
            keys_vec.push((i as i64, i as u64));
            values_vec.push((Some(i as u64), Some(i as u64)));
        }

        memtable_tests::write_kvs(&*memtable, 10, OpType::Put, &keys_vec, &values_vec);

        let dir = TempDir::new("write_parquet").unwrap();
        let path = dir.path().to_str().unwrap();
        let backend = Builder::default().root(path).build().unwrap();
        let object_store = ObjectStore::new(backend);
        let sst_file_name = "test-read-large.parquet";
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, Source::Iter(iter), object_store.clone());
        writer.write_sst(&WriteOptions {}).await.unwrap();

        let request = FilePurgeRequest {
            file_path: sst_file_name,
        };
        FilePurgeHandler {
            object_store: object_store.clone(),
        }
    }
}
