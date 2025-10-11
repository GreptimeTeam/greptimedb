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

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_meta::key::tombstone::TombstoneManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::RangeRequest;

use crate::Tool;
use crate::common::StoreConfig;
use crate::metadata::control::del::CLI_TOMBSTONE_PREFIX;

/// Delete key-value pairs logically from the metadata store.
#[derive(Debug, Default, Parser)]
pub struct DelKeyCommand {
    /// The key to delete from the metadata store.
    key: String,

    /// Delete key-value pairs with the given prefix.
    #[clap(long)]
    prefix: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

impl DelKeyCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kv_backend = self.store.build().await?;
        Ok(Box::new(DelKeyTool {
            key: self.key.clone(),
            prefix: self.prefix,
            key_deleter: KeyDeleter::new(kv_backend),
        }))
    }
}

struct KeyDeleter {
    kv_backend: KvBackendRef,
    tombstone_manager: TombstoneManager,
}

impl KeyDeleter {
    fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            kv_backend: kv_backend.clone(),
            tombstone_manager: TombstoneManager::new_with_prefix(kv_backend, CLI_TOMBSTONE_PREFIX),
        }
    }

    async fn delete(&self, key: &str, prefix: bool) -> Result<usize, BoxedError> {
        let mut req = RangeRequest::default().with_keys_only();
        if prefix {
            req = req.with_prefix(key.as_bytes());
        } else {
            req = req.with_key(key.as_bytes());
        }
        let resp = self.kv_backend.range(req).await.map_err(BoxedError::new)?;
        let keys = resp.kvs.iter().map(|kv| kv.key.clone()).collect::<Vec<_>>();
        self.tombstone_manager
            .create(keys)
            .await
            .map_err(BoxedError::new)
    }
}

struct DelKeyTool {
    key: String,
    prefix: bool,
    key_deleter: KeyDeleter,
}

#[async_trait]
impl Tool for DelKeyTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let deleted = self.key_deleter.delete(&self.key, self.prefix).await?;
        // Print the number of deleted keys.
        println!("{}", deleted);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::kv_backend::chroot::ChrootKvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::{KvBackend, KvBackendRef};
    use common_meta::rpc::store::RangeRequest;

    use crate::metadata::control::del::CLI_TOMBSTONE_PREFIX;
    use crate::metadata::control::del::key::KeyDeleter;
    use crate::metadata::control::test_utils::put_key;

    #[tokio::test]
    async fn test_delete_keys() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let key_deleter = KeyDeleter::new(kv_backend.clone());
        put_key(&kv_backend, "foo", "bar").await;
        put_key(&kv_backend, "foo/bar", "baz").await;
        put_key(&kv_backend, "foo/baz", "qux").await;
        let deleted = key_deleter.delete("foo", true).await.unwrap();
        assert_eq!(deleted, 3);
        let deleted = key_deleter.delete("foo/bar", false).await.unwrap();
        assert_eq!(deleted, 0);

        let chroot = ChrootKvBackend::new(CLI_TOMBSTONE_PREFIX.as_bytes().to_vec(), kv_backend);
        let req = RangeRequest::default().with_prefix(b"foo");
        let resp = chroot.range(req).await.unwrap();
        assert_eq!(resp.kvs.len(), 3);
        assert_eq!(resp.kvs[0].key, b"foo");
        assert_eq!(resp.kvs[0].value, b"bar");
        assert_eq!(resp.kvs[1].key, b"foo/bar");
        assert_eq!(resp.kvs[1].value, b"baz");
        assert_eq!(resp.kvs[2].key, b"foo/baz");
        assert_eq!(resp.kvs[2].value, b"qux");
    }
}
