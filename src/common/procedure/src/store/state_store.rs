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

use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use futures::{Stream, StreamExt};
use object_store::{EntryMode, ObjectStore};
use snafu::ResultExt;

use crate::error::{DeleteStateSnafu, ListStateSnafu, PutStateSnafu, Result};

/// Key value from state store.
pub type KeyValue = (String, Vec<u8>);

/// Stream that yields [KeyValue].
pub type KeyValueStream = Pin<Box<dyn Stream<Item = Result<KeyValue>> + Send>>;

/// Storage layer for persisting procedure's state.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Puts `key` and `value` into the store.
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()>;

    /// Returns the key-value pairs under `path` in top down way.
    ///
    /// # Note
    /// - There is no guarantee about the order of the keys in the stream.
    /// - The `path` must ends with `/`.
    async fn walk_top_down(&self, path: &str) -> Result<KeyValueStream>;

    /// Deletes key-value pairs by `keys`.
    async fn batch_delete(&self, keys: &[String]) -> Result<()>;

    /// Deletes one key-value pair by `key`. Return `Ok` if the key
    /// does not exist.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// Reference counted pointer to [StateStore].
pub(crate) type StateStoreRef = Arc<dyn StateStore>;

/// [StateStore] based on [ObjectStore].
#[derive(Debug)]
pub struct ObjectStateStore {
    store: ObjectStore,
}

impl ObjectStateStore {
    /// Returns a new [ObjectStateStore] with specific `store`.
    pub fn new(store: ObjectStore) -> ObjectStateStore {
        ObjectStateStore { store }
    }
}

#[async_trait]
impl StateStore for ObjectStateStore {
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.store
            .write(key, value)
            .await
            .map_err(|e| {
                BoxedError::new(PlainError::new(
                    e.to_string(),
                    StatusCode::StorageUnavailable,
                ))
            })
            .context(PutStateSnafu { key })
    }

    async fn walk_top_down(&self, path: &str) -> Result<KeyValueStream> {
        let mut lister = self
            .store
            .lister_with(path)
            .recursive(true)
            .await
            .map_err(|e| {
                BoxedError::new(PlainError::new(
                    e.to_string(),
                    StatusCode::StorageUnavailable,
                ))
            })
            .context(ListStateSnafu { path })?;

        let store = self.store.clone();

        let path_string = path.to_string();
        let stream = try_stream!({
            while let Some(res) = lister.next().await {
                let entry = res
                    .map_err(|e| {
                        BoxedError::new(PlainError::new(
                            e.to_string(),
                            StatusCode::StorageUnavailable,
                        ))
                    })
                    .context(ListStateSnafu { path: &path_string })?;
                let key = entry.path();

                if let EntryMode::FILE = entry.metadata().mode() {
                    let value = store
                        .read(key)
                        .await
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                e.to_string(),
                                StatusCode::StorageUnavailable,
                            ))
                        })
                        .context(ListStateSnafu { path: key })?;
                    yield (key.to_string(), value);
                }
            }
        });

        Ok(Box::pin(stream))
    }

    async fn batch_delete(&self, keys: &[String]) -> Result<()> {
        self.store
            .remove(keys.to_vec())
            .await
            .with_context(|_| DeleteStateSnafu {
                key: format!("{:?}", keys),
            })?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.store
            .delete(key)
            .await
            .with_context(|_| DeleteStateSnafu { key })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use futures_util::TryStreamExt;
    use object_store::services::Fs as Builder;

    use super::*;

    #[tokio::test]
    async fn test_object_state_store() {
        let dir = create_temp_dir("state_store");
        let store_dir = dir.path().to_str().unwrap();
        let mut builder = Builder::default();
        let _ = builder.root(store_dir);

        let object_store = ObjectStore::new(builder).unwrap().finish();
        let state_store = ObjectStateStore::new(object_store);

        let data: Vec<_> = state_store
            .walk_top_down("/")
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(data.is_empty());

        state_store.put("a/1", b"v1".to_vec()).await.unwrap();
        state_store.put("a/2", b"v2".to_vec()).await.unwrap();
        state_store.put("b/1", b"v3".to_vec()).await.unwrap();

        let mut data: Vec<_> = state_store
            .walk_top_down("/")
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            vec![
                ("a/1".to_string(), b"v1".to_vec()),
                ("a/2".to_string(), b"v2".to_vec()),
                ("b/1".to_string(), b"v3".to_vec())
            ],
            data
        );

        let mut data: Vec<_> = state_store
            .walk_top_down("a/")
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            vec![
                ("a/1".to_string(), b"v1".to_vec()),
                ("a/2".to_string(), b"v2".to_vec()),
            ],
            data
        );

        state_store
            .batch_delete(&["a/2".to_string(), "b/1".to_string()])
            .await
            .unwrap();
        let mut data: Vec<_> = state_store
            .walk_top_down("a/")
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(vec![("a/1".to_string(), b"v1".to_vec()),], data);
    }

    #[tokio::test]
    async fn test_object_state_store_delete() {
        let dir = create_temp_dir("state_store_list");
        let store_dir = dir.path().to_str().unwrap();
        let mut builder = Builder::default();
        let _ = builder.root(store_dir);

        let object_store = ObjectStore::new(builder).unwrap().finish();
        let state_store = ObjectStateStore::new(object_store);

        state_store.put("a/1", b"v1".to_vec()).await.unwrap();
        state_store.put("a/2", b"v2".to_vec()).await.unwrap();
        state_store.put("b/1", b"v3".to_vec()).await.unwrap();

        state_store.delete("b/1").await.unwrap();

        let mut data: Vec<_> = state_store
            .walk_top_down("a/")
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            vec![
                ("a/1".to_string(), b"v1".to_vec()),
                ("a/2".to_string(), b"v2".to_vec()),
            ],
            data
        );

        // Delete returns Ok even the key doesn't exist.
        state_store.delete("b/1").await.unwrap();
    }
}
