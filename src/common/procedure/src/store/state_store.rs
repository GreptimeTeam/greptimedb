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

use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use object_store::{ObjectMode, ObjectStore};
use snafu::ResultExt;

use crate::error::{DeleteStateSnafu, Error, PutStateSnafu, Result};

/// Key value from state store.
type KeyValue = (String, Vec<u8>);

/// Stream that yields [KeyValue].
type KeyValueStream = Pin<Box<dyn Stream<Item = Result<KeyValue>> + Send>>;

/// Storage layer for persisting procedure's state.
#[async_trait]
pub(crate) trait StateStore: Send + Sync {
    /// Puts `key` and `value` into the store.
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()>;

    /// Returns the key-value pairs under `path` in top down way.
    ///
    /// # Note
    /// - There is no guarantee about the order of the keys in the stream.
    /// - The `path` must ends with `/`.
    async fn walk_top_down(&self, path: &str) -> Result<KeyValueStream>;

    /// Deletes key-value pairs by `keys`.
    async fn delete(&self, keys: &[String]) -> Result<()>;
}

/// Reference counted pointer to [StateStore].
pub(crate) type StateStoreRef = Arc<dyn StateStore>;

/// [StateStore] based on [ObjectStore].
#[derive(Debug)]
pub(crate) struct ObjectStateStore {
    store: ObjectStore,
}

impl ObjectStateStore {
    /// Returns a new [ObjectStateStore] with specific `store`.
    pub(crate) fn new(store: ObjectStore) -> ObjectStateStore {
        ObjectStateStore { store }
    }
}

#[async_trait]
impl StateStore for ObjectStateStore {
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let object = self.store.object(key);
        object.write(value).await.context(PutStateSnafu { key })
    }

    async fn walk_top_down(&self, path: &str) -> Result<KeyValueStream> {
        let path_string = path.to_string();

        let lister = self
            .store
            .object(path)
            .scan()
            .await
            .map_err(|e| Error::ListState {
                path: path_string.clone(),
                source: e,
            })?;

        let stream = lister
            .try_filter_map(|entry| async move {
                let key = entry.path();
                let key_value = match entry.mode().await? {
                    ObjectMode::FILE => {
                        let value = entry.read().await?;

                        Some((key.to_string(), value))
                    }
                    ObjectMode::DIR | ObjectMode::Unknown => None,
                };

                Ok(key_value)
            })
            .map_err(move |e| Error::ListState {
                path: path_string.clone(),
                source: e,
            });

        Ok(Box::pin(stream))
    }

    async fn delete(&self, keys: &[String]) -> Result<()> {
        for key in keys {
            let object = self.store.object(key);
            object.delete().await.context(DeleteStateSnafu { key })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use object_store::services::Fs as Builder;
    use object_store::ObjectStoreBuilder;

    use super::*;

    #[tokio::test]
    async fn test_object_state_store() {
        let dir = tempfile::Builder::new()
            .prefix("state_store")
            .tempdir()
            .unwrap();
        let store_dir = dir.path().to_str().unwrap();
        let accessor = Builder::default().root(store_dir).build().unwrap();
        let object_store = ObjectStore::new(accessor).finish();
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
            .delete(&["a/2".to_string(), "b/1".to_string()])
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
}
