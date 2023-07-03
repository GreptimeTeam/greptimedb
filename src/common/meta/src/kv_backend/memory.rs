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

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::RwLock;

use async_stream::stream;
use async_trait::async_trait;
use serde::Serializer;

use crate::error::Error;
use crate::kv_backend::{Kv, KvBackend, ValueIter};

pub struct MemoryKvBackend {
    kvs: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Display for MemoryKvBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kvs = self.kvs.read().unwrap();
        for (k, v) in kvs.iter() {
            f.serialize_str(&String::from_utf8_lossy(k))?;
            f.serialize_str(" -> ")?;
            f.serialize_str(&String::from_utf8_lossy(v))?;
            f.serialize_str("\n")?;
        }
        Ok(())
    }
}

impl Default for MemoryKvBackend {
    fn default() -> Self {
        Self {
            kvs: RwLock::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl KvBackend for MemoryKvBackend {
    type Error = Error;

    fn range<'a, 'b>(&'a self, prefix: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        let kvs = self.kvs.read().unwrap();
        let kvs = kvs.clone();

        let prefix = prefix.to_vec();
        Box::pin(stream!({
            for (k, v) in kvs.range(prefix.clone()..) {
                if !k.starts_with(&prefix) {
                    break;
                }
                yield Ok(Kv(k.clone(), v.clone()));
            }
        }))
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let mut kvs = self.kvs.write().unwrap();
        let _ = kvs.insert(key.to_vec(), val.to_vec());
        Ok(())
    }

    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<Result<(), Option<Vec<u8>>>, Error> {
        let key = key.to_vec();
        let val = val.to_vec();

        let mut kvs = self.kvs.write().unwrap();
        let existed = kvs.entry(key);
        Ok(match existed {
            Entry::Vacant(e) => {
                if expect.is_empty() {
                    let _ = e.insert(val);
                    Ok(())
                } else {
                    Err(None)
                }
            }
            Entry::Occupied(mut existed) => {
                if existed.get() == expect {
                    let _ = existed.insert(val);
                    Ok(())
                } else {
                    Err(Some(existed.get().clone()))
                }
            }
        })
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error> {
        let mut kvs = self.kvs.write().unwrap();
        if end.is_empty() {
            let _ = kvs.remove(key);
        } else {
            let start = key.to_vec();
            let end = end.to_vec();
            let range = start..end;

            kvs.retain(|k, _| !range.contains(k));
        }
        Ok(())
    }

    async fn move_value(&self, from_key: &[u8], to_key: &[u8]) -> Result<(), Error> {
        let mut kvs = self.kvs.write().unwrap();
        if let Some(v) = kvs.remove(from_key) {
            let _ = kvs.insert(to_key.to_vec(), v);
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_memory_kv_backend() {
        let backend = MemoryKvBackend::default();

        for i in 1..10 {
            let key = format!("key{}", i);
            let val = format!("val{}", i);
            assert!(backend.set(key.as_bytes(), val.as_bytes()).await.is_ok());
        }

        let result = backend
            .compare_and_set(b"hello", b"what", b"world")
            .await
            .unwrap();
        assert!(result.unwrap_err().is_none());

        let result = backend
            .compare_and_set(b"hello", b"", b"world")
            .await
            .unwrap();
        assert!(result.is_ok());

        let result = backend
            .compare_and_set(b"hello", b"world", b"greptime")
            .await
            .unwrap();
        assert!(result.is_ok());

        let result = backend
            .compare_and_set(b"hello", b"world", b"what")
            .await
            .unwrap();
        assert_eq!(result.unwrap_err().unwrap(), b"greptime");

        assert!(backend.delete_range(b"key1", &[]).await.is_ok());
        assert!(backend.delete_range(b"key3", b"key9").await.is_ok());

        assert!(backend.move_value(b"key9", b"key10").await.is_ok());

        assert_eq!(
            backend.to_string(),
            r#"hello -> greptime
key10 -> val9
key2 -> val2
"#
        );

        let range = backend.range(b"key").try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], Kv(b"key10".to_vec(), b"val9".to_vec()));
        assert_eq!(range[1], Kv(b"key2".to_vec(), b"val2".to_vec()));
    }
}
