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
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

pub use client::{CachedMetaKvBackend, MetaKvBackend};
use futures::Stream;
use futures_util::StreamExt;
pub use manager::RemoteCatalogManager;

use crate::error::Error;

mod client;
mod manager;

#[cfg(feature = "testing")]
pub mod mock;
pub mod region_alive_keeper;

#[derive(Debug, Clone)]
pub struct Kv(pub Vec<u8>, pub Vec<u8>);

pub type ValueIter<'a, E> = Pin<Box<dyn Stream<Item = Result<Kv, E>> + Send + 'a>>;

#[async_trait::async_trait]
pub trait KvBackend: Send + Sync {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b;

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error>;

    /// Compare and set value of key. `expect` is the expected value, if backend's current value associated
    /// with key is the same as `expect`, the value will be updated to `val`.
    ///
    /// - If the compare-and-set operation successfully updated value, this method will return an `Ok(Ok())`
    /// - If associated value is not the same as `expect`, no value will be updated and an `Ok(Err(Vec<u8>))`
    /// will be returned, the `Err(Vec<u8>)` indicates the current associated value of key.
    /// - If any error happens during operation, an `Err(Error)` will be returned.
    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<Result<(), Option<Vec<u8>>>, Error>;

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error>;

    async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.delete_range(key, &[]).await
    }

    /// Default get is implemented based on `range` method.
    async fn get(&self, key: &[u8]) -> Result<Option<Kv>, Error> {
        let mut iter = self.range(key);
        while let Some(r) = iter.next().await {
            let kv = r?;
            if kv.0 == key {
                return Ok(Some(kv));
            }
        }
        return Ok(None);
    }

    /// MoveValue atomically renames the key to the given updated key.
    async fn move_value(&self, from_key: &[u8], to_key: &[u8]) -> Result<(), Error>;

    fn as_any(&self) -> &dyn Any;
}

pub type KvBackendRef = Arc<dyn KvBackend>;

#[async_trait::async_trait]
pub trait KvCacheInvalidator: Send + Sync {
    async fn invalidate_key(&self, key: &[u8]);
}

pub type KvCacheInvalidatorRef = Arc<dyn KvCacheInvalidator>;

#[cfg(test)]
mod tests {
    use async_stream::stream;

    use super::*;

    struct MockKvBackend {}

    #[async_trait::async_trait]
    impl KvBackend for MockKvBackend {
        fn range<'a, 'b>(&'a self, _key: &[u8]) -> ValueIter<'b, Error>
        where
            'a: 'b,
        {
            Box::pin(stream!({
                for i in 0..3 {
                    yield Ok(Kv(
                        i.to_string().as_bytes().to_vec(),
                        i.to_string().as_bytes().to_vec(),
                    ))
                }
            }))
        }

        async fn set(&self, _key: &[u8], _val: &[u8]) -> Result<(), Error> {
            unimplemented!()
        }

        async fn compare_and_set(
            &self,
            _key: &[u8],
            _expect: &[u8],
            _val: &[u8],
        ) -> Result<Result<(), Option<Vec<u8>>>, Error> {
            unimplemented!()
        }

        async fn delete_range(&self, _key: &[u8], _end: &[u8]) -> Result<(), Error> {
            unimplemented!()
        }

        async fn move_value(&self, _from_key: &[u8], _to_key: &[u8]) -> Result<(), Error> {
            unimplemented!()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_get() {
        let backend = MockKvBackend {};

        let result = backend.get(0.to_string().as_bytes()).await;
        assert_eq!(0.to_string().as_bytes(), result.unwrap().unwrap().0);

        let result = backend.get(1.to_string().as_bytes()).await;
        assert_eq!(1.to_string().as_bytes(), result.unwrap().unwrap().0);

        let result = backend.get(2.to_string().as_bytes()).await;
        assert_eq!(2.to_string().as_bytes(), result.unwrap().unwrap().0);

        let result = backend.get(3.to_string().as_bytes()).await;
        assert!(result.unwrap().is_none());
    }
}
