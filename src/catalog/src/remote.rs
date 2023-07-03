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

pub use client::{CachedMetaKvBackend, MetaKvBackend};
pub use manager::RemoteCatalogManager;

mod client;
mod manager;

#[cfg(feature = "testing")]
pub mod mock;
pub mod region_alive_keeper;

#[async_trait::async_trait]
pub trait KvCacheInvalidator: Send + Sync {
    async fn invalidate_key(&self, key: &[u8]);
}

pub type KvCacheInvalidatorRef = Arc<dyn KvCacheInvalidator>;

#[cfg(test)]
mod tests {
    use std::any::Any;

    use async_stream::stream;
    use common_meta::kv_backend::{Kv, KvBackend, ValueIter};

    use crate::error::Error;

    struct MockKvBackend {}

    #[async_trait::async_trait]
    impl KvBackend for MockKvBackend {
        type Error = Error;

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
