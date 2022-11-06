use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

pub use client::MetaKvBackend;
use futures::Stream;
use futures_util::StreamExt;
pub use manager::{RemoteCatalogManager, RemoteCatalogProvider, RemoteSchemaProvider};

use crate::error::Error;

mod client;
mod manager;

#[derive(Debug, Clone)]
pub struct Kv(pub Vec<u8>, pub Vec<u8>);

pub type ValueIter<'a, E> = Pin<Box<dyn Stream<Item = Result<Kv, E>> + Send + 'a>>;

#[async_trait::async_trait]
pub trait KvBackend: Send + Sync {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b;

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error>;

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
}

pub type KvBackendRef = Arc<dyn KvBackend>;

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
