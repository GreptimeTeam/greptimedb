use std::pin::Pin;

use common_error::ext::ErrorExt;
use futures::Stream;
use futures_util::StreamExt;

mod client;
mod consts;
mod helper;
mod manager;

#[derive(Debug, Clone)]
pub struct Kv(Vec<u8>, Vec<u8>);

pub type ValueIter<E> = Pin<Box<dyn Stream<Item = Result<Kv, E>> + Send + Sync>>;

#[async_trait::async_trait]
pub trait KvBackend {
    type Error: ErrorExt;

    fn range(&self, key: &[u8]) -> ValueIter<Self::Error>;

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Self::Error>;

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Self::Error>;

    /// Default get is implemented based on `range` method.
    async fn get(&self, key: &[u8]) -> Result<Option<Kv>, Self::Error> {
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

#[cfg(test)]
mod tests {
    use async_stream::stream;

    use super::*;

    struct MockKvBackend {}

    #[async_trait::async_trait]
    impl KvBackend for MockKvBackend {
        type Error = crate::error::Error;

        fn range(&self, _key: &[u8]) -> ValueIter<Self::Error> {
            Box::pin(stream!({
                for i in 0..3 {
                    yield Ok(Kv(
                        i.to_string().as_bytes().to_vec(),
                        i.to_string().as_bytes().to_vec(),
                    ))
                }
            }))
        }

        async fn set(&self, _key: &[u8], _val: &[u8]) -> Result<(), Self::Error> {
            unimplemented!()
        }

        async fn delete_range(&self, _key: &[u8], _end: &[u8]) -> Result<(), Self::Error> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_get() {
        let backend = MockKvBackend {};

        assert_eq!(
            0.to_string().as_bytes(),
            backend
                .get(0.to_string().as_bytes())
                .await
                .unwrap()
                .unwrap()
                .0
        );

        assert_eq!(
            1.to_string().as_bytes(),
            backend
                .get(1.to_string().as_bytes())
                .await
                .unwrap()
                .unwrap()
                .0
        );

        assert_eq!(
            2.to_string().as_bytes(),
            backend
                .get(2.to_string().as_bytes())
                .await
                .unwrap()
                .unwrap()
                .0
        );

        assert!(backend
            .get(3.to_string().as_bytes())
            .await
            .unwrap()
            .is_none());
    }
}
