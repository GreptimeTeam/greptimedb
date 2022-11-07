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
}

pub type KvBackendRef = Arc<dyn KvBackend>;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_stream::stream;
    use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
    use common_telemetry::tracing::info;
    use common_telemetry::warn;
    use meta_client::client::MetaClientBuilder;

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

    async fn cas(backend: Arc<MetaKvBackend>, key: &[u8]) {
        loop {
            let (prev, prev_bytes) = match backend.get(key).await.unwrap() {
                None => (0, vec![]),
                Some(kv) => (
                    String::from_utf8_lossy(&kv.1).parse().unwrap(),
                    kv.1.clone(),
                ),
            };

            match backend
                .compare_and_set(
                    key,
                    prev_bytes.as_slice(),
                    (prev + 1).to_string().as_bytes(),
                )
                .await
                .unwrap()
            {
                Ok(_) => {
                    info!("CAS finished: {}", prev + 1);
                    break;
                }
                Err(e) => {
                    let cur = match e {
                        None => "none".to_string(),
                        Some(v) => String::from_utf8_lossy(&v).to_string(),
                    };
                    warn!("Failed to updated value: {}", cur)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cas() {
        common_telemetry::init_default_ut_logging();
        let id = (1000u64, 2000u64);
        let config = ChannelConfig::new()
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true);
        let channel_manager = ChannelManager::with_config(config);
        let mut meta_client = MetaClientBuilder::new(id.0, id.1)
            .enable_heartbeat()
            .enable_router()
            .enable_store()
            .channel_manager(channel_manager)
            .build();
        meta_client
            .start(&["proxy.huanglei.rocks:22009"])
            .await
            .unwrap();
        // required only when the heartbeat_client is enabled
        meta_client.ask_leader().await.unwrap();

        info!("ask leader...");
        let backend = Arc::new(MetaKvBackend {
            client: meta_client,
        });

        let key = "__test_cas".as_bytes();
        let thread_num = 100;
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        for _ in 0..thread_num {
            let backend = backend.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                cas(backend.clone(), key).await;
                tx.send(()).await.unwrap();
            });
        }

        let mut finished = 0;
        while let Some(_) = rx.recv().await {
            finished += 1;
            info!("Finished thread: {}", finished);
            if finished == thread_num {
                break;
            }
        }

        let res = String::from_utf8_lossy(&backend.get(key).await.unwrap().unwrap().1).to_string();
        backend.delete(key).await.unwrap();
        assert_eq!(thread_num.to_string(), res);
    }
}
