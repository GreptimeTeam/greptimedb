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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_telemetry::debug;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use snafu::ensure;

use crate::error::{self, Result};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{RangeRequest, RangeResponse};
use crate::rpc::KeyValue;
use crate::util::get_next_prefix_key;

pub type KeyValueDecoderFn<K, V> = dyn Fn(KeyValue) -> Result<(K, V)> + Send + Sync;

enum PaginationStreamState<K, V> {
    /// At the start of reading.
    Init,
    /// Decoding key value pairs.
    Decoding(SimpleKeyValueDecoder<K, V>),
    /// Retrieving data from backend.
    Reading(BoxFuture<'static, Result<(PaginationStreamFactory, Option<RangeResponse>)>>),
    /// Error
    Error,
}

/// The Range Request's default page size.
///
/// It dependents on upstream KvStore server side grpc message size limitation.
/// (e.g., etcd has default grpc message size limitation is 4MiB)
///
/// Generally, almost all metadata is smaller than is 2700 Byte.
/// Therefore, We can set the [DEFAULT_PAGE_SIZE] to 1536 statically.
///
/// TODO(weny): Considers updating the default page size dynamically.
pub const DEFAULT_PAGE_SIZE: usize = 1536;

struct PaginationStreamFactory {
    kv: KvBackendRef,
    /// key is the first key for the range, If range_end is not given, the
    /// request only looks up key.
    pub key: Vec<u8>,
    /// range_end is the upper bound on the requested range [key, range_end).
    /// If range_end is '\0', the range is all keys >= key.
    /// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    /// then the range request gets all keys prefixed with key.
    /// If both key and range_end are '\0', then the range request returns all
    /// keys.
    pub range_end: Vec<u8>,

    /// page_size is the pagination page size.
    page_size: usize,
    /// keys_only when set returns only the keys and not the values.
    pub keys_only: bool,

    /// It reduces the page size if the response size exceeds the limit.
    pub adaptive_page_size: usize,

    pub more: bool,
}

impl PaginationStreamFactory {
    pub fn new(
        kv: &KvBackendRef,
        key: Vec<u8>,
        range_end: Vec<u8>,
        page_size: usize,
        keys_only: bool,
        more: bool,
    ) -> Self {
        Self {
            kv: kv.clone(),
            key,
            range_end,
            page_size,
            keys_only,
            more,
            adaptive_page_size: if page_size == 0 {
                DEFAULT_ADAPTIVE_PAGE_SIZE
            } else {
                page_size
            },
        }
    }
}

const DEFAULT_ADAPTIVE_PAGE_SIZE: usize = 1024;

impl PaginationStreamFactory {
    fn try_reduce_adaptive_page_size(&mut self) -> Result<()> {
        self.adaptive_page_size /= 2;

        ensure!(
            self.adaptive_page_size != 0,
            error::UnexpectedSnafu {
                err_msg: "Exceeded maximum number of adaptive range retries"
            }
        );

        Ok(())
    }

    /// Decreases the `page size` if the response message size exceeds the limitation.
    /// TODO(weny): Considers to add an E2e test.
    #[async_recursion::async_recursion]
    async fn adaptive_range(&mut self, req: RangeRequest) -> Result<RangeResponse> {
        match self.kv.range(req.clone()).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                if err.is_exceeded_size_limit() {
                    self.try_reduce_adaptive_page_size()?;
                    debug!("Reset page_size to {}", self.adaptive_page_size);

                    self.adaptive_range(req.with_limit(self.adaptive_page_size as i64))
                        .await
                } else {
                    Err(err)
                }
            }
        }
    }

    pub async fn read_next(mut self) -> Result<(Self, Option<RangeResponse>)> {
        if self.more {
            let resp = self
                .adaptive_range(RangeRequest {
                    key: self.key.clone(),
                    range_end: self.range_end.clone(),
                    limit: self.adaptive_page_size as i64,
                    keys_only: self.keys_only,
                })
                .await?;

            let key = resp
                .kvs
                .last()
                .map(|kv| kv.key.clone())
                .unwrap_or_else(Vec::new);

            let next_key = get_next_prefix_key(&key);

            Ok((
                Self {
                    kv: self.kv,
                    key: next_key,
                    range_end: self.range_end,
                    page_size: self.page_size,
                    keys_only: self.keys_only,
                    more: resp.more,
                    adaptive_page_size: self.adaptive_page_size,
                },
                Some(resp),
            ))
        } else {
            Ok((self, None))
        }
    }
}

pub struct PaginationStream<K, V> {
    state: PaginationStreamState<K, V>,
    decoder_fn: Arc<KeyValueDecoderFn<K, V>>,
    factory: Option<PaginationStreamFactory>,
}

impl<K, V> PaginationStream<K, V> {
    pub fn new(
        kv: KvBackendRef,
        req: RangeRequest,
        page_size: usize,
        decoder_fn: Arc<KeyValueDecoderFn<K, V>>,
    ) -> Self {
        Self {
            state: PaginationStreamState::Init,
            decoder_fn,
            factory: Some(PaginationStreamFactory::new(
                &kv,
                req.key,
                req.range_end,
                page_size,
                req.keys_only,
                true,
            )),
        }
    }
}

struct SimpleKeyValueDecoder<K, V> {
    kv: VecDeque<KeyValue>,
    decoder: Arc<KeyValueDecoderFn<K, V>>,
}

impl<K, V> Iterator for SimpleKeyValueDecoder<K, V> {
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(kv) = self.kv.pop_front() {
            Some((self.decoder)(kv))
        } else {
            None
        }
    }
}

impl<K, V> Stream for PaginationStream<K, V> {
    type Item = Result<(K, V)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                PaginationStreamState::Decoding(decoder) => match decoder.next() {
                    Some(Ok(result)) => return Poll::Ready(Some(Ok(result))),
                    Some(Err(e)) => {
                        self.state = PaginationStreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                    None => self.state = PaginationStreamState::Init,
                },
                PaginationStreamState::Init => {
                    let factory = self.factory.take().expect("lost factory");
                    if !factory.more {
                        return Poll::Ready(None);
                    }
                    let fut = factory.read_next().boxed();
                    self.state = PaginationStreamState::Reading(fut);
                }
                PaginationStreamState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((factory, Some(resp))) => {
                        self.factory = Some(factory);
                        let decoder = SimpleKeyValueDecoder {
                            kv: resp.kvs.into(),
                            decoder: self.decoder_fn.clone(),
                        };
                        self.state = PaginationStreamState::Decoding(decoder);
                    }
                    Ok((factory, None)) => {
                        self.factory = Some(factory);
                        self.state = PaginationStreamState::Init;
                    }
                    Err(e) => {
                        self.state = PaginationStreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                PaginationStreamState::Error => return Poll::Ready(None), // Ends the stream as error happens.
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;
    use std::collections::BTreeMap;

    use futures::TryStreamExt;

    use super::*;
    use crate::error::{Error, Result};
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;
    use crate::rpc::store::PutRequest;

    fn decoder(kv: KeyValue) -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((kv.key.clone(), kv.value))
    }

    #[test]
    fn test_try_reduce_page_size() {
        let kv_backend = Arc::new(MemoryKvBackend::<Error>::new()) as _;

        let mut factory =
            PaginationStreamFactory::new(&kv_backend, vec![], vec![], 2, false, false);

        // new adaptive page size: 1
        factory.try_reduce_adaptive_page_size().unwrap();

        // new adaptive page size: 0
        assert_matches!(
            factory.try_reduce_adaptive_page_size().unwrap_err(),
            error::Error::Unexpected { .. }
        );

        let mut factory =
            PaginationStreamFactory::new(&kv_backend, vec![], vec![], 1024, false, false);

        factory.try_reduce_adaptive_page_size().unwrap();

        assert_eq!(factory.adaptive_page_size, 512);

        factory.try_reduce_adaptive_page_size().unwrap();

        assert_eq!(factory.adaptive_page_size, 256);

        let mut factory =
            PaginationStreamFactory::new(&kv_backend, vec![], vec![], 0, false, false);

        factory.try_reduce_adaptive_page_size().unwrap();

        assert_eq!(factory.adaptive_page_size, DEFAULT_ADAPTIVE_PAGE_SIZE / 2);
    }

    #[tokio::test]
    async fn test_range_empty() {
        let kv_backend = Arc::new(MemoryKvBackend::<Error>::new());

        let stream = PaginationStream::new(
            kv_backend.clone(),
            RangeRequest {
                key: b"a".to_vec(),
                ..Default::default()
            },
            DEFAULT_PAGE_SIZE,
            Arc::new(decoder),
        );
        let kv = stream.try_collect::<Vec<_>>().await.unwrap();

        assert!(kv.is_empty());
    }

    #[tokio::test]
    async fn test_range() {
        let kv_backend = Arc::new(MemoryKvBackend::<Error>::new());
        let total = 26;

        let mut expected = BTreeMap::<Vec<u8>, ()>::new();
        for i in 0..total {
            let key = vec![97 + i];

            assert!(kv_backend
                .put(PutRequest {
                    key: key.clone(),
                    value: key.clone(),
                    ..Default::default()
                })
                .await
                .is_ok());

            expected.insert(key, ());
        }

        let key = b"a".to_vec();
        let range_end = b"f".to_vec();

        let stream = PaginationStream::new(
            kv_backend.clone(),
            RangeRequest {
                key,
                range_end,
                ..Default::default()
            },
            2,
            Arc::new(decoder),
        );
        let kv = stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|kv| kv.0)
            .collect::<Vec<_>>();

        assert_eq!(vec![vec![97], vec![98], vec![99], vec![100], vec![101]], kv);
    }
}
