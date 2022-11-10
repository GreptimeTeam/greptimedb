use std::ops::Range;
use std::sync::Arc;

use api::v1::meta::CompareAndPutRequest;
use snafu::ensure;
use tokio::sync::Mutex;

use crate::error::{self, Result};
use crate::keys;
use crate::service::store::kv::KvStoreRef;

pub type SequenceRef = Arc<Sequence>;

pub struct Sequence {
    inner: Mutex<Inner>,
}

impl Sequence {
    pub fn new(name: impl AsRef<str>, step: u64, generator: KvStoreRef) -> Self {
        let name = format!("{}-{}", keys::SEQ_PREFIX, name.as_ref());
        Self {
            inner: Mutex::new(Inner {
                name,
                generator,
                next: 0,
                step,
                range: None,
                fouce_quit: 1024,
            }),
        }
    }

    pub async fn next(&self) -> Result<u64> {
        let mut inner = self.inner.lock().await;
        inner.next().await
    }
}

struct Inner {
    name: String,
    generator: KvStoreRef,
    next: u64,
    step: u64,
    range: Option<Range<u64>>,
    fouce_quit: usize,
}

impl Inner {
    pub async fn next(&mut self) -> Result<u64> {
        for _ in 0..self.fouce_quit {
            match &self.range {
                Some(range) => {
                    if range.contains(&self.next) {
                        let res = Ok(self.next);
                        self.next += 1;
                        return res;
                    }
                    self.range = None;
                }
                None => {
                    self.range = Some(self.next_range().await?);
                    self.next = self.range.as_ref().unwrap().start;
                }
            }
        }

        error::NextSequenceSnafu {
            err_msg: format!("{}.next()", &self.name),
        }
        .fail()
    }

    pub async fn next_range(&self) -> Result<Range<u64>> {
        let key = self.name.as_bytes();
        let mut start = self.next;
        for _ in 0..self.fouce_quit {
            let expect = if start == 0 {
                vec![]
            } else {
                u64::to_le_bytes(start).to_vec()
            };
            let value = u64::to_le_bytes(start + self.step);

            let req = CompareAndPutRequest {
                key: key.to_vec(),
                expect,
                value: value.to_vec(),
                ..Default::default()
            };

            let res = self.generator.compare_and_put(req).await?;

            if !res.success {
                if let Some(kv) = res.prev_kv {
                    let mid = std::mem::size_of::<u64>();
                    ensure!(
                        kv.value.len() == mid,
                        error::UnexceptedSequenceValueSnafu {
                            err_msg: format!("key={}, unexpected value={:?}", self.name, kv.value)
                        }
                    );
                    let (int_bytes, _) = kv.value.split_at(mid);
                    start = u64::from_le_bytes(int_bytes.try_into().unwrap());
                } else {
                    start = 0;
                }
                continue;
            }

            return Ok(Range {
                start,
                end: start + self.step,
            });
        }

        error::NextSequenceSnafu {
            err_msg: format!("{}.next_range()", &self.name),
        }
        .fail()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::service::store::{kv::KvStore, memory::MemStore};

    #[tokio::test]
    async fn test_sequence() {
        let kv_store = Arc::new(MemStore::new());
        let seq = Sequence::new("test_seq", 10, kv_store);

        for i in 0..100 {
            assert_eq!(i, seq.next().await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_sequence_fouce_quit() {
        struct Noop;

        #[async_trait::async_trait]
        impl KvStore for Noop {
            async fn range(
                &self,
                _: api::v1::meta::RangeRequest,
            ) -> Result<api::v1::meta::RangeResponse> {
                unreachable!()
            }

            async fn put(
                &self,
                _: api::v1::meta::PutRequest,
            ) -> Result<api::v1::meta::PutResponse> {
                unreachable!()
            }

            async fn batch_put(
                &self,
                _: api::v1::meta::BatchPutRequest,
            ) -> Result<api::v1::meta::BatchPutResponse> {
                unreachable!()
            }

            async fn compare_and_put(
                &self,
                _: CompareAndPutRequest,
            ) -> Result<api::v1::meta::CompareAndPutResponse> {
                Ok(api::v1::meta::CompareAndPutResponse::default())
            }

            async fn delete_range(
                &self,
                _: api::v1::meta::DeleteRangeRequest,
            ) -> Result<api::v1::meta::DeleteRangeResponse> {
                unreachable!()
            }
        }

        let kv_store = Arc::new(Noop {});
        let seq = Sequence::new("test_seq", 10, kv_store);

        let next = seq.next().await;
        assert!(next.is_err());
    }
}
