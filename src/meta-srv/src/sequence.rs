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
    pub fn new(name: impl AsRef<str>, initial: u64, step: u64, generator: KvStoreRef) -> Self {
        let name = format!("{}-{}", keys::SEQ_PREFIX, name.as_ref());
        let step = step.max(1);
        Self {
            inner: Mutex::new(Inner {
                name,
                generator,
                initial,
                next: initial,
                step,
                range: None,
                force_quit: 1024,
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
    // The initial(minimal) value of the sequence.
    initial: u64,
    // The next available sequences(if it is in the range,
    // otherwise it need to fetch from generator again).
    next: u64,
    // Fetch several sequences at once: [start, start + step).
    step: u64,
    // The range of available sequences for the local cache.
    range: Option<Range<u64>>,
    // Used to avoid dead loops.
    force_quit: usize,
}

impl Inner {
    /// 1. returns the `next` value directly if it is in the `range` (local cache)
    /// 2. fetch(CAS) next `range` from the `generator`
    /// 3. jump to step 1
    pub async fn next(&mut self) -> Result<u64> {
        for _ in 0..self.force_quit {
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
                    let range = self.next_range().await?;
                    self.next = range.start;
                    self.range = Some(range);
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
        for _ in 0..self.force_quit {
            let expect = if start == self.initial {
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
                    let value = kv.value;
                    ensure!(
                        value.len() == std::mem::size_of::<u64>(),
                        error::UnexceptedSequenceValueSnafu {
                            err_msg: format!("key={}, unexpected value={:?}", self.name, value)
                        }
                    );
                    start = u64::from_le_bytes(value.try_into().unwrap());
                } else {
                    start = self.initial;
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
    use crate::service::store::kv::KvStore;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_sequence() {
        let kv_store = Arc::new(MemStore::new());
        let initial = 1024;
        let seq = Sequence::new("test_seq", initial, 10, kv_store);

        for i in initial..initial + 100 {
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
        let seq = Sequence::new("test_seq", 0, 10, kv_store);

        let next = seq.next().await;
        assert!(next.is_err());
    }
}
