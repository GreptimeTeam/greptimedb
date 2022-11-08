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
}

impl Inner {
    pub async fn next(&mut self) -> Result<u64> {
        loop {
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
    }

    pub async fn next_range(&self) -> Result<Range<u64>> {
        let key = self.name.as_bytes();
        let mut start = self.next;
        loop {
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
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_sequence() {
        let kv_store = Arc::new(MemStore::new());
        let seq = Sequence::new("test_seq", 10, kv_store);

        for i in 0..100 {
            assert_eq!(i, seq.next().await.unwrap());
        }
    }
}
