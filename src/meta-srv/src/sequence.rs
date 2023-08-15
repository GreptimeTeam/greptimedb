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

use std::ops::Range;
use std::sync::Arc;

use common_meta::rpc::store::CompareAndPutRequest;
use snafu::{ensure, OptionExt};
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

            let value = start
                .checked_add(self.step)
                .context(error::SequenceOutOfRangeSnafu {
                    name: &self.name,
                    start,
                    step: self.step,
                })?;
            let value = u64::to_le_bytes(value);

            let req = CompareAndPutRequest {
                key: key.to_vec(),
                expect,
                value: value.to_vec(),
            };

            let res = self.generator.compare_and_put(req).await?;

            if !res.success {
                if let Some(kv) = res.prev_kv {
                    let value = kv.value;
                    ensure!(
                        value.len() == std::mem::size_of::<u64>(),
                        error::UnexpectedSequenceValueSnafu {
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
    use std::any::Any;
    use std::sync::Arc;

    use common_meta::kv_backend::{KvBackend, TxnService};
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, CompareAndPutResponse, DeleteRangeRequest,
        DeleteRangeResponse, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
        RangeRequest, RangeResponse,
    };

    use super::*;
    use crate::error::Error;
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
    async fn test_sequence_out_of_rage() {
        let kv_store = Arc::new(MemStore::new());
        let initial = u64::MAX - 10;
        let seq = Sequence::new("test_seq", initial, 10, kv_store);

        for _ in 0..10 {
            let _ = seq.next().await.unwrap();
        }

        let res = seq.next().await;
        assert!(res.is_err());
        assert!(matches!(
            res.unwrap_err(),
            error::Error::SequenceOutOfRange { .. }
        ))
    }

    #[tokio::test]
    async fn test_sequence_force_quit() {
        struct Noop;

        impl TxnService for Noop {
            type Error = Error;
        }

        #[async_trait::async_trait]
        impl KvBackend for Noop {
            fn name(&self) -> &str {
                "Noop"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            async fn range(&self, _: RangeRequest) -> Result<RangeResponse> {
                unreachable!()
            }

            async fn put(&self, _: PutRequest) -> Result<PutResponse> {
                unreachable!()
            }

            async fn batch_put(&self, _: BatchPutRequest) -> Result<BatchPutResponse> {
                unreachable!()
            }

            async fn batch_get(&self, _: BatchGetRequest) -> Result<BatchGetResponse> {
                unreachable!()
            }

            async fn compare_and_put(
                &self,
                _: CompareAndPutRequest,
            ) -> Result<CompareAndPutResponse> {
                Ok(CompareAndPutResponse::default())
            }

            async fn delete_range(&self, _: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
                unreachable!()
            }

            async fn batch_delete(&self, _: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
                unreachable!()
            }

            async fn move_value(&self, _: MoveValueRequest) -> Result<MoveValueResponse> {
                unreachable!()
            }
        }

        let kv_store = Arc::new(Noop {});
        let seq = Sequence::new("test_seq", 0, 10, kv_store);

        let next = seq.next().await;
        assert!(next.is_err());
    }
}
