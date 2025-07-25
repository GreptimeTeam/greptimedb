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

use snafu::ensure;
use tokio::sync::Mutex;

use crate::error::{self, Result};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::CompareAndPutRequest;

pub type SequenceRef = Arc<Sequence>;

pub(crate) const SEQ_PREFIX: &str = "__meta_seq";

pub struct SequenceBuilder {
    name: String,
    initial: u64,
    step: u64,
    generator: KvBackendRef,
    max: u64,
}

fn seq_name(name: impl AsRef<str>) -> String {
    format!("{}-{}", SEQ_PREFIX, name.as_ref())
}

impl SequenceBuilder {
    pub fn new(name: impl AsRef<str>, generator: KvBackendRef) -> Self {
        Self {
            name: seq_name(name),
            initial: 0,
            step: 1,
            generator,
            max: u64::MAX,
        }
    }

    pub fn initial(self, initial: u64) -> Self {
        Self { initial, ..self }
    }

    pub fn step(self, step: u64) -> Self {
        Self { step, ..self }
    }

    pub fn max(self, max: u64) -> Self {
        Self { max, ..self }
    }

    pub fn build(self) -> Sequence {
        Sequence {
            inner: Mutex::new(Inner {
                name: self.name,
                generator: self.generator,
                initial: self.initial,
                next: self.initial,
                step: self.step,
                range: None,
                force_quit: 1024,
                max: self.max,
            }),
        }
    }
}

pub struct Sequence {
    inner: Mutex<Inner>,
}

impl Sequence {
    pub async fn next(&self) -> Result<u64> {
        let mut inner = self.inner.lock().await;
        inner.next().await
    }

    pub async fn min_max(&self) -> Range<u64> {
        let inner = self.inner.lock().await;
        inner.initial..inner.max
    }

    pub async fn peek(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.next
    }

    pub async fn set(&self, next: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.set(next).await
    }
}

struct Inner {
    name: String,
    generator: KvBackendRef,
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
    max: u64,
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

        let mut expect = if start == self.initial {
            vec![]
        } else {
            u64::to_le_bytes(start).to_vec()
        };

        for _ in 0..self.force_quit {
            let step = self.step.min(self.max - start);

            ensure!(
                step > 0,
                error::NextSequenceSnafu {
                    err_msg: format!("next sequence exhausted, max: {}", self.max)
                }
            );

            // No overflow: step <= self.max - start -> step + start <= self.max <= u64::MAX
            let value = u64::to_le_bytes(start + step);

            let req = CompareAndPutRequest {
                key: key.to_vec(),
                expect,
                value: value.to_vec(),
            };

            let res = self.generator.compare_and_put(req).await?;

            if !res.success {
                if let Some(kv) = res.prev_kv {
                    let v: [u8; 8] = match kv.value.clone().try_into() {
                        Ok(a) => a,
                        Err(v) => {
                            return error::UnexpectedSequenceValueSnafu {
                                err_msg: format!("Not a valid u64 for '{}': {v:?}", self.name),
                            }
                            .fail()
                        }
                    };
                    let v = u64::from_le_bytes(v);
                    // If the existed value is smaller than the initial, we should start from the initial.
                    start = v.max(self.initial);
                    expect = kv.value;
                } else {
                    start = self.initial;
                    expect = vec![];
                }
                continue;
            }

            return Ok(Range {
                start,
                end: start + step,
            });
        }

        error::NextSequenceSnafu {
            err_msg: format!("{}.next_range()", &self.name),
        }
        .fail()
    }

    pub async fn set(&mut self, next: u64) -> Result<()> {
        ensure!(
            next > self.next,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "The next value {} is not greater than the current next value {}",
                    next, self.next
                ),
            }
        );

        let key = self.name.as_bytes();
        let expect = self
            .generator
            .get(key)
            .await?
            .map(|kv| kv.value)
            .unwrap_or_default();

        let req = CompareAndPutRequest {
            key: key.to_vec(),
            expect,
            value: u64::to_le_bytes(next).to_vec(),
        };
        let res = self.generator.compare_and_put(req).await?;
        ensure!(
            res.success,
            error::UnexpectedSnafu {
                err_msg: format!("Failed to reset sequence {} to {}", self.name, next),
            }
        );

        // Reset the sequence to the initial value.
        self.initial = next;
        self.next = next;
        self.range = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;
    use std::sync::Arc;

    use itertools::{Itertools, MinMaxResult};
    use tokio::sync::mpsc;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::{KvBackend, TxnService};
    use crate::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, CompareAndPutResponse, DeleteRangeRequest,
        DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
    };

    #[tokio::test]
    async fn test_sequence_with_existed_value() {
        async fn test(exist: u64, expected: Vec<u64>) {
            let kv_backend = Arc::new(MemoryKvBackend::default());

            let exist = u64::to_le_bytes(exist);
            kv_backend
                .put(PutRequest::new().with_key(seq_name("s")).with_value(exist))
                .await
                .unwrap();

            let initial = 100;
            let seq = SequenceBuilder::new("s", kv_backend)
                .initial(initial)
                .build();

            let mut actual = Vec::with_capacity(expected.len());
            for _ in 0..expected.len() {
                actual.push(seq.next().await.unwrap());
            }
            assert_eq!(actual, expected);
        }

        // put a value not greater than the "initial", the sequence should start from "initial"
        test(1, vec![100, 101, 102]).await;
        test(100, vec![100, 101, 102]).await;

        // put a value greater than the "initial", the sequence should start from the put value
        test(200, vec![200, 201, 202]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sequence_with_contention() {
        let seq = Arc::new(
            SequenceBuilder::new("s", Arc::new(MemoryKvBackend::default()))
                .initial(1024)
                .build(),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        // Spawn 10 tasks to concurrently get the next sequence. Each task will get 100 sequences.
        for _ in 0..10 {
            tokio::spawn({
                let seq = seq.clone();
                let tx = tx.clone();
                async move {
                    for _ in 0..100 {
                        tx.send(seq.next().await.unwrap()).unwrap()
                    }
                }
            });
        }

        // Test that we get 1000 unique sequences, and start from 1024 to 2023.
        let mut nums = HashSet::new();
        let mut c = 0;
        while c < 1000
            && let Some(x) = rx.recv().await
        {
            nums.insert(x);
            c += 1;
        }
        assert_eq!(nums.len(), 1000);
        let MinMaxResult::MinMax(min, max) = nums.iter().minmax() else {
            unreachable!("nums has more than one elements");
        };
        assert_eq!(*min, 1024);
        assert_eq!(*max, 2023);
    }

    #[tokio::test]
    async fn test_sequence() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let initial = 1024;
        let seq = SequenceBuilder::new("test_seq", kv_backend)
            .initial(initial)
            .build();

        for i in initial..initial + 100 {
            assert_eq!(i, seq.next().await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_sequence_set() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let seq = SequenceBuilder::new("test_seq", kv_backend.clone())
            .initial(1024)
            .step(10)
            .build();
        seq.set(1025).await.unwrap();
        assert_eq!(seq.next().await.unwrap(), 1025);
        let err = seq.set(1025).await.unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert_eq!(seq.next().await.unwrap(), 1026);

        seq.set(1048).await.unwrap();
        // Recreate the sequence to test the sequence is reset correctly.
        let seq = SequenceBuilder::new("test_seq", kv_backend)
            .initial(1024)
            .step(10)
            .build();
        assert_eq!(seq.next().await.unwrap(), 1048);
    }

    #[tokio::test]
    async fn test_sequence_out_of_rage() {
        let seq = SequenceBuilder::new("test_seq", Arc::new(MemoryKvBackend::default()))
            .initial(u64::MAX - 10)
            .step(10)
            .build();

        for _ in 0..10 {
            let _ = seq.next().await.unwrap();
        }

        let res = seq.next().await;
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Error::NextSequence { .. }))
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
        }

        let seq = SequenceBuilder::new("test_seq", Arc::new(Noop)).build();

        let next = seq.next().await;
        assert!(next.is_err());
    }
}
