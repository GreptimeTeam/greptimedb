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

use common_telemetry::{debug, warn};
use snafu::ensure;
use tokio::sync::Mutex;

use crate::ddl::allocator::resource_id::ResourceIdAllocator;
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

#[async_trait::async_trait]
impl ResourceIdAllocator for Sequence {
    async fn next(&self) -> Result<u64> {
        self.next().await
    }

    async fn peek(&self) -> Result<u64> {
        self.peek().await
    }

    async fn jump_to(&self, next: u64) -> Result<()> {
        self.jump_to(next).await
    }

    async fn min_max(&self) -> Range<u64> {
        self.min_max().await
    }
}

impl Sequence {
    /// Returns the next value and increments the sequence.
    pub async fn next(&self) -> Result<u64> {
        let mut inner = self.inner.lock().await;
        inner.next().await
    }

    /// Returns the range of available sequences.
    pub async fn min_max(&self) -> Range<u64> {
        let inner = self.inner.lock().await;
        inner.initial..inner.max
    }

    /// Returns the current value stored in the remote storage without incrementing the sequence.
    ///
    /// This function always fetches the true current state from the remote storage (KV backend),
    /// ignoring any local cache to provide the most accurate view of the sequence's remote state.
    /// It does not consume or advance the sequence value.
    ///
    /// Note: Since this always queries the remote storage, it may be slower than `next()` but
    /// provides the most accurate and up-to-date information about the sequence state.
    pub async fn peek(&self) -> Result<u64> {
        let inner = self.inner.lock().await;
        inner.peek().await
    }

    /// Jumps to the given value.
    ///
    /// The next value must be greater than both:
    /// 1. The current local next value
    /// 2. The current value stored in the remote storage (KV backend)
    ///
    /// This ensures the sequence can only move forward and maintains consistency
    /// across different instances accessing the same sequence.
    pub async fn jump_to(&self, next: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.jump_to(next).await
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
                        debug!("sequence {} next: {}", self.name, self.next);
                        return res;
                    }
                    self.range = None;
                }
                None => {
                    let range = self.next_range().await?;
                    self.next = range.start;
                    self.range = Some(range);
                    debug!(
                        "sequence {} next: {}, range: {:?}",
                        self.name, self.next, self.range
                    );
                }
            }
        }

        error::NextSequenceSnafu {
            err_msg: format!("{}.next()", &self.name),
        }
        .fail()
    }

    /// Returns the current value from remote storage without advancing the sequence.
    /// If no value exists in remote storage, returns the initial value.
    pub async fn peek(&self) -> Result<u64> {
        let key = self.name.as_bytes();
        let value = self.generator.get(key).await?.map(|kv| kv.value);
        let next = if let Some(value) = value {
            let next = self.initial.max(self.parse_sequence_value(value)?);
            debug!("The next value of sequence {} is {}", self.name, next);
            next
        } else {
            debug!(
                "The next value of sequence {} is not set, use initial value {}",
                self.name, self.initial
            );
            self.initial
        };

        Ok(next)
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
                    let v = self.parse_sequence_value(kv.value.clone())?;
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

    /// Jumps to the given value.
    ///
    /// The next value must be greater than both:
    /// 1. The current local next value (self.next)
    /// 2. The current value stored in the remote storage (KV backend)
    ///
    /// This ensures the sequence can only move forward and maintains consistency
    /// across different instances accessing the same sequence.
    pub async fn jump_to(&mut self, next: u64) -> Result<()> {
        let key = self.name.as_bytes();
        let current = self.generator.get(key).await?.map(|kv| kv.value);

        let curr_val = match &current {
            Some(val) => self.initial.max(self.parse_sequence_value(val.clone())?),
            None => self.initial,
        };

        ensure!(
            next > curr_val,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "The next value {} is not greater than the current next value {}",
                    next, curr_val
                ),
            }
        );

        let expect = current.unwrap_or_default();

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
        warn!("Sequence {} jumped to {}", self.name, next);
        // Reset the sequence to the initial value.
        self.initial = next;
        self.next = next;
        self.range = None;

        Ok(())
    }

    /// Converts a Vec<u8> to u64 with proper error handling for sequence values
    fn parse_sequence_value(&self, value: Vec<u8>) -> Result<u64> {
        let v: [u8; 8] = match value.try_into() {
            Ok(a) => a,
            Err(v) => {
                return error::UnexpectedSequenceValueSnafu {
                    err_msg: format!("Not a valid u64 for '{}': {v:?}", self.name),
                }
                .fail();
            }
        };
        Ok(u64::from_le_bytes(v))
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
        seq.jump_to(1025).await.unwrap();
        assert_eq!(seq.next().await.unwrap(), 1025);
        let err = seq.jump_to(1025).await.unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert_eq!(seq.next().await.unwrap(), 1026);

        seq.jump_to(1048).await.unwrap();
        // Recreate the sequence to test the sequence is reset correctly.
        let seq = SequenceBuilder::new("test_seq", kv_backend)
            .initial(1024)
            .step(10)
            .build();
        assert_eq!(seq.next().await.unwrap(), 1048);
    }

    #[tokio::test]
    async fn test_sequence_out_of_range() {
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

    #[tokio::test]
    async fn test_sequence_peek() {
        common_telemetry::init_default_ut_logging();
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let seq = SequenceBuilder::new("test_seq", kv_backend.clone())
            .step(10)
            .initial(1024)
            .build();
        // The sequence value in the kv backend is not set, so the peek value should be the initial value.
        assert_eq!(seq.peek().await.unwrap(), 1024);

        for i in 0..11 {
            let v = seq.next().await.unwrap();
            assert_eq!(v, 1024 + i);
        }
        let seq = SequenceBuilder::new("test_seq", kv_backend)
            .initial(1024)
            .build();
        // The sequence is not initialized, it will fetch the value from the kv backend.
        assert_eq!(seq.peek().await.unwrap(), 1044);
    }

    #[tokio::test]
    async fn test_sequence_peek_shared_storage() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let shared_seq = "shared_seq";

        // Create two sequence instances with the SAME name but DIFFERENT configs
        let seq1 = SequenceBuilder::new(shared_seq, kv_backend.clone())
            .initial(100)
            .step(5)
            .build();
        let seq2 = SequenceBuilder::new(shared_seq, kv_backend.clone())
            .initial(200) // different initial
            .step(3) // different step
            .build();

        // Initially both return their own initial values when no remote value exists
        assert_eq!(seq1.peek().await.unwrap(), 100);
        assert_eq!(seq2.peek().await.unwrap(), 200);

        // seq1 calls next() to allocate range and update remote storage
        assert_eq!(seq1.next().await.unwrap(), 100);
        // After seq1.next(), remote storage has 100 + seq1.step(5) = 105

        // seq2 should now see the updated remote value through peek(), not its own initial(200)
        assert_eq!(seq1.peek().await.unwrap(), 105);
        assert_eq!(seq2.peek().await.unwrap(), 200); // sees seq1's update, but use its own initial(200)

        // seq2 calls next(), should start from its initial(200)
        assert_eq!(seq2.next().await.unwrap(), 200);
        // After seq2.next(), remote storage updated to 200 + seq2.step(3) = 203

        // Both should see the new remote value (seq2's step was used)
        assert_eq!(seq1.peek().await.unwrap(), 203);
        assert_eq!(seq2.peek().await.unwrap(), 203);

        // seq1 calls next(), should start from its next(105)
        assert_eq!(seq1.next().await.unwrap(), 101);
        assert_eq!(seq1.next().await.unwrap(), 102);
        assert_eq!(seq1.next().await.unwrap(), 103);
        assert_eq!(seq1.next().await.unwrap(), 104);
        assert_eq!(seq1.next().await.unwrap(), 203);
        // After seq1.next(), remote storage updated to 203 + seq1.step(5) = 208
        assert_eq!(seq1.peek().await.unwrap(), 208);
        assert_eq!(seq2.peek().await.unwrap(), 208);
    }

    #[tokio::test]
    async fn test_sequence_peek_initial_max_logic() {
        let kv_backend = Arc::new(MemoryKvBackend::default());

        // Manually set a small value in storage
        let key = seq_name("test_max").into_bytes();
        kv_backend
            .put(
                PutRequest::new()
                    .with_key(key)
                    .with_value(u64::to_le_bytes(50)),
            )
            .await
            .unwrap();

        // Create sequence with larger initial value
        let seq = SequenceBuilder::new("test_max", kv_backend)
            .initial(100) // larger than remote value (50)
            .build();

        // peek() should return max(initial, remote) = max(100, 50) = 100
        assert_eq!(seq.peek().await.unwrap(), 100);

        // next() should start from the larger initial value
        assert_eq!(seq.next().await.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_sequence_initial_greater_than_storage() {
        let kv_backend = Arc::new(MemoryKvBackend::default());

        // Test sequence behavior when initial > storage value
        // This verifies the max(storage, initial) logic works correctly

        // Step 1: Establish a low value in storage
        let seq1 = SequenceBuilder::new("max_test", kv_backend.clone())
            .initial(10)
            .step(5)
            .build();
        assert_eq!(seq1.next().await.unwrap(), 10); // storage: 15

        // Step 2: Create sequence with much larger initial
        let seq2 = SequenceBuilder::new("max_test", kv_backend.clone())
            .initial(100) // much larger than storage (15)
            .step(5)
            .build();

        // seq2 should start from max(15, 100) = 100 (its initial value)
        assert_eq!(seq2.next().await.unwrap(), 100); // storage updated to: 105
        assert_eq!(seq2.peek().await.unwrap(), 105);

        // Step 3: Verify subsequent sequences continue from updated storage
        let seq3 = SequenceBuilder::new("max_test", kv_backend)
            .initial(50) // smaller than current storage (105)
            .step(1)
            .build();

        // seq3 should use max(105, 50) = 105 (storage value)
        assert_eq!(seq3.peek().await.unwrap(), 105);
        assert_eq!(seq3.next().await.unwrap(), 105); // storage: 106

        // This demonstrates the correct max(storage, initial) behavior:
        // - Sequences never generate values below their initial requirement
        // - Storage always reflects the highest allocated value
        // - Value gaps (15-99) are acceptable to maintain minimum constraints
    }
}
