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

use std::pin::Pin;

use async_stream::stream;
use futures::{Stream, StreamExt};

use crate::error;
use crate::error::Result;
use crate::store::state_store::KeySet;

struct CollectingState {
    key: String,
    pairs: Vec<(String, Vec<u8>)>,
}

impl CollectingState {
    fn new(key: String, value: Vec<u8>) -> Self {
        // A surviving suffix may be the first key in a damaged checkpoint.
        let base = super::ParsedKey::checkpoint_base_of_segment(&key)
            .unwrap_or(&key)
            .to_owned();
        Self {
            key: base,
            pairs: vec![(key, value)],
        }
    }

    fn finish(mut self) -> Result<(KeySet, Vec<u8>)> {
        let corrupt = |reason| error::Error::CorruptCheckpoint {
            key: self.key.clone(),
            keys: self.pairs.iter().map(|(key, _)| key.clone()).collect(),
            reason,
        };
        if self.pairs[0].0 != self.key {
            return Err(corrupt("missing base segment".to_string()));
        }
        if self.pairs.len() == 1 {
            let (key, value) = self.pairs.pop().unwrap();
            return Ok((KeySet::new(key, 0), value));
        }
        let prefix = KeySet::with_prefix(&self.key);
        let mut indexed = Vec::with_capacity(self.pairs.len());
        indexed.push((0, 0));
        for (position, (key, _)) in self.pairs.iter().enumerate().skip(1) {
            let idx = key
                .strip_prefix(&prefix)
                .and_then(|suffix| suffix.parse::<usize>().ok())
                .ok_or_else(|| corrupt(format!("invalid segment key {key}")))?;
            indexed.push((idx, position));
        }
        indexed.sort_unstable();
        for (expected, (actual, _)) in indexed.iter().enumerate() {
            if expected != *actual {
                return Err(corrupt(format!(
                    "expected segment {expected}, found {actual}"
                )));
            }
        }
        let mut value = Vec::with_capacity(self.pairs.iter().map(|(_, part)| part.len()).sum());
        for (_, position) in indexed {
            value.extend_from_slice(&self.pairs[position].1);
        }
        Ok((KeySet::new(self.key, self.pairs.len() - 1), value))
    }
}

type Upstream = dyn Stream<Item = Result<(String, Vec<u8>)>> + Send;

/// Merges multiple values that have the same prefix of the key
/// from `upstream` into a single value.
pub fn multiple_value_stream(
    mut upstream: Pin<Box<Upstream>>,
) -> impl Stream<Item = Result<(KeySet, Vec<u8>)>> {
    stream! {
        let mut collecting: Option<CollectingState> = None;
        while let Some(item) = upstream.next().await {
            let (key, value) = match item {
                Ok(pair) => pair,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            if let Some(current) = &mut collecting
                && key.strip_prefix(&current.key).is_some_and(|suffix| suffix.starts_with('/'))
            {
                current.pairs.push((key, value));
                continue;
            }
            if let Some(previous) = collecting.replace(CollectingState::new(key, value)) {
                yield previous.finish();
            }
        }
        if let Some(last) = collecting {
            yield last.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use futures::TryStreamExt;
    use futures::stream::{self};

    use super::*;
    use crate::error::{self};

    #[test]
    fn test_key_set_keys() {
        let key = KeySet::new("baz".to_string(), 3);
        let keys = key.keys();
        assert_eq!(keys.len(), 4);
        assert_eq!(&keys[0], "baz");
        assert_eq!(&keys[1], &KeySet::with_segment_suffix("baz", 1));
    }

    #[tokio::test]
    async fn test_merge_multiple_values() {
        let upstream = stream::iter(vec![
            Ok(("foo".to_string(), vec![0, 1, 2, 3])),
            Ok(("foo/0002".to_string(), vec![6, 7])),
            Ok(("foo/0003".to_string(), vec![8])),
            Ok(("foo/0001".to_string(), vec![4, 5])),
            Ok(("bar".to_string(), vec![0, 1, 2, 3])),
            Ok(("baz".to_string(), vec![0, 1, 2, 3])),
            Ok(("baz/0003".to_string(), vec![8])),
            Ok(("baz/0001".to_string(), vec![4, 5])),
            Ok(("baz/0002".to_string(), vec![6, 7])),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        let keys = key.keys();
        assert_eq!(keys[0], "foo");
        assert_eq!(keys.len(), 4);
        assert_eq!(value, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        let keys = key.keys();
        assert_eq!(keys[0], "bar");
        assert_eq!(keys.len(), 1);
        assert_eq!(value, vec![0, 1, 2, 3]);
        let (key, value) = stream.try_next().await.unwrap().unwrap();
        let keys = key.keys();
        assert_eq!(keys[0], "baz");
        assert_eq!(keys.len(), 4);
        assert_eq!(value, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(stream.try_next().await.unwrap().is_none());
        // Call again
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_missing_segments_leave_next_checkpoint_readable() {
        let id = crate::ProcedureId::random();
        let base = format!("procedure/{id}/0000000007.rollback");
        let suffix = KeySet::with_segment_suffix(&base, 2);
        let next = format!("procedure/{id}/0000000008.step");
        for keys in [vec![suffix.clone()], vec![base.clone(), suffix.clone()]] {
            let upstream = stream::iter(
                keys.clone()
                    .into_iter()
                    .chain([next.clone()])
                    .map(|key| Ok((key, vec![1]))),
            );
            let mut merged = Box::pin(multiple_value_stream(Box::pin(upstream)));
            assert_matches!(merged.try_next().await, Err(error::Error::CorruptCheckpoint {
                key, keys: observed, ..
            }) if key == base && observed == keys);
            let (key, value) = merged.try_next().await.unwrap().unwrap();
            assert_eq!(key.key(), next);
            assert_eq!(value, vec![1]);
            assert!(merged.try_next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_empty_upstream() {
        let upstream = stream::iter(vec![]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        assert!(stream.try_next().await.unwrap().is_none());
        // Call again
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_multiple_values_stream_err() {
        let upstream = stream::iter(vec![
            Err(error::UnexpectedSnafu { err_msg: "mock" }.build()),
            Ok(("foo".to_string(), vec![0, 1, 2, 3])),
            Ok(("foo/0001".to_string(), vec![4, 5])),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        let err = stream.try_next().await.unwrap_err();
        assert_matches!(err, error::Error::Unexpected { .. });

        let upstream = stream::iter(vec![
            Ok(("foo".to_string(), vec![0, 1, 2, 3])),
            Ok(("foo/0001".to_string(), vec![4, 5])),
            Err(error::UnexpectedSnafu { err_msg: "mock" }.build()),
        ]);
        let mut stream = Box::pin(multiple_value_stream(Box::pin(upstream)));
        let err = stream.try_next().await.unwrap_err();
        assert_matches!(err, error::Error::Unexpected { .. });
    }
}
