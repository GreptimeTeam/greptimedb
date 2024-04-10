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

use async_stream::try_stream;
use futures::{Stream, TryStreamExt};
use snafu::{ensure, ResultExt};

use super::state_store::KeySet;
use crate::error;
use crate::error::Result;

struct CollectingState {
    pairs: Vec<(String, Vec<u8>)>,
}

fn parse_segments(segments: Vec<(String, Vec<u8>)>, prefix: &str) -> Result<Vec<(usize, Vec<u8>)>> {
    segments
        .into_iter()
        .map(|(key, value)| {
            let suffix = key.trim_start_matches(prefix);
            let index = suffix
                .parse::<usize>()
                .context(error::ParseSegmentKeySnafu { key })?;

            Ok((index, value))
        })
        .collect::<Result<Vec<_>>>()
}

/// Merges multiple values into a single key-value pair.
/// Returns an error if:
/// - Part values are lost.
/// - Failed to parse the key of segment.
fn merge_multiple_values(
    CollectingState { mut pairs }: CollectingState,
) -> Result<(KeySet, Vec<u8>)> {
    if pairs.len() == 1 {
        // Safety: must exist.
        let (key, value) = pairs.into_iter().next().unwrap();
        Ok((KeySet::new(key, 0), value))
    } else {
        let segments = pairs.split_off(1);
        // Safety: must exist.
        let (key, value) = pairs.into_iter().next().unwrap();
        let prefix = KeySet::with_prefix(&key);
        let mut parsed_segments = parse_segments(segments, &prefix)?;
        parsed_segments.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Safety: `parsed_segments` must larger than 0.
        let segment_num = parsed_segments.last().unwrap().0;
        ensure!(
            // The segment index start from 1.
            parsed_segments.len() == segment_num,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "Corrupted segment keys, parsed segment indexes: {:?}",
                    parsed_segments
                        .into_iter()
                        .map(|(key, _)| key)
                        .collect::<Vec<_>>()
                )
            }
        );

        let segment_values = parsed_segments.into_iter().map(|(_, value)| value);
        let mut values = Vec::with_capacity(segment_values.len() + 1);
        values.push(value);
        values.extend(segment_values);

        Ok((KeySet::new(key, segment_num), values.concat()))
    }
}

impl CollectingState {
    fn new(key: String, value: Vec<u8>) -> CollectingState {
        Self {
            pairs: vec![(key, value)],
        }
    }

    fn push(&mut self, key: String, value: Vec<u8>) {
        self.pairs.push((key, value));
    }

    fn key(&self) -> &str {
        self.pairs[0].0.as_str()
    }
}

type Upstream = dyn Stream<Item = Result<(String, Vec<u8>)>> + Send;

/// Merges multiple values that have the same prefix of the key
/// from `upstream` into a single value.
pub fn multiple_value_stream(
    mut upstream: Pin<Box<Upstream>>,
) -> impl Stream<Item = Result<(KeySet, Vec<u8>)>> {
    try_stream! {
        let mut collecting: Option<CollectingState> = None;
        while let Some((key, value)) = upstream.try_next().await? {
            match collecting.take() {
                Some(mut current) => {
                    if key.starts_with(current.key()) {
                        // Pushes the key value pair into `collecting`.
                        current.push(key, value);
                        collecting = Some(current);
                    } else {
                        // Starts to collect next key value pair.
                        collecting = Some(CollectingState::new(key, value));
                        yield merge_multiple_values(current)?;
                    }
                }
                None => collecting = Some(CollectingState::new(key, value)),
            }
        }
        if let Some(current) = collecting.take() {
            yield merge_multiple_values(current)?
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use futures::stream::{self};
    use futures::TryStreamExt;

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
