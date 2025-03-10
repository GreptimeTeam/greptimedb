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

use std::cmp::Ordering;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Stream, StreamExt};
use pin_project::pin_project;

use crate::bitmap::Bitmap;
use crate::inverted_index::create::sort::SortedStream;
use crate::inverted_index::error::Result;
use crate::Bytes;

/// A [`Stream`] implementation that merges two sorted streams into a single sorted stream
#[pin_project]
pub struct MergeSortedStream {
    stream1: Option<SortedStream>,
    peek1: Option<(Bytes, Bitmap)>,

    stream2: Option<SortedStream>,
    peek2: Option<(Bytes, Bitmap)>,
}

impl MergeSortedStream {
    /// Creates a new `MergeSortedStream` that will return elements from `stream1` and `stream2`
    /// in sorted order, merging duplicate items by unioning their bitmaps
    pub fn merge(stream1: SortedStream, stream2: SortedStream) -> SortedStream {
        Box::new(MergeSortedStream {
            stream1: Some(stream1),
            peek1: None,

            stream2: Some(stream2),
            peek2: None,
        })
    }
}

impl Stream for MergeSortedStream {
    type Item = Result<(Bytes, Bitmap)>;

    /// Polls both streams and returns the next item from the stream that has the smaller next item.
    /// If both streams have the same next item, the bitmaps are unioned together.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if let (None, Some(stream1)) = (&this.peek1, this.stream1.as_mut()) {
            match ready!(stream1.poll_next_unpin(cx)) {
                Some(item) => *this.peek1 = Some(item?),
                None => *this.stream1 = None, // `stream1` is exhausted, don't poll it next time
            }
        }

        if let (None, Some(stream2)) = (&this.peek2, this.stream2.as_mut()) {
            match ready!(stream2.poll_next_unpin(cx)) {
                Some(item) => *this.peek2 = Some(item?),
                None => *this.stream2 = None, // `stream2` is exhausted, don't poll it next time
            }
        }

        Poll::Ready(match (this.peek1.take(), this.peek2.take()) {
            (Some((v1, b1)), Some((v2, b2))) => match v1.cmp(&v2) {
                Ordering::Less => {
                    *this.peek2 = Some((v2, b2)); // Preserve the rest of `stream2`
                    Some(Ok((v1, b1)))
                }
                Ordering::Greater => {
                    *this.peek1 = Some((v1, b1)); // Preserve the rest of `stream1`
                    Some(Ok((v2, b2)))
                }
                Ordering::Equal => Some(Ok((v1, merge_bitmaps(b1, b2)))),
            },
            (None, Some(item)) | (Some(item), None) => Some(Ok(item)),
            (None, None) => None,
        })
    }
}

/// Merges two bitmaps by bit-wise OR'ing them together, preserving all bits from both
fn merge_bitmaps(mut bitmap1: Bitmap, bitmap2: Bitmap) -> Bitmap {
    bitmap1.union(bitmap2);
    bitmap1
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use greptime_proto::v1::index::BitmapType;

    use super::*;
    use crate::inverted_index::error::Error;

    fn bitmap(bytes: &[u8]) -> Bitmap {
        Bitmap::from_lsb0_bytes(bytes, BitmapType::Roaring)
    }

    fn sorted_stream_from_vec(vec: Vec<(Bytes, Bitmap)>) -> SortedStream {
        Box::new(stream::iter(vec.into_iter().map(Ok::<_, Error>)))
    }

    #[tokio::test]
    async fn test_merge_sorted_stream_non_overlapping() {
        let stream1 = sorted_stream_from_vec(vec![
            (Bytes::from("apple"), bitmap(&[0b10101010])),
            (Bytes::from("orange"), bitmap(&[0b01010101])),
        ]);
        let stream2 = sorted_stream_from_vec(vec![
            (Bytes::from("banana"), bitmap(&[0b10101010])),
            (Bytes::from("peach"), bitmap(&[0b01010101])),
        ]);

        let mut merged_stream = MergeSortedStream::merge(stream1, stream2);

        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("apple"));
        assert_eq!(item.1, bitmap(&[0b10101010]));
        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("banana"));
        assert_eq!(item.1, bitmap(&[0b10101010]));
        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("orange"));
        assert_eq!(item.1, bitmap(&[0b01010101]));
        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("peach"));
        assert_eq!(item.1, bitmap(&[0b01010101]));
        assert!(merged_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_sorted_stream_overlapping() {
        let stream1 = sorted_stream_from_vec(vec![
            (Bytes::from("apple"), bitmap(&[0b10101010])),
            (Bytes::from("orange"), bitmap(&[0b10101010])),
        ]);
        let stream2 = sorted_stream_from_vec(vec![
            (Bytes::from("apple"), bitmap(&[0b01010101])),
            (Bytes::from("peach"), bitmap(&[0b01010101])),
        ]);

        let mut merged_stream = MergeSortedStream::merge(stream1, stream2);

        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("apple"));
        assert_eq!(item.1, bitmap(&[0b11111111]));
        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("orange"));
        assert_eq!(item.1, bitmap(&[0b10101010]));
        let item = merged_stream.next().await.unwrap().unwrap();
        assert_eq!(item.0, Bytes::from("peach"));
        assert_eq!(item.1, bitmap(&[0b01010101]));
        assert!(merged_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_sorted_stream_empty_streams() {
        let stream1 = sorted_stream_from_vec(vec![]);
        let stream2 = sorted_stream_from_vec(vec![]);

        let mut merged_stream = MergeSortedStream::merge(stream1, stream2);
        assert!(merged_stream.next().await.is_none());
    }
}
