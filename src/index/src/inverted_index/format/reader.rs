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
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use greptime_proto::v1::index::InvertedIndexMetas;
use snafu::ResultExt;

use crate::bitmap::{Bitmap, BitmapType};
use crate::inverted_index::error::{DecodeBitmapSnafu, DecodeFstSnafu, Result};
pub use crate::inverted_index::format::reader::blob::InvertedIndexBlobReader;
use crate::inverted_index::FstMap;

mod blob;
mod footer;

/// InvertedIndexReader defines an asynchronous reader of inverted index data
#[mockall::automock]
#[async_trait]
pub trait InvertedIndexReader: Send + Sync {
    /// Seeks to given offset and reads data with exact size as provided.
    async fn range_read(&self, offset: u64, size: u32) -> Result<Vec<u8>>;

    /// Reads the bytes in the given ranges.
    async fn read_vec(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut result = Vec::with_capacity(ranges.len());
        for range in ranges {
            let data = self
                .range_read(range.start, (range.end - range.start) as u32)
                .await?;
            result.push(Bytes::from(data));
        }
        Ok(result)
    }

    /// Retrieves metadata of all inverted indices stored within the blob.
    async fn metadata(&self) -> Result<Arc<InvertedIndexMetas>>;

    /// Retrieves the finite state transducer (FST) map from the given offset and size.
    async fn fst(&self, offset: u64, size: u32) -> Result<FstMap> {
        let fst_data = self.range_read(offset, size).await?;
        FstMap::new(fst_data).context(DecodeFstSnafu)
    }

    /// Retrieves the multiple finite state transducer (FST) maps from the given ranges.
    async fn fst_vec(&mut self, ranges: &[Range<u64>]) -> Result<Vec<FstMap>> {
        self.read_vec(ranges)
            .await?
            .into_iter()
            .map(|bytes| FstMap::new(bytes.to_vec()).context(DecodeFstSnafu))
            .collect::<Result<Vec<_>>>()
    }

    /// Retrieves the bitmap from the given offset and size.
    async fn bitmap(&self, offset: u64, size: u32, bitmap_type: BitmapType) -> Result<Bitmap> {
        self.range_read(offset, size).await.and_then(|bytes| {
            Bitmap::deserialize_from(&bytes, bitmap_type).context(DecodeBitmapSnafu)
        })
    }

    /// Retrieves the multiple bitmaps from the given ranges.
    async fn bitmap_deque(
        &mut self,
        ranges: &[(Range<u64>, BitmapType)],
    ) -> Result<VecDeque<Bitmap>> {
        let (ranges, types): (Vec<_>, Vec<_>) = ranges.iter().cloned().unzip();
        let bytes = self.read_vec(&ranges).await?;
        bytes
            .into_iter()
            .zip(types)
            .map(|(bytes, bitmap_type)| {
                Bitmap::deserialize_from(&bytes, bitmap_type).context(DecodeBitmapSnafu)
            })
            .collect::<Result<VecDeque<_>>>()
    }
}
