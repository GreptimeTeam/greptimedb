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
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use greptime_proto::v1::index::InvertedIndexMetas;
use snafu::ResultExt;

use crate::bitmap::{Bitmap, BitmapType};
use crate::inverted_index::FstMap;
use crate::inverted_index::error::{DecodeBitmapSnafu, DecodeFstSnafu, Result};
pub use crate::inverted_index::format::reader::blob::InvertedIndexBlobReader;

mod blob;
mod footer;

/// Metrics for inverted index read operations.
#[derive(Default, Clone)]
pub struct InvertedIndexReadMetrics {
    /// Total byte size to read.
    pub total_bytes: u64,
    /// Total number of ranges to read.
    pub total_ranges: usize,
    /// Elapsed time to fetch data.
    pub fetch_elapsed: Duration,
    /// Number of cache hits.
    pub cache_hit: usize,
    /// Number of cache misses.
    pub cache_miss: usize,
}

impl std::fmt::Debug for InvertedIndexReadMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            total_bytes,
            total_ranges,
            fetch_elapsed,
            cache_hit,
            cache_miss,
        } = self;

        // If both total_bytes and cache_hit are 0, we didn't read anything.
        if *total_bytes == 0 && *cache_hit == 0 {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;

        if *total_bytes > 0 {
            write!(f, "\"total_bytes\":{}", total_bytes)?;
        }
        if *cache_hit > 0 {
            if *total_bytes > 0 {
                write!(f, ", ")?;
            }
            write!(f, "\"cache_hit\":{}", cache_hit)?;
        }

        if *total_ranges > 0 {
            write!(f, ", \"total_ranges\":{}", total_ranges)?;
        }
        if !fetch_elapsed.is_zero() {
            write!(f, ", \"fetch_elapsed\":\"{:?}\"", fetch_elapsed)?;
        }
        if *cache_miss > 0 {
            write!(f, ", \"cache_miss\":{}", cache_miss)?;
        }

        write!(f, "}}")
    }
}

impl InvertedIndexReadMetrics {
    /// Merges another metrics into this one.
    pub fn merge_from(&mut self, other: &Self) {
        self.total_bytes += other.total_bytes;
        self.total_ranges += other.total_ranges;
        self.fetch_elapsed += other.fetch_elapsed;
        self.cache_hit += other.cache_hit;
        self.cache_miss += other.cache_miss;
    }
}

/// InvertedIndexReader defines an asynchronous reader of inverted index data
#[mockall::automock]
#[async_trait]
pub trait InvertedIndexReader: Send + Sync {
    /// Seeks to given offset and reads data with exact size as provided.
    async fn range_read<'a>(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<u8>>;

    /// Reads the bytes in the given ranges.
    async fn read_vec<'a>(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<Bytes>>;

    /// Retrieves metadata of all inverted indices stored within the blob.
    async fn metadata<'a>(
        &self,
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Arc<InvertedIndexMetas>>;

    /// Retrieves the finite state transducer (FST) map from the given offset and size.
    async fn fst<'a>(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<FstMap> {
        let fst_data = self.range_read(offset, size, metrics).await?;
        FstMap::new(fst_data).context(DecodeFstSnafu)
    }

    /// Retrieves the multiple finite state transducer (FST) maps from the given ranges.
    async fn fst_vec<'a>(
        &mut self,
        ranges: &[Range<u64>],
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<FstMap>> {
        self.read_vec(ranges, metrics)
            .await?
            .into_iter()
            .map(|bytes| FstMap::new(bytes.to_vec()).context(DecodeFstSnafu))
            .collect::<Result<Vec<_>>>()
    }

    /// Retrieves the bitmap from the given offset and size.
    async fn bitmap<'a>(
        &self,
        offset: u64,
        size: u32,
        bitmap_type: BitmapType,
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Bitmap> {
        self.range_read(offset, size, metrics)
            .await
            .and_then(|bytes| {
                Bitmap::deserialize_from(&bytes, bitmap_type).context(DecodeBitmapSnafu)
            })
    }

    /// Retrieves the multiple bitmaps from the given ranges.
    async fn bitmap_deque<'a>(
        &mut self,
        ranges: &[(Range<u64>, BitmapType)],
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<VecDeque<Bitmap>> {
        let (ranges, types): (Vec<_>, Vec<_>) = ranges.iter().cloned().unzip();
        let bytes = self.read_vec(&ranges, metrics).await?;
        bytes
            .into_iter()
            .zip(types)
            .map(|(bytes, bitmap_type)| {
                Bitmap::deserialize_from(&bytes, bitmap_type).context(DecodeBitmapSnafu)
            })
            .collect::<Result<VecDeque<_>>>()
    }
}
