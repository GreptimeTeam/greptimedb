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

use std::ops::{Range, Rem};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytemuck::try_cast_slice;
use bytes::Bytes;
use common_base::range_read::RangeReader;
use fastbloom::BloomFilter;
use greptime_proto::v1::index::{BloomFilterLoc, BloomFilterMeta};
use prost::Message;
use snafu::{ResultExt, ensure};

use crate::bloom_filter::SEED;
use crate::bloom_filter::error::{
    DecodeProtoSnafu, FileSizeTooSmallSnafu, IoSnafu, Result, UnexpectedMetaSizeSnafu,
};

/// Minimum size of the bloom filter, which is the size of the length of the bloom filter.
const BLOOM_META_LEN_SIZE: u64 = 4;

/// Default prefetch size of bloom filter meta.
pub const DEFAULT_PREFETCH_SIZE: u64 = 8192; // 8KiB

/// Metrics for bloom filter read operations.
#[derive(Default, Clone)]
pub struct BloomFilterReadMetrics {
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

impl std::fmt::Debug for BloomFilterReadMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut first = true;

        if self.total_bytes > 0 {
            write!(f, "\"total_bytes\":{}", self.total_bytes)?;
            first = false;
        }
        if self.total_ranges > 0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"total_ranges\":{}", self.total_ranges)?;
            first = false;
        }
        if !self.fetch_elapsed.is_zero() {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"fetch_elapsed\":\"{:?}\"", self.fetch_elapsed)?;
            first = false;
        }
        if self.cache_hit > 0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"cache_hit\":{}", self.cache_hit)?;
            first = false;
        }
        if self.cache_miss > 0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"cache_miss\":{}", self.cache_miss)?;
        }

        write!(f, "}}")
    }
}

impl BloomFilterReadMetrics {
    /// Merges another metrics into this one.
    pub fn merge_from(&mut self, other: &Self) {
        self.total_bytes += other.total_bytes;
        self.total_ranges += other.total_ranges;
        self.fetch_elapsed += other.fetch_elapsed;
        self.cache_hit += other.cache_hit;
        self.cache_miss += other.cache_miss;
    }
}

/// Safely converts bytes to Vec<u64> using bytemuck for optimal performance.
/// Faster than chunking and converting each piece individually.
///
/// The input bytes are a sequence of little-endian u64s.
pub fn bytes_to_u64_vec(bytes: &Bytes) -> Vec<u64> {
    // drop tailing things, this keeps the same behavior with `chunks_exact`.
    let aligned_length = bytes.len() - bytes.len().rem(std::mem::size_of::<u64>());
    let byte_slice = &bytes[..aligned_length];

    // Try fast path first: direct cast if aligned
    let u64_vec = if let Ok(u64_slice) = try_cast_slice::<u8, u64>(byte_slice) {
        u64_slice.to_vec()
    } else {
        // Slow path: create aligned Vec<u64> and copy data
        let u64_count = byte_slice.len() / std::mem::size_of::<u64>();
        let mut u64_vec = Vec::<u64>::with_capacity(u64_count);

        // SAFETY: We're creating a properly sized slice from uninitialized but allocated memory
        // to copy bytes into. The slice has exactly the right size for the byte data.
        let dest_slice = unsafe {
            std::slice::from_raw_parts_mut(u64_vec.as_mut_ptr() as *mut u8, byte_slice.len())
        };
        dest_slice.copy_from_slice(byte_slice);

        // SAFETY: We've just initialized exactly u64_count elements worth of bytes
        unsafe { u64_vec.set_len(u64_count) };
        u64_vec
    };

    // Convert from platform endianness to little endian if needed
    // Just in case.
    #[cfg(target_endian = "little")]
    {
        u64_vec
    }
    #[cfg(target_endian = "big")]
    {
        u64_vec.into_iter().map(|x| x.swap_bytes()).collect()
    }
}

/// `BloomFilterReader` reads the bloom filter from the file.
#[async_trait]
pub trait BloomFilterReader: Sync {
    /// Reads range of bytes from the file.
    async fn range_read(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Bytes>;

    /// Reads bunch of ranges from the file.
    async fn read_vec(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Vec<Bytes>>;

    /// Reads the meta information of the bloom filter.
    async fn metadata(
        &self,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<BloomFilterMeta>;

    /// Reads a bloom filter with the given location.
    async fn bloom_filter(
        &self,
        loc: &BloomFilterLoc,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<BloomFilter> {
        let bytes = self.range_read(loc.offset, loc.size as _, metrics).await?;
        let vec = bytes_to_u64_vec(&bytes);
        let bm = BloomFilter::from_vec(vec)
            .seed(&SEED)
            .expected_items(loc.element_count as _);
        Ok(bm)
    }

    async fn bloom_filter_vec(
        &self,
        locs: &[BloomFilterLoc],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Vec<BloomFilter>> {
        let ranges = locs
            .iter()
            .map(|l| l.offset..l.offset + l.size)
            .collect::<Vec<_>>();
        let bss = self.read_vec(&ranges, metrics).await?;

        let mut result = Vec::with_capacity(bss.len());
        for (bs, loc) in bss.into_iter().zip(locs.iter()) {
            let vec = bytes_to_u64_vec(&bs);
            let bm = BloomFilter::from_vec(vec)
                .seed(&SEED)
                .expected_items(loc.element_count as _);
            result.push(bm);
        }

        Ok(result)
    }
}

/// `BloomFilterReaderImpl` reads the bloom filter from the file.
pub struct BloomFilterReaderImpl<R: RangeReader> {
    /// The underlying reader.
    reader: R,
}

impl<R: RangeReader> BloomFilterReaderImpl<R> {
    /// Creates a new `BloomFilterReaderImpl` with the given reader.
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl<R: RangeReader> BloomFilterReader for BloomFilterReaderImpl<R> {
    async fn range_read(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Bytes> {
        let start = metrics.as_ref().map(|_| Instant::now());
        let result = self
            .reader
            .read(offset..offset + size as u64)
            .await
            .context(IoSnafu)?;

        if let Some(m) = metrics {
            m.total_ranges += 1;
            m.total_bytes += size as u64;
            if let Some(start) = start {
                m.fetch_elapsed += start.elapsed();
            }
        }

        Ok(result)
    }

    async fn read_vec(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Vec<Bytes>> {
        let start = metrics.as_ref().map(|_| Instant::now());
        let result = self.reader.read_vec(ranges).await.context(IoSnafu)?;

        if let Some(m) = metrics {
            m.total_ranges += ranges.len();
            m.total_bytes += ranges.iter().map(|r| r.end - r.start).sum::<u64>();
            if let Some(start) = start {
                m.fetch_elapsed += start.elapsed();
            }
        }

        Ok(result)
    }

    async fn metadata(
        &self,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<BloomFilterMeta> {
        let metadata = self.reader.metadata().await.context(IoSnafu)?;
        let file_size = metadata.content_length;

        let mut meta_reader =
            BloomFilterMetaReader::new(&self.reader, file_size, Some(DEFAULT_PREFETCH_SIZE));
        meta_reader.metadata(metrics).await
    }
}

/// `BloomFilterMetaReader` reads the metadata of the bloom filter.
struct BloomFilterMetaReader<R: RangeReader> {
    reader: R,
    file_size: u64,
    prefetch_size: u64,
}

impl<R: RangeReader> BloomFilterMetaReader<R> {
    pub fn new(reader: R, file_size: u64, prefetch_size: Option<u64>) -> Self {
        Self {
            reader,
            file_size,
            prefetch_size: prefetch_size
                .unwrap_or(BLOOM_META_LEN_SIZE)
                .max(BLOOM_META_LEN_SIZE),
        }
    }

    /// Reads the metadata of the bloom filter.
    ///
    /// It will first prefetch some bytes from the end of the file,
    /// then parse the metadata from the prefetch bytes.
    pub async fn metadata(
        &mut self,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<BloomFilterMeta> {
        ensure!(
            self.file_size >= BLOOM_META_LEN_SIZE,
            FileSizeTooSmallSnafu {
                size: self.file_size,
            }
        );

        let start = metrics.as_ref().map(|_| Instant::now());
        let meta_start = self.file_size.saturating_sub(self.prefetch_size);
        let suffix = self
            .reader
            .read(meta_start..self.file_size)
            .await
            .context(IoSnafu)?;
        let suffix_len = suffix.len();
        let length = u32::from_le_bytes(Self::read_tailing_four_bytes(&suffix)?) as u64;
        self.validate_meta_size(length)?;

        if length > suffix_len as u64 - BLOOM_META_LEN_SIZE {
            let metadata_start = self.file_size - length - BLOOM_META_LEN_SIZE;
            let meta = self
                .reader
                .read(metadata_start..self.file_size - BLOOM_META_LEN_SIZE)
                .await
                .context(IoSnafu)?;

            if let Some(m) = metrics {
                // suffix read + meta read
                m.total_ranges += 2;
                // Ignores the meta length size to simplify the calculation.
                m.total_bytes += self.file_size.min(self.prefetch_size) + length;
                if let Some(start) = start {
                    m.fetch_elapsed += start.elapsed();
                }
            }

            BloomFilterMeta::decode(meta).context(DecodeProtoSnafu)
        } else {
            if let Some(m) = metrics {
                // suffix read only
                m.total_ranges += 1;
                m.total_bytes += self.file_size.min(self.prefetch_size);
                if let Some(start) = start {
                    m.fetch_elapsed += start.elapsed();
                }
            }

            let metadata_start = self.file_size - length - BLOOM_META_LEN_SIZE - meta_start;
            let meta = &suffix[metadata_start as usize..suffix_len - BLOOM_META_LEN_SIZE as usize];
            BloomFilterMeta::decode(meta).context(DecodeProtoSnafu)
        }
    }

    fn read_tailing_four_bytes(suffix: &[u8]) -> Result<[u8; 4]> {
        let suffix_len = suffix.len();
        ensure!(
            suffix_len >= 4,
            FileSizeTooSmallSnafu {
                size: suffix_len as u64
            }
        );
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&suffix[suffix_len - 4..suffix_len]);

        Ok(bytes)
    }

    fn validate_meta_size(&self, length: u64) -> Result<()> {
        let max_meta_size = self.file_size - BLOOM_META_LEN_SIZE;
        ensure!(
            length <= max_meta_size,
            UnexpectedMetaSizeSnafu {
                max_meta_size,
                actual_meta_size: length,
            }
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use futures::io::Cursor;

    use super::*;
    use crate::bloom_filter::creator::BloomFilterCreator;
    use crate::external_provider::MockExternalTempFileProvider;

    async fn mock_bloom_filter_bytes() -> Vec<u8> {
        let mut writer = Cursor::new(vec![]);
        let mut creator = BloomFilterCreator::new(
            2,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        creator
            .push_row_elems(vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        creator
            .push_row_elems(vec![b"c".to_vec(), b"d".to_vec()])
            .await
            .unwrap();
        creator
            .push_row_elems(vec![b"e".to_vec(), b"f".to_vec()])
            .await
            .unwrap();

        creator.finish(&mut writer).await.unwrap();

        writer.into_inner()
    }

    #[tokio::test]
    async fn test_bloom_filter_meta_reader() {
        let bytes = mock_bloom_filter_bytes().await;
        let file_size = bytes.len() as u64;

        for prefetch in [0u64, file_size / 2, file_size, file_size + 10] {
            let mut reader =
                BloomFilterMetaReader::new(bytes.clone(), file_size as _, Some(prefetch));
            let meta = reader.metadata(None).await.unwrap();

            assert_eq!(meta.rows_per_segment, 2);
            assert_eq!(meta.segment_count, 2);
            assert_eq!(meta.row_count, 3);
            assert_eq!(meta.bloom_filter_locs.len(), 2);

            assert_eq!(meta.bloom_filter_locs[0].offset, 0);
            assert_eq!(meta.bloom_filter_locs[0].element_count, 4);
            assert_eq!(
                meta.bloom_filter_locs[1].offset,
                meta.bloom_filter_locs[0].size
            );
            assert_eq!(meta.bloom_filter_locs[1].element_count, 2);
        }
    }

    #[tokio::test]
    async fn test_bloom_filter_reader() {
        let bytes = mock_bloom_filter_bytes().await;

        let reader = BloomFilterReaderImpl::new(bytes);
        let meta = reader.metadata(None).await.unwrap();

        assert_eq!(meta.bloom_filter_locs.len(), 2);
        let bf = reader
            .bloom_filter(&meta.bloom_filter_locs[0], None)
            .await
            .unwrap();
        assert!(bf.contains(&b"a"));
        assert!(bf.contains(&b"b"));
        assert!(bf.contains(&b"c"));
        assert!(bf.contains(&b"d"));

        let bf = reader
            .bloom_filter(&meta.bloom_filter_locs[1], None)
            .await
            .unwrap();
        assert!(bf.contains(&b"e"));
        assert!(bf.contains(&b"f"));
    }
}
