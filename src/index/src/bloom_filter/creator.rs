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

mod finalize_segment;
mod intermediate_codec;

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use finalize_segment::FinalizedBloomFilterStorage;
use futures::{AsyncWrite, AsyncWriteExt, StreamExt};
use snafu::ResultExt;

use crate::bloom_filter::error::{IoSnafu, Result, SerdeJsonSnafu};
use crate::bloom_filter::{BloomFilterMeta, BloomFilterSegmentLocation, Bytes, SEED};
use crate::external_provider::ExternalTempFileProvider;

/// The false positive rate of the Bloom filter.
pub const FALSE_POSITIVE_RATE: f64 = 0.01;

/// `BloomFilterCreator` is responsible for creating and managing bloom filters
/// for a set of elements. It divides the rows into segments and creates
/// bloom filters for each segment.
///
/// # Format
///
/// The bloom filter creator writes the following format to the writer:
///
/// ```text
/// +--------------------+--------------------+-----+----------------------+----------------------+
/// | Bloom filter 0     | Bloom filter 1     | ... | BloomFilterMeta      | Meta size            |
/// +--------------------+--------------------+-----+----------------------+----------------------+
/// |<- bytes (size 0) ->|<- bytes (size 1) ->| ... |<- json (meta size) ->|<- u32 LE (4 bytes) ->|
/// ```
///
pub struct BloomFilterCreator {
    /// The number of rows per segment set by the user.
    rows_per_segment: usize,

    /// Row count that added to the bloom filter so far.
    accumulated_row_count: usize,

    /// A set of distinct elements in the current segment.
    cur_seg_distinct_elems: HashSet<Bytes>,

    /// The memory usage of the current segment's distinct elements.
    cur_seg_distinct_elems_mem_usage: usize,

    /// Storage for finalized Bloom filters.
    finalized_bloom_filters: FinalizedBloomFilterStorage,

    /// Global memory usage of the bloom filter creator.
    global_memory_usage: Arc<AtomicUsize>,
}

impl BloomFilterCreator {
    /// Creates a new `BloomFilterCreator` with the specified number of rows per segment.
    ///
    /// # PANICS
    ///
    /// `rows_per_segment` <= 0
    pub fn new(
        rows_per_segment: usize,
        intermediate_provider: Arc<dyn ExternalTempFileProvider>,
        global_memory_usage: Arc<AtomicUsize>,
        global_memory_usage_threshold: Option<usize>,
    ) -> Self {
        assert!(
            rows_per_segment > 0,
            "rows_per_segment must be greater than 0"
        );

        Self {
            rows_per_segment,
            accumulated_row_count: 0,
            cur_seg_distinct_elems: HashSet::default(),
            cur_seg_distinct_elems_mem_usage: 0,
            global_memory_usage: global_memory_usage.clone(),
            finalized_bloom_filters: FinalizedBloomFilterStorage::new(
                intermediate_provider,
                global_memory_usage,
                global_memory_usage_threshold,
            ),
        }
    }

    /// Adds multiple rows of elements to the bloom filter. If the number of accumulated rows
    /// reaches `rows_per_segment`, it finalizes the current segment.
    pub async fn push_n_row_elems(
        &mut self,
        mut nrows: usize,
        elems: impl IntoIterator<Item = Bytes>,
    ) -> Result<()> {
        if nrows == 0 {
            return Ok(());
        }
        if nrows == 1 {
            return self.push_row_elems(elems).await;
        }

        let elems = elems.into_iter().collect::<Vec<_>>();
        while nrows > 0 {
            let rows_to_seg_end =
                self.rows_per_segment - (self.accumulated_row_count % self.rows_per_segment);
            let rows_to_push = nrows.min(rows_to_seg_end);
            nrows -= rows_to_push;

            self.accumulated_row_count += rows_to_push;

            let mut mem_diff = 0;
            for elem in &elems {
                let len = elem.len();
                let is_new = self.cur_seg_distinct_elems.insert(elem.clone());
                if is_new {
                    mem_diff += len;
                }
            }
            self.cur_seg_distinct_elems_mem_usage += mem_diff;
            self.global_memory_usage
                .fetch_add(mem_diff, Ordering::Relaxed);

            if self.accumulated_row_count % self.rows_per_segment == 0 {
                self.finalize_segment().await?;
            }
        }

        Ok(())
    }

    /// Adds a row of elements to the bloom filter. If the number of accumulated rows
    /// reaches `rows_per_segment`, it finalizes the current segment.
    pub async fn push_row_elems(&mut self, elems: impl IntoIterator<Item = Bytes>) -> Result<()> {
        self.accumulated_row_count += 1;

        let mut mem_diff = 0;
        for elem in elems.into_iter() {
            let len = elem.len();
            let is_new = self.cur_seg_distinct_elems.insert(elem);
            if is_new {
                mem_diff += len;
            }
        }
        self.cur_seg_distinct_elems_mem_usage += mem_diff;
        self.global_memory_usage
            .fetch_add(mem_diff, Ordering::Relaxed);

        if self.accumulated_row_count % self.rows_per_segment == 0 {
            self.finalize_segment().await?;
        }

        Ok(())
    }

    /// Finalizes any remaining segments and writes the bloom filters and metadata to the provided writer.
    pub async fn finish(&mut self, mut writer: impl AsyncWrite + Unpin) -> Result<()> {
        if !self.cur_seg_distinct_elems.is_empty() {
            self.finalize_segment().await?;
        }

        let mut meta = BloomFilterMeta {
            rows_per_segment: self.rows_per_segment,
            row_count: self.accumulated_row_count,
            ..Default::default()
        };

        let mut segs = self.finalized_bloom_filters.drain().await?;
        while let Some(segment) = segs.next().await {
            let segment = segment?;
            writer
                .write_all(&segment.bloom_filter_bytes)
                .await
                .context(IoSnafu)?;

            let size = segment.bloom_filter_bytes.len();
            meta.bloom_filter_segments.push(BloomFilterSegmentLocation {
                offset: meta.bloom_filter_segments_size as _,
                size: size as _,
                elem_count: segment.element_count,
            });
            meta.bloom_filter_segments_size += size;
            meta.seg_count += 1;
        }

        let meta_bytes = serde_json::to_vec(&meta).context(SerdeJsonSnafu)?;
        writer.write_all(&meta_bytes).await.context(IoSnafu)?;

        let meta_size = meta_bytes.len() as u32;
        writer
            .write_all(&meta_size.to_le_bytes())
            .await
            .context(IoSnafu)?;
        writer.flush().await.unwrap();

        Ok(())
    }

    /// Returns the memory usage of the creating bloom filter.
    pub fn memory_usage(&self) -> usize {
        self.cur_seg_distinct_elems_mem_usage + self.finalized_bloom_filters.memory_usage()
    }

    async fn finalize_segment(&mut self) -> Result<()> {
        let elem_count = self.cur_seg_distinct_elems.len();
        self.finalized_bloom_filters
            .add(self.cur_seg_distinct_elems.drain(), elem_count)
            .await?;

        self.global_memory_usage
            .fetch_sub(self.cur_seg_distinct_elems_mem_usage, Ordering::Relaxed);
        self.cur_seg_distinct_elems_mem_usage = 0;
        Ok(())
    }
}

impl Drop for BloomFilterCreator {
    fn drop(&mut self) {
        self.global_memory_usage
            .fetch_sub(self.cur_seg_distinct_elems_mem_usage, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use fastbloom::BloomFilter;
    use futures::io::Cursor;

    use super::*;
    use crate::external_provider::MockExternalTempFileProvider;

    /// Converts a slice of bytes to a vector of `u64`.
    pub fn u64_vec_from_bytes(bytes: &[u8]) -> Vec<u64> {
        bytes
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn test_bloom_filter_creator() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = BloomFilterCreator::new(
            2,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        creator
            .push_row_elems(vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert!(creator.cur_seg_distinct_elems_mem_usage > 0);
        assert!(creator.memory_usage() > 0);

        creator
            .push_row_elems(vec![b"c".to_vec(), b"d".to_vec()])
            .await
            .unwrap();
        // Finalize the first segment
        assert_eq!(creator.cur_seg_distinct_elems_mem_usage, 0);
        assert!(creator.memory_usage() > 0);

        creator
            .push_row_elems(vec![b"e".to_vec(), b"f".to_vec()])
            .await
            .unwrap();
        assert!(creator.cur_seg_distinct_elems_mem_usage > 0);
        assert!(creator.memory_usage() > 0);

        creator.finish(&mut writer).await.unwrap();

        let bytes = writer.into_inner();
        let total_size = bytes.len();
        let meta_size_offset = total_size - 4;
        let meta_size = u32::from_le_bytes((&bytes[meta_size_offset..]).try_into().unwrap());

        let meta_bytes = &bytes[total_size - meta_size as usize - 4..total_size - 4];
        let meta: BloomFilterMeta = serde_json::from_slice(meta_bytes).unwrap();

        assert_eq!(meta.rows_per_segment, 2);
        assert_eq!(meta.seg_count, 2);
        assert_eq!(meta.row_count, 3);
        assert_eq!(
            meta.bloom_filter_segments_size + meta_bytes.len() + 4,
            total_size
        );

        let mut bfs = Vec::new();
        for segment in meta.bloom_filter_segments {
            let bloom_filter_bytes =
                &bytes[segment.offset as usize..(segment.offset + segment.size) as usize];
            let v = u64_vec_from_bytes(bloom_filter_bytes);
            let bloom_filter = BloomFilter::from_vec(v)
                .seed(&SEED)
                .expected_items(segment.elem_count);
            bfs.push(bloom_filter);
        }

        assert_eq!(bfs.len(), 2);
        assert!(bfs[0].contains(&b"a"));
        assert!(bfs[0].contains(&b"b"));
        assert!(bfs[0].contains(&b"c"));
        assert!(bfs[0].contains(&b"d"));
        assert!(bfs[1].contains(&b"e"));
        assert!(bfs[1].contains(&b"f"));
    }

    #[tokio::test]
    async fn test_bloom_filter_creator_batch_push() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator: BloomFilterCreator = BloomFilterCreator::new(
            2,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        creator
            .push_n_row_elems(5, vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert!(creator.cur_seg_distinct_elems_mem_usage > 0);
        assert!(creator.memory_usage() > 0);

        creator
            .push_n_row_elems(5, vec![b"c".to_vec(), b"d".to_vec()])
            .await
            .unwrap();
        assert_eq!(creator.cur_seg_distinct_elems_mem_usage, 0);
        assert!(creator.memory_usage() > 0);

        creator
            .push_n_row_elems(10, vec![b"e".to_vec(), b"f".to_vec()])
            .await
            .unwrap();
        assert_eq!(creator.cur_seg_distinct_elems_mem_usage, 0);
        assert!(creator.memory_usage() > 0);

        creator.finish(&mut writer).await.unwrap();

        let bytes = writer.into_inner();
        let total_size = bytes.len();
        let meta_size_offset = total_size - 4;
        let meta_size = u32::from_le_bytes((&bytes[meta_size_offset..]).try_into().unwrap());

        let meta_bytes = &bytes[total_size - meta_size as usize - 4..total_size - 4];
        let meta: BloomFilterMeta = serde_json::from_slice(meta_bytes).unwrap();

        assert_eq!(meta.rows_per_segment, 2);
        assert_eq!(meta.seg_count, 10);
        assert_eq!(meta.row_count, 20);
        assert_eq!(
            meta.bloom_filter_segments_size + meta_bytes.len() + 4,
            total_size
        );

        let mut bfs = Vec::new();
        for segment in meta.bloom_filter_segments {
            let bloom_filter_bytes =
                &bytes[segment.offset as usize..(segment.offset + segment.size) as usize];
            let v = u64_vec_from_bytes(bloom_filter_bytes);
            let bloom_filter = BloomFilter::from_vec(v)
                .seed(&SEED)
                .expected_items(segment.elem_count);
            bfs.push(bloom_filter);
        }

        assert_eq!(bfs.len(), 10);
        for bf in bfs.iter().take(3) {
            assert!(bf.contains(&b"a"));
            assert!(bf.contains(&b"b"));
        }
        for bf in bfs.iter().take(5).skip(2) {
            assert!(bf.contains(&b"c"));
            assert!(bf.contains(&b"d"));
        }
        for bf in bfs.iter().take(10).skip(5) {
            assert!(bf.contains(&b"e"));
            assert!(bf.contains(&b"f"));
        }
    }
}
