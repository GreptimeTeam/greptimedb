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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use finalize_segment::FinalizedBloomFilterStorage;
use futures::{AsyncWrite, AsyncWriteExt, StreamExt};
use greptime_proto::v1::index::{BloomFilterLoc, BloomFilterMeta};
use prost::Message;
use snafu::ResultExt;

use crate::Bytes;
use crate::bloom_filter::error::{IoSnafu, Result};
use crate::bloom_filter::{PrehashedBuildHasher, element_hash};
use crate::external_provider::ExternalTempFileProvider;

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

    /// Distinct element hashes (see [`element_hash`]) in the current segment.
    ///
    /// Elements with equal hashes set the same bits, so deduplicating by hash loses
    /// nothing and avoids copying the values.
    cur_seg_distinct_elems: HashSet<u64, PrehashedBuildHasher>,

    /// The memory usage of the current segment's distinct elements.
    cur_seg_distinct_elems_mem_usage: usize,

    /// Storage for finalized Bloom filters.
    finalized_bloom_filters: FinalizedBloomFilterStorage,

    /// Row count that finalized so far.
    finalized_row_count: usize,

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
        false_positive_rate: f64,
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
                false_positive_rate,
                intermediate_provider,
                global_memory_usage,
                global_memory_usage_threshold,
            ),
            finalized_row_count: 0,
        }
    }

    /// Adds multiple rows of elements to the bloom filter. If the number of accumulated rows
    /// reaches `rows_per_segment`, it finalizes the current segment.
    pub async fn push_n_row_elems(
        &mut self,
        nrows: usize,
        elems: impl IntoIterator<Item = Bytes>,
    ) -> Result<()> {
        let hashes = elems
            .into_iter()
            .map(|e| element_hash(&e))
            .collect::<Vec<_>>();
        self.push_n_row_hashes(nrows, &hashes).await
    }

    /// Adds `nrows` copies of a single borrowed value (or null). Row counts advance for
    /// nulls as well.
    pub async fn push_n_row_elem(&mut self, nrows: usize, elem: Option<&[u8]>) -> Result<()> {
        match elem {
            Some(elem) => self.push_n_row_hashes(nrows, &[element_hash(elem)]).await,
            None => self.push_n_row_hashes(nrows, &[]).await,
        }
    }

    /// Adds a row of elements to the bloom filter. If the number of accumulated rows
    /// reaches `rows_per_segment`, it finalizes the current segment.
    pub async fn push_row_elems(&mut self, elems: impl IntoIterator<Item = Bytes>) -> Result<()> {
        self.push_row_hashes(elems.into_iter().map(|e| element_hash(&e)))
            .await
    }

    /// Adds a row of element hashes computed by [`element_hash`].
    pub async fn push_row_hashes(&mut self, hashes: impl IntoIterator<Item = u64>) -> Result<()> {
        self.accumulated_row_count += 1;
        self.insert_hashes(hashes);

        if self
            .accumulated_row_count
            .is_multiple_of(self.rows_per_segment)
        {
            self.finalize_segment().await?;
            self.finalized_row_count = self.accumulated_row_count;
        }

        Ok(())
    }

    /// Adds `nrows` rows that all contain the element hashes in `hashes`.
    pub async fn push_n_row_hashes(&mut self, mut nrows: usize, hashes: &[u64]) -> Result<()> {
        while nrows > 0 {
            let rows_to_seg_end =
                self.rows_per_segment - (self.accumulated_row_count % self.rows_per_segment);
            let rows_to_push = nrows.min(rows_to_seg_end);
            nrows -= rows_to_push;
            self.accumulated_row_count += rows_to_push;
            self.insert_hashes(hashes.iter().copied());

            if self
                .accumulated_row_count
                .is_multiple_of(self.rows_per_segment)
            {
                self.finalize_segment().await?;
                self.finalized_row_count = self.accumulated_row_count;
            }
        }

        Ok(())
    }

    fn insert_hashes(&mut self, hashes: impl IntoIterator<Item = u64>) {
        let old_len = self.cur_seg_distinct_elems.len();
        // Not `extend`: it reserves for the iterator's length, which counts duplicate
        // tokens, and the capacity survives `drain` at segment boundaries.
        for hash in hashes {
            self.cur_seg_distinct_elems.insert(hash);
        }
        let mem_diff = (self.cur_seg_distinct_elems.len() - old_len) * size_of::<u64>();
        if mem_diff > 0 {
            self.cur_seg_distinct_elems_mem_usage += mem_diff;
            self.global_memory_usage
                .fetch_add(mem_diff, Ordering::Relaxed);
        }
    }

    /// Finalizes any remaining segments and writes the bloom filters and metadata to the provided writer.
    pub async fn finish(&mut self, mut writer: impl AsyncWrite + Unpin) -> Result<()> {
        if self.accumulated_row_count > self.finalized_row_count {
            self.finalize_segment().await?;
        }

        let mut meta = BloomFilterMeta {
            rows_per_segment: self.rows_per_segment as _,
            row_count: self.accumulated_row_count as _,
            ..Default::default()
        };

        let (indices, mut segs) = self.finalized_bloom_filters.drain().await?;
        meta.segment_loc_indices = indices.into_iter().map(|i| i as u64).collect();
        meta.segment_count = meta.segment_loc_indices.len() as _;

        while let Some(segment) = segs.next().await {
            let segment = segment?;
            writer
                .write_all(&segment.bloom_filter_bytes)
                .await
                .context(IoSnafu)?;

            let size = segment.bloom_filter_bytes.len() as u64;
            meta.bloom_filter_locs.push(BloomFilterLoc {
                offset: meta.bloom_filter_size as _,
                size,
                element_count: segment.element_count as _,
            });
            meta.bloom_filter_size += size;
        }

        let meta_bytes = meta.encode_to_vec();
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
    use crate::bloom_filter::SEED;
    use crate::external_provider::MockExternalTempFileProvider;

    /// Converts a slice of bytes to a vector of `u64`.
    pub fn u64_vec_from_bytes(bytes: &[u8]) -> Vec<u64> {
        bytes
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn test_duplicate_hashes_do_not_grow_segment_set() {
        let mut creator = BloomFilterCreator::new(
            4,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );
        creator
            .push_row_hashes(std::iter::repeat_n(7, 1_000_000))
            .await
            .unwrap();
        assert_eq!(creator.cur_seg_distinct_elems.len(), 1);
        assert!(creator.cur_seg_distinct_elems.capacity() < 16);
    }

    #[tokio::test]
    async fn test_bloom_filter_creator() {
        let mut writer = Cursor::new(Vec::new());
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
        let meta = BloomFilterMeta::decode(meta_bytes).unwrap();

        assert_eq!(meta.rows_per_segment, 2);
        assert_eq!(meta.segment_count, 2);
        assert_eq!(meta.row_count, 3);
        assert_eq!(
            meta.bloom_filter_size as usize + meta_bytes.len() + 4,
            total_size
        );

        let mut bfs = Vec::new();
        for segment in meta.bloom_filter_locs {
            let bloom_filter_bytes =
                &bytes[segment.offset as usize..(segment.offset + segment.size) as usize];
            let v = u64_vec_from_bytes(bloom_filter_bytes);
            let bloom_filter = BloomFilter::from_vec(v)
                .seed(&SEED)
                .expected_items(segment.element_count as usize);
            bfs.push(bloom_filter);
        }

        assert_eq!(meta.segment_loc_indices.len(), 2);

        let bf0 = &bfs[meta.segment_loc_indices[0] as usize];
        assert!(bf0.contains(&b"a"));
        assert!(bf0.contains(&b"b"));
        assert!(bf0.contains(&b"c"));
        assert!(bf0.contains(&b"d"));

        let bf1 = &bfs[meta.segment_loc_indices[1] as usize];
        assert!(bf1.contains(&b"e"));
        assert!(bf1.contains(&b"f"));
    }

    #[tokio::test]
    async fn test_bloom_filter_creator_batch_push() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator: BloomFilterCreator = BloomFilterCreator::new(
            2,
            0.01,
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
        let meta = BloomFilterMeta::decode(meta_bytes).unwrap();

        assert_eq!(meta.rows_per_segment, 2);
        assert_eq!(meta.segment_count, 10);
        assert_eq!(meta.row_count, 20);
        assert_eq!(
            meta.bloom_filter_size as usize + meta_bytes.len() + 4,
            total_size
        );

        let mut bfs = Vec::new();
        for segment in meta.bloom_filter_locs {
            let bloom_filter_bytes =
                &bytes[segment.offset as usize..(segment.offset + segment.size) as usize];
            let v = u64_vec_from_bytes(bloom_filter_bytes);
            let bloom_filter = BloomFilter::from_vec(v)
                .seed(&SEED)
                .expected_items(segment.element_count as _);
            bfs.push(bloom_filter);
        }

        // 4 bloom filters to serve 10 segments
        assert_eq!(bfs.len(), 4);
        assert_eq!(meta.segment_loc_indices.len(), 10);

        for idx in meta.segment_loc_indices.iter().take(3) {
            let bf = &bfs[*idx as usize];
            assert!(bf.contains(&b"a"));
            assert!(bf.contains(&b"b"));
        }
        for idx in meta.segment_loc_indices.iter().take(5).skip(2) {
            let bf = &bfs[*idx as usize];
            assert!(bf.contains(&b"c"));
            assert!(bf.contains(&b"d"));
        }
        for idx in meta.segment_loc_indices.iter().take(10).skip(5) {
            let bf = &bfs[*idx as usize];
            assert!(bf.contains(&b"e"));
            assert!(bf.contains(&b"f"));
        }
    }

    #[tokio::test]
    async fn borrowed_single_value_matches_owned_rows_across_segments() {
        let make_creator = || {
            BloomFilterCreator::new(
                3,
                0.01,
                Arc::new(MockExternalTempFileProvider::new()),
                Arc::new(AtomicUsize::new(0)),
                None,
            )
        };
        let mut borrowed = make_creator();
        let mut owned = make_creator();
        // Zero rows, nulls, empty values, duplicates and runs crossing segment boundaries.
        for (rows, elem) in [
            (0, Some(b"ignored".as_slice())),
            (1, None),
            (5, Some(b"".as_slice())),
            (1, Some(b"".as_slice())),
            (8, Some(b"label".as_slice())),
            (1, None),
        ] {
            borrowed.push_n_row_elem(rows, elem).await.unwrap();
            owned
                .push_n_row_elems(rows, elem.map(<[u8]>::to_vec))
                .await
                .unwrap();
            assert_eq!(borrowed.memory_usage(), owned.memory_usage());
        }
        let mut borrowed_blob = Cursor::new(Vec::new());
        let mut owned_blob = Cursor::new(Vec::new());
        borrowed.finish(&mut borrowed_blob).await.unwrap();
        owned.finish(&mut owned_blob).await.unwrap();
        assert_eq!(borrowed_blob.into_inner(), owned_blob.into_inner());
    }

    #[tokio::test]
    async fn test_final_seg_all_null() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = BloomFilterCreator::new(
            2,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        creator
            .push_n_row_elems(4, vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        creator.push_row_elems(Vec::new()).await.unwrap();

        creator.finish(&mut writer).await.unwrap();

        let bytes = writer.into_inner();
        let total_size = bytes.len();
        let meta_size_offset = total_size - 4;
        let meta_size = u32::from_le_bytes((&bytes[meta_size_offset..]).try_into().unwrap());

        let meta_bytes = &bytes[total_size - meta_size as usize - 4..total_size - 4];
        let meta = BloomFilterMeta::decode(meta_bytes).unwrap();

        assert_eq!(meta.rows_per_segment, 2);
        assert_eq!(meta.segment_count, 3);
        assert_eq!(meta.row_count, 5);
    }
}
