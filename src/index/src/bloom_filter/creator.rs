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

use std::collections::HashSet;

use fastbloom::BloomFilter;
use futures::{AsyncWrite, AsyncWriteExt};
use snafu::ResultExt;

use crate::bloom_filter::error::{IoSnafu, Result, SerdeJsonSnafu};
use crate::bloom_filter::{BloomFilterMeta, BloomFilterSegmentLocation, Bytes, SEED};

/// The false positive rate of the Bloom filter.
const FALSE_POSITIVE_RATE: f64 = 0.01;

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
}

impl BloomFilterCreator {
    /// Creates a new `BloomFilterCreator` with the specified number of rows per segment.
    ///
    /// # PANICS
    ///
    /// `rows_per_segment` <= 0
    pub fn new(rows_per_segment: usize) -> Self {
        assert!(
            rows_per_segment > 0,
            "rows_per_segment must be greater than 0"
        );

        Self {
            rows_per_segment,
            accumulated_row_count: 0,
            cur_seg_distinct_elems: HashSet::default(),
            cur_seg_distinct_elems_mem_usage: 0,
            finalized_bloom_filters: FinalizedBloomFilterStorage::default(),
        }
    }

    /// Adds a row of elements to the bloom filter. If the number of accumulated rows
    /// reaches `rows_per_segment`, it finalizes the current segment.
    pub fn push_row_elems(&mut self, elems: impl IntoIterator<Item = Bytes>) {
        self.accumulated_row_count += 1;
        for elem in elems.into_iter() {
            let len = elem.len();
            let is_new = self.cur_seg_distinct_elems.insert(elem);
            if is_new {
                self.cur_seg_distinct_elems_mem_usage += len;
            }
        }

        if self.accumulated_row_count % self.rows_per_segment == 0 {
            self.finalize_segment();
        }
    }

    /// Finalizes any remaining segments and writes the bloom filters and metadata to the provided writer.
    pub async fn finish(&mut self, mut writer: impl AsyncWrite + Unpin) -> Result<()> {
        if !self.cur_seg_distinct_elems.is_empty() {
            self.finalize_segment();
        }

        let mut meta = BloomFilterMeta {
            rows_per_segment: self.rows_per_segment,
            seg_count: self.finalized_bloom_filters.len(),
            row_count: self.accumulated_row_count,
            ..Default::default()
        };

        let mut buf = Vec::new();
        for segment in self.finalized_bloom_filters.drain() {
            let slice = segment.bloom_filter.as_slice();
            buf.clear();
            write_u64_slice(&mut buf, slice);
            writer.write_all(&buf).await.context(IoSnafu)?;

            let size = buf.len();
            meta.bloom_filter_segments.push(BloomFilterSegmentLocation {
                offset: meta.bloom_filter_segments_size as _,
                size: size as _,
                elem_count: segment.element_count,
            });
            meta.bloom_filter_segments_size += size;
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

    fn finalize_segment(&mut self) {
        let elem_count = self.cur_seg_distinct_elems.len();
        self.finalized_bloom_filters
            .add(self.cur_seg_distinct_elems.drain(), elem_count);
        self.cur_seg_distinct_elems_mem_usage = 0;
    }
}

/// Storage for finalized Bloom filters.
///
/// TODO(zhongzc): Add support for storing intermediate bloom filters on disk to control memory usage.
#[derive(Debug, Default)]
struct FinalizedBloomFilterStorage {
    /// Bloom filters that are stored in memory.
    in_memory: Vec<FinalizedBloomFilterSegment>,
}

impl FinalizedBloomFilterStorage {
    fn memory_usage(&self) -> usize {
        self.in_memory.iter().map(|s| s.size).sum()
    }

    /// Adds a new finalized Bloom filter to the storage.
    ///
    /// TODO(zhongzc): Add support for flushing to disk.
    fn add(&mut self, elems: impl IntoIterator<Item = Bytes>, elem_count: usize) {
        let mut bf = BloomFilter::with_false_pos(FALSE_POSITIVE_RATE)
            .seed(&SEED)
            .expected_items(elem_count);
        for elem in elems.into_iter() {
            bf.insert(&elem);
        }

        let cbf = FinalizedBloomFilterSegment::new(bf, elem_count);
        self.in_memory.push(cbf);
    }

    fn len(&self) -> usize {
        self.in_memory.len()
    }

    fn drain(&mut self) -> impl Iterator<Item = FinalizedBloomFilterSegment> + '_ {
        self.in_memory.drain(..)
    }
}

/// A finalized Bloom filter segment.
#[derive(Debug)]
struct FinalizedBloomFilterSegment {
    /// The underlying Bloom filter.
    bloom_filter: BloomFilter,

    /// The number of elements in the Bloom filter.
    element_count: usize,

    /// The occupied memory size of the Bloom filter.
    size: usize,
}

impl FinalizedBloomFilterSegment {
    fn new(bloom_filter: BloomFilter, elem_count: usize) -> Self {
        let memory_usage = std::mem::size_of_val(bloom_filter.as_slice());
        Self {
            bloom_filter,
            element_count: elem_count,
            size: memory_usage,
        }
    }
}

/// Writes a slice of `u64` to the buffer in little-endian order.
fn write_u64_slice(buf: &mut Vec<u8>, slice: &[u64]) {
    buf.reserve(std::mem::size_of_val(slice));
    for &x in slice {
        buf.extend_from_slice(&x.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use futures::io::Cursor;

    use super::*;

    fn u64_vec_from_bytes(bytes: &[u8]) -> Vec<u64> {
        bytes
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn test_bloom_filter_creator() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = BloomFilterCreator::new(2);

        creator.push_row_elems(vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(creator.cur_seg_distinct_elems_mem_usage > 0);
        assert!(creator.memory_usage() > 0);

        creator.push_row_elems(vec![b"c".to_vec(), b"d".to_vec()]);
        // Finalize the first segment
        assert!(creator.cur_seg_distinct_elems_mem_usage == 0);
        assert!(creator.memory_usage() > 0);

        creator.push_row_elems(vec![b"e".to_vec(), b"f".to_vec()]);
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
}
