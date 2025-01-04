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
use std::ops::Range;

use crate::bloom_filter::error::Result;
use crate::bloom_filter::reader::BloomFilterReader;
use crate::bloom_filter::{BloomFilterMeta, Bytes};

pub struct BloomFilterApplier {
    reader: Box<dyn BloomFilterReader + Send>,
    meta: BloomFilterMeta,
}

impl BloomFilterApplier {
    pub async fn new(mut reader: Box<dyn BloomFilterReader + Send>) -> Result<Self> {
        let meta = reader.metadata().await?;

        Ok(Self { reader, meta })
    }

    /// Searches ranges of rows that match the given probes in the given search range.
    pub async fn search(
        &mut self,
        probes: &HashSet<Bytes>,
        search_range: Range<usize>,
    ) -> Result<Vec<Range<usize>>> {
        let rows_per_segment = self.meta.rows_per_segment;
        let start_seg = search_range.start / rows_per_segment;
        let end_seg = search_range.end.div_ceil(rows_per_segment);

        let locs = &self.meta.bloom_filter_segments[start_seg..end_seg];
        let bfs = self.reader.bloom_filter_vec(locs).await?;

        let mut ranges: Vec<Range<usize>> = Vec::with_capacity(end_seg - start_seg);
        for (seg_id, bloom) in (start_seg..end_seg).zip(bfs) {
            let start = seg_id * rows_per_segment;
            for probe in probes {
                if bloom.contains(probe) {
                    let end = (start + rows_per_segment).min(search_range.end);
                    let start = start.max(search_range.start);

                    match ranges.last_mut() {
                        Some(last) if last.end == start => {
                            last.end = end;
                        }
                        _ => {
                            ranges.push(start..end);
                        }
                    }
                    break;
                }
            }
        }

        Ok(ranges)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use futures::io::Cursor;

    use super::*;
    use crate::bloom_filter::creator::BloomFilterCreator;
    use crate::bloom_filter::reader::BloomFilterReaderImpl;
    use crate::external_provider::MockExternalTempFileProvider;

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_appliter() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = BloomFilterCreator::new(
            4,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        let rows = vec![
            vec![b"row00".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row01".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row02".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row03".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row04".to_vec(), b"seg01".to_vec(), b"overl".to_vec()],
            vec![b"row05".to_vec(), b"seg01".to_vec(), b"overl".to_vec()],
            vec![b"row06".to_vec(), b"seg01".to_vec(), b"overp".to_vec()],
            vec![b"row07".to_vec(), b"seg01".to_vec(), b"overp".to_vec()],
            vec![b"row08".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row09".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row10".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row11".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
        ];

        let cases = vec![
            (vec![b"row00".to_vec()], 0..12, vec![0..4]), // search one row in full range
            (vec![b"row05".to_vec()], 4..8, vec![4..8]),  // search one row in partial range
            (vec![b"row03".to_vec()], 4..8, vec![]), // search for a row that doesn't exist in the partial range
            (
                vec![b"row01".to_vec(), b"row06".to_vec()],
                0..12,
                vec![0..8],
            ), // search multiple rows in multiple ranges
            (vec![b"row99".to_vec()], 0..12, vec![]), // search for a row that doesn't exist in the full range
            (vec![b"row00".to_vec()], 12..12, vec![]), // search in an empty range
            (
                vec![b"row04".to_vec(), b"row05".to_vec()],
                0..12,
                vec![4..8],
            ), // search multiple rows in same segment
            (vec![b"seg01".to_vec()], 0..12, vec![4..8]), // search rows in a segment
            (vec![b"seg01".to_vec()], 6..12, vec![6..8]), // search rows in a segment in partial range
            (vec![b"overl".to_vec()], 0..12, vec![0..8]), // search rows in multiple segments
            (vec![b"overl".to_vec()], 2..12, vec![2..8]), // search range starts from the middle of a segment
            (vec![b"overp".to_vec()], 0..10, vec![4..10]), // search range ends at the middle of a segment
        ];

        for row in rows {
            creator.push_row_elems(row).await.unwrap();
        }

        creator.finish(&mut writer).await.unwrap();

        let bytes = writer.into_inner();

        let reader = BloomFilterReaderImpl::new(bytes);

        let mut applier = BloomFilterApplier::new(Box::new(reader)).await.unwrap();

        for (probes, search_range, expected) in cases {
            let probes: HashSet<Bytes> = probes.into_iter().collect();
            let ranges = applier.search(&probes, search_range).await.unwrap();
            assert_eq!(ranges, expected);
        }
    }
}
