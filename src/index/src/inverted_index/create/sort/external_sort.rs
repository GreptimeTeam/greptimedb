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

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use common_telemetry::{debug, error};
use futures::stream;
use snafu::ResultExt;

use roaring::RoaringBitmap;

use crate::bitmap::Bitmap;
use crate::external_provider::ExternalTempFileProvider;
use crate::inverted_index::create::sort::intermediate_rw::{
    IntermediateReader, IntermediateWriter,
};
use crate::inverted_index::create::sort::merge_stream::MergeSortedStream;
use crate::inverted_index::create::sort::{SortOutput, SortedStream, Sorter};
use crate::inverted_index::create::sort_create::SorterFactory;
use crate::inverted_index::error::{IntermediateSnafu, Result};
use crate::{Bytes, BytesRef};

/// Segments of one value. Rows arrive in order, so segment ids only grow.
struct Posting {
    segments: RoaringBitmap,
    last_segment: u32,
}

/// Estimated fixed heap cost of a buffered value besides its bytes and segment ids.
const POSTING_OVERHEAD: usize = 64;
/// Estimated cost of one segment id in a roaring array container.
const SEGMENT_SIZE: usize = size_of::<u16>();

impl Posting {
    /// Adds `start..=end` and returns the estimated memory growth.
    fn push(&mut self, start: u32, end: u32) -> usize {
        if end <= self.last_segment {
            return 0;
        }
        let start = start.max(self.last_segment + 1);
        self.segments.insert_range(start..=end);
        self.last_segment = end;
        (end - start + 1) as usize * SEGMENT_SIZE
    }
}

/// `ExternalSorter` manages the sorting of data using both in-memory structures and external files.
/// It dumps data to external files when the in-memory buffer crosses a certain memory threshold.
pub struct ExternalSorter {
    /// The index name associated with the sorting operation
    index_name: String,

    /// Manages creation and access to external temporary files
    temp_file_provider: Arc<dyn ExternalTempFileProvider>,

    /// Bitmap indicating which segments have null values
    segment_null_bitmap: Bitmap,

    /// In-memory buffer to hold values and their corresponding bitmaps until memory threshold is exceeded
    values_buffer: HashMap<Bytes, Posting, ahash::RandomState>,

    /// Count of all rows ingested so far
    total_row_count: usize,

    /// The number of rows per group for bitmap indexing which determines how rows are
    /// batched for indexing. It is used to determine which segment a row belongs to.
    segment_row_count: NonZeroUsize,

    /// Tracks memory usage of the buffer
    current_memory_usage: usize,

    /// The threshold of current memory usage below which the buffer is not dumped, even if the global memory
    /// usage exceeds `global_memory_usage_sort_limit`. This allows for smaller buffers to remain in memory,
    /// providing a buffer against unnecessary dumps to external files, which can be costly in terms of performance.
    /// `None` indicates that only the global memory usage threshold is considered for dumping the buffer.
    current_memory_usage_threshold: Option<usize>,

    /// Tracks the global memory usage of all sorters
    global_memory_usage: Arc<AtomicUsize>,

    /// The memory usage limit that, when exceeded by the global memory consumption of all sorters, necessitates
    /// a reassessment of buffer retention. Surpassing this limit signals that there is a high overall memory pressure,
    /// potentially requiring buffer dumping to external storage for memory relief.
    /// `None` value indicates that no specific global memory usage threshold is established for triggering buffer dumps.
    global_memory_usage_sort_limit: Option<usize>,
}

#[async_trait]
impl Sorter for ExternalSorter {
    /// Pushes n identical values into the in-memory buffer; returns whether it should be
    /// spilled.
    fn push_n(&mut self, value: Option<BytesRef<'_>>, n: usize) -> bool {
        if n == 0 {
            return false;
        }

        let segment_index_range = self.segment_index_range(n);
        self.total_row_count += n;

        if let Some(value) = value {
            let memory_diff = self.push_not_null(value, segment_index_range);
            self.account_memory(memory_diff)
        } else {
            self.segment_null_bitmap.insert_range(segment_index_range);
            false
        }
    }

    async fn spill(&mut self) -> Result<()> {
        self.dump_buffer().await
    }

    /// Finalizes the sorting operation, merging data from both in-memory buffer and external files
    /// into a sorted stream
    async fn output(&mut self) -> Result<SortOutput> {
        let readers = self
            .temp_file_provider
            .read_all(&self.index_name)
            .await
            .context(IntermediateSnafu)?;

        // TODO(zhongzc): k-way merge instead of 2-way merge

        let mut tree_nodes: VecDeque<SortedStream> = VecDeque::with_capacity(readers.len() + 1);
        tree_nodes.push_back(Box::new(stream::iter(
            Self::sorted(mem::take(&mut self.values_buffer)).map(Ok),
        )));
        for (_, reader) in readers {
            tree_nodes.push_back(IntermediateReader::new(reader).into_stream().await?);
        }

        while tree_nodes.len() >= 2 {
            // every turn, the length of tree_nodes will be reduced by 1 until only one stream left
            let stream1 = tree_nodes.pop_front().unwrap();
            let stream2 = tree_nodes.pop_front().unwrap();
            let merged_stream = MergeSortedStream::merge(stream1, stream2);
            tree_nodes.push_back(merged_stream);
        }

        Ok(SortOutput {
            segment_null_bitmap: mem::take(&mut self.segment_null_bitmap),
            sorted_stream: tree_nodes.pop_front().unwrap(),
            total_row_count: self.total_row_count,
        })
    }
}

impl ExternalSorter {
    /// Constructs a new `ExternalSorter`
    pub fn new(
        index_name: String,
        temp_file_provider: Arc<dyn ExternalTempFileProvider>,
        segment_row_count: NonZeroUsize,
        current_memory_usage_threshold: Option<usize>,
        global_memory_usage: Arc<AtomicUsize>,
        global_memory_usage_sort_limit: Option<usize>,
    ) -> Self {
        Self {
            index_name,
            temp_file_provider,

            segment_null_bitmap: Bitmap::new_bitvec(), // bitvec is more efficient for many null values
            values_buffer: HashMap::default(),

            total_row_count: 0,
            segment_row_count,

            current_memory_usage: 0,
            current_memory_usage_threshold,
            global_memory_usage,
            global_memory_usage_sort_limit,
        }
    }

    /// Generates a factory function that creates new `ExternalSorter` instances
    pub fn factory(
        temp_file_provider: Arc<dyn ExternalTempFileProvider>,
        current_memory_usage_threshold: Option<usize>,
        global_memory_usage: Arc<AtomicUsize>,
        global_memory_usage_sort_limit: Option<usize>,
    ) -> SorterFactory {
        Box::new(move |index_name, segment_row_count| {
            Box::new(Self::new(
                index_name,
                temp_file_provider.clone(),
                segment_row_count,
                current_memory_usage_threshold,
                global_memory_usage.clone(),
                global_memory_usage_sort_limit,
            ))
        })
    }

    /// Pushes the non-null values to the values buffer and sets the bits within
    /// the specified range in the given bitmap to true.
    /// Returns the memory usage difference of the buffer after the operation.
    fn push_not_null(
        &mut self,
        value: BytesRef<'_>,
        segment_index_range: RangeInclusive<usize>,
    ) -> usize {
        let (start, end) = (
            *segment_index_range.start() as u32,
            *segment_index_range.end() as u32,
        );
        match self.values_buffer.get_mut(value) {
            Some(posting) => posting.push(start, end),
            None => {
                let mut posting = Posting {
                    segments: RoaringBitmap::new(),
                    last_segment: 0,
                };
                posting.segments.insert_range(start..=end);
                posting.last_segment = end;
                self.values_buffer.insert(value.to_vec(), posting);
                value.len() + POSTING_OVERHEAD + (end - start + 1) as usize * SEGMENT_SIZE
            }
        }
    }

    /// Drains `values` sorted by value.
    fn sorted(
        values: HashMap<Bytes, Posting, ahash::RandomState>,
    ) -> impl Iterator<Item = (Bytes, Bitmap)> {
        let mut values = values.into_iter().collect::<Vec<_>>();
        values.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        values
            .into_iter()
            .map(|(value, posting)| (value, Bitmap::Roaring(posting.segments)))
    }

    /// Records `memory_diff` and returns whether the buffer exceeds the thresholds and
    /// should be offloaded to external storage.
    fn account_memory(&mut self, memory_diff: usize) -> bool {
        self.current_memory_usage += memory_diff;
        let memory_usage = self.current_memory_usage;
        self.global_memory_usage
            .fetch_add(memory_diff, Ordering::Relaxed);

        let Some(limit) = self.global_memory_usage_sort_limit else {
            return false;
        };
        if self.global_memory_usage.load(Ordering::Relaxed) < limit {
            return false;
        }
        if let Some(current_threshold) = self.current_memory_usage_threshold
            && memory_usage < current_threshold
        {
            return false;
        }
        true
    }

    async fn dump_buffer(&mut self) -> Result<()> {
        let memory_usage = self.current_memory_usage;
        let file_id = &format!("{:012}", self.total_row_count);
        let index_name = &self.index_name;
        let writer = self
            .temp_file_provider
            .create(index_name, file_id)
            .await
            .context(IntermediateSnafu)?;

        let values = mem::take(&mut self.values_buffer);
        self.global_memory_usage
            .fetch_sub(memory_usage, Ordering::Relaxed);
        self.current_memory_usage = 0;

        let entries = values.len();
        IntermediateWriter::new(writer).write_all(Self::sorted(values)).await.inspect(|_|
            debug!("Dumped {entries} entries ({memory_usage} bytes) to intermediate file {file_id} for index {index_name}")
        ).inspect_err(|e|
            error!(e; "Failed to dump {entries} entries to intermediate file {file_id} for index {index_name}")
        )
    }

    /// Determines the segment index range for the row index range
    /// `[row_begin, row_begin + n - 1]`
    fn segment_index_range(&self, n: usize) -> RangeInclusive<usize> {
        let row_begin = self.total_row_count;
        let start = self.segment_index(row_begin);
        let end = self.segment_index(row_begin + n - 1);
        start..=end
    }

    /// Determines the segment index for the given row index
    fn segment_index(&self, row_index: usize) -> usize {
        row_index / self.segment_row_count
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::iter;
    use std::sync::Mutex;

    use futures::{AsyncRead, StreamExt};
    use rand::Rng;
    use tokio::io::duplex;
    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    use std::collections::BTreeMap;

    use super::*;
    use crate::external_provider::MockExternalTempFileProvider;

    async fn test_external_sorter(
        current_memory_usage_threshold: Option<usize>,
        global_memory_usage_sort_limit: Option<usize>,
        segment_row_count: usize,
        row_count: usize,
        batch_push: bool,
    ) {
        let mut mock_provider = MockExternalTempFileProvider::new();

        let mock_files: Arc<Mutex<HashMap<String, Box<dyn AsyncRead + Unpin + Send>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        mock_provider.expect_create().returning({
            let files = Arc::clone(&mock_files);
            move |index_name, file_id| {
                assert_eq!(index_name, "test");
                let mut files = files.lock().unwrap();
                let (writer, reader) = duplex(1024 * 1024);
                files.insert(file_id.to_string(), Box::new(reader.compat()));
                Ok(Box::new(writer.compat_write()))
            }
        });

        mock_provider.expect_read_all().returning({
            let files = Arc::clone(&mock_files);
            move |index_name| {
                assert_eq!(index_name, "test");
                let mut files = files.lock().unwrap();
                Ok(files.drain().collect::<Vec<_>>())
            }
        });

        let mut sorter = ExternalSorter::new(
            "test".to_owned(),
            Arc::new(mock_provider),
            NonZeroUsize::new(segment_row_count).unwrap(),
            current_memory_usage_threshold,
            Arc::new(AtomicUsize::new(0)),
            global_memory_usage_sort_limit,
        );

        let mut sorted_result = if batch_push {
            let (dic_values, sorted_result) =
                dictionary_values_and_sorted_result(row_count, segment_row_count);

            for (value, n) in dic_values {
                if sorter.push_n(value.as_deref(), n) {
                    sorter.spill().await.unwrap();
                }
            }

            sorted_result
        } else {
            let (mock_values, sorted_result) =
                shuffle_values_and_sorted_result(row_count, segment_row_count);

            for value in mock_values {
                if sorter.push_n(value.as_deref(), 1) {
                    sorter.spill().await.unwrap();
                }
            }

            sorted_result
        };

        let SortOutput {
            segment_null_bitmap,
            mut sorted_stream,
            total_row_count,
        } = sorter.output().await.unwrap();
        assert_eq!(total_row_count, row_count);
        let n = sorted_result.remove(&None);
        assert_eq!(
            segment_null_bitmap.iter_ones().collect::<Vec<_>>(),
            n.unwrap_or_default()
        );
        for (value, offsets) in sorted_result {
            let item = sorted_stream.next().await.unwrap().unwrap();
            assert_eq!(item.0, value.unwrap());
            assert_eq!(item.1.iter_ones().collect::<Vec<_>>(), offsets);
        }
    }

    #[tokio::test]
    async fn test_external_sorter_pure_in_memory() {
        let current_memory_usage_threshold = None;
        let global_memory_usage_sort_limit = None;
        let total_row_count_cases = vec![0, 100, 1000, 10000];
        let segment_row_count_cases = vec![1, 10, 100, 1000];
        let batch_push_cases = vec![false, true];

        for total_row_count in total_row_count_cases {
            for segment_row_count in &segment_row_count_cases {
                for batch_push in &batch_push_cases {
                    test_external_sorter(
                        current_memory_usage_threshold,
                        global_memory_usage_sort_limit,
                        *segment_row_count,
                        total_row_count,
                        *batch_push,
                    )
                    .await;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_external_sorter_pure_external() {
        let current_memory_usage_threshold = None;
        let global_memory_usage_sort_limit = Some(0);
        let total_row_count_cases = vec![0, 100, 1000, 10000];
        let segment_row_count_cases = vec![1, 10, 100, 1000];
        let batch_push_cases = vec![false, true];

        for total_row_count in total_row_count_cases {
            for segment_row_count in &segment_row_count_cases {
                for batch_push in &batch_push_cases {
                    test_external_sorter(
                        current_memory_usage_threshold,
                        global_memory_usage_sort_limit,
                        *segment_row_count,
                        total_row_count,
                        *batch_push,
                    )
                    .await;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_external_sorter_mixed() {
        let current_memory_usage_threshold = vec![None, Some(2048)];
        let global_memory_usage_sort_limit = Some(1024);
        let total_row_count_cases = vec![0, 100, 1000, 10000];
        let segment_row_count_cases = vec![1, 10, 100, 1000];
        let batch_push_cases = vec![false, true];

        for total_row_count in total_row_count_cases {
            for segment_row_count in &segment_row_count_cases {
                for batch_push in &batch_push_cases {
                    for current_memory_usage_threshold in &current_memory_usage_threshold {
                        test_external_sorter(
                            *current_memory_usage_threshold,
                            global_memory_usage_sort_limit,
                            *segment_row_count,
                            total_row_count,
                            *batch_push,
                        )
                        .await;
                    }
                }
            }
        }
    }

    fn random_option_bytes(size: usize) -> Option<Vec<u8>> {
        let mut rng = rand::rng();

        if rng.random() {
            let mut buffer = vec![0u8; size];
            rng.fill(&mut buffer[..]);
            Some(buffer)
        } else {
            None
        }
    }

    type Values = Vec<Option<Bytes>>;
    type DictionaryValues = Vec<(Option<Bytes>, usize)>;
    type ValueSegIds = BTreeMap<Option<Bytes>, Vec<usize>>;

    fn shuffle_values_and_sorted_result(
        row_count: usize,
        segment_row_count: usize,
    ) -> (Values, ValueSegIds) {
        let mock_values = iter::repeat_with(|| random_option_bytes(100))
            .take(row_count)
            .collect::<Vec<_>>();

        let sorted_result = sorted_result(&mock_values, segment_row_count);
        (mock_values, sorted_result)
    }

    fn dictionary_values_and_sorted_result(
        row_count: usize,
        segment_row_count: usize,
    ) -> (DictionaryValues, ValueSegIds) {
        let mut n = row_count;
        let mut rng = rand::rng();
        let mut dic_values = Vec::new();

        while n > 0 {
            let size = rng.random_range(1..=n);
            let value = random_option_bytes(100);
            dic_values.push((value, size));
            n -= size;
        }

        let mock_values = dic_values
            .iter()
            .flat_map(|(value, size)| std::iter::repeat_n(value.clone(), *size))
            .collect::<Vec<_>>();

        let sorted_result = sorted_result(&mock_values, segment_row_count);
        (dic_values, sorted_result)
    }

    fn sorted_result(values: &Values, segment_row_count: usize) -> ValueSegIds {
        let mut sorted_result = BTreeMap::new();
        for (row_index, value) in values.iter().enumerate() {
            let to_add_segment_index = row_index / segment_row_count;
            let indices = sorted_result.entry(value.clone()).or_insert_with(Vec::new);

            if indices.last() != Some(&to_add_segment_index) {
                indices.push(to_add_segment_index);
            }
        }

        sorted_result
    }
}
