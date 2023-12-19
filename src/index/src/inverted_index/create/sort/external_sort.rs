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

use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::BitVec;
use common_telemetry::logging;
use futures::stream;

use crate::inverted_index::create::sort::external_provider::ExternalTempFileProvider;
use crate::inverted_index::create::sort::intermediate_rw::{
    IntermediateReader, IntermediateWriter,
};
use crate::inverted_index::create::sort::merge_stream::MergeSortedStream;
use crate::inverted_index::create::sort::{SortOutput, SortedStream, Sorter};
use crate::inverted_index::create::sort_create::SorterFactory;
use crate::inverted_index::error::Result;
use crate::inverted_index::{Bytes, BytesRef};

/// `ExternalSorter` manages the sorting of data using both in-memory structures and external files.
/// It dumps data to external files when the in-memory buffer crosses a certain memory threshold.
pub struct ExternalSorter {
    /// The index name associated with the sorting operation
    index_name: String,

    /// Manages creation and access to external temporary files
    temp_file_provider: Arc<dyn ExternalTempFileProvider>,

    /// Bitmap indicating which segments have null values
    segment_null_bitmap: BitVec,

    /// In-memory buffer to hold values and their corresponding bitmaps until memory threshold is exceeded
    values_buffer: BTreeMap<Bytes, BitVec>,

    /// Count of all rows ingested so far
    total_row_count: usize,

    /// The number of rows per group for bitmap indexing which determines how rows are
    /// batched for indexing. It is used to determine which segment a row belongs to.
    segment_row_count: NonZeroUsize,

    /// Tracks memory usage of the buffer
    current_memory_usage: usize,

    /// The memory usage threshold at which the buffer should be dumped to an external file.
    /// `None` indicates that the buffer should never be dumped.
    memory_usage_threshold: Option<usize>,
}

#[async_trait]
impl Sorter for ExternalSorter {
    /// Pushes n identical values into the sorter, adding them to the in-memory buffer and dumping
    /// the buffer to an external file if necessary
    async fn push_n(&mut self, value: Option<BytesRef<'_>>, n: usize) -> Result<()> {
        if n == 0 {
            return Ok(());
        }

        let segment_index_range = self.segment_index_range(n);
        self.total_row_count += n;

        if let Some(value) = value {
            let memory_diff = self.push_not_null(value, segment_index_range);
            self.may_dump_buffer(memory_diff).await
        } else {
            set_bits(&mut self.segment_null_bitmap, segment_index_range);
            Ok(())
        }
    }

    /// Finalizes the sorting operation, merging data from both in-memory buffer and external files
    /// into a sorted stream
    async fn output(&mut self) -> Result<SortOutput> {
        let readers = self.temp_file_provider.read_all(&self.index_name).await?;

        // TODO(zhongzc): k-way merge instead of 2-way merge

        let mut tree_nodes: VecDeque<SortedStream> = VecDeque::with_capacity(readers.len() + 1);
        tree_nodes.push_back(Box::new(stream::iter(
            mem::take(&mut self.values_buffer).into_iter().map(Ok),
        )));
        for reader in readers {
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
        memory_usage_threshold: Option<usize>,
    ) -> Self {
        Self {
            index_name,
            temp_file_provider,

            segment_null_bitmap: BitVec::new(),
            values_buffer: BTreeMap::new(),

            total_row_count: 0,
            segment_row_count,

            current_memory_usage: 0,
            memory_usage_threshold,
        }
    }

    /// Generates a factory function that creates new `ExternalSorter` instances
    pub fn factory(
        temp_file_provider: Arc<dyn ExternalTempFileProvider>,
        memory_usage_threshold: Option<usize>,
    ) -> SorterFactory {
        Box::new(move |index_name, segment_row_count| {
            Box::new(Self::new(
                index_name,
                temp_file_provider.clone(),
                segment_row_count,
                memory_usage_threshold,
            ))
        })
    }

    /// Pushes the non-null values to the values buffer and sets the bits within
    /// the specified range in the given BitVec to true.
    /// Returns the memory usage difference of the buffer after the operation.
    fn push_not_null(
        &mut self,
        value: BytesRef<'_>,
        segment_index_range: RangeInclusive<usize>,
    ) -> usize {
        match self.values_buffer.get_mut(value) {
            Some(bitmap) => {
                let old_len = bitmap.as_raw_slice().len();
                set_bits(bitmap, segment_index_range);

                bitmap.as_raw_slice().len() - old_len
            }
            None => {
                let mut bitmap = BitVec::default();
                set_bits(&mut bitmap, segment_index_range);

                let mem_diff = bitmap.as_raw_slice().len() + value.len();
                self.values_buffer.insert(value.to_vec(), bitmap);

                mem_diff
            }
        }
    }

    /// Checks if the in-memory buffer exceeds the threshold and offloads it to external storage if necessary
    async fn may_dump_buffer(&mut self, memory_diff: usize) -> Result<()> {
        self.current_memory_usage += memory_diff;
        if self.memory_usage_threshold.is_none()
            || self.current_memory_usage < self.memory_usage_threshold.unwrap()
        {
            return Ok(());
        }

        let file_id = &format!("{:012}", self.total_row_count);
        let index_name = &self.index_name;
        let writer = self.temp_file_provider.create(index_name, file_id).await?;

        let memory_usage = self.current_memory_usage;
        let values = mem::take(&mut self.values_buffer);
        self.current_memory_usage = 0;

        let entries = values.len();
        IntermediateWriter::new(writer).write_all(values).await.inspect(|_|
            logging::debug!("Dumped {entries} entries ({memory_usage} bytes) to intermediate file {file_id} for index {index_name}")
        ).inspect_err(|e|
            logging::error!("Failed to dump {entries} entries to intermediate file {file_id} for index {index_name}. Error: {e}")
        )
    }

    /// Determines the segment index range for the row index range
    /// `[self.total_row_count, self.total_row_count + n - 1]`
    fn segment_index_range(&self, n: usize) -> RangeInclusive<usize> {
        let start = self.segment_index(self.total_row_count);
        let end = self.segment_index(self.total_row_count + n - 1);
        start..=end
    }

    /// Determines the segment index for the given row index
    fn segment_index(&self, row_index: usize) -> usize {
        row_index / self.segment_row_count
    }
}

/// Sets the bits within the specified range in the given `BitVec` to true
fn set_bits(bitmap: &mut BitVec, index_range: RangeInclusive<usize>) {
    if *index_range.end() >= bitmap.len() {
        bitmap.resize(index_range.end() + 1, false);
    }
    for index in index_range {
        bitmap.set(index, true);
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

    use super::*;
    use crate::inverted_index::create::sort::external_provider::MockExternalTempFileProvider;

    async fn test_external_sorter(
        memory_usage_threshold: Option<usize>,
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
                let (writer, reader) = duplex(8 * 1024);
                files.insert(file_id.to_string(), Box::new(reader.compat()));
                Ok(Box::new(writer.compat_write()))
            }
        });

        mock_provider.expect_read_all().returning({
            let files = Arc::clone(&mock_files);
            move |index_name| {
                assert_eq!(index_name, "test");
                let mut files = files.lock().unwrap();
                Ok(files.drain().map(|f| f.1).collect::<Vec<_>>())
            }
        });

        let mut sorter = ExternalSorter::new(
            "test".to_owned(),
            Arc::new(mock_provider),
            NonZeroUsize::new(segment_row_count).unwrap(),
            memory_usage_threshold,
        );

        let mut sorted_result = if batch_push {
            let (dic_values, sorted_result) =
                dictionary_values_and_sorted_result(row_count, segment_row_count);

            for (value, n) in dic_values {
                sorter.push_n(value.as_deref(), n).await.unwrap();
            }

            sorted_result
        } else {
            let (mock_values, sorted_result) =
                shuffle_values_and_sorted_result(row_count, segment_row_count);

            for value in mock_values {
                sorter.push(value.as_deref()).await.unwrap();
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
        let memory_usage_threshold = None;
        let total_row_count_cases = vec![0, 100, 1000, 10000];
        let segment_row_count_cases = vec![1, 10, 100, 1000];
        let batch_push_cases = vec![false, true];

        for total_row_count in total_row_count_cases {
            for segment_row_count in &segment_row_count_cases {
                for batch_push in &batch_push_cases {
                    test_external_sorter(
                        memory_usage_threshold,
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
        let memory_usage_threshold = Some(0);
        let total_row_count_cases = vec![0, 100, 1000, 10000];
        let segment_row_count_cases = vec![1, 10, 100, 1000];
        let batch_push_cases = vec![false, true];

        for total_row_count in total_row_count_cases {
            for segment_row_count in &segment_row_count_cases {
                for batch_push in &batch_push_cases {
                    test_external_sorter(
                        memory_usage_threshold,
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
        let memory_usage_threshold = Some(1024);
        let total_row_count_cases = vec![0, 100, 1000, 10000];
        let segment_row_count_cases = vec![1, 10, 100, 1000];
        let batch_push_cases = vec![false, true];

        for total_row_count in total_row_count_cases {
            for segment_row_count in &segment_row_count_cases {
                for batch_push in &batch_push_cases {
                    test_external_sorter(
                        memory_usage_threshold,
                        *segment_row_count,
                        total_row_count,
                        *batch_push,
                    )
                    .await;
                }
            }
        }
    }

    fn random_option_bytes(size: usize) -> Option<Vec<u8>> {
        let mut rng = rand::thread_rng();

        if rng.gen() {
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
        let mut rng = rand::thread_rng();
        let mut dic_values = Vec::new();

        while n > 0 {
            let size = rng.gen_range(1..=n);
            let value = random_option_bytes(100);
            dic_values.push((value, size));
            n -= size;
        }

        let mock_values = dic_values
            .iter()
            .flat_map(|(value, size)| iter::repeat(value.clone()).take(*size))
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
