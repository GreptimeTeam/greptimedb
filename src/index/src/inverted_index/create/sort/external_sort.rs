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

use std::collections::BTreeMap;
use std::mem;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::BitVec;
use futures::stream;

use crate::inverted_index::create::sort::external_provider::ExternalTempFileProvider;
use crate::inverted_index::create::sort::intermediate_rw::{
    IntermediateReader, IntermediateWriter,
};
use crate::inverted_index::create::sort::merge_stream::MergeSortedStream;
use crate::inverted_index::create::sort::{SortOutput, SortedStream, Sorter};
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
    /// Pushes a value into the sorter, adding it to the in-memory buffer and dumping the buffer to
    /// an external file if necessary
    async fn push(&mut self, value: Option<BytesRef<'_>>) -> Result<()> {
        self.push_n(value, 1).await
    }

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

        let buf_values = mem::take(&mut self.values_buffer).into_iter();
        let mut merging_sorted_stream: SortedStream = Box::new(stream::iter(buf_values.map(Ok)));

        // Sequentially merge each intermediate file's stream into the merged stream
        //
        // TODO(zhongzc): k-way merge
        for reader in readers {
            let intermediate = IntermediateReader::new(reader).into_stream().await?;
            merging_sorted_stream = MergeSortedStream::merge(merging_sorted_stream, intermediate);
        }

        Ok(SortOutput {
            segment_null_bitmap: mem::take(&mut self.segment_null_bitmap),
            sorted_stream: merging_sorted_stream,
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

        let values = mem::take(&mut self.values_buffer);
        let file_id = &format!("{:012}", self.total_row_count);

        let writer = self
            .temp_file_provider
            .create(&self.index_name, file_id)
            .await?;
        IntermediateWriter::new(writer).write_all(values).await?;

        self.current_memory_usage = 0;
        Ok(())
    }

    /// Determines the segment index range for the row index range
    /// [self.total_row_count, self.total_row_count + n - 1]
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

    fn generate_random_bytes_option(size: usize) -> Option<Vec<u8>> {
        let mut rng = rand::thread_rng();

        if rng.gen() {
            let mut buffer = vec![0u8; size];
            rng.fill(&mut buffer[..]);
            Some(buffer)
        } else {
            None
        }
    }

    type ValueSegIds = BTreeMap<Option<Vec<u8>>, Vec<usize>>;

    fn shuffle_values_and_sorted_result(row_count: usize) -> (Vec<Option<Vec<u8>>>, ValueSegIds) {
        let mut mock_values = iter::repeat_with(|| generate_random_bytes_option(100))
            .take(row_count)
            .collect::<Vec<_>>();

        let mut sorted_result = BTreeMap::new();
        for (i, value) in mock_values.iter_mut().enumerate() {
            sorted_result
                .entry(value.clone())
                .or_insert_with(Vec::new)
                .push(i);
        }

        (mock_values, sorted_result)
    }

    #[tokio::test]
    async fn test_external_sorter_pure_in_memory() {
        let memory_usage_threshold = None;

        let mut mock_provider = MockExternalTempFileProvider::new();
        mock_provider.expect_create().never();
        mock_provider.expect_read_all().returning(|_| Ok(vec![]));

        let mut sorter = ExternalSorter::new(
            "test".to_owned(),
            Arc::new(mock_provider),
            NonZeroUsize::new(1).unwrap(),
            memory_usage_threshold,
        );

        let (mock_values, mut sorted_result) = shuffle_values_and_sorted_result(100);

        for value in mock_values {
            sorter.push(value.as_deref()).await.unwrap();
        }

        let SortOutput {
            segment_null_bitmap,
            mut sorted_stream,
            total_row_count,
        } = sorter.output().await.unwrap();
        assert_eq!(total_row_count, 100);
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
    async fn test_external_sorter_pure_external() {
        let memory_usage_threshold = Some(0);

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
            NonZeroUsize::new(1).unwrap(),
            memory_usage_threshold,
        );

        let (mock_values, mut sorted_result) = shuffle_values_and_sorted_result(100);

        for value in mock_values {
            sorter.push(value.as_deref()).await.unwrap();
        }

        let SortOutput {
            segment_null_bitmap,
            mut sorted_stream,
            total_row_count,
        } = sorter.output().await.unwrap();
        assert_eq!(total_row_count, 100);
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
    async fn test_external_sorter_mixed() {
        let memory_usage_threshold = Some(1024);

        let mut mock_provider = MockExternalTempFileProvider::new();

        let mock_files: Arc<Mutex<HashMap<String, Box<dyn AsyncRead + Unpin + Send>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        mock_provider.expect_create().times(1..).returning({
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
            NonZeroUsize::new(1).unwrap(),
            memory_usage_threshold,
        );

        let (mock_values, mut sorted_result) = shuffle_values_and_sorted_result(100);

        for value in &mock_values {
            sorter.push(value.as_deref()).await.unwrap();
        }

        let SortOutput {
            segment_null_bitmap,
            mut sorted_stream,
            total_row_count,
        } = sorter.output().await.unwrap();
        assert_eq!(total_row_count, 100);
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
}
