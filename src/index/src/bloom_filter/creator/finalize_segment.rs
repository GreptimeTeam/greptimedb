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

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use asynchronous_codec::{FramedRead, FramedWrite};
use fastbloom::BloomFilter;
use futures::stream::StreamExt;
use futures::{stream, AsyncWriteExt, Stream};
use snafu::ResultExt;

use super::intermediate_codec::IntermediateBloomFilterCodecV1;
use crate::bloom_filter::creator::{FALSE_POSITIVE_RATE, SEED};
use crate::bloom_filter::error::{IntermediateSnafu, IoSnafu, Result};
use crate::bloom_filter::Bytes;
use crate::external_provider::ExternalTempFileProvider;

/// The minimum memory usage threshold for flushing in-memory Bloom filters to disk.
const MIN_MEMORY_USAGE_THRESHOLD: usize = 1024 * 1024; // 1MB

/// Storage for finalized Bloom filters.
pub struct FinalizedBloomFilterStorage {
    /// Indices of the segments in the sequence of finalized Bloom filters.
    segment_indices: Vec<usize>,

    /// Bloom filters that are stored in memory.
    in_memory: Vec<FinalizedBloomFilterSegment>,

    /// Used to generate unique file IDs for intermediate Bloom filters.
    intermediate_file_id_counter: usize,

    /// Prefix for intermediate Bloom filter files.
    intermediate_prefix: String,

    /// The provider for intermediate Bloom filter files.
    intermediate_provider: Arc<dyn ExternalTempFileProvider>,

    /// The memory usage of the in-memory Bloom filters.
    memory_usage: usize,

    /// The global memory usage provided by the user to track the
    /// total memory usage of the creating Bloom filters.
    global_memory_usage: Arc<AtomicUsize>,

    /// The threshold of the global memory usage of the creating Bloom filters.
    global_memory_usage_threshold: Option<usize>,

    /// Records the number of flushed segments.
    flushed_seg_count: usize,
}

impl FinalizedBloomFilterStorage {
    /// Creates a new `FinalizedBloomFilterStorage`.
    pub fn new(
        intermediate_provider: Arc<dyn ExternalTempFileProvider>,
        global_memory_usage: Arc<AtomicUsize>,
        global_memory_usage_threshold: Option<usize>,
    ) -> Self {
        let external_prefix = format!("intm-bloom-filters-{}", uuid::Uuid::new_v4());
        Self {
            segment_indices: Vec::new(),
            in_memory: Vec::new(),
            intermediate_file_id_counter: 0,
            intermediate_prefix: external_prefix,
            intermediate_provider,
            memory_usage: 0,
            global_memory_usage,
            global_memory_usage_threshold,
            flushed_seg_count: 0,
        }
    }

    /// Returns the memory usage of the storage.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Adds a new finalized Bloom filter to the storage.
    ///
    /// If the memory usage exceeds the threshold, flushes the in-memory Bloom filters to disk.
    pub async fn add(
        &mut self,
        elems: impl IntoIterator<Item = Bytes>,
        element_count: usize,
    ) -> Result<()> {
        let mut bf = BloomFilter::with_false_pos(FALSE_POSITIVE_RATE)
            .seed(&SEED)
            .expected_items(element_count);
        for elem in elems.into_iter() {
            bf.insert(&elem);
        }

        let fbf = FinalizedBloomFilterSegment::from(bf, element_count);

        // Reuse the last segment if it is the same as the current one.
        if self.in_memory.last() == Some(&fbf) {
            self.segment_indices
                .push(self.flushed_seg_count + self.in_memory.len() - 1);
            return Ok(());
        }

        // Update memory usage.
        let memory_diff = fbf.bloom_filter_bytes.len();
        self.memory_usage += memory_diff;
        self.global_memory_usage
            .fetch_add(memory_diff, Ordering::Relaxed);

        // Add the finalized Bloom filter to the in-memory storage.
        self.in_memory.push(fbf);
        self.segment_indices
            .push(self.flushed_seg_count + self.in_memory.len() - 1);

        // Flush to disk if necessary.

        // Do not flush if memory usage is too low.
        if self.memory_usage < MIN_MEMORY_USAGE_THRESHOLD {
            return Ok(());
        }

        // Check if the global memory usage exceeds the threshold and flush to disk if necessary.
        if let Some(threshold) = self.global_memory_usage_threshold {
            let global = self.global_memory_usage.load(Ordering::Relaxed);

            if global > threshold {
                self.flush_in_memory_to_disk().await?;

                self.global_memory_usage
                    .fetch_sub(self.memory_usage, Ordering::Relaxed);
                self.memory_usage = 0;
            }
        }

        Ok(())
    }

    /// Drains the storage and returns indieces of the segments and a stream of finalized Bloom filters.
    pub async fn drain(
        &mut self,
    ) -> Result<(
        Vec<usize>,
        Pin<Box<dyn Stream<Item = Result<FinalizedBloomFilterSegment>> + Send + '_>>,
    )> {
        // FAST PATH: memory only
        if self.intermediate_file_id_counter == 0 {
            return Ok((
                std::mem::take(&mut self.segment_indices),
                Box::pin(stream::iter(self.in_memory.drain(..).map(Ok))),
            ));
        }

        // SLOW PATH: memory + disk
        let mut on_disk = self
            .intermediate_provider
            .read_all(&self.intermediate_prefix)
            .await
            .context(IntermediateSnafu)?;
        on_disk.sort_unstable_by(|x, y| x.0.cmp(&y.0));

        let streams = on_disk
            .into_iter()
            .map(|(_, reader)| FramedRead::new(reader, IntermediateBloomFilterCodecV1::default()));

        let in_memory_stream = stream::iter(self.in_memory.drain(..)).map(Ok);
        Ok((
            std::mem::take(&mut self.segment_indices),
            Box::pin(stream::iter(streams).flatten().chain(in_memory_stream)),
        ))
    }

    /// Flushes the in-memory Bloom filters to disk.
    async fn flush_in_memory_to_disk(&mut self) -> Result<()> {
        let file_id = self.intermediate_file_id_counter;
        self.intermediate_file_id_counter += 1;
        self.flushed_seg_count += self.in_memory.len();

        let file_id = format!("{:08}", file_id);
        let mut writer = self
            .intermediate_provider
            .create(&self.intermediate_prefix, &file_id)
            .await
            .context(IntermediateSnafu)?;

        let fw = FramedWrite::new(&mut writer, IntermediateBloomFilterCodecV1::default());
        // `forward()` will flush and close the writer when the stream ends
        if let Err(e) = stream::iter(self.in_memory.drain(..).map(Ok))
            .forward(fw)
            .await
        {
            writer.close().await.context(IoSnafu)?;
            writer.flush().await.context(IoSnafu)?;
            return Err(e);
        }

        Ok(())
    }
}

impl Drop for FinalizedBloomFilterStorage {
    fn drop(&mut self) {
        self.global_memory_usage
            .fetch_sub(self.memory_usage, Ordering::Relaxed);
    }
}

/// A finalized Bloom filter segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBloomFilterSegment {
    /// The underlying Bloom filter bytes.
    pub bloom_filter_bytes: Vec<u8>,

    /// The number of elements in the Bloom filter.
    pub element_count: usize,
}

impl FinalizedBloomFilterSegment {
    fn from(bf: BloomFilter, elem_count: usize) -> Self {
        let bf_slice = bf.as_slice();
        let mut bloom_filter_bytes = Vec::with_capacity(std::mem::size_of_val(bf_slice));
        for &x in bf_slice {
            bloom_filter_bytes.extend_from_slice(&x.to_le_bytes());
        }

        Self {
            bloom_filter_bytes,
            element_count: elem_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use futures::AsyncRead;
    use tokio::io::duplex;
    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    use super::*;
    use crate::bloom_filter::creator::tests::u64_vec_from_bytes;
    use crate::external_provider::MockExternalTempFileProvider;

    #[tokio::test]
    async fn test_finalized_bloom_filter_storage() {
        let mut mock_provider = MockExternalTempFileProvider::new();

        let mock_files: Arc<Mutex<HashMap<String, Box<dyn AsyncRead + Unpin + Send>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        mock_provider.expect_create().returning({
            let files = Arc::clone(&mock_files);
            move |file_group, file_id| {
                assert!(file_group.starts_with("intm-bloom-filters-"));
                let mut files = files.lock().unwrap();
                let (writer, reader) = duplex(2 * 1024 * 1024);
                files.insert(file_id.to_string(), Box::new(reader.compat()));
                Ok(Box::new(writer.compat_write()))
            }
        });

        mock_provider.expect_read_all().returning({
            let files = Arc::clone(&mock_files);
            move |file_group| {
                assert!(file_group.starts_with("intm-bloom-filters-"));
                let mut files = files.lock().unwrap();
                Ok(files.drain().collect::<Vec<_>>())
            }
        });

        let global_memory_usage = Arc::new(AtomicUsize::new(0));
        let global_memory_usage_threshold = Some(1024 * 1024); // 1MB
        let provider = Arc::new(mock_provider);
        let mut storage = FinalizedBloomFilterStorage::new(
            provider,
            global_memory_usage.clone(),
            global_memory_usage_threshold,
        );

        let elem_count = 2000;
        let batch = 1000;
        let dup_batch = 200;

        for i in 0..(batch - dup_batch) {
            let elems = (elem_count * i..elem_count * (i + 1)).map(|x| x.to_string().into_bytes());
            storage.add(elems, elem_count).await.unwrap();
        }
        for _ in 0..dup_batch {
            storage.add(Some(vec![]), 1).await.unwrap();
        }

        // Flush happens.
        assert!(storage.intermediate_file_id_counter > 0);

        // Drain the storage.
        let (indices, mut stream) = storage.drain().await.unwrap();
        assert_eq!(indices.len(), batch);

        for (i, idx) in indices.iter().enumerate().take(batch - dup_batch) {
            let segment = stream.next().await.unwrap().unwrap();
            assert_eq!(segment.element_count, elem_count);

            let v = u64_vec_from_bytes(&segment.bloom_filter_bytes);

            // Check the correctness of the Bloom filter.
            let bf = BloomFilter::from_vec(v)
                .seed(&SEED)
                .expected_items(segment.element_count);
            for elem in (elem_count * i..elem_count * (i + 1)).map(|x| x.to_string().into_bytes()) {
                assert!(bf.contains(&elem));
            }
            assert_eq!(indices[i], *idx);
        }

        // Check the correctness of the duplicated segments.
        let dup_seg = stream.next().await.unwrap().unwrap();
        assert_eq!(dup_seg.element_count, 1);
        assert!(stream.next().await.is_none());
        assert!(indices[(batch - dup_batch)..batch]
            .iter()
            .all(|&x| x == batch - dup_batch));
    }

    #[tokio::test]
    async fn test_finalized_bloom_filter_storage_all_dup() {
        let mock_provider = MockExternalTempFileProvider::new();
        let global_memory_usage = Arc::new(AtomicUsize::new(0));
        let global_memory_usage_threshold = Some(1024 * 1024); // 1MB
        let provider = Arc::new(mock_provider);
        let mut storage = FinalizedBloomFilterStorage::new(
            provider,
            global_memory_usage.clone(),
            global_memory_usage_threshold,
        );

        let batch = 1000;
        for _ in 0..batch {
            storage.add(Some(vec![]), 1).await.unwrap();
        }

        // Drain the storage.
        let (indices, mut stream) = storage.drain().await.unwrap();

        let bf = stream.next().await.unwrap().unwrap();
        assert_eq!(bf.element_count, 1);

        assert!(stream.next().await.is_none());

        assert_eq!(indices.len(), batch);
        assert!(indices.iter().all(|&x| x == 0));
    }
}
