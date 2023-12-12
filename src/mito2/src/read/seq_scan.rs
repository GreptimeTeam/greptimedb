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

//! Sequential scan.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::{debug, error};
use common_time::range::TimestampRange;
use snafu::ResultExt;
use table::predicate::Predicate;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManager, CacheManagerRef};
use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::metrics::READ_STAGE_ELAPSED;
use crate::read::compat::{self, CompatReader};
use crate::read::merge::MergeReaderBuilder;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::ScanParallism;
use crate::read::{BatchReader, BoxedBatchReader, BoxedBatchStream, Source};
use crate::sst::file::FileHandle;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary key, time index`.
pub struct SeqScan {
    /// Region SST access layer.
    access_layer: AccessLayerRef,
    /// Maps projected Batches to RecordBatches.
    mapper: Arc<ProjectionMapper>,
    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Memtables to scan.
    memtables: Vec<MemtableRef>,
    /// Handles to SST files to scan.
    files: Vec<FileHandle>,
    /// Cache.
    cache_manager: Option<CacheManagerRef>,
    /// Ignores file not found error.
    ignore_file_not_found: bool,
    /// Parallelism to scan data.
    parallelism: ScanParallism,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    #[must_use]
    pub(crate) fn new(access_layer: AccessLayerRef, mapper: ProjectionMapper) -> SeqScan {
        SeqScan {
            access_layer,
            mapper: Arc::new(mapper),
            time_range: None,
            predicate: None,
            memtables: Vec::new(),
            files: Vec::new(),
            cache_manager: None,
            ignore_file_not_found: false,
            parallelism: ScanParallism::default(),
        }
    }

    /// Sets time range filter for time index.
    #[must_use]
    pub(crate) fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Sets predicate to push down.
    #[must_use]
    pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Sets memtables to read.
    #[must_use]
    pub(crate) fn with_memtables(mut self, memtables: Vec<MemtableRef>) -> Self {
        self.memtables = memtables;
        self
    }

    /// Sets files to read.
    #[must_use]
    pub(crate) fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Sets cache for this query.
    #[must_use]
    pub(crate) fn with_cache(mut self, cache: Option<CacheManagerRef>) -> Self {
        self.cache_manager = cache;
        self
    }

    /// Ignores file not found error.
    #[must_use]
    pub(crate) fn with_ignore_file_not_found(mut self, ignore: bool) -> Self {
        self.ignore_file_not_found = ignore;
        self
    }

    /// Sets scan parallelism.
    #[must_use]
    pub(crate) fn with_parallelism(mut self, parallelism: ScanParallism) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Builds a stream for the query.
    pub async fn build_stream(&self) -> Result<SendableRecordBatchStream> {
        let start = Instant::now();
        let mut metrics = Metrics::default();
        let use_parallel = self.use_parallel_reader();
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let mut reader = if use_parallel {
            self.build_parallel_reader().await?
        } else {
            self.build_reader().await?
        };
        let elapsed = start.elapsed();
        metrics.build_reader_cost = elapsed;
        metrics.scan_cost = elapsed;

        // Creates a stream to poll the batch reader and convert batch into record batch.
        let mapper = self.mapper.clone();
        let cache_manager = self.cache_manager.clone();
        let parallelism = self.parallelism.parallelism;
        let stream = try_stream! {
            let cache = cache_manager.as_ref().map(|cache| cache.as_ref());
            while let Some(batch) =
                Self::fetch_record_batch(&mut reader, &mapper, cache, &mut metrics).await?
            {
                yield batch;
            }

            debug!(
                "Seq scan finished, region_id: {:?}, metrics: {:?}, use_parallel: {}, parallelism: {}",
                mapper.metadata().region_id, metrics, use_parallel, parallelism,
            );
            // Update metrics.
            READ_STAGE_ELAPSED.with_label_values(&["total"]).observe(metrics.scan_cost.as_secs_f64());
        };
        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Builds a [BoxedBatchReader] from sequential scan.
    pub async fn build_reader(&self) -> Result<BoxedBatchReader> {
        // Scans all memtables and SSTs. Builds a merge reader to merge results.
        let sources = self.build_sources().await?;
        let mut builder = MergeReaderBuilder::from_sources(sources);
        Ok(Box::new(builder.build().await?))
    }

    /// Builds a [BoxedBatchReader] that can scan memtables and SSTs in parallel.
    async fn build_parallel_reader(&self) -> Result<BoxedBatchReader> {
        assert!(self.parallelism.allow_parallel_scan());
        // Scall all memtables and SSTs.
        let sources = self.build_sources().await?;
        let semaphore = Arc::new(Semaphore::new(self.parallelism.parallelism));
        // Spawn a task for each source.
        let sources = sources
            .into_iter()
            .map(|source| {
                let stream = self.spawn_scan_task(source, semaphore.clone());
                Source::Stream(stream)
            })
            .collect();
        let mut builder = MergeReaderBuilder::from_sources(sources);
        Ok(Box::new(builder.build().await?))
    }

    /// Builds and returns sources to read.
    async fn build_sources(&self) -> Result<Vec<Source>> {
        let mut sources = Vec::with_capacity(self.memtables.len() + self.files.len());
        for mem in &self.memtables {
            let iter = mem.iter(Some(self.mapper.column_ids()), self.predicate.clone());
            sources.push(Source::Iter(iter));
        }
        for file in &self.files {
            let maybe_reader = self
                .access_layer
                .read_sst(file.clone())
                .predicate(self.predicate.clone())
                .time_range(self.time_range)
                .projection(Some(self.mapper.column_ids().to_vec()))
                .cache(self.cache_manager.clone())
                .build()
                .await;
            let reader = match maybe_reader {
                Ok(reader) => reader,
                Err(e) => {
                    if e.is_object_not_found() && self.ignore_file_not_found {
                        error!(e; "File to scan does not exist, region_id: {}, file: {}", file.region_id(), file.file_id());
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
            if compat::has_same_columns(self.mapper.metadata(), reader.metadata()) {
                sources.push(Source::Reader(Box::new(reader)));
            } else {
                // They have different schema. We need to adapt the batch first so the
                // mapper can convert it.
                let compat_reader =
                    CompatReader::new(&self.mapper, reader.metadata().clone(), reader)?;
                sources.push(Source::Reader(Box::new(compat_reader)));
            }
        }

        Ok(sources)
    }

    /// Returns whether to use a parallel reader.
    fn use_parallel_reader(&self) -> bool {
        self.parallelism.allow_parallel_scan() && (self.files.len() + self.memtables.len()) > 1
    }

    /// Scan the input source in another task.
    fn spawn_scan_task(&self, mut input: Source, semaphore: Arc<Semaphore>) -> BoxedBatchStream {
        let (sender, receiver) = mpsc::channel(self.parallelism.channel_size);
        tokio::spawn(async move {
            loop {
                // We release the permit before sending result to avoid the task waiting on
                // the channel with the permit holded
                let maybe_batch = {
                    // Safety: We never close the semaphore.
                    let _permit = semaphore.acquire().await.unwrap();
                    input.next_batch().await
                };
                match maybe_batch {
                    Ok(Some(batch)) => {
                        let _ = sender.send(Ok(batch)).await;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Box::pin(ReceiverStream::new(receiver))
    }

    /// Fetch a batch from the reader and convert it into a record batch.
    async fn fetch_record_batch(
        reader: &mut dyn BatchReader,
        mapper: &ProjectionMapper,
        cache: Option<&CacheManager>,
        metrics: &mut Metrics,
    ) -> common_recordbatch::error::Result<Option<RecordBatch>> {
        let start = Instant::now();

        let Some(batch) = reader
            .next_batch()
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
        else {
            metrics.scan_cost += start.elapsed();

            return Ok(None);
        };

        let convert_start = Instant::now();
        let record_batch = mapper.convert(&batch, cache)?;
        metrics.convert_cost += convert_start.elapsed();
        metrics.scan_cost += start.elapsed();

        Ok(Some(record_batch))
    }
}

/// Metrics for [SeqScan].
#[derive(Debug, Default)]
struct Metrics {
    /// Duration to build the reader.
    build_reader_cost: Duration,
    /// Duration to scan data.
    scan_cost: Duration,
    /// Duration to convert batches.
    convert_cost: Duration,
}

#[cfg(test)]
impl SeqScan {
    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        self.memtables.len()
    }

    /// Returns number of SST files to scan.
    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        self.files.iter().map(|file| file.file_id()).collect()
    }
}
