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

//! Arrow to Parquet writer that encodes the columns of a row group in parallel.

use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common_base::cancellation::Cancelled;
use common_runtime::JoinHandle;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves,
};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{KeyValue, ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use tokio_util::sync::CancellationToken;

use crate::sst::parquet::writer::SstWriteGuard;

/// Runtime whose blocking pool encodes the columns of a writer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EncodeRuntime {
    /// The global runtime.
    #[default]
    Global,
    /// The compaction runtime.
    Compact,
}

impl EncodeRuntime {
    fn spawn<F, R>(self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match self {
            EncodeRuntime::Global => common_runtime::spawn_blocking_global(f),
            EncodeRuntime::Compact => common_runtime::spawn_blocking_compact(f),
        }
    }
}

/// Column writers of one top-level field: its index and its leaf writers.
type FieldWriters = (usize, Vec<(usize, ArrowColumnWriter)>);

/// Output of one encode task: column chunks by leaf index, and the time spent
/// on each field.
#[derive(Default)]
pub(crate) struct EncodedFields {
    chunks: Vec<(usize, ArrowColumnChunk)>,
    costs: Vec<(usize, Duration)>,
    // Kept after chunks so dropping an unobserved JoinHandle result releases
    // its encoded buffers before the execution admission.
    write_guard: Option<Arc<dyn SstWriteGuard>>,
}

/// Writes Parquet like [`parquet::arrow::AsyncArrowWriter`], but encodes the
/// columns of each row group on up to `parallelism` blocking tasks.
///
/// A row group encodes in the background while the next one is buffered, so at
/// most two row groups of input are held in memory. Every column writer sees the
/// same input as with [`ArrowWriter`], so the output is byte-for-byte identical.
/// The bytes of a row group reach the async writer when the next row group
/// starts or on [`Self::flush`], one row group later than with `AsyncArrowWriter`.
pub struct ParallelArrowWriter<W> {
    /// Assembles row groups and the footer into an in-memory buffer.
    file_writer: SerializedFileWriter<Vec<u8>>,
    /// Creates the column writers of each row group.
    factory: Arc<ArrowRowGroupWriterFactory>,
    arrow_schema: SchemaRef,
    /// Top-level field of each leaf column.
    leaf_fields: Vec<usize>,
    /// Encode time of each field in the last row group, used to balance tasks.
    field_costs: Vec<Duration>,
    async_writer: W,
    max_row_group_rows: usize,
    parallelism: usize,
    runtime: EncodeRuntime,
    /// Slices of input buffered for the next row group.
    buffered: Vec<RecordBatch>,
    buffered_rows: usize,
    /// Tasks encoding the previous row group.
    encoding: Vec<JoinHandle<Result<EncodedFields>>>,
    num_row_groups: usize,
    cancellation: Option<CancellationToken>,
    /// Bounded writers encode columns concurrently but do not overlap row groups.
    working_limit: Option<usize>,
    buffered_bytes: usize,
    /// Output ownership may also retain execution admission until detached
    /// blocking encoders finish after their async writer is cancelled.
    write_guard: Option<Arc<dyn SstWriteGuard>>,
}

impl<W: AsyncFileWriter> ParallelArrowWriter<W> {
    /// Returns whether the writer can reproduce [`ArrowWriter`]'s row groups for
    /// `props`, which holds only when row groups are limited by row count alone.
    pub fn supports(props: &WriterProperties) -> bool {
        props.max_row_group_row_count().is_some() && props.max_row_group_bytes().is_none()
    }

    /// Creates a writer. `props` must be [supported](Self::supports).
    pub fn try_new(
        async_writer: W,
        arrow_schema: SchemaRef,
        props: WriterProperties,
        parallelism: usize,
        runtime: EncodeRuntime,
    ) -> Result<Self> {
        let max_row_group_rows = match props.max_row_group_row_count() {
            Some(rows) if Self::supports(&props) => rows.max(1),
            _ => {
                return Err(ParquetError::General(
                    "parallel writer requires a row-count-only row group limit".to_string(),
                ));
            }
        };
        let (file_writer, factory) =
            ArrowWriter::try_new(Vec::new(), arrow_schema.clone(), Some(props))?
                .into_serialized_writer()?;
        let schema = file_writer.schema_descr();
        let leaf_fields = (0..schema.num_columns())
            .map(|leaf| schema.get_column_root_idx(leaf))
            .collect();
        let num_fields = arrow_schema.fields().len();
        Ok(Self {
            file_writer,
            factory: Arc::new(factory),
            arrow_schema,
            leaf_fields,
            field_costs: vec![Duration::ZERO; num_fields],
            async_writer,
            max_row_group_rows,
            parallelism: parallelism.max(1),
            runtime,
            buffered: Vec::new(),
            buffered_rows: 0,
            encoding: Vec::new(),
            num_row_groups: 0,
            cancellation: None,
            working_limit: None,
            buffered_bytes: 0,
            write_guard: None,
        })
    }

    pub(crate) fn with_cancellation(mut self, cancellation: Option<CancellationToken>) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub(crate) fn with_write_guard(mut self, guard: Option<Arc<dyn SstWriteGuard>>) -> Self {
        self.write_guard = guard;
        self
    }

    pub(crate) fn with_working_limit(mut self, limit: Option<usize>) -> Self {
        self.working_limit = limit;
        self
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.buffered_bytes
    }

    pub(crate) fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
        self.file_writer.flushed_row_groups()
    }

    /// Buffers `batch`, starting to encode each row group once it is full.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            check_cancelled(self.cancellation.as_ref())?;
            let len = (self.max_row_group_rows - self.buffered_rows).min(batch.num_rows() - offset);
            let slice = batch.slice(offset, len);
            if let Some(limit) = self.working_limit {
                // Count retained buffers, including dictionary values. Shared buffers may
                // be counted more than once; the bounded path is conservative.
                self.buffered_bytes = self
                    .buffered_bytes
                    .saturating_add(slice.get_array_memory_size());
                check_working_memory(self.buffered_bytes, limit)?;
            }
            self.buffered.push(slice);
            self.buffered_rows += len;
            offset += len;
            if self.buffered_rows == self.max_row_group_rows {
                self.encode_buffered().await?;
                if self.working_limit.is_some() {
                    // The second workspace reserved by the caller covers finalization,
                    // not another raw row group alongside the column encoders.
                    self.finish_encoding().await?;
                }
            }
        }
        Ok(())
    }

    /// Writes all buffered rows as a row group, like [`ArrowWriter::flush`].
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffered_rows > 0 {
            self.encode_buffered().await?;
        }
        self.finish_encoding().await
    }

    /// Writes the remaining rows and the footer, then completes the async writer.
    pub async fn close(mut self) -> Result<ParquetMetaData> {
        self.finish().await
    }

    pub(crate) fn append_key_value_metadata(&mut self, kv: KeyValue) {
        self.file_writer.append_key_value_metadata(kv);
    }

    /// Finalizes the file while retaining the upload for abort if it fails.
    pub(crate) async fn finish(&mut self) -> Result<ParquetMetaData> {
        self.flush().await?;
        let metadata = self.file_writer.finish()?;
        self.write_buffered_bytes().await?;
        self.async_writer.complete().await?;
        Ok(metadata)
    }

    /// Discards buffered input and waits for encoders without writing a failed SST.
    pub(crate) async fn abort(mut self) -> W {
        self.buffered.clear();
        abort_encoding_tasks(&mut self.encoding).await;
        self.async_writer
    }

    /// Installs controlled tasks to test SST write-error cleanup independently of codec timing.
    #[cfg(test)]
    pub(crate) fn set_encoding_tasks_for_test(
        &mut self,
        tasks: Vec<JoinHandle<Result<EncodedFields>>>,
    ) {
        assert!(self.encoding.is_empty());
        self.encoding = tasks;
    }

    /// Starts encoding the buffered rows once the previous row group is written.
    async fn encode_buffered(&mut self) -> Result<()> {
        self.finish_encoding().await?;
        check_cancelled(self.cancellation.as_ref())?;

        let batches: Arc<[RecordBatch]> = mem::take(&mut self.buffered).into();
        self.buffered_rows = 0;
        let budget = self.working_limit.map(|limit| {
            Arc::new(EncodingBudget {
                bytes: AtomicUsize::new(self.buffered_bytes),
                limit,
            })
        });
        self.buffered_bytes = 0;
        let writers = self.factory.create_column_writers(self.num_row_groups)?;
        if let Some(budget) = budget.as_ref() {
            for writer in &writers {
                budget.observe(&mut 0, writer.memory_size())?;
            }
        }
        self.num_row_groups += 1;

        // One array produces all leaves of a field, so a field is never split.
        let mut fields: Vec<FieldWriters> = (0..self.arrow_schema.fields().len())
            .map(|field| (field, Vec::new()))
            .collect();
        for (leaf, writer) in writers.into_iter().enumerate() {
            fields[self.leaf_fields[leaf]].1.push((leaf, writer));
        }
        self.encoding = assign_fields(fields, &self.field_costs, self.parallelism)
            .into_iter()
            .map(|fields| {
                let batches = batches.clone();
                let schema = self.arrow_schema.clone();
                let cancellation = self.cancellation.clone();
                let budget = budget.clone();
                let write_guard = self.write_guard.clone();
                self.runtime.spawn(move || {
                    let mut result = encode_fields(
                        &schema,
                        &batches,
                        fields,
                        cancellation.as_ref(),
                        budget.as_deref(),
                    );
                    // A dropped async receiver does not stop a running blocking task.
                    // Retain admission with returned chunks as well as the input arrays.
                    drop(batches);
                    if let Ok(encoded) = &mut result {
                        encoded.write_guard = write_guard;
                    }
                    result
                })
            })
            .collect();
        Ok(())
    }

    /// Waits for the row group being encoded and appends it to the file.
    async fn finish_encoding(&mut self) -> Result<()> {
        if self.encoding.is_empty() {
            return Ok(());
        }
        let mut guards = Vec::new();
        let mut chunks = Vec::with_capacity(self.leaf_fields.len());
        let mut tasks = mem::take(&mut self.encoding).into_iter();
        while let Some(task) = tasks.next() {
            let encoded = task
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
                .and_then(|encoded| encoded);
            let EncodedFields {
                chunks: task_chunks,
                costs,
                write_guard,
            } = match encoded {
                Ok(encoded) => encoded,
                Err(error) => {
                    abort_encoding_tasks(tasks.as_mut_slice()).await;
                    return Err(error);
                }
            };
            guards.push(write_guard);
            chunks.extend(task_chunks);
            for (field, cost) in costs {
                self.field_costs[field] = cost;
            }
        }
        chunks.sort_unstable_by_key(|(leaf, _)| *leaf);
        let mut row_group = self.file_writer.next_row_group()?;
        for (_, chunk) in chunks {
            chunk.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        if let Some(limit) = self.working_limit {
            check_working_memory(self.file_writer.inner_mut().capacity(), limit)?;
        }
        let result = self.write_buffered_bytes().await;
        drop(guards);
        result
    }

    /// Moves the bytes assembled so far to the async writer.
    async fn write_buffered_bytes(&mut self) -> Result<()> {
        let bytes = mem::take(self.file_writer.inner_mut());
        if bytes.is_empty() {
            return Ok(());
        }
        self.async_writer.write(Bytes::from(bytes)).await
    }
}

/// Aborts queued blocking work and joins running work before its budget is released.
async fn abort_encoding_tasks(tasks: &mut [JoinHandle<Result<EncodedFields>>]) {
    for task in tasks.iter() {
        task.abort();
    }
    for task in tasks {
        // Cleanup failures must not replace the original encoding or source error.
        let _ = task.await;
    }
}

/// Splits `fields` into at most `parallelism` tasks, giving the most expensive
/// field to the least loaded task first. Fields cost nothing before any row
/// group is measured, which spreads them evenly.
fn assign_fields(
    mut fields: Vec<FieldWriters>,
    costs: &[Duration],
    parallelism: usize,
) -> Vec<Vec<FieldWriters>> {
    let num_tasks = parallelism.min(fields.len()).max(1);
    fields.sort_by_key(|(field, _)| std::cmp::Reverse(costs[*field]));
    let mut tasks: Vec<(Duration, Vec<FieldWriters>)> = (0..num_tasks)
        .map(|_| (Duration::ZERO, Vec::new()))
        .collect();
    for field in fields {
        let key = |(load, fields): &(Duration, Vec<FieldWriters>)| (*load, fields.len());
        let mut lightest = 0;
        for task in 1..tasks.len() {
            if key(&tasks[task]) < key(&tasks[lightest]) {
                lightest = task;
            }
        }
        tasks[lightest].0 += costs[field.0];
        tasks[lightest].1.push(field);
    }
    tasks
        .into_iter()
        .map(|(_, fields)| fields)
        .filter(|fields| !fields.is_empty())
        .collect()
}

fn check_cancelled(cancellation: Option<&CancellationToken>) -> Result<()> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(ParquetError::External(Box::new(Cancelled)));
    }
    Ok(())
}

/// Tracks the sum of per-column high-water marks until the entire row group is
/// written. Completed column chunks remain charged while sibling tasks run.
struct EncodingBudget {
    bytes: AtomicUsize,
    limit: usize,
}

impl EncodingBudget {
    fn observe(&self, peak: &mut usize, bytes: usize) -> Result<()> {
        if bytes > *peak {
            let growth = bytes - *peak;
            *peak = bytes;
            let previous = self
                .bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                    Some(old.saturating_add(growth))
                })
                .unwrap();
            check_working_memory(previous.saturating_add(growth), self.limit)?;
        }
        Ok(())
    }
}

fn check_working_memory(bytes: usize, limit: usize) -> Result<()> {
    if bytes > limit {
        return Err(ParquetError::External(Box::new(
            crate::error::SstWriteMemoryExceededSnafu {
                kind: "encoder",
                bytes,
                limit,
            }
            .build(),
        )));
    }
    Ok(())
}

/// Encodes `fields` of all `batches` and closes their column writers.
fn encode_fields(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    fields: Vec<FieldWriters>,
    cancellation: Option<&CancellationToken>,
    budget: Option<&EncodingBudget>,
) -> Result<EncodedFields> {
    let mut chunks = Vec::new();
    let mut costs = Vec::with_capacity(fields.len());
    for (field, mut writers) in fields {
        let start = Instant::now();
        // Initial writer allocations were charged before any encode task started.
        let mut peaks = writers
            .iter()
            .map(|(_, writer)| writer.memory_size())
            .collect::<Vec<_>>();
        for batch in batches {
            check_cancelled(cancellation)?;
            let leaves = compute_leaves(schema.field(field), batch.column(field))?;
            if leaves.len() != writers.len() {
                return Err(ParquetError::General(format!(
                    "field {} has {} leaf arrays but {} column writers",
                    schema.field(field).name(),
                    leaves.len(),
                    writers.len()
                )));
            }
            for ((leaf, (_, writer)), peak) in leaves.iter().zip(writers.iter_mut()).zip(&mut peaks)
            {
                check_cancelled(cancellation)?;
                writer.write(leaf)?;
                if let Some(budget) = budget {
                    budget.observe(peak, writer.memory_size())?;
                }
            }
        }
        for ((leaf, writer), mut peak) in writers.into_iter().zip(peaks) {
            check_cancelled(cancellation)?;
            let chunk = writer.close()?;
            if let Some(budget) = budget {
                budget.observe(
                    &mut peak,
                    usize::try_from(chunk.close().bytes_written).unwrap_or(usize::MAX),
                )?;
            }
            chunks.push((leaf, chunk));
        }
        costs.push((field, start.elapsed()));
    }
    Ok(EncodedFields {
        chunks,
        costs,
        write_guard: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use common_runtime::runtime::{BuilderBuild, RuntimeTrait};
    use datatypes::arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema, UInt32Type};
    use parquet::basic::{Compression, ZstdLevel};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;
    use crate::compaction::memory_manager::new_compaction_memory_manager;

    fn schema() -> SchemaRef {
        let nested = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        Arc::new(Schema::new(vec![
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, true),
            Field::new(
                "tag",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("nested", DataType::Struct(nested), true),
        ]))
    }

    fn batch(start: usize, len: usize) -> RecordBatch {
        let rows = start..start + len;
        let ints = Int64Array::from_iter_values(rows.clone().map(|i| i as i64 * 7));
        let floats = Float64Array::from_iter(
            rows.clone()
                .map(|i| (i % 5 != 0).then_some((i % 13) as f64 / 3.0)),
        );
        let tags: DictionaryArray<UInt32Type> = rows
            .clone()
            .map(|i| format!("tag-{}", i % 3))
            .collect::<Vec<_>>()
            .iter()
            .map(String::as_str)
            .collect();
        let nested = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from_iter(
                    rows.clone().map(|i| (i % 4 != 0).then_some(i as i32)),
                )) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from_iter(
                    rows.map(|i| (i % 6 != 0).then(|| format!("v{i}"))),
                )) as ArrayRef,
            ),
        ]);
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(ints),
                Arc::new(floats),
                Arc::new(tags),
                Arc::new(nested),
            ],
        )
        .unwrap()
    }

    fn props(row_group_rows: usize) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_row_count(Some(row_group_rows))
            .set_data_page_row_count_limit(16)
            .build()
    }

    /// One step of a write sequence: a batch of `len` rows, or a flush.
    #[derive(Clone, Copy)]
    enum Step {
        Write(usize),
        Flush,
    }

    fn write_serial(steps: &[Step], props: WriterProperties) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema(), Some(props)).unwrap();
        let mut start = 0;
        for step in steps {
            match *step {
                Step::Write(len) => {
                    writer.write(&batch(start, len)).unwrap();
                    start += len;
                }
                Step::Flush => writer.flush().unwrap(),
            }
        }
        writer.close().unwrap();
        buf
    }

    async fn write_parallel(
        steps: &[Step],
        props: WriterProperties,
        parallelism: usize,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ParallelArrowWriter::try_new(
            &mut buf,
            schema(),
            props,
            parallelism,
            EncodeRuntime::Global,
        )
        .unwrap();
        let mut start = 0;
        for step in steps {
            match *step {
                Step::Write(len) => {
                    writer.write(&batch(start, len)).await.unwrap();
                    start += len;
                }
                Step::Flush => writer.flush().await.unwrap(),
            }
        }
        writer.close().await.unwrap();
        buf
    }

    #[tokio::test]
    async fn test_bounded_parallel_writer_matches_arrow_writer() {
        let steps = [
            Step::Write(13),
            Step::Write(120),
            Step::Flush,
            Step::Write(9),
        ];
        let expected = write_serial(&steps, props(64));
        for parallelism in [1, 2, 4] {
            let mut output = Vec::new();
            let mut writer = ParallelArrowWriter::try_new(
                &mut output,
                schema(),
                props(64),
                parallelism,
                EncodeRuntime::Global,
            )
            .unwrap()
            .with_working_limit(Some(64 * 1024 * 1024));
            let mut start = 0;
            for step in steps {
                match step {
                    Step::Write(len) => {
                        writer.write(&batch(start, len)).await.unwrap();
                        start += len;
                        assert!(
                            writer.encoding.is_empty(),
                            "bounded writes must join row-group encoders"
                        );
                    }
                    Step::Flush => writer.flush().await.unwrap(),
                }
            }
            writer.close().await.unwrap();
            assert_eq!(expected, output);
        }
    }

    #[tokio::test]
    async fn test_bounded_parallel_writer_checks_buffered_and_encoder_memory() {
        for limit in [1, batch(0, 64).get_array_memory_size()] {
            let mut output = Vec::new();
            let mut writer = ParallelArrowWriter::try_new(
                &mut output,
                schema(),
                props(64),
                4,
                EncodeRuntime::Global,
            )
            .unwrap()
            .with_working_limit(Some(limit));
            let error = writer.write(&batch(0, 64)).await.unwrap_err();
            let ParquetError::External(source) = error else {
                panic!("Expected a typed memory budget error, got {error:?}");
            };
            assert!(
                matches!(
                    source.downcast_ref::<crate::error::Error>(),
                    Some(crate::error::Error::SstWriteMemoryExceeded { .. })
                ),
                "Unexpected budget error: {source:?}"
            );
            writer.abort().await;
        }
    }

    #[test]
    fn test_encoding_budget_retains_finished_column_peaks() {
        let budget = EncodingBudget {
            bytes: AtomicUsize::new(10),
            limit: 30,
        };
        let mut first = 0;
        budget.observe(&mut first, 12).unwrap();
        budget.observe(&mut first, 4).unwrap();
        assert_eq!(22, budget.bytes.load(Ordering::Relaxed));
        let mut second = 0;
        assert!(budget.observe(&mut second, 9).is_err());
    }

    #[tokio::test]
    async fn test_parallel_writer_matches_arrow_writer() {
        use Step::*;
        let cases: Vec<(&str, Vec<Step>)> = vec![
            ("empty", vec![]),
            ("partial row group", vec![Write(10)]),
            ("exact row groups", vec![Write(64), Write(64)]),
            (
                "batches across row groups",
                vec![Write(7), Write(100), Write(3), Write(250), Write(1)],
            ),
            ("batch spanning many row groups", vec![Write(1000)]),
            (
                "explicit flushes",
                vec![Write(10), Flush, Flush, Write(70), Flush, Write(5)],
            ),
        ];
        for (name, steps) in cases {
            let expected = write_serial(&steps, props(64));
            for parallelism in [1, 2, 3, 16] {
                let actual = write_parallel(&steps, props(64), parallelism).await;
                assert!(
                    expected == actual,
                    "case {name}, parallelism {parallelism}: {} bytes != {} bytes",
                    expected.len(),
                    actual.len()
                );
            }
        }
    }

    #[rstest::rstest]
    #[case::encode_error(false)]
    #[case::encode_panic(true)]
    #[tokio::test]
    async fn test_encode_failure_drains_tasks_before_releasing_budget(#[case] panics: bool) {
        let runtime: common_runtime::Runtime = common_runtime::Builder::default()
            .worker_threads(1)
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let mut writer = ParallelArrowWriter::try_new(
            Vec::new(),
            schema(),
            props(64),
            3,
            EncodeRuntime::Compact,
        )
        .unwrap();
        let memory = new_compaction_memory_manager(1024 * 1024);
        let budget = memory.try_acquire(memory.limit_bytes()).unwrap();
        let batches: Arc<[RecordBatch]> = vec![batch(0, 4)].into();
        let retained_batches = Arc::downgrade(&batches);

        let failed = runtime.spawn_blocking(move || -> Result<EncodedFields> {
            if panics {
                panic!("encode task failed");
            }
            Err(ParquetError::General("encode task failed".to_string()))
        });
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let column = writer.factory.create_column_writers(0).unwrap().remove(0);
        let running_batches = batches.clone();
        let running = runtime.spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.blocking_recv().unwrap();
            let _encoded = encode_fields(
                &schema(),
                &running_batches,
                vec![(0, vec![(0, column)])],
                None,
                None,
            )?;
            Err(ParquetError::General("later encoding failure".to_string()))
        });
        // With one blocking thread, this also proves the first task has ended.
        timeout(Duration::from_secs(10), started_rx)
            .await
            .unwrap()
            .unwrap();
        let queued_started = Arc::new(AtomicBool::new(false));
        let queued_started_task = queued_started.clone();
        let queued = runtime.spawn_blocking(move || {
            queued_started_task.store(true, Ordering::SeqCst);
            drop(batches);
            Ok(EncodedFields::default())
        });
        writer.encoding = vec![failed, running, queued];

        // Model the compaction caller's guard: it is released when flush returns.
        let mut finish = Box::pin(async {
            let _budget = budget;
            writer.flush().await
        });
        assert!(futures::poll!(finish.as_mut()).is_pending());
        assert!(memory.try_acquire(memory.limit_bytes()).is_none());
        assert!(retained_batches.upgrade().is_some());

        release_tx.send(()).unwrap();
        let error = timeout(Duration::from_secs(10), finish)
            .await
            .unwrap()
            .unwrap_err();
        if panics {
            let ParquetError::External(source) = error else {
                panic!("expected the original task panic, got {error}");
            };
            assert!(
                source
                    .downcast_ref::<common_runtime::JoinError>()
                    .unwrap()
                    .is_panic()
            );
        } else {
            assert!(
                matches!(error, ParquetError::General(message) if message == "encode task failed")
            );
        }
        assert!(!queued_started.load(Ordering::SeqCst));
        assert!(retained_batches.upgrade().is_none());
        assert!(memory.try_acquire(memory.limit_bytes()).is_some());
        assert!(writer.file_writer.flushed_row_groups().is_empty());
        assert!(writer.async_writer.is_empty());
    }

    #[test]
    fn test_parallel_writer_requires_row_count_limit() {
        assert!(ParallelArrowWriter::<Vec<u8>>::supports(&props(64)));
        let byte_limited = WriterProperties::builder()
            .set_max_row_group_bytes(Some(1024))
            .build();
        assert!(!ParallelArrowWriter::<Vec<u8>>::supports(&byte_limited));
        let unlimited = WriterProperties::builder()
            .set_max_row_group_row_count(None)
            .build();
        assert!(!ParallelArrowWriter::<Vec<u8>>::supports(&unlimited));
        assert!(
            ParallelArrowWriter::try_new(Vec::new(), schema(), unlimited, 2, EncodeRuntime::Global)
                .is_err()
        );
    }

    #[test]
    fn test_assign_fields_balances_measured_costs() {
        let (file_writer, factory) = ArrowWriter::try_new(Vec::new(), schema(), Some(props(64)))
            .unwrap()
            .into_serialized_writer()
            .unwrap();
        let schema_descr = file_writer.schema_descr();
        let mut fields: Vec<FieldWriters> = (0..4).map(|field| (field, Vec::new())).collect();
        for (leaf, writer) in factory
            .create_column_writers(0)
            .unwrap()
            .into_iter()
            .enumerate()
        {
            fields[schema_descr.get_column_root_idx(leaf)]
                .1
                .push((leaf, writer));
        }
        // The nested field owns both struct leaves.
        assert_eq!(2, fields[3].1.len());

        let costs = [10, 9, 1, 1].map(Duration::from_millis);
        let tasks = assign_fields(fields, &costs, 2);
        let mut loads: Vec<_> = tasks
            .iter()
            .map(|task| {
                task.iter()
                    .map(|(field, _)| costs[*field])
                    .sum::<Duration>()
            })
            .collect();
        loads.sort();
        assert_eq!(
            vec![Duration::from_millis(10), Duration::from_millis(11)],
            loads
        );
        let mut assigned: Vec<_> = tasks.iter().flatten().map(|(field, _)| *field).collect();
        assigned.sort();
        assert_eq!(vec![0, 1, 2, 3], assigned);
    }
}
