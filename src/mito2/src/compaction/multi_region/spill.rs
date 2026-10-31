// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Attempt-local ordered runs. All encoding, iteration and file I/O methods are
//! synchronous and must be called on the blocking compaction pool. Drop only
//! schedules cleanup, so cancelling an async owner never performs file I/O inline.

use std::collections::{HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common_base::cancellation::CancellationHandle;
use common_telemetry::warn;
use datatypes::arrow::array::ArrayData;
use datatypes::arrow::ipc::reader::StreamReader;
use datatypes::arrow::ipc::writer::StreamWriter;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt, ensure};
use tokio_util::sync::CancellationToken;

use crate::compaction::multi_region::{MultiRegionCompactionContext, MultiRegionCompactionPlan};
use crate::error::{
    ComputeArrowSnafu, MultiRegionCompactionResourceSnafu, MultiRegionCompactionSpillSnafu, Result,
};
use crate::memtable::BoxedRecordBatchIterator;

pub(crate) struct RunStore {
    memory: Arc<Budget>,
    metadata: Arc<Budget>,
    disk: Arc<Budget>,
    directory: Arc<SpillDirectory>,
    cancellation: Arc<CancellationHandle>,
    stop: CancellationToken,
    batch_limit: usize,
}

impl RunStore {
    pub(crate) async fn new(
        plan: &MultiRegionCompactionPlan,
        context: &MultiRegionCompactionContext,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        let path = context
            .spill_directory
            .join(uuid::Uuid::new_v4().to_string());
        // Install the guard before awaiting creation: cancellation must not leak a directory.
        let directory = Arc::new(SpillDirectory(path));
        let creating = directory.clone();
        common_runtime::spawn_blocking_compact(move || {
            std::fs::create_dir_all(&creating.0).context(MultiRegionCompactionSpillSnafu)
        })
        .await
        .context(crate::error::JoinSnafu)??;
        Ok(Arc::new(Self {
            memory: Budget::new(
                (plan.memory_limit / 2) as u64,
                "Routed batch cache exhausted",
            ),
            metadata: Budget::new(
                (plan.memory_limit / 8) as u64,
                "Too many regions or time windows",
            ),
            disk: Budget::new(
                plan.spill_limit,
                "Multi-region temporary disk quota exhausted",
            ),
            directory,
            cancellation: context.cancellation.clone(),
            stop,
            batch_limit: plan.memory_limit / 8,
        }))
    }

    pub(crate) fn reserve_metadata(&self, bytes: usize) -> Result<Reservation> {
        self.metadata
            .reserve(bytes as u64)
            .context(MultiRegionCompactionResourceSnafu {
                reason: self.metadata.reason,
            })
    }

    pub(crate) fn check_cancelled(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled() && !self.cancellation.is_cancelled(),
            crate::error::CompactionCancelledSnafu
        );
        Ok(())
    }

    pub(crate) fn spilled_bytes(&self) -> u64 {
        self.disk.total.load(Ordering::Relaxed)
    }

    pub(crate) fn peak_buffered_bytes(&self) -> u64 {
        self.memory.peak.load(Ordering::Relaxed)
    }
}

struct Budget {
    limit: u64,
    used: AtomicU64,
    peak: AtomicU64,
    total: AtomicU64,
    reason: &'static str,
}

impl Budget {
    fn new(limit: u64, reason: &'static str) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            total: AtomicU64::new(0),
            reason,
        })
    }

    fn reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        let previous = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .ok()?;
        self.peak.fetch_max(previous + bytes, Ordering::Relaxed);
        self.total.fetch_add(bytes, Ordering::Relaxed);
        Some(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}

pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
}

impl Reservation {
    fn grow(&mut self, bytes: u64) -> Result<()> {
        let mut extra = self
            .budget
            .reserve(bytes)
            .context(MultiRegionCompactionResourceSnafu {
                reason: self.budget.reason,
            })?;
        self.bytes += extra.bytes;
        extra.bytes = 0;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

struct SpillDirectory(PathBuf);

impl Drop for SpillDirectory {
    fn drop(&mut self) {
        let path = self.0.clone();
        common_runtime::spawn_blocking_compact(move || {
            if let Err(error) = std::fs::remove_dir_all(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(error; "Failed to remove multi-region spill directory: {}", path.display());
            }
        });
    }
}

pub(crate) struct BufferedBatch {
    batch: RecordBatch,
    _reservation: Reservation,
}

/// A single sorted stream, not an arbitrary concatenation of different sources.
pub(crate) enum Run {
    Memory(VecDeque<BufferedBatch>),
    Disk(DiskRun),
}

impl Default for Run {
    fn default() -> Self {
        Self::Memory(VecDeque::new())
    }
}

impl Run {
    pub(crate) fn from_batch(batch: RecordBatch, store: &RunStore) -> Result<Self> {
        let mut run = Self::default();
        run.push(batch, store)?;
        Ok(run)
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Memory(batches) if batches.is_empty())
    }

    pub(crate) fn push(&mut self, batch: RecordBatch, store: &RunStore) -> Result<()> {
        store.check_cancelled()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let bytes = batch_memory_size(&batch);
        ensure!(
            bytes <= store.batch_limit,
            MultiRegionCompactionResourceSnafu {
                reason: format!(
                    "Run batch of {} rows uses {bytes} bytes, exceeding working-batch budget {}",
                    batch.num_rows(),
                    store.batch_limit
                ),
            }
        );
        if let Self::Memory(batches) = self {
            // Account for both Arrow storage and per-batch queue/lease metadata.
            if let Some(reservation) = store.memory.reserve((bytes + 128) as u64) {
                batches.push_back(BufferedBatch {
                    batch,
                    _reservation: reservation,
                });
                return Ok(());
            }
            let mut disk = DiskRun::new(store);
            while let Some(buffered) = batches.pop_front() {
                disk.append(&buffered.batch, store)?;
            }
            *self = Self::Disk(disk);
        }
        if let Self::Disk(disk) = self {
            disk.append(&batch, store)?;
        }
        Ok(())
    }

    /// Appends fragments from the same source, in source order.
    pub(crate) fn append(&mut self, other: Self, store: &RunStore) -> Result<()> {
        if self.is_empty() {
            *self = other;
            return Ok(());
        }
        for batch in other.into_iter()? {
            self.push(batch?, store)?;
        }
        Ok(())
    }

    pub(crate) fn into_iter(self) -> Result<BoxedRecordBatchIterator> {
        match self {
            Self::Memory(batches) => Ok(Box::new(
                batches.into_iter().map(|buffered| Ok(buffered.batch)),
            )),
            Self::Disk(run) => {
                let file = File::open(&run.path).context(MultiRegionCompactionSpillSnafu)?;
                let remaining = run.reservation.as_ref().map_or(0, |lease| lease.bytes);
                Ok(Box::new(DiskIterator {
                    file: Some(file),
                    run,
                    remaining,
                }))
            }
        }
    }
}

/// IPC readers can share one allocation across several column buffers. Arrow's
/// per-array estimate charges that entire allocation for every slice, rejecting
/// valid batches after spilling. Deduplicate allocations within this batch, but
/// retain Arrow's metadata cost and conservatively charge shared batches separately.
fn batch_memory_size(batch: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    let duplicated: usize = batch
        .columns()
        .iter()
        .map(|array| duplicated_buffer_bytes(&array.to_data(), &mut seen))
        .sum();
    batch.get_array_memory_size() - duplicated
}

fn duplicated_buffer_bytes(data: &ArrayData, seen: &mut HashSet<NonNull<u8>>) -> usize {
    let duplicated: usize = data
        .buffers()
        .iter()
        .chain(data.nulls().map(|nulls| nulls.buffer()))
        .filter(|buffer| !seen.insert(buffer.data_ptr()))
        .map(|buffer| buffer.capacity())
        .sum();
    duplicated
        + data
            .child_data()
            .iter()
            .map(|child| duplicated_buffer_bytes(child, seen))
            .sum::<usize>()
}

pub(crate) struct DiskRun {
    path: PathBuf,
    reservation: Option<Reservation>,
    max_frame_size: usize,
    directory: Arc<SpillDirectory>,
}

impl DiskRun {
    fn new(store: &RunStore) -> Self {
        Self {
            path: store.directory.0.join(uuid::Uuid::new_v4().to_string()),
            reservation: Some(Reservation {
                budget: store.disk.clone(),
                bytes: 0,
            }),
            max_frame_size: store.batch_limit * 2,
            directory: store.directory.clone(),
        }
    }

    /// Frames standalone Arrow IPC streams so appending doesn't retain one open
    /// descriptor or one batch-offset entry for every source/window combination.
    fn append(&mut self, batch: &RecordBatch, store: &RunStore) -> Result<()> {
        store.check_cancelled()?;
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
                .context(ComputeArrowSnafu)?;
            writer.write(batch).context(ComputeArrowSnafu)?;
            writer.finish().context(ComputeArrowSnafu)?;
        }
        ensure!(
            bytes.len() <= store.batch_limit * 2,
            MultiRegionCompactionResourceSnafu {
                reason: "Encoded run batch exceeds the compaction working-batch budget",
            }
        );
        self.reservation
            .as_mut()
            .context(MultiRegionCompactionResourceSnafu {
                reason: "Cannot append to a released spill run",
            })?
            .grow(bytes.len() as u64 + 8)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .context(MultiRegionCompactionSpillSnafu)?;
        file.write_all(&(bytes.len() as u64).to_le_bytes())
            .context(MultiRegionCompactionSpillSnafu)?;
        file.write_all(&bytes)
            .context(MultiRegionCompactionSpillSnafu)?;
        Ok(())
    }

    fn remove(&mut self) -> Result<()> {
        std::fs::remove_file(&self.path).context(MultiRegionCompactionSpillSnafu)?;
        self.reservation.take();
        Ok(())
    }
}

impl Drop for DiskRun {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        let path = self.path.clone();
        let directory = self.directory.clone();
        // Keep disk quota and the directory alive until deletion has completed.
        common_runtime::spawn_blocking_compact(move || {
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(error; "Failed to remove multi-region spill run: {}", path.display());
            }
            drop(reservation);
            drop(directory);
        });
    }
}

struct DiskIterator {
    file: Option<File>,
    run: DiskRun,
    remaining: u64,
}

impl DiskIterator {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == 0 {
            // Close before unlinking, including on Windows.
            if self.file.take().is_some() {
                self.run.remove()?;
            }
            return Ok(None);
        }
        let file = self
            .file
            .as_mut()
            .context(MultiRegionCompactionResourceSnafu {
                reason: "Spill run closed before EOF",
            })?;
        let mut length = [0; 8];
        file.read_exact(&mut length)
            .context(MultiRegionCompactionSpillSnafu)?;
        let length = u64::from_le_bytes(length);
        ensure!(
            self.remaining >= 8
                && length <= self.remaining - 8
                && length <= self.run.max_frame_size as u64,
            MultiRegionCompactionResourceSnafu {
                reason: "Invalid spill frame length",
            }
        );
        let length = usize::try_from(length).map_err(|_| {
            MultiRegionCompactionResourceSnafu {
                reason: "Spill frame length overflow",
            }
            .build()
        })?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes)
            .context(MultiRegionCompactionSpillSnafu)?;
        self.remaining -= length as u64 + 8;
        let mut reader =
            StreamReader::try_new(Cursor::new(bytes), None).context(ComputeArrowSnafu)?;
        let batch = reader
            .next()
            .transpose()
            .context(ComputeArrowSnafu)?
            .context(MultiRegionCompactionResourceSnafu {
                reason: "Empty spill frame",
            })?;
        Ok(Some(batch))
    }
}

impl Iterator for DiskIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}
