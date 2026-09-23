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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{self, Result};
use crate::statement::export_logical_tables::{
    ActiveWriter, LogicalTableExportLimits, LogicalTableProjection, check_cancelled,
    map_writer_error,
};

const PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Request-wide downstream payload and writer admission, separate from query memory.
pub(crate) struct ExportWriteBudget {
    writers: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_writers: usize,
}

impl ExportWriteBudget {
    pub(crate) fn new(parallelism: usize) -> Arc<Self> {
        let max_writers = parallelism.max(1);
        Arc::new(Self {
            writers: Arc::new(Semaphore::new(max_writers)),
            bytes: Arc::new(Semaphore::new(PAYLOAD_BYTES)),
            max_writers,
        })
    }

    #[cfg(test)]
    pub(crate) fn available(&self) -> (usize, usize) {
        (
            self.writers.available_permits(),
            self.bytes.available_permits(),
        )
    }

    pub(crate) async fn writer(&self, token: &CancellationToken) -> Result<OwnedSemaphorePermit> {
        tokio::select! {
            biased;
            _ = token.cancelled() => error::LogicalTableExportCancelledSnafu.fail(),
            permit = self.writers.clone().acquire_owned() => permit.map_err(|_| error::UnexpectedSnafu { violated: "writer budget closed" }.build()),
        }
    }

    pub(crate) async fn reserve(
        &self,
        size: usize,
        token: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit> {
        ensure!(
            size <= PAYLOAD_BYTES,
            error::LogicalTableExportResourceSnafu {
                reason: "batch backing buffers exceed request payload budget"
            }
        );
        tokio::select! {
            biased;
            _ = token.cancelled() => error::LogicalTableExportCancelledSnafu.fail(),
            permit = self.bytes.clone().acquire_many_owned(size as u32) => permit.map_err(|_| error::UnexpectedSnafu { violated: "payload budget closed" }.build()),
        }
    }

    /// Includes full retained input allocations, conversion capacity growth and
    /// array metadata. Shared backing is conservatively charged for each payload.
    pub(crate) fn conversion_budget(
        batch: &RecordBatch,
        requested: usize,
    ) -> Result<(usize, usize)> {
        let backing = batch.get_array_memory_size();
        let overhead = batch.num_columns().saturating_mul(1024);
        let available = PAYLOAD_BYTES
            .saturating_sub(backing)
            .saturating_sub(overhead)
            / 4;
        let conversion = requested.min(available);
        ensure!(
            conversion > 0,
            error::LogicalTableExportResourceSnafu {
                reason: "batch backing buffers exceed request payload budget"
            }
        );
        Ok((conversion, backing.saturating_add(overhead)))
    }
}

pub(crate) struct Payload {
    pub(crate) batch: RecordBatch,
    pub(crate) permit: OwnedSemaphorePermit,
}

pub(crate) struct TableWriters {
    current: Option<(u32, mpsc::Sender<Payload>)>,
    tasks: FuturesUnordered<JoinHandle<Result<()>>>,
    budget: Arc<ExportWriteBudget>,
}

impl TableWriters {
    pub(crate) fn new(budget: Arc<ExportWriteBudget>) -> Self {
        Self {
            current: None,
            tasks: FuturesUnordered::new(),
            budget,
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_tasks(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) fn table_id(&self) -> Option<u32> {
        self.current.as_ref().map(|(id, _)| *id)
    }

    pub(crate) fn close_input(&mut self) {
        self.current = None;
    }

    pub(crate) async fn open(
        &mut self,
        id: u32,
        table: &LogicalTableProjection,
        store: &ObjectStore,
        limits: LogicalTableExportLimits,
        token: &CancellationToken,
    ) -> Result<()> {
        // EOF must precede acquiring the next slot, including when P is one.
        self.close_input();
        let permit = self.budget.writer(token).await?;
        self.reap_for_admission(token).await?;
        let writer = ActiveWriter::open(table, store, limits).await?;
        let (sender, receiver) = mpsc::channel(2);
        self.current = Some((id, sender));
        let token = token.clone();
        self.tasks.push(common_runtime::spawn_global(async move {
            let _permit = permit;
            let guard = token.clone().drop_guard();
            let result = run_writer(writer, receiver, &token).await;
            if result.is_ok() {
                guard.disarm();
            }
            result
        }));
        Ok(())
    }

    async fn reap_for_admission(&mut self, token: &CancellationToken) -> Result<()> {
        while let Some(Some(result)) = self.tasks.next().now_or_never() {
            result.context(error::JoinTaskSnafu)??;
        }
        // A worker can release its permit before its JoinHandle becomes ready.
        while self.tasks.len() > self.budget.max_writers {
            let result = tokio::select! {
                biased;
                _ = token.cancelled() => return error::LogicalTableExportCancelledSnafu.fail(),
                result = self.tasks.next() => result.context(error::UnexpectedSnafu { violated: "writer task queue unexpectedly empty" })?,
            };
            result.context(error::JoinTaskSnafu)??;
        }
        Ok(())
    }

    pub(crate) async fn send(&self, payload: Payload, token: &CancellationToken) -> Result<()> {
        let (_, sender) = self.current.as_ref().context(error::UnexpectedSnafu {
            violated: "missing logical writer",
        })?;
        tokio::select! {
            biased;
            _ = token.cancelled() => error::LogicalTableExportCancelledSnafu.fail(),
            result = sender.send(payload) => result.map_err(|_| error::LogicalTableExportCancelledSnafu.build()),
        }
    }

    pub(crate) async fn drain(
        &mut self,
        result: Result<()>,
        token: &CancellationToken,
    ) -> Result<()> {
        self.close_input();
        let mut first_error = result.err();
        if first_error.is_some() {
            token.cancel();
        }
        while let Some(result) = self.tasks.next().await {
            if let Err(err) = result.context(error::JoinTaskSnafu).and_then(|r| r) {
                retain_error(&mut first_error, err);
                token.cancel();
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

pub(crate) fn retain_error(first: &mut Option<error::Error>, error: error::Error) {
    if first.as_ref().is_none_or(|err| {
        matches!(
            err,
            error::Error::LogicalTableExportCancelled { .. }
                | error::Error::DatabaseExportCancelled { .. }
        )
    }) {
        *first = Some(error);
    }
}

async fn run_writer(
    mut writer: ActiveWriter,
    mut receiver: mpsc::Receiver<Payload>,
    token: &CancellationToken,
) -> Result<()> {
    let result = async {
        loop {
            let payload = tokio::select! {
                biased;
                _ = token.cancelled() => return error::LogicalTableExportCancelledSnafu.fail(),
                payload = receiver.recv() => payload,
            };
            let Some(Payload { batch, permit }) = payload else {
                break;
            };
            let result = writer.writer.write(batch, Some(token)).await;
            drop(permit);
            result.map_err(|error| map_writer_error(error, &writer.path))?;
        }
        writer
            .writer
            .finish(Some(token))
            .await
            .map_err(|error| map_writer_error(error, &writer.path))?;
        check_cancelled(token)
    }
    .await;
    if result.is_err() {
        token.cancel();
        receiver.close();
        drop(receiver);
        if let Err(error) = writer.writer.abort().await {
            common_telemetry::warn!(error; "Failed to abort Metric export file");
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn released_permit_does_not_hide_pending_join_handles() {
        for parallelism in [1, 4] {
            let budget = ExportWriteBudget::new(parallelism);
            let token = CancellationToken::new();
            let mut writers = TableWriters::new(budget.clone());
            let mut resume = Vec::new();
            for _ in 0..=parallelism {
                let permit = budget.writer(&token).await.unwrap();
                let (released_tx, released_rx) = oneshot::channel();
                let (resume_tx, resume_rx) = oneshot::channel();
                writers.tasks.push(common_runtime::spawn_global(async move {
                    drop(permit);
                    released_tx.send(()).unwrap();
                    resume_rx.await.unwrap();
                    Ok(())
                }));
                released_rx.await.unwrap();
                resume.push(resume_tx);
            }
            assert_eq!(budget.available().0, parallelism);

            let mut reap = Box::pin(writers.reap_for_admission(&token));
            assert!(reap.as_mut().now_or_never().is_none());
            resume.pop().unwrap().send(()).unwrap();
            reap.await.unwrap();
            assert_eq!(writers.pending_tasks(), parallelism);

            for tx in resume {
                tx.send(()).unwrap();
            }
            writers.drain(Ok(()), &token).await.unwrap();
            assert_eq!(budget.available().0, parallelism);
        }
    }
}
