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

//! Process-local acknowledgements for a finite write request.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// Counts only confirmed storage writes, including partial successes.
#[derive(Default)]
pub struct WriteProgress {
    written: AtomicU64,
    accepted: AtomicBool,
    // A queued write must keep its decoder's memory reservation alive.
    _reservation: Option<Arc<dyn Send + Sync>>,
    on_write: Option<Box<dyn Fn(u64) + Send + Sync>>,
    pending: AtomicUsize,
    completed: Notify,
}

/// Keeps a queued submission pending until the worker reports its result.
pub struct PendingWrite(Arc<WriteProgress>);

impl Drop for PendingWrite {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.completed.notify_waiters();
        }
    }
}

impl fmt::Debug for WriteProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteProgress")
            .field("written", &self.written())
            .field("accepted", &self.accepted())
            .finish_non_exhaustive()
    }
}

impl WriteProgress {
    pub fn start(self: &Arc<Self>) -> PendingWrite {
        self.pending.fetch_add(1, Ordering::Relaxed);
        PendingWrite(self.clone())
    }

    /// Waits for previously queued submissions before returning a failed request's counts.
    pub async fn wait(&self) {
        loop {
            let notified = self.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.pending.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    pub fn with_reservation(reservation: Arc<dyn Send + Sync>) -> Self {
        Self {
            _reservation: Some(reservation),
            ..Self::default()
        }
    }

    pub fn add_written(&self, rows: usize) {
        self.written.fetch_add(rows as u64, Ordering::Relaxed);
        self.observe(rows as u64);
    }

    pub fn with_observer(mut self, observer: impl Fn(u64) + Send + Sync + 'static) -> Self {
        self.on_write = Some(Box::new(observer));
        self
    }

    /// A synchronous whole-request acknowledgement also covers handlers without region tracking.
    pub fn confirm(&self, rows: u64) {
        let previous = self.written.fetch_max(rows, Ordering::Relaxed);
        self.observe(rows.saturating_sub(previous));
    }

    fn observe(&self, rows: u64) {
        if rows > 0
            && let Some(observer) = &self.on_write
        {
            observer(rows);
        }
    }

    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Records an acknowledgement returned before storage completion.
    pub fn mark_accepted(&self) {
        self.accepted.store(true, Ordering::Relaxed);
    }

    pub fn accepted(&self) -> bool {
        self.accepted.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn confirmations_do_not_double_count_and_contexts_keep_local_ownership() {
        let observed = Arc::new(AtomicUsize::new(0));
        let counter = observed.clone();
        let progress = Arc::new(WriteProgress::default().with_observer(move |rows| {
            counter.fetch_add(rows as usize, Ordering::Relaxed);
        }));
        let mut context = crate::context::QueryContext::with("greptime", "public");
        context.set_write_progress(progress.clone());
        let fork = context.with_write_admission();
        assert!(Arc::ptr_eq(fork.write_progress().unwrap(), &progress));
        let wire = api::v1::QueryContext::from(context);
        let received = crate::context::QueryContext::try_from(wire).unwrap();
        assert!(received.write_progress().is_none());
        progress.add_written(2);
        progress.confirm(3);
        progress.confirm(3);
        progress.mark_accepted();
        assert_eq!(progress.written(), 3);
        assert_eq!(observed.load(Ordering::Relaxed), 3);
        assert!(progress.accepted());
    }
}
