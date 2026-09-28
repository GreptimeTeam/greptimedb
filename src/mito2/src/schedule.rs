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

use std::sync::{Arc, Mutex};

use common_base::cancellation::CancellationHandle;

pub mod remote_job_scheduler;
pub mod scheduler;

/// Shared state for cooperatively cancelling a task until it starts committing.
#[derive(Debug, Clone)]
pub(crate) struct CancellableTaskState {
    cancel_handle: Arc<CancellationHandle>,
    commit_started: Arc<Mutex<bool>>,
}

impl CancellableTaskState {
    pub(crate) fn new() -> Self {
        Self {
            cancel_handle: Arc::new(CancellationHandle::default()),
            commit_started: Arc::new(Mutex::new(false)),
        }
    }

    pub(crate) fn cancel_handle(&self) -> Arc<CancellationHandle> {
        self.cancel_handle.clone()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel_handle.is_cancelled()
    }

    /// Starts the non-cancellable commit phase.
    ///
    /// Returns false if cancellation was requested first.
    pub(crate) fn mark_commit_started(&self) -> bool {
        let mut commit_started = self.commit_started.lock().unwrap();
        if self.cancel_handle.is_cancelled() {
            return false;
        }
        *commit_started = true;
        true
    }

    pub(crate) fn request_cancel(&self) -> RequestCancelResult {
        // Hold the commit lock while cancelling to serialize cancellation with commit startup.
        let commit_started = self.commit_started.lock().unwrap();
        if *commit_started {
            return RequestCancelResult::TooLateToCancel;
        }
        if self.cancel_handle.is_cancelled() {
            return RequestCancelResult::AlreadyCancelling;
        }

        self.cancel_handle.cancel();
        RequestCancelResult::CancelIssued
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestCancelResult {
    CancelIssued,
    AlreadyCancelling,
    TooLateToCancel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancellable_task_state_transitions() {
        let state = CancellableTaskState::new();
        assert_eq!(RequestCancelResult::CancelIssued, state.request_cancel());
        assert!(state.is_cancelled());
        assert_eq!(
            RequestCancelResult::AlreadyCancelling,
            state.request_cancel()
        );
        assert!(!state.mark_commit_started());

        let state = CancellableTaskState::new();
        assert!(state.mark_commit_started());
        assert_eq!(RequestCancelResult::TooLateToCancel, state.request_cancel());
    }
}
