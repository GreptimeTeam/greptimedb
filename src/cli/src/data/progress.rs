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

//! Minimal internal progress abstraction for Export/Import V2.
//!
//! This is intentionally small and log/internal oriented. It does not touch
//! stdout and is safe for non-interactive runs. A TTY progress bar (e.g.
//! `indicatif`) and a `--progress` CLI flag are deliberately out of scope; they
//! can layer on top of this trait later without changing call sites.

/// Receives progress events from long-running Export/Import V2 work.
///
/// The trait is object-safe so callers can take `&dyn ProgressReporter` and stay
/// agnostic about the concrete implementation (no-op in production today, a
/// recording fake in tests).
pub(crate) trait ProgressReporter: Send + Sync {
    /// Begins a phase with an optional known total number of units.
    fn start_phase(&self, name: &str, total: Option<u64>);

    /// Advances the current phase by `delta` units.
    fn inc(&self, delta: u64);

    /// Marks the current phase as finished.
    fn finish_phase(&self);
}

/// A reporter that discards every event. Used as the production default and in
/// tests that do not care about progress.
pub(crate) struct NoopProgress;

impl ProgressReporter for NoopProgress {
    fn start_phase(&self, _name: &str, _total: Option<u64>) {}
    fn inc(&self, _delta: u64) {}
    fn finish_phase(&self) {}
}

/// RAII guard for a started progress phase.
///
/// This keeps future stateful reporters safe on every early-return path after a
/// phase starts. Call [`Self::finish`] to end the phase at a deliberate point;
/// otherwise the guard finishes it when dropped.
#[must_use = "dropping the guard immediately finishes the phase"]
pub(crate) struct ProgressPhase<'a> {
    reporter: &'a dyn ProgressReporter,
    finished: bool,
}

impl<'a> ProgressPhase<'a> {
    pub(crate) fn start(
        reporter: &'a dyn ProgressReporter,
        name: &str,
        total: Option<u64>,
    ) -> Self {
        reporter.start_phase(name, total);
        Self {
            reporter,
            finished: false,
        }
    }

    pub(crate) fn finish(mut self) {
        self.finish_once();
    }

    fn finish_once(&mut self) {
        if !self.finished {
            self.reporter.finish_phase();
            self.finished = true;
        }
    }
}

impl Drop for ProgressPhase<'_> {
    fn drop(&mut self) {
        self.finish_once();
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::Mutex;

    use super::ProgressReporter;

    /// A single recorded progress event, used to assert progress behavior in
    /// unit tests.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum ProgressEvent {
        StartPhase { name: String, total: Option<u64> },
        Inc { delta: u64 },
        FinishPhase,
    }

    /// A reporter that records every event in order for later assertions.
    #[derive(Default)]
    pub(crate) struct RecordingProgress {
        events: Mutex<Vec<ProgressEvent>>,
    }

    impl RecordingProgress {
        pub(crate) fn events(&self) -> Vec<ProgressEvent> {
            self.events.lock().unwrap().clone()
        }

        /// Sum of all `inc` deltas recorded so far.
        pub(crate) fn total_inc(&self) -> u64 {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    ProgressEvent::Inc { delta } => Some(*delta),
                    _ => None,
                })
                .sum()
        }

        fn push(&self, event: ProgressEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    impl ProgressReporter for RecordingProgress {
        fn start_phase(&self, name: &str, total: Option<u64>) {
            self.push(ProgressEvent::StartPhase {
                name: name.to_string(),
                total,
            });
        }

        fn inc(&self, delta: u64) {
            self.push(ProgressEvent::Inc { delta });
        }

        fn finish_phase(&self) {
            self.push(ProgressEvent::FinishPhase);
        }
    }
}
