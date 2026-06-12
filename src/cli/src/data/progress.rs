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
//! stdout and is safe for non-interactive runs. [`LogProgress`] backs the
//! import-v2 `--progress` flag for non-interactive runs by routing events to
//! stderr, while [`IndicatifProgress`] renders an interactive bar on a TTY.
//! Both implement [`ProgressReporter`], so call sites stay agnostic.

use std::io::{self, Write};
use std::sync::Mutex;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

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

/// A lightweight reporter that logs phase lifecycle and progress through the
/// stderr. It never touches stdout, so it is safe for non-interactive runs and
/// keeps dry-run output clean.
pub(crate) struct LogProgress {
    phase: Mutex<Option<PhaseState>>,
}

struct PhaseState {
    name: String,
    total: Option<u64>,
    done: u64,
}

impl LogProgress {
    pub(crate) fn new() -> Self {
        Self {
            phase: Mutex::new(None),
        }
    }
}

fn write_progress_line(line: String) {
    let _ = writeln!(io::stderr().lock(), "{line}");
}

impl ProgressReporter for LogProgress {
    fn start_phase(&self, name: &str, total: Option<u64>) {
        let Ok(mut phase) = self.phase.lock() else {
            return;
        };
        *phase = Some(PhaseState {
            name: name.to_string(),
            total,
            done: 0,
        });
        match total {
            Some(total) => write_progress_line(format!("Starting phase '{name}' ({total} units)")),
            None => write_progress_line(format!("Starting phase '{name}'")),
        }
    }

    fn inc(&self, delta: u64) {
        let Ok(mut guard) = self.phase.lock() else {
            return;
        };
        if let Some(phase) = guard.as_mut() {
            phase.done += delta;
            match phase.total {
                Some(total) => {
                    write_progress_line(format!("Phase '{}': {}/{}", phase.name, phase.done, total))
                }
                None => write_progress_line(format!("Phase '{}': {}", phase.name, phase.done)),
            }
        }
    }

    fn finish_phase(&self) {
        let Ok(mut guard) = self.phase.lock() else {
            return;
        };
        if let Some(phase) = guard.take() {
            write_progress_line(format!(
                "Finished phase '{}' ({} units)",
                phase.name, phase.done
            ));
        }
    }
}

/// A reporter that renders an interactive progress bar via `indicatif`.
///
/// It draws to stderr through an explicit [`ProgressDrawTarget::stderr`] so it
/// never collides with stdout (e.g. dry-run SQL). Phases with a known total get
/// a determinate bar; unknown totals fall back to an animated spinner. Each
/// phase clears itself on finish via [`ProgressBar::finish_and_clear`].
pub(crate) struct IndicatifProgress {
    bar: Mutex<Option<ProgressBar>>,
}

impl IndicatifProgress {
    pub(crate) fn new() -> Self {
        Self {
            bar: Mutex::new(None),
        }
    }
}

impl ProgressReporter for IndicatifProgress {
    fn start_phase(&self, name: &str, total: Option<u64>) {
        let Ok(mut guard) = self.bar.lock() else {
            return;
        };

        // Replacing any prior phase: clear it before starting the next.
        if let Some(prev) = guard.take() {
            prev.finish_and_clear();
        }

        let bar = ProgressBar::with_draw_target(total, ProgressDrawTarget::stderr());
        match total {
            Some(_) => {
                let style =
                    ProgressStyle::with_template("{msg} [{bar:40}] {pos}/{len} ({percent}%)")
                        .unwrap_or_else(|_| ProgressStyle::default_bar())
                        .progress_chars("=>-");
                bar.set_style(style);
            }
            None => {
                let style = ProgressStyle::with_template("{spinner} {msg} {pos}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner());
                bar.set_style(style);
            }
        }
        bar.set_message(name.to_string());
        *guard = Some(bar);
    }

    fn inc(&self, delta: u64) {
        let Ok(guard) = self.bar.lock() else {
            return;
        };
        if let Some(bar) = guard.as_ref() {
            bar.inc(delta);
        }
    }

    fn finish_phase(&self) {
        let Ok(mut guard) = self.bar.lock() else {
            return;
        };
        if let Some(bar) = guard.take() {
            bar.finish_and_clear();
        }
    }
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
mod tests {
    use super::*;

    #[test]
    fn test_log_progress_is_safe_across_phase_lifecycle() {
        // LogProgress takes only `&self`, so it must drive a full phase
        // lifecycle (including an out-of-phase `inc`) without panicking.
        let progress = LogProgress::new();
        let reporter: &dyn ProgressReporter = &progress;

        reporter.inc(1); // No active phase yet: must be a no-op, not a panic.
        reporter.start_phase("Import data tasks", Some(2));
        reporter.inc(1);
        reporter.inc(1);
        reporter.finish_phase();
        reporter.finish_phase(); // Idempotent: finishing twice is harmless.
    }

    #[test]
    fn test_indicatif_progress_is_safe_across_phase_lifecycle() {
        // IndicatifProgress takes only `&self`, so it must survive a full
        // lifecycle (including determinate and spinner phases, an out-of-phase
        // `inc`, and double finish) without panicking. The draw target is
        // stderr, which is non-interactive under the test harness, so nothing
        // actually renders.
        let progress = IndicatifProgress::new();
        let reporter: &dyn ProgressReporter = &progress;

        reporter.inc(1); // No active phase yet: must be a no-op, not a panic.
        reporter.start_phase("Import data tasks", Some(2));
        reporter.inc(1);
        reporter.inc(1);
        reporter.start_phase("Streaming", None); // Spinner phase replaces the bar.
        reporter.inc(5);
        reporter.finish_phase();
        reporter.finish_phase(); // Idempotent: finishing twice is harmless.
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
