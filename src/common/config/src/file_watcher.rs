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

//! Common file watching utilities for configuration hot-reloading.
//!
//! This module provides a generic file watcher that can be used to watch
//! files for changes and trigger callbacks when changes occur.

use std::path::Path;
use std::sync::mpsc::channel;

use common_telemetry::{error, info, warn};
use notify::{EventKind, RecursiveMode, Watcher};
use snafu::ResultExt;

use crate::error::{FileWatchSnafu, Result};

/// Configuration for the file watcher behavior.
#[derive(Debug, Clone, Default)]
pub struct FileWatcherConfig {
    /// Whether to include Remove events in addition to Modify and Create.
    pub include_remove_events: bool,
}

impl FileWatcherConfig {
    /// Create a config that watches for modify and create events only.
    pub fn modify_and_create() -> Self {
        Self {
            include_remove_events: false,
        }
    }

    /// Create a config that also watches for remove events.
    pub fn with_remove_events() -> Self {
        Self {
            include_remove_events: true,
        }
    }
}

/// A builder for creating file watchers with flexible configuration.
pub struct FileWatcherBuilder {
    config: FileWatcherConfig,
    paths: Vec<Box<Path>>,
}

impl FileWatcherBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: FileWatcherConfig::default(),
            paths: Vec::new(),
        }
    }

    /// Set the watcher configuration.
    pub fn config(mut self, config: FileWatcherConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a file path to watch.
    pub fn watch_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.paths.push(path.as_ref().into());
        self
    }

    /// Add multiple file paths to watch.
    pub fn watch_paths<P: AsRef<Path>, I: IntoIterator<Item = P>>(mut self, paths: I) -> Self {
        for path in paths {
            self.paths.push(path.as_ref().into());
        }
        self
    }

    /// Build and spawn the file watcher with the given callback.
    ///
    /// The callback is invoked when relevant file events are detected.
    /// The spawned watcher thread runs for the lifetime of the process.
    pub fn spawn<F>(self, callback: F) -> Result<()>
    where
        F: Fn() + Send + 'static,
    {
        let (tx, rx) = channel::<notify::Result<notify::Event>>();
        let mut watcher =
            notify::recommended_watcher(tx).context(FileWatchSnafu { path: "<none>" })?;

        for path in &self.paths {
            watcher
                .watch(path, RecursiveMode::NonRecursive)
                .with_context(|_| FileWatchSnafu {
                    path: path.display().to_string(),
                })?;
        }

        let config = self.config;
        let paths_display: Vec<_> = self.paths.iter().map(|p| p.display().to_string()).collect();

        info!("Spawning file watcher for paths: {:?}", paths_display);

        std::thread::spawn(move || {
            // Keep watcher alive in the thread
            let _watcher = watcher;

            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        if !is_relevant_event(&event.kind, &config) {
                            continue;
                        }

                        info!(?event.kind, ?event.paths, "Detected file change");
                        callback();
                    }
                    Err(err) => {
                        warn!("File watcher error: {}", err);
                    }
                }
            }

            error!("File watcher channel closed unexpectedly");
        });

        Ok(())
    }
}

impl Default for FileWatcherBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if an event kind is relevant based on the configuration.
fn is_relevant_event(kind: &EventKind, config: &FileWatcherConfig) -> bool {
    match kind {
        EventKind::Modify(_) | EventKind::Create(_) => true,
        EventKind::Remove(_) => config.include_remove_events,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use common_test_util::temp_dir::create_temp_dir;

    use super::*;

    #[test]
    fn test_file_watcher_detects_changes() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher");
        let file_path = dir.path().join("test_file.txt");

        // Create initial file
        std::fs::write(&file_path, "initial content").unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        FileWatcherBuilder::new()
            .watch_path(&file_path)
            .config(FileWatcherConfig::modify_and_create())
            .spawn(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

        // Give watcher time to start
        std::thread::sleep(Duration::from_millis(100));

        // Modify the file
        std::fs::write(&file_path, "modified content").unwrap();

        // Wait for the event to be processed
        std::thread::sleep(Duration::from_millis(500));

        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "Watcher should have detected at least one change"
        );
    }
}
