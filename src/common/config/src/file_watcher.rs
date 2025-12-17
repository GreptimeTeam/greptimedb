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
//!
//! The watcher monitors the parent directory of each file rather than the
//! file itself. This ensures that file deletions and recreations are properly
//! tracked, which is common with editors that use atomic saves or when
//! configuration files are replaced.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;

use common_telemetry::{error, info, warn};
use notify::{EventKind, RecursiveMode, Watcher};
use snafu::ResultExt;

use crate::error::{FileWatchSnafu, InvalidPathSnafu, Result};

/// Configuration for the file watcher behavior.
#[derive(Debug, Clone, Default)]
pub struct FileWatcherConfig {
    /// Whether to include Remove events in addition to Modify and Create.
    pub include_remove_events: bool,
}

impl FileWatcherConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_modify_and_create(mut self) -> Self {
        self.include_remove_events = false;
        self
    }

    pub fn with_remove_events(mut self) -> Self {
        self.include_remove_events = true;
        self
    }
}

/// A builder for creating file watchers with flexible configuration.
///
/// The watcher monitors the parent directory of each file to handle file
/// deletion and recreation properly. Events are filtered to only trigger
/// callbacks for the specific files being watched.
pub struct FileWatcherBuilder {
    config: FileWatcherConfig,
    /// Canonicalized paths of files to watch.
    file_paths: Vec<PathBuf>,
}

impl FileWatcherBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: FileWatcherConfig::default(),
            file_paths: Vec::new(),
        }
    }

    /// Set the watcher configuration.
    pub fn config(mut self, config: FileWatcherConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a file path to watch.
    ///
    /// Returns an error if the path is a directory.
    /// The path is canonicalized for reliable comparison with events.
    pub fn watch_path<P: AsRef<Path>>(mut self, path: P) -> Result<Self> {
        let path = path.as_ref();
        snafu::ensure!(
            path.is_file(),
            InvalidPathSnafu {
                path: path.display().to_string(),
            }
        );

        self.file_paths.push(path.to_path_buf());
        Ok(self)
    }

    /// Add multiple file paths to watch.
    ///
    /// Returns an error if any path is a directory.
    pub fn watch_paths<P: AsRef<Path>, I: IntoIterator<Item = P>>(
        mut self,
        paths: I,
    ) -> Result<Self> {
        for path in paths {
            self = self.watch_path(path)?;
        }
        Ok(self)
    }

    /// Build and spawn the file watcher with the given callback.
    ///
    /// The callback is invoked when relevant file events are detected for
    /// the watched files. The watcher monitors the parent directories to
    /// handle file deletion and recreation properly.
    ///
    /// The spawned watcher thread runs for the lifetime of the process.
    pub fn spawn<F>(self, callback: F) -> Result<()>
    where
        F: Fn() + Send + 'static,
    {
        let (tx, rx) = channel::<notify::Result<notify::Event>>();
        let mut watcher =
            notify::recommended_watcher(tx).context(FileWatchSnafu { path: "<none>" })?;

        // Collect unique parent directories to watch
        let mut watched_dirs: HashSet<PathBuf> = HashSet::new();
        for file_path in &self.file_paths {
            if let Some(parent) = file_path.parent()
                && watched_dirs.insert(parent.to_path_buf())
            {
                watcher
                    .watch(parent, RecursiveMode::NonRecursive)
                    .context(FileWatchSnafu {
                        path: parent.display().to_string(),
                    })?;
            }
        }

        let config = self.config;
        let watched_files: HashSet<PathBuf> = self.file_paths.iter().cloned().collect();

        info!(
            "Spawning file watcher for paths: {:?} (watching parent directories)",
            self.file_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
        );

        std::thread::spawn(move || {
            // Keep watcher alive in the thread
            let _watcher = watcher;

            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        if !is_relevant_event(&event.kind, &config) {
                            continue;
                        }

                        // Check if any of the event paths match our watched files
                        let is_watched_file = event
                            .paths
                            .iter()
                            .any(|event_path| watched_files.contains(event_path));

                        if !is_watched_file {
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
            .unwrap()
            .config(FileWatcherConfig::new())
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

    #[test]
    fn test_file_watcher_detects_delete_and_recreate() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher_recreate");
        let file_path = dir.path().join("test_file.txt");

        // Create initial file
        std::fs::write(&file_path, "initial content").unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        FileWatcherBuilder::new()
            .watch_path(&file_path)
            .unwrap()
            .config(FileWatcherConfig::new())
            .spawn(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

        // Give watcher time to start
        std::thread::sleep(Duration::from_millis(100));

        // Delete the file
        std::fs::remove_file(&file_path).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        // Recreate the file - this should still be detected because we watch the directory
        std::fs::write(&file_path, "recreated content").unwrap();

        // Wait for the event to be processed
        std::thread::sleep(Duration::from_millis(500));

        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "Watcher should have detected file recreation"
        );
    }

    #[test]
    fn test_file_watcher_ignores_other_files() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher_other");
        let watched_file = dir.path().join("watched.txt");
        let other_file = dir.path().join("other.txt");

        // Create both files
        std::fs::write(&watched_file, "watched content").unwrap();
        std::fs::write(&other_file, "other content").unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        FileWatcherBuilder::new()
            .watch_path(&watched_file)
            .unwrap()
            .config(FileWatcherConfig::new())
            .spawn(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

        // Give watcher time to start
        std::thread::sleep(Duration::from_millis(100));

        // Modify the other file - should NOT trigger callback
        std::fs::write(&other_file, "modified other content").unwrap();

        // Wait for potential event
        std::thread::sleep(Duration::from_millis(500));

        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "Watcher should not have detected changes to other files"
        );

        // Now modify the watched file - SHOULD trigger callback
        std::fs::write(&watched_file, "modified watched content").unwrap();

        // Wait for the event to be processed
        std::thread::sleep(Duration::from_millis(500));

        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "Watcher should have detected change to watched file"
        );
    }
}
