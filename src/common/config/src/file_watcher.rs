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
//!
//! If the path goes through symlinks (the file itself or any ancestor
//! directory), the directory holding each symlink is watched as well, so that
//! swapping a symlink anywhere in the chain (e.g. `ln -sfn secrets.2 secrets`,
//! as done by secret managers like sops-nix or Kubernetes volume mounts) is
//! detected. The chain is re-resolved after every relevant event and the set
//! of watched directories is updated accordingly.

use std::collections::{HashSet, VecDeque};
use std::path::{Component, Path, PathBuf};
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
        Default::default()
    }

    pub fn include_remove_events(mut self) -> Self {
        self.include_remove_events = true;
        self
    }
}

/// A builder for creating file watchers with flexible configuration.
///
/// The watcher monitors the parent directory of each file to handle file
/// deletion and recreation properly. Events are filtered to only trigger
/// callbacks for the specific files being watched (or the symlinks leading
/// to them).
pub struct FileWatcherBuilder {
    config: FileWatcherConfig,
    /// Paths of files to watch, as configured. Symlinks in them are resolved
    /// when spawning and again after every relevant event.
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
    /// Symlinks in the path are followed, see [`FileWatcherBuilder::spawn`].
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
    /// handle file deletion and recreation properly. When a path goes through
    /// symlinks, the directory holding each symlink is watched too, and the
    /// watches are re-armed after every relevant event, so the callback also
    /// fires when a symlink anywhere in the chain is replaced.
    ///
    /// The spawned watcher thread runs for the lifetime of the process.
    pub fn spawn<F>(self, callback: F) -> Result<()>
    where
        F: Fn() + Send + 'static,
    {
        let (tx, rx) = channel::<notify::Result<notify::Event>>();
        let mut watcher =
            notify::recommended_watcher(tx).context(FileWatchSnafu { path: "<none>" })?;

        let mut targets = resolve_watch_targets(&self.file_paths);
        let mut skipped = Vec::new();
        for dir in &targets.dirs {
            match watcher.watch(dir, RecursiveMode::NonRecursive) {
                Ok(()) => {}
                // Only the directories holding the files are required, as
                // before symlink chains were followed. A directory holding a
                // symlink may be traversable but unreadable (e.g. mode 0711),
                // so it is skipped and retried on the next relevant event.
                // Until then, replacing the symlink inside it goes unnoticed
                // until the next event on the final file or on another
                // watched symlink.
                Err(err) if !targets.file_dirs.contains(dir) => {
                    warn!("Failed to watch {:?}, skipping: {}", dir, err);
                    skipped.push(dir.clone());
                }
                Err(err) => {
                    return Err(err).context(FileWatchSnafu {
                        path: dir.display().to_string(),
                    });
                }
            }
        }
        for dir in skipped {
            targets.dirs.remove(&dir);
        }

        let config = self.config;
        let file_paths = self.file_paths;

        info!(
            "Spawning file watcher for paths: {:?} (watching directories: {:?})",
            file_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>(),
            targets.dirs
        );

        std::thread::spawn(move || {
            // Keep watcher alive in the thread
            let mut watcher = watcher;

            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        if !is_relevant_event(&event.kind, &config)
                            || !event
                                .paths
                                .iter()
                                .any(|p| targets.entries.contains(&entry_key(p)))
                        {
                            continue;
                        }

                        info!(?event.kind, ?event.paths, "Detected folder change");
                        // A symlink in the chain may have been replaced, so
                        // re-resolve and re-arm the watches before reloading.
                        targets = rearm_watches(&mut watcher, &file_paths, targets);
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

/// Maximum number of symlinks followed while resolving a single path. This
/// mirrors Linux's `MAXSYMLINKS` and guards against symlink loops.
const MAX_SYMLINK_HOPS: usize = 40;

/// Directories to watch and the directory entries whose events are relevant.
#[derive(Debug, Default)]
struct WatchTargets {
    /// Directories holding a symlink of a chain, or the final file.
    dirs: HashSet<PathBuf>,
    /// The subset of `dirs` holding the final files.
    file_dirs: HashSet<PathBuf>,
    /// The symlinks of every chain and the final files, as they appear
    /// inside `dirs`, normalized with [`entry_key`].
    entries: HashSet<PathBuf>,
}

/// Key used to compare an event path with [`WatchTargets::entries`].
///
/// The entries are built from the configured spelling, which may differ in
/// case from what is on disk (and reported in events) on the usually
/// case-insensitive filesystems of macOS and Windows, so compare lowercased
/// there. At worst this lets a few extra events through on a case-sensitive
/// volume.
fn entry_key(path: &Path) -> PathBuf {
    #[cfg(any(target_os = "macos", windows))]
    {
        PathBuf::from(path.to_string_lossy().to_lowercase())
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        path.to_path_buf()
    }
}

/// Resolves every path component by component, following symlinks, and
/// collects what has to be watched to notice any change of what the path
/// points to.
///
/// For each symlink met on the way (the file itself or any ancestor
/// directory), the directory holding it is watched, so replacing the symlink
/// is noticed. The directory holding the final file is watched too, so
/// in-place edits are noticed. The collected directories are free of
/// symlinks; the collected entries are the symlinks themselves and the final
/// files, located inside those directories.
///
/// If a component is missing (e.g. a symlink being swapped non-atomically),
/// resolution of that path stops and its containing directory is watched, so
/// the watch is re-armed once the component shows up again.
fn resolve_watch_targets(paths: &[PathBuf]) -> WatchTargets {
    let mut targets = WatchTargets::default();
    for path in paths {
        resolve_one(path, &mut targets);
    }
    targets
}

fn resolve_one(path: &Path, targets: &mut WatchTargets) {
    let Ok(path) = std::path::absolute(path) else {
        return;
    };

    let mut pending: VecDeque<PathBuf> = path
        .components()
        .map(|c| PathBuf::from(c.as_os_str()))
        .collect();
    // Always free of symlinks, since each component is resolved before
    // being appended.
    let mut resolved = PathBuf::new();
    let mut hops = 0;

    while let Some(component) = pending.pop_front() {
        match component.components().next() {
            Some(Component::Prefix(_) | Component::RootDir) => resolved.push(&component),
            Some(Component::CurDir) | None => {}
            Some(Component::ParentDir) => {
                resolved.pop();
            }
            Some(Component::Normal(name)) => {
                let candidate = resolved.join(name);
                let is_last = pending.is_empty();
                match std::fs::symlink_metadata(&candidate) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        targets.dirs.insert(resolved.clone());
                        targets.entries.insert(entry_key(&candidate));
                        hops += 1;
                        let Ok(link) = std::fs::read_link(&candidate) else {
                            return;
                        };
                        if hops > MAX_SYMLINK_HOPS {
                            warn!("Too many levels of symlinks while resolving {:?}", path);
                            return;
                        }
                        // Relative targets resolve against `resolved`; absolute
                        // ones reset it through their root component.
                        for c in link.components().rev() {
                            pending.push_front(PathBuf::from(c.as_os_str()));
                        }
                    }
                    Ok(_) if !is_last => resolved = candidate,
                    Ok(_) | Err(_) => {
                        if is_last {
                            targets.file_dirs.insert(resolved.clone());
                        }
                        targets.dirs.insert(resolved);
                        targets.entries.insert(entry_key(&candidate));
                        return;
                    }
                }
            }
        }
    }
}

/// Re-resolves `file_paths` and updates the watched directories, returning
/// the new targets. Failures are logged and retried on the next event that
/// matches a chain entry.
fn rearm_watches(
    watcher: &mut impl Watcher,
    file_paths: &[PathBuf],
    old: WatchTargets,
) -> WatchTargets {
    let mut new = resolve_watch_targets(file_paths);

    for dir in old.dirs.difference(&new.dirs) {
        // The directory may already be gone; nothing to do then.
        let _ = watcher.unwatch(dir);
    }
    let mut failed = Vec::new();
    for dir in new.dirs.difference(&old.dirs) {
        match watcher.watch(dir, RecursiveMode::NonRecursive) {
            Ok(()) => info!("File watcher now watching {:?}", dir),
            Err(err) => {
                warn!("Failed to watch {:?}: {}", dir, err);
                failed.push(dir.clone());
            }
        }
    }
    for dir in failed {
        new.dirs.remove(&dir);
    }
    new
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use common_test_util::temp_dir::create_temp_dir;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(10);

    /// Polls `cond` until it holds or `TIMEOUT` elapses.
    fn wait_until(cond: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        cond()
    }

    /// Spawns a watcher on `path` and returns a counter of callback calls.
    fn spawn_counter(path: &Path) -> Arc<AtomicUsize> {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        FileWatcherBuilder::new()
            .watch_path(path)
            .unwrap()
            .config(FileWatcherConfig::new())
            .spawn(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        counter
    }

    /// Spawns a watcher on `path` whose callback stores the content read
    /// through `path`, like the real reload callbacks do.
    fn spawn_reader(path: &Path) -> Arc<Mutex<String>> {
        let content = Arc::new(Mutex::new(String::new()));
        let content_clone = content.clone();
        let path_owned = path.to_path_buf();
        FileWatcherBuilder::new()
            .watch_path(path)
            .unwrap()
            .spawn(move || {
                if let Ok(c) = std::fs::read_to_string(&path_owned) {
                    *content_clone.lock().unwrap() = c;
                }
            })
            .unwrap();
        content
    }

    #[test]
    fn test_file_watcher_detects_changes() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher");
        let file_path = dir.path().join("test_file.txt");
        std::fs::write(&file_path, "initial content").unwrap();

        // Watches are registered before `spawn` returns, so no need to wait.
        let counter = spawn_counter(&file_path);
        std::fs::write(&file_path, "modified content").unwrap();

        assert!(
            wait_until(|| counter.load(Ordering::SeqCst) >= 1),
            "Watcher should have detected at least one change"
        );
    }

    #[test]
    fn test_file_watcher_detects_delete_and_recreate() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher_recreate");
        let file_path = dir.path().join("test_file.txt");
        std::fs::write(&file_path, "initial content").unwrap();

        let content = spawn_reader(&file_path);

        std::fs::remove_file(&file_path).unwrap();
        // Recreate the file - this should still be detected because we watch the directory
        std::fs::write(&file_path, "recreated content").unwrap();

        assert!(
            wait_until(|| *content.lock().unwrap() == "recreated content"),
            "Watcher should have detected file recreation"
        );
    }

    #[test]
    fn test_file_watcher_ignores_unrelated_files() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher_unrelated");
        let file_path = dir.path().join("watched.txt");
        std::fs::write(&file_path, "initial content").unwrap();

        let counter = spawn_counter(&file_path);

        std::fs::write(dir.path().join("unrelated.txt"), "noise").unwrap();
        // Write the watched file afterwards: once its event is seen, the
        // earlier unrelated event has been processed too.
        std::fs::write(&file_path, "modified content").unwrap();
        assert!(wait_until(|| counter.load(Ordering::SeqCst) >= 1));
        std::thread::sleep(Duration::from_millis(200));
        let seen = counter.load(Ordering::SeqCst);

        // Only unrelated activity from now on.
        std::fs::write(dir.path().join("unrelated.txt"), "more noise").unwrap();
        std::fs::remove_file(dir.path().join("unrelated.txt")).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            seen,
            "Unrelated files in the watched directory must not trigger the callback"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_resolve_watch_targets_symlink_chain() {
        use std::os::unix::fs::symlink;

        let dir = create_temp_dir("test_resolve_watch_targets");
        let base = dir.path().canonicalize().unwrap();
        let keys = |paths: &[PathBuf]| paths.iter().map(|p| entry_key(p)).collect::<HashSet<_>>();

        // real/users <- secrets (relative dir symlink) <- conf/users (relative
        // file symlink going through `..`).
        std::fs::create_dir(base.join("real")).unwrap();
        std::fs::write(base.join("real/users"), "").unwrap();
        symlink("real", base.join("secrets")).unwrap();
        std::fs::create_dir(base.join("conf")).unwrap();
        symlink("../secrets/users", base.join("conf/users")).unwrap();

        let targets = resolve_watch_targets(&[base.join("conf/users")]);
        assert_eq!(
            targets.dirs,
            HashSet::from([base.clone(), base.join("conf"), base.join("real")])
        );
        assert_eq!(targets.file_dirs, HashSet::from([base.join("real")]));
        assert_eq!(
            targets.entries,
            keys(&[
                base.join("conf/users"),
                base.join("secrets"),
                base.join("real/users"),
            ])
        );

        // A plain file only needs its parent directory.
        let targets = resolve_watch_targets(&[base.join("real/users")]);
        assert_eq!(targets.dirs, HashSet::from([base.join("real")]));
        assert_eq!(targets.entries, keys(&[base.join("real/users")]));

        // A dangling link keeps the directory where the target should appear.
        std::fs::remove_file(base.join("secrets")).unwrap();
        let targets = resolve_watch_targets(&[base.join("conf/users")]);
        assert_eq!(
            targets.dirs,
            HashSet::from([base.clone(), base.join("conf")])
        );
        assert!(targets.entries.contains(&entry_key(&base.join("secrets"))));

        // Symlink loops terminate.
        symlink("loop", base.join("loop")).unwrap();
        let targets = resolve_watch_targets(&[base.join("loop")]);
        assert_eq!(targets.dirs, HashSet::from([base.clone()]));
    }

    #[cfg(unix)]
    #[test]
    fn test_file_watcher_detects_dir_symlink_swap() {
        use std::os::unix::fs::symlink;

        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher_symlink_swap");
        let base = dir.path();
        std::fs::create_dir(base.join("gen1")).unwrap();
        std::fs::write(base.join("gen1/file"), "1").unwrap();
        std::fs::create_dir(base.join("gen2")).unwrap();
        std::fs::write(base.join("gen2/file"), "2").unwrap();
        symlink(base.join("gen1"), base.join("current")).unwrap();
        std::fs::create_dir(base.join("conf")).unwrap();
        symlink(base.join("current/file"), base.join("conf/file")).unwrap();

        let content = spawn_reader(&base.join("conf/file"));

        // Swap `current` atomically, like `ln -sfn gen2 current`.
        symlink(base.join("gen2"), base.join("current.tmp")).unwrap();
        std::fs::rename(base.join("current.tmp"), base.join("current")).unwrap();
        assert!(
            wait_until(|| *content.lock().unwrap() == "2"),
            "Watcher should have detected the swap"
        );
        // Let any remaining events of the swap drain, so they can't read the
        // edit below by accident.
        std::thread::sleep(Duration::from_millis(200));

        // The watch follows the chain to the new target directory.
        std::fs::write(base.join("gen2/file"), "22").unwrap();
        assert!(
            wait_until(|| *content.lock().unwrap() == "22"),
            "Watcher should have detected the edit of the new target"
        );
    }

    /// A directory holding a symlink of the chain may be traversable but not
    /// readable. It can't be watched, but that must not prevent watching the
    /// file itself.
    #[cfg(unix)]
    #[test]
    fn test_file_watcher_skips_unreadable_link_dir() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_watcher_unreadable_dir");
        let base = dir.path();
        std::fs::create_dir(base.join("real")).unwrap();
        std::fs::write(base.join("real/file"), "1").unwrap();
        std::fs::create_dir(base.join("locked")).unwrap();
        symlink(base.join("real/file"), base.join("locked/file")).unwrap();

        /// Restores a readable mode on drop, even if the test panics: the
        /// temp dir can't be removed while `locked` is unreadable, since
        /// removing it requires listing its entries.
        struct RestoreMode(PathBuf);
        impl Drop for RestoreMode {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
            }
        }

        // Declared after `dir`, so it drops (restoring the mode) first.
        let _restore = RestoreMode(base.join("locked"));
        // Traversable (so `locked/file` still resolves) but not readable.
        std::fs::set_permissions(base.join("locked"), std::fs::Permissions::from_mode(0o311))
            .unwrap();
        if std::fs::read_dir(base.join("locked")).is_ok() {
            // Running as root (or with CAP_DAC_OVERRIDE): nothing to test.
            return;
        }

        // Would panic if watching `locked` were fatal.
        let content = spawn_reader(&base.join("locked/file"));

        // The directory of the final file is still watched.
        std::fs::write(base.join("real/file"), "2").unwrap();
        assert!(wait_until(|| *content.lock().unwrap() == "2"));
    }
}
