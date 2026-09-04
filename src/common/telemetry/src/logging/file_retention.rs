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

//! File-log retention based on the total size of managed log files.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use common_base::readable_size::ReadableSize;
use parking_lot::Mutex;
use tracing_appender::rolling::{RollingFileAppender, Rotation};

use crate::logging::LoggingOptions;

/// A managed file-log kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum LogFileKind {
    Default,
    Error,
    SlowQuery,
}

impl LogFileKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Default => "greptimedb",
            Self::Error => "greptimedb-err",
            Self::SlowQuery => "greptimedb-slow-queries",
        }
    }

    fn current_link_name(self) -> &'static str {
        match self {
            Self::Default => ".greptimedb.current",
            Self::Error => ".greptimedb-err.current",
            Self::SlowQuery => ".greptimedb-slow-queries.current",
        }
    }

    fn kind_from_file_name(name: &str) -> Option<Self> {
        [Self::SlowQuery, Self::Error, Self::Default]
            .into_iter()
            .find_map(|kind| {
                name.strip_prefix(&format!("{}.", kind.prefix()))
                    .filter(|suffix| Self::is_hourly_suffix(suffix))
                    .map(|_| kind)
            })
    }

    fn is_hourly_suffix(suffix: &str) -> bool {
        suffix.len() == 13
            && suffix.bytes().enumerate().all(|(index, byte)| {
                matches!(index, 4 | 7 | 10) && byte == b'-'
                    || !matches!(index, 4 | 7 | 10) && byte.is_ascii_digit()
            })
    }
}

#[derive(Clone, Debug)]
struct LogFile {
    kind: LogFileKind,
    path: PathBuf,
    last_modified: SystemTime,
    size: u64,
}

#[derive(Default)]
struct FileIndex {
    active: HashMap<LogFileKind, LogFile>,
    closed: BTreeMap<(SystemTime, PathBuf), LogFile>,
    total_size: u64,
}

impl FileIndex {
    fn track(&mut self, kind: LogFileKind, path: PathBuf, size: u64, last_modified: SystemTime) {
        let active = match self.active.entry(kind) {
            Entry::Occupied(mut entry) if entry.get().path != path => {
                let previous = entry.insert(LogFile {
                    kind,
                    path,
                    last_modified,
                    size: 0,
                });
                self.closed
                    .insert((previous.last_modified, previous.path.clone()), previous);
                entry.into_mut()
            }
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(LogFile {
                kind,
                path,
                last_modified,
                size: 0,
            }),
        };

        active.size = active.size.saturating_add(size);
        active.last_modified = last_modified;
        self.total_size = self.total_size.saturating_add(size);
    }

    fn count(&self, kind: LogFileKind) -> usize {
        usize::from(self.active.contains_key(&kind))
            + self
                .closed
                .values()
                .filter(|file| file.kind == kind)
                .count()
    }

    fn remove(&mut self, key: &(SystemTime, PathBuf)) {
        if let Some(file) = self.closed.remove(key) {
            self.total_size = self.total_size.saturating_sub(file.size);
        }
    }
}

#[derive(Default)]
struct RetentionState {
    kinds: HashSet<LogFileKind>,
    files: FileIndex,
    initialized: bool,
    cleanup_error_reported: bool,
}

/// Retains managed file logs within configured directory-size and file-count limits.
#[derive(Clone)]
pub(crate) struct DirectoryRetention {
    directory: Arc<PathBuf>,
    max_size: u64,
    max_log_files: usize,
    state: Arc<Mutex<RetentionState>>,
}

impl DirectoryRetention {
    /// Returns a retention manager when the configured limit is enabled.
    pub(crate) fn new(
        directory: impl Into<PathBuf>,
        max_size: ReadableSize,
        max_log_files: usize,
    ) -> Option<Self> {
        (max_size.as_bytes() > 0).then(|| Self {
            directory: Arc::new(directory.into()),
            max_size: max_size.as_bytes(),
            max_log_files,
            state: Arc::new(Mutex::new(RetentionState::default())),
        })
    }

    /// Loads the initial file state after all enabled file-log kinds are registered.
    pub(crate) fn initialize(&self) {
        let mut state = self.state.lock();
        if !self.ensure_initialized(&mut state) {
            return;
        }

        self.prune_files(&mut state);
        self.prune_size(&mut state, 0);
    }

    fn register(&self, kind: LogFileKind) {
        let mut state = self.state.lock();
        debug_assert!(!state.initialized);
        state.kinds.insert(kind);
    }

    fn reclaim(&self, incoming_size: u64) {
        let mut state = self.state.lock();
        if !self.ensure_initialized(&mut state) {
            return;
        }

        self.prune_size(&mut state, incoming_size);
    }

    fn track(&self, kind: LogFileKind, written: u64) {
        let latest_file = self.current(kind);
        let last_modified = SystemTime::now();
        let mut state = self.state.lock();
        if !self.ensure_initialized(&mut state) {
            return;
        }

        let path = match latest_file {
            Ok(latest_file) => latest_file,
            Err(_) => {
                self.report_error(&mut state, "resolving the latest log file");
                self.reconcile(&mut state);
                return;
            }
        };

        state.files.track(kind, path, written, last_modified);
        self.prune_files(&mut state);
        self.prune_size(&mut state, 0);
    }

    fn scan(&self, kinds: &HashSet<LogFileKind>) -> io::Result<FileIndex> {
        let active_paths = kinds
            .iter()
            .map(|kind| self.current(*kind).map(|path| (*kind, path)))
            .collect::<io::Result<HashMap<_, _>>>()?;
        let mut files = FileIndex::default();

        for entry in fs::read_dir(self.directory.as_ref())? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(kind) = LogFileKind::kind_from_file_name(file_name) else {
                continue;
            };

            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() {
                continue;
            }

            let file = LogFile {
                kind,
                path: path.clone(),
                last_modified: metadata.modified()?,
                size: metadata.len(),
            };
            files.total_size = files.total_size.saturating_add(file.size);
            if active_paths.get(&kind) == Some(&path) {
                files.active.insert(kind, file);
            } else {
                files.closed.insert((file.last_modified, path), file);
            }
        }

        if files.active.len() != active_paths.len() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "latest log symlink target does not exist",
            ));
        }

        Ok(files)
    }

    /// Rebuilds the in-memory index after an unexpected filesystem error.
    fn reconcile(&self, state: &mut RetentionState) -> bool {
        match self.scan(&state.kinds) {
            Ok(files) => {
                state.files = files;
                state.initialized = true;
                state.cleanup_error_reported = false;
                true
            }
            Err(_) => {
                self.report_error(state, "loading log directory");
                false
            }
        }
    }

    fn ensure_initialized(&self, state: &mut RetentionState) -> bool {
        state.initialized || self.reconcile(state)
    }

    fn current(&self, kind: LogFileKind) -> io::Result<PathBuf> {
        let symlink = self.directory.join(kind.current_link_name());
        let target = fs::read_link(&symlink)?;
        let Some(file_name) = target.file_name().and_then(|name| name.to_str()) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid latest log symlink {}", symlink.display()),
            ));
        };
        let Some(candidate) = LogFileKind::kind_from_file_name(file_name) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid latest log symlink {}", symlink.display()),
            ));
        };
        if candidate != kind {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid latest log symlink {}", symlink.display()),
            ));
        }

        Ok(self.directory.join(file_name))
    }

    fn prune_files(&self, state: &mut RetentionState) {
        if self.max_log_files == 0 {
            return;
        }

        for kind in state.kinds.clone() {
            while state.files.count(kind) > self.max_log_files {
                let Some((key, entry)) = state
                    .files
                    .closed
                    .iter()
                    .find(|(_, file)| file.kind == kind)
                    .map(|(key, entry)| (key.clone(), entry.clone()))
                else {
                    return;
                };

                if !self.delete(state, key, entry) {
                    return;
                }
            }
        }
    }

    fn prune_size(&self, state: &mut RetentionState, incoming_size: u64) {
        while state.files.total_size.saturating_add(incoming_size) > self.max_size {
            let Some((key, entry)) = state
                .files
                .closed
                .first_key_value()
                .map(|(key, entry)| (key.clone(), entry.clone()))
            else {
                // The limit is intentionally soft: active files and a single
                // oversized log record are never removed or truncated.
                self.report_error(
                    state,
                    "log directory exceeds the configured limit but only active files remain",
                );
                return;
            };

            if !self.delete(state, key, entry) {
                return;
            }
        }
    }

    fn delete(
        &self,
        state: &mut RetentionState,
        key: (SystemTime, PathBuf),
        file: LogFile,
    ) -> bool {
        match fs::remove_file(&file.path) {
            Ok(()) => {
                state.files.remove(&key);
                state.cleanup_error_reported = false;
                true
            }
            Err(error) => {
                self.report_error(state, &format!("removing {}: {error}", file.path.display()));
                if error.kind() == io::ErrorKind::NotFound {
                    self.reconcile(state);
                }
                false
            }
        }
    }

    #[allow(clippy::print_stderr)]
    fn report_error(&self, state: &mut RetentionState, message: &str) {
        if !state.cleanup_error_reported {
            // Do not use tracing here: this writer is itself on the tracing path.
            eprintln!(
                "Failed to retain log directory {}: {message}",
                self.directory.display()
            );
            state.cleanup_error_reported = true;
        }
    }
}

/// A [`RollingFileAppender`] with shared directory-size retention.
pub(crate) struct RetentionAppender {
    inner: RollingFileAppender,
    retention: DirectoryRetention,
    kind: LogFileKind,
}

impl RetentionAppender {
    fn new(inner: RollingFileAppender, retention: DirectoryRetention, kind: LogFileKind) -> Self {
        Self {
            inner,
            retention,
            kind,
        }
    }
}

impl Write for RetentionAppender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.retention.reclaim(buf.len() as u64);
        let written = self.inner.write(buf)?;
        self.retention.track(self.kind, written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Builds the existing hourly appender, optionally wrapped with retention.
pub(crate) fn build_file_appender(
    opts: &LoggingOptions,
    kind: LogFileKind,
    retention: Option<&DirectoryRetention>,
) -> Box<dyn Write + Send> {
    // Directory retention owns size and count pruning so its index remains authoritative.
    let upstream_max_log_files = if retention.is_some() {
        0
    } else {
        opts.max_log_files
    };
    let mut builder = RollingFileAppender::builder()
        .rotation(Rotation::HOURLY)
        .filename_prefix(kind.prefix())
        .max_log_files(upstream_max_log_files);
    if retention.is_some() {
        builder = builder.latest_symlink(kind.current_link_name());
    }

    let appender = builder.build(&opts.dir).unwrap_or_else(|error| {
        panic!(
            "initializing rolling file appender at {} failed: {}",
            opts.dir, error
        )
    });

    if let Some(retention) = retention {
        retention.register(kind);
        Box::new(RetentionAppender::new(appender, retention.clone(), kind))
    } else {
        Box::new(appender)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::path::{Path, PathBuf};
    use std::time::SystemTime;

    use common_base::readable_size::ReadableSize;
    use tempfile::TempDir;

    use super::FileIndex;
    use crate::logging::LoggingOptions;
    use crate::logging::file_retention::{DirectoryRetention, LogFileKind, build_file_appender};

    fn write_file(path: &Path, contents: &[u8]) {
        let mut file = File::create(path).unwrap();
        file.write_all(contents).unwrap();
    }

    #[cfg(unix)]
    fn register_default_kind(retention: &DirectoryRetention, directory: &Path, active: &Path) {
        retention.register(LogFileKind::Default);
        symlink(active, directory.join(".greptimedb.current")).unwrap();
    }

    #[test]
    fn test_disabled_retention() {
        assert!(DirectoryRetention::new("/tmp", ReadableSize::default(), 0).is_none());
    }

    #[test]
    fn test_recognizes_managed_file_name() {
        assert!(LogFileKind::kind_from_file_name("greptimedb.2026-01-01-00").is_some());
        assert!(LogFileKind::kind_from_file_name("greptimedb-err.2026-01-01-00").is_some());
        assert!(
            LogFileKind::kind_from_file_name("greptimedb-slow-queries.2026-01-01-00").is_some()
        );
        assert!(LogFileKind::kind_from_file_name("greptimedb.2026-01-01-00.1").is_none());
    }

    #[test]
    fn test_file_index_ignores_missing_closed_file() {
        let mut files = FileIndex::default();

        files.remove(&(SystemTime::UNIX_EPOCH, PathBuf::from("missing")));

        assert_eq!(files.total_size, 0);
    }

    #[cfg(unix)]
    #[test]
    fn test_file_appender_creates_current_link_before_first_write() {
        let directory = TempDir::new().unwrap();
        let opts = LoggingOptions {
            dir: directory.path().display().to_string(),
            ..Default::default()
        };
        let retention = DirectoryRetention::new(directory.path(), ReadableSize::mb(1), 0).unwrap();

        let _appender = build_file_appender(&opts, LogFileKind::Default, Some(&retention));

        let symlink = directory.path().join(".greptimedb.current");
        assert!(symlink.is_symlink());
        assert!(fs::read_link(symlink).unwrap().exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_removes_closed_files() {
        let directory = TempDir::new().unwrap();
        let old = directory.path().join("greptimedb.2026-01-01-00");
        let active = directory.path().join("greptimedb.2026-01-01-01");
        write_file(&old, b"old");
        write_file(&active, b"new");

        let retention = DirectoryRetention::new(directory.path(), ReadableSize(4), 0).unwrap();
        register_default_kind(&retention, directory.path(), &active);
        retention.initialize();

        assert!(!old.exists());
        assert!(active.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_keeps_active_file() {
        let directory = TempDir::new().unwrap();
        let active = directory.path().join("greptimedb.2026-01-01-00");
        write_file(&active, b"active");

        let retention = DirectoryRetention::new(directory.path(), ReadableSize(1), 0).unwrap();
        register_default_kind(&retention, directory.path(), &active);
        retention.initialize();

        assert!(active.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_tracks_rotated_file_in_memory() {
        let directory = TempDir::new().unwrap();
        let old = directory.path().join("greptimedb.2026-01-01-00");
        let active = directory.path().join("greptimedb.2026-01-01-01");
        write_file(&old, b"old");
        write_file(&active, b"");

        let retention = DirectoryRetention::new(directory.path(), ReadableSize(3), 0).unwrap();
        register_default_kind(&retention, directory.path(), &old);
        retention.initialize();
        fs::remove_file(directory.path().join(".greptimedb.current")).unwrap();
        symlink(&active, directory.path().join(".greptimedb.current")).unwrap();
        retention.track(LogFileKind::Default, 1);

        assert!(!old.exists());
        assert!(active.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_retries_initialization() {
        let directory = TempDir::new().unwrap();
        let old = directory.path().join("greptimedb.2026-01-01-00");
        let active = directory.path().join("greptimedb.2026-01-01-01");
        let retention = DirectoryRetention::new(directory.path(), ReadableSize(5), 0).unwrap();

        retention.register(LogFileKind::Default);
        retention.initialize();
        assert!(!retention.state.lock().initialized);

        write_file(&old, b"old");
        write_file(&active, b"new");
        symlink(&active, directory.path().join(".greptimedb.current")).unwrap();
        retention.reclaim(1);

        assert!(!old.exists());
        assert!(active.exists());
        assert!(retention.state.lock().initialized);
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_track_retries_initialization() {
        let directory = TempDir::new().unwrap();
        let active = directory.path().join("greptimedb.2026-01-01-00");
        let retention = DirectoryRetention::new(directory.path(), ReadableSize(1), 0).unwrap();

        retention.register(LogFileKind::Default);
        retention.initialize();
        assert!(!retention.state.lock().initialized);

        write_file(&active, b"");
        symlink(&active, directory.path().join(".greptimedb.current")).unwrap();
        retention.track(LogFileKind::Default, 0);

        let state = retention.state.lock();
        assert!(state.initialized);
        assert_eq!(state.files.active[&LogFileKind::Default].path, active);
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_reconciles_after_remove_failure() {
        let directory = TempDir::new().unwrap();
        let old = directory.path().join("greptimedb.2026-01-01-00");
        let active = directory.path().join("greptimedb.2026-01-01-01");
        write_file(&old, b"old");
        write_file(&active, b"new");

        let retention = DirectoryRetention::new(directory.path(), ReadableSize(6), 0).unwrap();
        register_default_kind(&retention, directory.path(), &active);
        retention.initialize();
        fs::remove_file(&old).unwrap();

        retention.reclaim(1);

        let state = retention.state.lock();
        assert!(state.files.closed.is_empty());
        assert_eq!(state.files.total_size, 3);
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_enforces_max_log_files() {
        let directory = TempDir::new().unwrap();
        let oldest = directory.path().join("greptimedb.2026-01-01-00");
        let old = directory.path().join("greptimedb.2026-01-01-01");
        let active = directory.path().join("greptimedb.2026-01-01-02");
        write_file(&oldest, b"oldest");
        write_file(&old, b"old");
        write_file(&active, b"active");

        let retention = DirectoryRetention::new(directory.path(), ReadableSize::gb(1), 2).unwrap();
        register_default_kind(&retention, directory.path(), &active);
        retention.initialize();

        assert!(!oldest.exists());
        assert!(old.exists());
        assert!(active.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_retention_enforces_max_log_files_after_rotation() {
        let directory = TempDir::new().unwrap();
        let oldest = directory.path().join("greptimedb.2026-01-01-00");
        let old = directory.path().join("greptimedb.2026-01-01-01");
        let active = directory.path().join("greptimedb.2026-01-01-02");
        write_file(&oldest, b"oldest");
        write_file(&old, b"old");

        let retention = DirectoryRetention::new(directory.path(), ReadableSize::gb(1), 2).unwrap();
        register_default_kind(&retention, directory.path(), &old);
        retention.initialize();
        write_file(&active, b"");
        fs::remove_file(directory.path().join(".greptimedb.current")).unwrap();
        symlink(&active, directory.path().join(".greptimedb.current")).unwrap();
        retention.track(LogFileKind::Default, 1);

        assert!(!oldest.exists());
        assert!(old.exists());
        assert!(active.exists());
    }

    #[test]
    fn test_unmanaged_files_are_ignored() {
        let directory = TempDir::new().unwrap();
        let unmanaged = directory.path().join("keep-me");
        write_file(&unmanaged, b"unmanaged");
        write_file(
            &directory.path().join("greptimedb.2026-01-01-00"),
            b"managed",
        );

        let retention = DirectoryRetention::new(directory.path(), ReadableSize(1), 0).unwrap();
        retention.initialize();

        assert!(unmanaged.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_managed_file_symlink_is_ignored() {
        let directory = TempDir::new().unwrap();
        let target = directory.path().join("target");
        write_file(&target, b"target");

        let link = directory.path().join("greptimedb.2026-01-01-00");
        symlink(&target, &link).unwrap();

        let retention = DirectoryRetention::new(directory.path(), ReadableSize(1), 0).unwrap();
        retention.initialize();

        assert!(link.exists());
    }
}
