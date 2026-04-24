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

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use snafu::{IntoError, OptionExt, ResultExt};
use tokio::io::AsyncWriteExt;

use crate::data::import_v2::error::{
    ImportStateIoSnafu, ImportStateLockedSnafu, ImportStateParseSnafu,
    ImportStateUnknownChunkSnafu, Result,
};
use crate::data::path::encode_path_segment;

const IMPORT_STATE_ROOT: &str = ".greptime";
const IMPORT_STATE_DIR: &str = "import_state";
static IMPORT_STATE_TMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ImportChunkStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImportChunkState {
    pub(crate) id: u32,
    pub(crate) status: ImportChunkStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImportState {
    pub(crate) snapshot_id: String,
    pub(crate) target_addr: String,
    pub(crate) updated_at: DateTime<Utc>,
    // Chunk counts are expected to stay below ~1000, so linear scans are acceptable here.
    pub(crate) chunks: Vec<ImportChunkState>,
}

impl ImportState {
    pub(crate) fn new<I>(
        snapshot_id: impl Into<String>,
        target_addr: impl Into<String>,
        chunk_ids: I,
    ) -> Self
    where
        I: IntoIterator<Item = u32>,
    {
        Self {
            snapshot_id: snapshot_id.into(),
            target_addr: target_addr.into(),
            updated_at: Utc::now(),
            chunks: chunk_ids
                .into_iter()
                .map(|id| ImportChunkState {
                    id,
                    status: ImportChunkStatus::Pending,
                    error: None,
                })
                .collect(),
        }
    }

    pub(crate) fn chunk_status(&self, chunk_id: u32) -> Option<ImportChunkStatus> {
        self.chunks
            .iter()
            .find(|chunk| chunk.id == chunk_id)
            .map(|chunk| chunk.status.clone())
    }

    pub(crate) fn set_chunk_status(
        &mut self,
        chunk_id: u32,
        status: ImportChunkStatus,
        error: Option<String>,
    ) -> Result<()> {
        let chunk = self
            .chunks
            .iter_mut()
            .find(|chunk| chunk.id == chunk_id)
            .context(ImportStateUnknownChunkSnafu { chunk_id })?;
        chunk.status = status;
        chunk.error = error;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ImportStateLockGuard {
    file: std::fs::File,
}

impl Drop for ImportStateLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(crate) fn default_state_path(snapshot_id: &str, target_addr: &str) -> Option<PathBuf> {
    let home = default_home_dir_with(|key| std::env::var_os(key));
    let cwd = std::env::current_dir().ok();
    default_state_path_with(home.as_deref(), cwd.as_deref(), snapshot_id, target_addr)
}

fn default_home_dir_with<F>(get: F) -> Option<PathBuf>
where
    F: for<'a> Fn(&'a str) -> Option<std::ffi::OsString>,
{
    get("HOME")
        .or_else(|| get("USERPROFILE"))
        .map(PathBuf::from)
        .or_else(|| {
            let drive = get("HOMEDRIVE")?;
            let path = get("HOMEPATH")?;
            Some(PathBuf::from(drive).join(path))
        })
}

fn default_state_path_with(
    home: Option<&Path>,
    cwd: Option<&Path>,
    snapshot_id: &str,
    target_addr: &str,
) -> Option<PathBuf> {
    let file_name = import_state_file_name(snapshot_id, target_addr);
    match (home, cwd) {
        (Some(home), _) => Some(
            home.join(IMPORT_STATE_ROOT)
                .join(IMPORT_STATE_DIR)
                .join(file_name),
        ),
        (None, Some(cwd)) => Some(cwd.join(file_name)),
        (None, None) => None,
    }
}

fn import_state_file_name(snapshot_id: &str, target_addr: &str) -> String {
    format!(
        ".import_state_{}_{}.json",
        encode_path_segment(snapshot_id),
        encode_path_segment(target_addr)
    )
}

pub(crate) async fn load_import_state(path: &Path) -> Result<Option<ImportState>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            let mut state: ImportState =
                serde_json::from_slice(&bytes).context(ImportStateParseSnafu)?;
            normalize_import_state_for_resume(&mut state);
            Ok(Some(state))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(source).context(ImportStateIoSnafu {
            path: path.display().to_string(),
        }),
    }
}

/// Caller must hold the lock acquired via `try_acquire_import_state_lock`.
pub(crate) async fn save_import_state(path: &Path, state: &ImportState) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context(ImportStateIoSnafu {
                path: parent.display().to_string(),
            })?;
    }

    let bytes =
        serde_json::to_vec_pretty(state).expect("ImportState should always be serializable");
    let tmp_path = unique_tmp_path(path);
    let mut file = tokio::fs::File::create(&tmp_path)
        .await
        .context(ImportStateIoSnafu {
            path: tmp_path.display().to_string(),
        })?;
    file.write_all(&bytes).await.context(ImportStateIoSnafu {
        path: tmp_path.display().to_string(),
    })?;
    file.sync_all().await.context(ImportStateIoSnafu {
        path: tmp_path.display().to_string(),
    })?;
    // Close before rename; Windows forbids renaming an open file.
    drop(file);

    tokio::fs::rename(&tmp_path, path)
        .await
        .context(ImportStateIoSnafu {
            path: path.display().to_string(),
        })?;
    sync_parent_dir(path).await?;
    Ok(())
}

pub(crate) fn try_acquire_import_state_lock(path: &Path) -> Result<ImportStateLockGuard> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context(ImportStateIoSnafu {
            path: parent.display().to_string(),
        })?;
    }

    let lock_path = import_state_lock_path(path);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .context(ImportStateIoSnafu {
            path: lock_path.display().to_string(),
        })?;
    file.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            ImportStateLockedSnafu {
                path: lock_path.display().to_string(),
            }
            .build()
        } else {
            ImportStateIoSnafu {
                path: lock_path.display().to_string(),
            }
            .into_error(error)
        }
    })?;

    Ok(ImportStateLockGuard { file })
}

fn unique_tmp_path(path: &Path) -> PathBuf {
    let pid = std::process::id();
    let seq = IMPORT_STATE_TMP_ID.fetch_add(1, Ordering::Relaxed);
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!("{file_name}.{pid}.{seq}.tmp"))
}

fn import_state_lock_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!("{file_name}.lock"))
}

fn normalize_import_state_for_resume(state: &mut ImportState) {
    for chunk in &mut state.chunks {
        if chunk.status == ImportChunkStatus::InProgress {
            chunk.status = ImportChunkStatus::Pending;
            chunk.error = None;
        }
    }
}

pub(crate) async fn delete_import_state(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {
            sync_parent_dir(path).await?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source).context(ImportStateIoSnafu {
            path: path.display().to_string(),
        }),
    }
}

#[cfg(unix)]
async fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    let dir = tokio::fs::File::open(parent)
        .await
        .context(ImportStateIoSnafu {
            path: parent.display().to_string(),
        })?;
    dir.sync_all().await.context(ImportStateIoSnafu {
        path: parent.display().to_string(),
    })?;
    Ok(())
}

#[cfg(not(unix))]
async fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;

    const CHILD_LOCK_PATH_ENV: &str = "GREPTIME_IMPORT_STATE_LOCK_PATH";
    const CHILD_LOCK_TEST: &str =
        "data::import_v2::state::tests::test_try_acquire_import_state_lock_child_process";

    #[test]
    fn test_import_state_new_initializes_pending_chunks() {
        let state = ImportState::new("snapshot-1", "127.0.0.1:4000", [1, 2]);

        assert_eq!(state.snapshot_id, "snapshot-1");
        assert_eq!(state.target_addr, "127.0.0.1:4000");
        assert_eq!(state.chunks.len(), 2);
        assert_eq!(state.chunks[0].status, ImportChunkStatus::Pending);
        assert_eq!(state.chunks[1].status, ImportChunkStatus::Pending);
    }

    #[test]
    fn test_set_chunk_status_updates_timestamp_and_error() {
        let mut state = ImportState::new("snapshot-1", "127.0.0.1:4000", [1]);
        let before = state.updated_at;
        state.updated_at = Utc::now() - chrono::Duration::seconds(10);

        state
            .set_chunk_status(1, ImportChunkStatus::Failed, Some("timeout".to_string()))
            .unwrap();
        assert_eq!(state.chunk_status(1), Some(ImportChunkStatus::Failed));
        assert_eq!(state.chunks[0].error.as_deref(), Some("timeout"));
        assert!(state.updated_at > before);
    }

    #[test]
    fn test_set_chunk_status_rejects_unknown_chunk_id() {
        let mut state = ImportState::new("snapshot-1", "127.0.0.1:4000", [1]);

        let error = state
            .set_chunk_status(99, ImportChunkStatus::Completed, None)
            .unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateUnknownChunk { chunk_id, .. } if chunk_id == 99
        ));
    }

    #[tokio::test]
    async fn test_save_and_load_import_state_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let mut state = ImportState::new("snapshot-1", "127.0.0.1:4000", [1, 2]);
        state
            .set_chunk_status(2, ImportChunkStatus::Completed, None)
            .unwrap();

        save_import_state(&path, &state).await.unwrap();
        let loaded = load_import_state(&path).await.unwrap().unwrap();

        assert_eq!(loaded.snapshot_id, state.snapshot_id);
        assert_eq!(loaded.target_addr, state.target_addr);
        assert_eq!(loaded.chunks, state.chunks);
    }

    #[tokio::test]
    async fn test_save_import_state_overwrites_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let mut state = ImportState::new("snapshot-1", "127.0.0.1:4000", [1]);
        save_import_state(&path, &state).await.unwrap();

        state
            .set_chunk_status(1, ImportChunkStatus::Completed, None)
            .unwrap();
        save_import_state(&path, &state).await.unwrap();

        let loaded = load_import_state(&path).await.unwrap().unwrap();
        assert_eq!(loaded.chunk_status(1), Some(ImportChunkStatus::Completed));
    }

    #[test]
    fn test_load_import_state_resets_in_progress_to_pending() {
        let mut state = ImportState::new("snapshot-1", "127.0.0.1:4000", [1, 2]);
        state
            .set_chunk_status(
                2,
                ImportChunkStatus::InProgress,
                Some("running".to_string()),
            )
            .unwrap();

        normalize_import_state_for_resume(&mut state);

        assert_eq!(state.chunk_status(1), Some(ImportChunkStatus::Pending));
        assert_eq!(state.chunk_status(2), Some(ImportChunkStatus::Pending));
        assert_eq!(state.chunks[1].error, None);
    }

    #[test]
    fn test_unique_tmp_path_generates_distinct_paths() {
        let path = Path::new("/tmp/import_state.json");

        let first = unique_tmp_path(path);
        let second = unique_tmp_path(path);

        assert_ne!(first, second);
        assert!(first.starts_with("/tmp"));
        assert!(second.starts_with("/tmp"));
        assert!(
            first
                .file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".tmp")
        );
        assert!(
            second
                .file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".tmp")
        );
    }

    #[test]
    fn test_try_acquire_import_state_lock_rejects_second_holder() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import_state.json");

        let _first = try_acquire_import_state_lock(&path).unwrap();
        // Import state locking guards concurrent CLI processes, so validate cross-process exclusion.
        let output = Command::new(std::env::current_exe().unwrap())
            .arg(CHILD_LOCK_TEST)
            .arg("--ignored")
            .arg("--exact")
            .env(CHILD_LOCK_PATH_ENV, &path)
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "child lock test failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("1 passed"),
            "child lock test did not run the expected ignored test\nstdout:\n{stdout}"
        );
    }

    #[test]
    #[ignore = "spawned by test_try_acquire_import_state_lock_rejects_second_holder"]
    fn test_try_acquire_import_state_lock_child_process() {
        let path = std::env::var_os(CHILD_LOCK_PATH_ENV)
            .expect("child lock path must be set by the parent test");
        let path = PathBuf::from(path);
        let error = try_acquire_import_state_lock(&path).unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateLocked { .. }
        ));
    }

    #[tokio::test]
    async fn test_delete_import_state_ignores_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.json");

        delete_import_state(&path).await.unwrap();
    }

    #[test]
    fn test_default_state_path_prefers_home_and_encodes_snapshot_id() {
        let home = tempdir().unwrap();
        let cwd = tempdir().unwrap();

        let path = default_state_path_with(
            Some(home.path()),
            Some(cwd.path()),
            "../snapshot",
            "127.0.0.1:4000",
        )
        .unwrap();

        assert_eq!(
            path,
            home.path()
                .join(IMPORT_STATE_ROOT)
                .join(IMPORT_STATE_DIR)
                .join(".import_state_%2E%2E%2Fsnapshot_127%2E0%2E0%2E1%3A4000.json")
        );
    }

    #[test]
    fn test_default_state_path_falls_back_to_cwd_when_home_missing() {
        let cwd = tempdir().unwrap();

        let path =
            default_state_path_with(None, Some(cwd.path()), "snapshot-1", "target-a").unwrap();

        assert_eq!(
            path,
            cwd.path().join(".import_state_snapshot-1_target-a.json")
        );
    }

    #[test]
    fn test_default_state_path_isolated_by_target_addr() {
        let cwd = tempdir().unwrap();

        let first = default_state_path_with(None, Some(cwd.path()), "snapshot-1", "127.0.0.1:4000")
            .unwrap();
        let second =
            default_state_path_with(None, Some(cwd.path()), "snapshot-1", "127.0.0.1:4001")
                .unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn test_default_home_dir_prefers_home() {
        let detected = default_home_dir_with(|key| match key {
            "HOME" => Some(std::ffi::OsString::from("/tmp/home")),
            "USERPROFILE" => Some(std::ffi::OsString::from("/tmp/userprofile")),
            _ => None,
        });

        assert_eq!(detected, Some(PathBuf::from("/tmp/home")));
    }

    #[test]
    fn test_default_home_dir_falls_back_to_userprofile() {
        let detected = default_home_dir_with(|key| match key {
            "USERPROFILE" => Some(std::ffi::OsString::from("/tmp/userprofile")),
            _ => None,
        });

        assert_eq!(detected, Some(PathBuf::from("/tmp/userprofile")));
    }

    #[test]
    fn test_default_home_dir_falls_back_to_home_drive_and_path() {
        let detected = default_home_dir_with(|key| match key {
            "HOMEDRIVE" => Some(std::ffi::OsString::from("/tmp")),
            "HOMEPATH" => Some(std::ffi::OsString::from("windows-home")),
            _ => None,
        });

        assert_eq!(detected, Some(PathBuf::from("/tmp").join("windows-home")));
    }
}
