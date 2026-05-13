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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use snafu::{IntoError, OptionExt, ResultExt};
use tokio::io::AsyncWriteExt;

use crate::data::import_v2::error::{
    ImportStateIoSnafu, ImportStateLockedSnafu, ImportStateParseSnafu, ImportStateUnknownTaskSnafu,
    Result,
};
use crate::data::path::encode_path_segment;

const IMPORT_STATE_ROOT: &str = ".greptime";
const IMPORT_STATE_DIR: &str = "import_state";
static IMPORT_STATE_TMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ImportTaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImportTaskKey {
    pub(crate) chunk_id: u32,
    pub(crate) schema: String,
}

impl ImportTaskKey {
    pub(crate) fn new(chunk_id: u32, schema: impl Into<String>) -> Self {
        Self {
            chunk_id,
            schema: schema.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImportTaskState {
    pub(crate) chunk_id: u32,
    pub(crate) schema: String,
    pub(crate) status: ImportTaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImportState {
    pub(crate) snapshot_id: String,
    pub(crate) target_addr: String,
    pub(crate) catalog: String,
    pub(crate) schemas: Vec<String>,
    #[serde(default)]
    pub(crate) ddl_completed: bool,
    pub(crate) updated_at: DateTime<Utc>,
    // Tasks are (chunk-schema) tuples and can reach the tens of thousands;
    // linear scans here are accepted because per-task work is dominated by
    // network I/O and an fsync, but if the bound grows further this should be
    // backed by a HashMap<(chunk_id, schema), index> rebuilt after load.
    pub(crate) tasks: Vec<ImportTaskState>,
}

impl ImportState {
    pub(crate) fn new<I>(
        snapshot_id: impl Into<String>,
        target_addr: impl Into<String>,
        catalog: impl Into<String>,
        schemas: &[String],
        tasks: I,
    ) -> Self
    where
        I: IntoIterator<Item = ImportTaskKey>,
    {
        Self {
            snapshot_id: snapshot_id.into(),
            target_addr: target_addr.into(),
            catalog: catalog.into(),
            schemas: canonical_schema_selection(schemas),
            ddl_completed: false,
            updated_at: Utc::now(),
            tasks: tasks
                .into_iter()
                .map(|task| ImportTaskState {
                    chunk_id: task.chunk_id,
                    schema: task.schema,
                    status: ImportTaskStatus::Pending,
                    error: None,
                })
                .collect(),
        }
    }

    pub(crate) fn mark_ddl_completed(&mut self) {
        self.ddl_completed = true;
        self.updated_at = Utc::now();
    }

    pub(crate) fn task_status(&self, chunk_id: u32, schema: &str) -> Option<ImportTaskStatus> {
        self.tasks
            .iter()
            .find(|task| task.chunk_id == chunk_id && task.schema == schema)
            .map(|task| task.status)
    }

    pub(crate) fn set_task_status(
        &mut self,
        chunk_id: u32,
        schema: &str,
        status: ImportTaskStatus,
        error: Option<String>,
    ) -> Result<()> {
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.chunk_id == chunk_id && task.schema == schema)
            .context(ImportStateUnknownTaskSnafu {
                chunk_id,
                schema: schema.to_string(),
            })?;
        task.status = status;
        task.error = error;
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

pub(crate) fn default_state_path(
    snapshot_id: &str,
    target_addr: &str,
    catalog: &str,
    schemas: &[String],
) -> Option<PathBuf> {
    let home = default_home_dir_with(|key| std::env::var_os(key));
    let cwd = std::env::current_dir().ok();
    default_state_path_with(
        home.as_deref(),
        cwd.as_deref(),
        snapshot_id,
        target_addr,
        catalog,
        schemas,
    )
}

fn default_home_dir_with<F>(get: F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<std::ffi::OsString>,
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
    catalog: &str,
    schemas: &[String],
) -> Option<PathBuf> {
    let file_name = import_state_file_name(snapshot_id, target_addr, catalog, schemas);
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

fn import_state_file_name(
    snapshot_id: &str,
    target_addr: &str,
    catalog: &str,
    schemas: &[String],
) -> String {
    format!(
        ".import_state_{}_{}_{}.json",
        encode_path_segment(snapshot_id),
        encode_path_segment(target_addr),
        import_identity_hash(catalog, schemas)
    )
}

pub(crate) fn canonical_schema_selection(schemas: &[String]) -> Vec<String> {
    let mut canonicalized = schemas
        .iter()
        .map(|schema| schema.to_ascii_lowercase())
        .collect::<Vec<_>>();
    canonicalized.sort();
    canonicalized.dedup();
    canonicalized
}

/// FNV-1a over `(catalog, schemas)`. The output is part of the persisted state
/// filename, so we cannot use `std::collections::hash_map::DefaultHasher` -
/// Rust does not guarantee its algorithm across releases, which would make a
/// state file written by one toolchain undiscoverable by another.
fn import_identity_hash(catalog: &str, schemas: &[String]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = FNV_OFFSET;
    hash = hash_bytes(hash, catalog.as_bytes());
    // 0xff cannot appear in valid UTF-8, so it works as an unambiguous
    // field separator between adjacent identifiers.
    hash = hash_bytes(hash, &[0xff]);
    for schema in canonical_schema_selection(schemas) {
        hash = hash_bytes(hash, schema.as_bytes());
        hash = hash_bytes(hash, &[0xff]);
    }
    format!("{hash:016x}")
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
        if is_lock_contention(&error) {
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

fn is_lock_contention(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock
        || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
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
    for task in &mut state.tasks {
        if task.status == ImportTaskStatus::InProgress {
            task.status = ImportTaskStatus::Pending;
            task.error = None;
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

    fn schemas() -> Vec<String> {
        vec!["public".to_string(), "analytics".to_string()]
    }

    fn tasks() -> Vec<ImportTaskKey> {
        vec![
            ImportTaskKey::new(1, "public"),
            ImportTaskKey::new(2, "analytics"),
        ]
    }

    #[test]
    fn test_import_state_new_initializes_pending_tasks() {
        let state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
            tasks(),
        );

        assert_eq!(state.snapshot_id, "snapshot-1");
        assert_eq!(state.target_addr, "127.0.0.1:4000");
        assert_eq!(state.catalog, "greptime");
        assert_eq!(state.schemas, vec!["analytics", "public"]);
        assert_eq!(state.tasks.len(), 2);
        assert_eq!(state.tasks[0].status, ImportTaskStatus::Pending);
        assert_eq!(state.tasks[1].status, ImportTaskStatus::Pending);
    }

    #[test]
    fn test_set_task_status_updates_timestamp_and_error() {
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
            tasks(),
        );
        let before = state.updated_at;
        state.updated_at = Utc::now() - chrono::Duration::seconds(10);

        state
            .set_task_status(
                1,
                "public",
                ImportTaskStatus::Failed,
                Some("timeout".to_string()),
            )
            .unwrap();
        assert_eq!(
            state.task_status(1, "public"),
            Some(ImportTaskStatus::Failed)
        );
        assert_eq!(state.tasks[0].error.as_deref(), Some("timeout"));
        assert!(state.updated_at > before);
    }

    #[test]
    fn test_set_task_status_rejects_unknown_task() {
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
            tasks(),
        );

        let error = state
            .set_task_status(99, "public", ImportTaskStatus::Completed, None)
            .unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateUnknownTask { chunk_id, schema, .. }
                if chunk_id == 99 && schema == "public"
        ));
    }

    #[tokio::test]
    async fn test_save_and_load_import_state_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
            tasks(),
        );
        state
            .set_task_status(2, "analytics", ImportTaskStatus::Completed, None)
            .unwrap();

        save_import_state(&path, &state).await.unwrap();
        let loaded = load_import_state(&path).await.unwrap().unwrap();

        assert_eq!(loaded.snapshot_id, state.snapshot_id);
        assert_eq!(loaded.target_addr, state.target_addr);
        assert_eq!(loaded.catalog, state.catalog);
        assert_eq!(loaded.schemas, state.schemas);
        assert_eq!(loaded.tasks, state.tasks);
    }

    #[tokio::test]
    async fn test_save_import_state_overwrites_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
            tasks(),
        );
        save_import_state(&path, &state).await.unwrap();

        state
            .set_task_status(1, "public", ImportTaskStatus::Completed, None)
            .unwrap();
        save_import_state(&path, &state).await.unwrap();

        let loaded = load_import_state(&path).await.unwrap().unwrap();
        assert_eq!(
            loaded.task_status(1, "public"),
            Some(ImportTaskStatus::Completed)
        );
    }

    #[test]
    fn test_load_import_state_resets_in_progress_to_pending() {
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
            tasks(),
        );
        state
            .set_task_status(
                2,
                "analytics",
                ImportTaskStatus::InProgress,
                Some("running".to_string()),
            )
            .unwrap();

        normalize_import_state_for_resume(&mut state);

        assert_eq!(
            state.task_status(1, "public"),
            Some(ImportTaskStatus::Pending)
        );
        assert_eq!(
            state.task_status(2, "analytics"),
            Some(ImportTaskStatus::Pending)
        );
        assert_eq!(state.tasks[1].error, None);
    }

    #[test]
    fn test_unique_tmp_path_generates_distinct_paths() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import_state.json");

        let first = unique_tmp_path(&path);
        let second = unique_tmp_path(&path);

        assert_ne!(first, second);
        assert!(first.starts_with(dir.path()));
        assert!(second.starts_with(dir.path()));
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
    fn test_lock_contention_detection_accepts_platform_error() {
        let error = fs2::lock_contended_error();

        assert!(is_lock_contention(&error));
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
            "greptime",
            &schemas(),
        )
        .unwrap();

        assert_eq!(
            path.parent().unwrap(),
            home.path().join(IMPORT_STATE_ROOT).join(IMPORT_STATE_DIR)
        );
        let file_name = path.file_name().unwrap().to_string_lossy();
        assert!(file_name.starts_with(".import_state_%2E%2E%2Fsnapshot_127%2E0%2E0%2E1%3A4000_"));
        assert!(file_name.ends_with(".json"));
    }

    #[test]
    fn test_default_state_path_falls_back_to_cwd_when_home_missing() {
        let cwd = tempdir().unwrap();

        let path = default_state_path_with(
            None,
            Some(cwd.path()),
            "snapshot-1",
            "target-a",
            "greptime",
            &schemas(),
        )
        .unwrap();

        assert_eq!(path.parent().unwrap(), cwd.path());
        let file_name = path.file_name().unwrap().to_string_lossy();
        assert!(file_name.starts_with(".import_state_snapshot-1_target-a_"));
        assert!(file_name.ends_with(".json"));
    }

    #[test]
    fn test_default_state_path_isolated_by_target_addr() {
        let cwd = tempdir().unwrap();

        let first = default_state_path_with(
            None,
            Some(cwd.path()),
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &schemas(),
        )
        .unwrap();
        let second = default_state_path_with(
            None,
            Some(cwd.path()),
            "snapshot-1",
            "127.0.0.1:4001",
            "greptime",
            &schemas(),
        )
        .unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn test_default_state_path_isolated_by_catalog_and_schemas() {
        let cwd = tempdir().unwrap();
        let public_only = vec!["public".to_string()];
        let analytics_only = vec!["analytics".to_string()];

        let first = default_state_path_with(
            None,
            Some(cwd.path()),
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &public_only,
        )
        .unwrap();
        let second = default_state_path_with(
            None,
            Some(cwd.path()),
            "snapshot-1",
            "127.0.0.1:4000",
            "other",
            &public_only,
        )
        .unwrap();
        let third = default_state_path_with(
            None,
            Some(cwd.path()),
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &analytics_only,
        )
        .unwrap();

        assert_ne!(first, second);
        assert_ne!(first, third);
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
