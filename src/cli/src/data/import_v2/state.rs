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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::data::import_v2::error::{
    ImportStateIoSnafu, ImportStateParseSnafu, ImportStateSerializeSnafu,
    ImportStateUnknownChunkSnafu, Result,
};
use crate::data::path::encode_path_segment;

const IMPORT_STATE_DIR: &str = ".greptime/import_state";

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

pub(crate) fn default_state_path(snapshot_id: &str) -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let cwd = std::env::current_dir().ok();
    default_state_path_with(home.as_deref(), cwd.as_deref(), snapshot_id)
}

fn default_state_path_with(
    home: Option<&Path>,
    cwd: Option<&Path>,
    snapshot_id: &str,
) -> Option<PathBuf> {
    let file_name = import_state_file_name(snapshot_id);
    match (home, cwd) {
        (Some(home), _) => Some(home.join(IMPORT_STATE_DIR).join(file_name)),
        (None, Some(cwd)) => Some(cwd.join(file_name)),
        (None, None) => None,
    }
}

fn import_state_file_name(snapshot_id: &str) -> String {
    format!(".import_state_{}.json", encode_path_segment(snapshot_id))
}

pub(crate) async fn load_import_state(path: &Path) -> Result<Option<ImportState>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .context(ImportStateParseSnafu)
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(source).context(ImportStateIoSnafu {
            path: path.display().to_string(),
        }),
    }
}

pub(crate) async fn save_import_state(path: &Path, state: &ImportState) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context(ImportStateIoSnafu {
                path: parent.display().to_string(),
            })?;
    }

    let bytes = serde_json::to_vec_pretty(state).context(ImportStateSerializeSnafu)?;
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, bytes)
        .await
        .context(ImportStateIoSnafu {
            path: tmp_path.display().to_string(),
        })?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .context(ImportStateIoSnafu {
            path: path.display().to_string(),
        })?;
    Ok(())
}

pub(crate) async fn delete_import_state(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source).context(ImportStateIoSnafu {
            path: path.display().to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;

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
    async fn test_delete_import_state_ignores_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.json");

        delete_import_state(&path).await.unwrap();
    }

    #[test]
    fn test_default_state_path_prefers_home_and_encodes_snapshot_id() {
        let home = Path::new("/tmp/home");
        let cwd = Path::new("/tmp/cwd");

        let path = default_state_path_with(Some(home), Some(cwd), "../snapshot").unwrap();

        assert_eq!(
            path,
            home.join(IMPORT_STATE_DIR)
                .join(".import_state_%2E%2E%2Fsnapshot.json")
        );
    }

    #[test]
    fn test_default_state_path_falls_back_to_cwd_when_home_missing() {
        let cwd = Path::new("/tmp/cwd");

        let path = default_state_path_with(None, Some(cwd), "snapshot-1").unwrap();

        assert_eq!(path, cwd.join(".import_state_snapshot-1.json"));
    }
}
