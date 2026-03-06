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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::{GenerateImplicitData, ResultExt, ensure};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::error::{
    Error, Result, StateOperationSnafu, StateParseSnafu, StateSerializeSnafu,
    StateSnapshotMismatchSnafu, StateTargetMismatchSnafu,
};
use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportChunkState {
    pub id: u32,
    pub status: ChunkStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportState {
    pub snapshot_id: Uuid,
    pub target_addr: String,
    pub updated_at: DateTime<Utc>,
    pub chunks: Vec<ImportChunkState>,
}

impl ImportState {
    fn new(snapshot_id: Uuid, target_addr: String, chunks: &[ChunkMeta]) -> Self {
        let chunks = chunks
            .iter()
            .map(|chunk| ImportChunkState {
                id: chunk.id,
                status: if chunk.status == ChunkStatus::Skipped {
                    ChunkStatus::Completed
                } else {
                    ChunkStatus::Pending
                },
                error: None,
            })
            .collect();

        Self {
            snapshot_id,
            target_addr,
            updated_at: Utc::now(),
            chunks,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResumeStats {
    pub completed: usize,
    pub pending: usize,
}

pub struct ImportStateStore {
    path: PathBuf,
    state: Mutex<ImportState>,
}

impl ImportStateStore {
    pub async fn load_or_init(
        snapshot_id: Uuid,
        target_addr: &str,
        chunks: &[ChunkMeta],
    ) -> Result<(Self, bool)> {
        let path = default_state_path(snapshot_id)?;
        if !tokio::fs::try_exists(&path)
            .await
            .context(StateOperationSnafu {
                operation: "check state existence",
                path: path.display().to_string(),
            })?
        {
            let state = ImportState::new(snapshot_id, target_addr.to_string(), chunks);
            let store = Self {
                path,
                state: Mutex::new(state),
            };
            store.persist().await?;
            return Ok((store, false));
        }

        let bytes = tokio::fs::read(&path).await.context(StateOperationSnafu {
            operation: "read state",
            path: path.display().to_string(),
        })?;
        let state: ImportState = serde_json::from_slice(&bytes).context(StateParseSnafu {
            path: path.display().to_string(),
        })?;

        ensure!(
            state.snapshot_id == snapshot_id,
            StateSnapshotMismatchSnafu {
                expected: snapshot_id,
                found: state.snapshot_id
            }
        );
        ensure!(
            state.target_addr == target_addr,
            StateTargetMismatchSnafu {
                expected: target_addr,
                found: state.target_addr.clone()
            }
        );

        Ok((
            Self {
                path,
                state: Mutex::new(state),
            },
            true,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn resume_stats(&self) -> ResumeStats {
        let state = self.state.lock().await;
        let completed = state
            .chunks
            .iter()
            .filter(|chunk| chunk.status == ChunkStatus::Completed)
            .count();
        let pending = state
            .chunks
            .iter()
            .filter(|chunk| chunk.status != ChunkStatus::Completed)
            .count();
        ResumeStats { completed, pending }
    }

    pub async fn is_completed(&self, chunk_id: u32) -> bool {
        let state = self.state.lock().await;
        state
            .chunks
            .iter()
            .find(|chunk| chunk.id == chunk_id)
            .map(|chunk| chunk.status == ChunkStatus::Completed)
            .unwrap_or(false)
    }

    pub async fn update_chunk(
        &self,
        chunk_id: u32,
        status: ChunkStatus,
        error: Option<String>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(chunk) = state.chunks.iter_mut().find(|chunk| chunk.id == chunk_id) {
            chunk.status = status;
            chunk.error = error;
        }
        state.updated_at = Utc::now();
        self.persist_locked(&state).await
    }

    pub async fn persist(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.persist_locked(&state).await
    }

    async fn persist_locked(&self, state: &ImportState) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent)
            .await
            .context(StateOperationSnafu {
                operation: "create state directory",
                path: parent.display().to_string(),
            })?;

        let bytes = serde_json::to_vec_pretty(state).context(StateSerializeSnafu {
            path: self.path.display().to_string(),
        })?;

        let tmp = self.path.with_extension("tmp");
        tokio::fs::write(&tmp, bytes)
            .await
            .context(StateOperationSnafu {
                operation: "write state temp file",
                path: tmp.display().to_string(),
            })?;
        tokio::fs::rename(&tmp, &self.path)
            .await
            .context(StateOperationSnafu {
                operation: "rename state temp file",
                path: self.path.display().to_string(),
            })?;
        Ok(())
    }

    pub async fn remove_file(&self) -> Result<bool> {
        if !tokio::fs::try_exists(&self.path)
            .await
            .context(StateOperationSnafu {
                operation: "check state existence",
                path: self.path.display().to_string(),
            })?
        {
            return Ok(false);
        }

        tokio::fs::remove_file(&self.path)
            .await
            .context(StateOperationSnafu {
                operation: "remove state",
                path: self.path.display().to_string(),
            })?;
        Ok(true)
    }
}

pub fn default_state_path(snapshot_id: Uuid) -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| Error::StateOperation {
        operation: "resolve home directory".into(),
        path: "HOME".into(),
        error: std::io::Error::new(std::io::ErrorKind::NotFound, "HOME is not set"),
        location: snafu::Location::generate(),
    })?;
    let base = PathBuf::from(home);
    Ok(base
        .join(".greptime")
        .join("import_state")
        .join(format!("{snapshot_id}.json")))
}
