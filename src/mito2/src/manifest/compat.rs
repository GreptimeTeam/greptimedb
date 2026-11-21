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

//! Reject unpublished prototype task-state records without adding persisted fields.
//! IgnoredAny skips file metadata without allocating a second manifest tree.

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use serde::Deserialize;
use serde::de::IgnoredAny;
use snafu::{ResultExt, ensure};

use crate::error::{Result, SerdeJsonSnafu, UnsupportedOperationSnafu, Utf8Snafu};

/// Serialized tag of [`crate::manifest::action::RegionMetaAction::Edit`].
///
/// The delta guard inspects raw JSON, so serde cannot select the variant for it.
/// Keep in sync with the enum; `compat::tests` fails if the tag drifts.
const EDIT_ACTION_TAG: &str = "Edit";

#[derive(Deserialize)]
struct TaskStateGuard {
    table_compaction: Option<IgnoredAny>,
    extensions: Option<IgnoredAny>,
}

impl TaskStateGuard {
    fn validate(&self) -> Result<()> {
        ensure!(self.table_compaction.is_none() && self.extensions.is_none(), UnsupportedOperationSnafu {
            err_msg: "Legacy compaction task state requires explicit migration; it cannot be discarded".to_string(),
        });
        Ok(())
    }
}

/// Validates a standalone manifest, such as a staging blob.
///
/// Staging manifests are recovery evidence like checkpoints and deltas, so they
/// must not silently drop prototype task state either.
pub(crate) fn validate_manifest(bytes: &[u8]) -> Result<()> {
    let data = std::str::from_utf8(bytes).context(Utf8Snafu)?;
    let state: TaskStateGuard = serde_json::from_str(data).context(SerdeJsonSnafu)?;
    state.validate()
}

pub(crate) fn validate_checkpoint(json: &str) -> Result<()> {
    #[derive(Deserialize)]
    struct CheckpointGuard {
        checkpoint: Option<TaskStateGuard>,
    }
    let guard: CheckpointGuard = serde_json::from_str(json).context(SerdeJsonSnafu)?;
    if let Some(state) = guard.checkpoint {
        state.validate()?;
    }
    Ok(())
}

pub(crate) fn validate_actions(json: &str) -> Result<()> {
    #[derive(Deserialize)]
    struct ActionListGuard {
        actions: Vec<HashMap<String, TaskStateGuard>>,
    }
    let guard: ActionListGuard = serde_json::from_str(json).context(SerdeJsonSnafu)?;
    for action in guard.actions {
        if let Some(edit) = action.get(EDIT_ACTION_TAG) {
            edit.validate()?;
        }
    }
    Ok(())
}
