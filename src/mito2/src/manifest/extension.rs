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

//! Feature-owned state shares the file-list transaction and checkpoint.
//!
//! Flattening preserves the legacy extension wire key. Other namespaces use the
//! reserved `extensions` map. Unsupported extensions fail during decoding, before
//! replay, migration or GC; unrelated optional core fields remain forward-compatible.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
#[cfg(feature = "enterprise")]
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};
use store_api::storage::RegionId;

use crate::error::Result;
#[cfg(feature = "enterprise")]
use crate::manifest::table_compaction::{TableCompactionAction, TableCompactionState};
use crate::sst::file::FileMeta;

/// Extension operations persisted atomically with a region edit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExtensionEdit {
    #[cfg(feature = "enterprise")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_compaction: Option<Box<TableCompactionAction>>,
    #[cfg(not(feature = "enterprise"))]
    #[serde(
        default,
        rename = "table_compaction",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "reject_unsupported_value"
    )]
    unsupported_table_compaction: Option<serde_json::Value>,
    #[serde(
        default,
        rename = "extensions",
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "reject_unsupported"
    )]
    unsupported: BTreeMap<String, serde_json::Value>,
}

impl ManifestExtensionEdit {
    pub fn is_empty(&self) -> bool {
        #[cfg(feature = "enterprise")]
        if self.table_compaction.is_some() {
            return false;
        }
        self.unsupported.is_empty()
    }

    /// Staging edits carry complete extension snapshots; later snapshots win.
    pub(crate) fn merge(&mut self, other: Self) {
        #[cfg(feature = "enterprise")]
        if other.table_compaction.is_some() {
            self.table_compaction = other.table_compaction;
        }
        self.unsupported.extend(other.unsupported);
    }
}

#[cfg(feature = "enterprise")]
impl From<TableCompactionAction> for ManifestExtensionEdit {
    fn from(action: TableCompactionAction) -> Self {
        Self {
            table_compaction: Some(Box::new(action)),
            ..Default::default()
        }
    }
}

/// Feature-owned state restored identically into the manifest and query Version.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExtensions {
    #[cfg(feature = "enterprise")]
    #[serde(default, skip_serializing_if = "TableCompactionState::is_empty")]
    pub table_compaction: Arc<TableCompactionState>,
    #[cfg(not(feature = "enterprise"))]
    #[serde(
        default,
        rename = "table_compaction",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "reject_unsupported_value"
    )]
    unsupported_table_compaction: Option<serde_json::Value>,
    #[serde(
        default,
        rename = "extensions",
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "reject_unsupported"
    )]
    unsupported: BTreeMap<String, serde_json::Value>,
}

impl ManifestExtensions {
    pub fn is_empty(&self) -> bool {
        #[cfg(feature = "enterprise")]
        if !self.table_compaction.is_empty() {
            return false;
        }
        self.unsupported.is_empty()
    }

    pub(crate) fn apply(&mut self, edit: ManifestExtensionEdit) {
        #[cfg(feature = "enterprise")]
        if let Some(action) = edit.table_compaction {
            Arc::make_mut(&mut self.table_compaction).apply(*action);
        }
        self.unsupported.extend(edit.unsupported);
    }

    pub(crate) fn retire_files<'a>(&mut self, files: impl IntoIterator<Item = &'a FileMeta>) {
        #[cfg(feature = "enterprise")]
        Arc::make_mut(&mut self.table_compaction).retire_files(files);
        #[cfg(not(feature = "enterprise"))]
        let _ = files;
    }

    pub(crate) fn reconcile_files(&mut self, files: &[FileMeta]) {
        #[cfg(feature = "enterprise")]
        Arc::make_mut(&mut self.table_compaction).reconcile_files(files);
        #[cfg(not(feature = "enterprise"))]
        let _ = files;
    }

    pub(crate) fn merge_from(&mut self, other: &Self, region_id: RegionId) -> Result<()> {
        #[cfg(feature = "enterprise")]
        Arc::make_mut(&mut self.table_compaction).merge_from(&other.table_compaction, region_id)?;
        #[cfg(not(feature = "enterprise"))]
        let _ = (other, region_id);
        Ok(())
    }

    pub(crate) fn into_edit(self) -> ManifestExtensionEdit {
        #[cfg(feature = "enterprise")]
        if !self.table_compaction.is_empty() {
            return TableCompactionAction::Remap {
                state: Box::new((*self.table_compaction).clone()),
            }
            .into();
        }
        ManifestExtensionEdit::default()
    }
}

#[cfg(not(feature = "enterprise"))]
fn reject_unsupported_value<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<serde_json::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    if Option::<serde_json::Value>::deserialize(deserializer)?.is_some() {
        return Err(serde::de::Error::custom(
            "Unsupported manifest extension; enterprise capability is required",
        ));
    }
    Ok(None)
}

fn reject_unsupported<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, serde_json::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let fields = BTreeMap::<String, serde_json::Value>::deserialize(deserializer)?;
    if let Some(name) = fields.keys().next() {
        return Err(serde::de::Error::custom(format!(
            "Unsupported manifest extension '{name}'; refusing to discard persistent state"
        )));
    }
    Ok(fields)
}
