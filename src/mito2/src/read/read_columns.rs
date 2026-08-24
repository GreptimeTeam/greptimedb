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

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use datatypes::arrow::datatypes::DataType;
use datatypes::json::JsonSettings;
use datatypes::types::json_type::JsonNativeType;
use store_api::storage::ColumnId;

pub(crate) type JsonReadTargets = Arc<BTreeMap<ColumnId, JsonReadTarget>>;

/// JSON2 output requested by a read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonReadTarget {
    /// Query-time logical projection.
    Projection(JsonNativeType),
    /// Compaction-time rewrite into a fixed physical layout.
    Rewrite(Json2TargetLayout),
}

/// Fixed JSON2 physical layout used while rewriting compaction input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Json2TargetLayout {
    pub(crate) data_type: DataType,
    pub(crate) extension_metadata: String,
    pub(crate) target_layout: JsonSettings,
}

/// Logical columns to read from a region.
///
/// Read columns describe which logical root columns should be read from storage.
/// JSON2 columns can carry query-time target types that are later translated to
/// physical nested parquet paths by the parquet reader.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReadColumns {
    pub col_ids: Vec<ColumnId>,
    json_read_targets: JsonReadTargets,
}

impl Hash for ReadColumns {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.col_ids.hash(state);
        self.json_read_targets.len().hash(state);
        for (id, target) in self.json_read_targets.iter() {
            id.hash(state);
            match target {
                JsonReadTarget::Projection(target) => {
                    0_u8.hash(state);
                    target.hash(state);
                }
                // Rewrite targets are compaction-only and never used as range cache keys.
                JsonReadTarget::Rewrite(_) => 1_u8.hash(state),
            }
        }
    }
}

impl ReadColumns {
    /// Creates read columns from logical column ids.
    ///
    /// This preserves the input order and duplicate ids.
    pub fn new<I>(col_ids: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        Self {
            col_ids: col_ids.into_iter().collect(),
            json_read_targets: Arc::default(),
        }
    }

    /// Attaches query-time JSON2 projection types.
    pub fn with_json_target_types(
        mut self,
        json_target_types: BTreeMap<ColumnId, JsonNativeType>,
    ) -> Self {
        self.json_read_targets = Arc::new(
            json_target_types
                .into_iter()
                .map(|(id, target)| (id, JsonReadTarget::Projection(target)))
                .collect(),
        );
        self
    }

    /// Attaches fixed JSON2 physical layouts used by compaction readers.
    pub(crate) fn with_json2_target_layouts(
        mut self,
        layouts: BTreeMap<ColumnId, Json2TargetLayout>,
    ) -> Self {
        Arc::make_mut(&mut self.json_read_targets).extend(
            layouts
                .into_iter()
                .map(|(id, layout)| (id, JsonReadTarget::Rewrite(layout))),
        );
        self
    }

    pub fn is_empty(&self) -> bool {
        self.col_ids.is_empty()
    }

    pub fn column_ids_iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.col_ids.iter().copied()
    }

    pub fn column_ids(&self) -> Vec<ColumnId> {
        self.column_ids_iter().collect()
    }

    pub(crate) fn json_read_targets(&self) -> &JsonReadTargets {
        &self.json_read_targets
    }

    pub(crate) fn json_read_target(&self, column_id: &ColumnId) -> Option<&JsonReadTarget> {
        self.json_read_targets.get(column_id)
    }

    /// Returns the query-time JSON2 projection type for a column.
    pub fn json_projection_type(&self, column_id: ColumnId) -> Option<&JsonNativeType> {
        match self.json_read_targets.get(&column_id) {
            Some(JsonReadTarget::Projection(target)) => Some(target),
            _ => None,
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.col_ids.capacity() * mem::size_of::<ColumnId>()
            + self.col_ids.len() * mem::size_of::<ColumnId>()
            + self.json_read_targets.len() * (size_of::<ColumnId>() + size_of::<JsonReadTarget>())
    }
}
