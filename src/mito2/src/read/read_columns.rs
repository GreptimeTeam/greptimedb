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
use std::mem;
use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use store_api::storage::ColumnId;

pub(crate) type JsonTargetTypes = Arc<BTreeMap<ColumnId, ConcreteDataType>>;

/// Logical columns to read from a region.
///
/// Read columns describe which logical root columns should be read from storage.
/// JSON2 columns can carry query-time target types that are later translated to
/// physical nested parquet paths by the parquet reader.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumns {
    pub col_ids: Vec<ColumnId>,
    json_target_types: JsonTargetTypes,
}

impl ReadColumns {
    pub fn new<I>(col_ids: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        Self {
            col_ids: col_ids.into_iter().collect(),
            json_target_types: Arc::default(),
        }
    }

    pub fn with_json_target_types(
        mut self,
        json_target_types: BTreeMap<ColumnId, ConcreteDataType>,
    ) -> Self {
        self.json_target_types = Arc::new(json_target_types);
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

    pub fn json_target_types(&self) -> &JsonTargetTypes {
        &self.json_target_types
    }

    pub fn json_target_type(&self, column_id: ColumnId) -> Option<&ConcreteDataType> {
        self.json_target_types.get(&column_id)
    }

    pub fn estimated_size(&self) -> usize {
        self.col_ids.capacity() * mem::size_of::<ColumnId>()
            + self.col_ids.len() * mem::size_of::<ColumnId>()
            + self.json_target_types.len()
                * (mem::size_of::<ColumnId>() + mem::size_of::<ConcreteDataType>())
    }
}
