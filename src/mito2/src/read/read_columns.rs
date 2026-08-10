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

use datatypes::prelude::ConcreteDataType;
use store_api::storage::ColumnId;

/// Logical columns to read from a region.
///
/// Read columns describe which logical root columns should be read from storage.
/// JSON2 columns can carry query-time target types that are later translated to
/// physical nested parquet paths by the parquet reader.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumns {
    pub cols: Vec<ReadColumn>,
    json_target_types: BTreeMap<ColumnId, ConcreteDataType>,
}

impl ReadColumns {
    pub fn from_deduped_column_ids<I>(column_ids: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let cols = column_ids.into_iter().map(ReadColumn::new).collect();
        ReadColumns {
            cols,
            json_target_types: BTreeMap::new(),
        }
    }

    pub fn new(
        column_ids: impl IntoIterator<Item = ColumnId>,
        json_target_types: BTreeMap<ColumnId, ConcreteDataType>,
    ) -> Self {
        let cols = column_ids.into_iter().map(ReadColumn::new).collect();
        ReadColumns {
            cols,
            json_target_types,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn column_ids_iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.cols.iter().map(|column| column.column_id)
    }

    pub fn column_ids(&self) -> Vec<ColumnId> {
        self.column_ids_iter().collect()
    }

    pub fn columns(&self) -> &[ReadColumn] {
        &self.cols
    }

    pub fn json_target_types(&self) -> &BTreeMap<ColumnId, ConcreteDataType> {
        &self.json_target_types
    }

    pub fn json_target_type(&self, column_id: ColumnId) -> Option<&ConcreteDataType> {
        self.json_target_types.get(&column_id)
    }

    pub fn estimated_size(&self) -> usize {
        self.cols.capacity() * mem::size_of::<ReadColumn>()
            + self
                .cols
                .iter()
                .map(ReadColumn::estimated_size)
                .sum::<usize>()
            + self.json_target_types.len()
                * (mem::size_of::<ColumnId>() + mem::size_of::<ConcreteDataType>())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumn {
    pub column_id: ColumnId,
}

impl ReadColumn {
    pub fn new(column_id: ColumnId) -> Self {
        Self { column_id }
    }

    pub fn estimated_size(&self) -> usize {
        mem::size_of::<ColumnId>()
    }
}
