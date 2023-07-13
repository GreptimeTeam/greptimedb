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

//! Metadata of mito columns.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datatypes::schema::{try_parse_int, Metadata, COMMENT_KEY};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use storage::metadata::{internal_column_descs, is_internal_field_column};
use store_api::storage::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnId, ColumnSchema, RowKeyDescriptor,
    RowKeyDescriptorBuilder,
};

use crate::error::{
    AccessKvMetadataSnafu, BuildColumnDescriptorSnafu, ColIdExistsSnafu, ColNameExistsSnafu,
    MissingTimestampSnafu, ReservedColumnSnafu, Result, ToColumnSchemaSnafu,
};
use crate::manifest::action::RawColumnsMetadata;

const METADATA_COLUMN_ID_KEY: &str = "greptime:storage:column_id";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub desc: ColumnDescriptor,
}

impl ColumnMetadata {
    #[inline]
    pub fn id(&self) -> ColumnId {
        self.desc.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.desc.name
    }

    /// Convert `self` to [`ColumnSchema`] for building a [`StoreSchema`](crate::schema::StoreSchema). This
    /// would store additional metadatas to the ColumnSchema.
    pub fn to_column_schema(&self) -> Result<ColumnSchema> {
        let desc = &self.desc;

        ColumnSchema::new(&desc.name, desc.data_type.clone(), desc.is_nullable())
            .with_metadata(self.to_metadata())
            .with_time_index(self.desc.is_time_index())
            .with_default_constraint(desc.default_constraint().cloned())
            .context(ToColumnSchemaSnafu)
    }

    /// Convert [`ColumnSchema`] in [`StoreSchema`](crate::schema::StoreSchema) to [`ColumnMetadata`].
    pub fn from_column_schema(column_schema: &ColumnSchema) -> Result<ColumnMetadata> {
        let metadata = column_schema.metadata();
        let column_id =
            try_parse_int(metadata, METADATA_COLUMN_ID_KEY, None).context(AccessKvMetadataSnafu)?;
        let comment = metadata.get(COMMENT_KEY).cloned().unwrap_or_default();

        let desc = ColumnDescriptorBuilder::new(
            column_id,
            &column_schema.name,
            column_schema.data_type.clone(),
        )
        .is_nullable(column_schema.is_nullable())
        .is_time_index(column_schema.is_time_index())
        .default_constraint(column_schema.default_constraint().cloned())
        .comment(comment)
        .build()
        .context(BuildColumnDescriptorSnafu)?;

        Ok(ColumnMetadata { desc })
    }

    fn to_metadata(&self) -> Metadata {
        let mut metadata = Metadata::new();
        let _ = metadata.insert(METADATA_COLUMN_ID_KEY.to_string(), self.desc.id.to_string());
        if !self.desc.comment.is_empty() {
            let _ = metadata.insert(COMMENT_KEY.to_string(), self.desc.comment.clone());
        }

        metadata
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnsMetadata {
    /// All columns.
    ///
    /// Columns are organized in the following order:
    /// ```text
    /// key columns, timestamp, [version,] value columns, internal columns
    /// ```
    ///
    /// The key columns, timestamp and version forms the row key.
    pub columns: Vec<ColumnMetadata>,
    /// Maps column name to index of columns, used to fast lookup column by name.
    pub name_to_col_index: HashMap<String, usize>,
    /// Exclusive end index of row key columns.
    pub row_key_end: usize,
    /// Index of timestamp key column.
    pub timestamp_key_index: usize,
    /// Exclusive end index of user columns.
    ///
    /// Columns in `[user_column_end..)` are internal columns.
    pub user_column_end: usize,
}

impl ColumnsMetadata {
    /// Returns an iterator to all row key columns.
    ///
    /// Row key columns includes all key columns, the timestamp column and the
    /// optional version column.
    pub fn iter_row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter().take(self.row_key_end)
    }

    /// Returns an iterator to all value columns (internal columns are excluded).
    pub fn iter_field_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns[self.row_key_end..self.user_column_end].iter()
    }

    pub fn iter_user_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter().take(self.user_column_end)
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.row_key_end
    }

    #[inline]
    pub fn num_field_columns(&self) -> usize {
        self.user_column_end - self.row_key_end
    }

    #[inline]
    pub fn timestamp_key_index(&self) -> usize {
        self.timestamp_key_index
    }

    #[inline]
    pub fn row_key_end(&self) -> usize {
        self.row_key_end
    }

    #[inline]
    pub fn user_column_end(&self) -> usize {
        self.user_column_end
    }

    #[inline]
    pub fn column_metadata(&self, idx: usize) -> &ColumnMetadata {
        &self.columns[idx]
    }

    pub fn to_row_key_descriptor(&self) -> RowKeyDescriptor {
        let mut builder = RowKeyDescriptorBuilder::default();
        for (idx, column) in self.iter_row_key_columns().enumerate() {
            // Not a timestamp column.
            if idx != self.timestamp_key_index {
                builder = builder.push_column(column.desc.clone());
            }
        }
        builder = builder.timestamp(self.column_metadata(self.timestamp_key_index).desc.clone());
        // Since the metadata is built from descriptor, so it should always be able to build the descriptor back.
        builder.build().unwrap()
    }
}

pub type ColumnsMetadataRef = Arc<ColumnsMetadata>;

impl From<&ColumnsMetadata> for RawColumnsMetadata {
    fn from(data: &ColumnsMetadata) -> RawColumnsMetadata {
        RawColumnsMetadata {
            columns: data.columns.clone(),
            row_key_end: data.row_key_end,
            timestamp_key_index: data.timestamp_key_index,
            user_column_end: data.user_column_end,
        }
    }
}

impl From<RawColumnsMetadata> for ColumnsMetadata {
    fn from(raw: RawColumnsMetadata) -> ColumnsMetadata {
        let name_to_col_index = raw
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.desc.name.clone(), i))
            .collect();

        ColumnsMetadata {
            columns: raw.columns,
            name_to_col_index,
            row_key_end: raw.row_key_end,
            timestamp_key_index: raw.timestamp_key_index,
            user_column_end: raw.user_column_end,
        }
    }
}

#[derive(Default)]
pub struct ColumnsMetadataBuilder {
    pub columns: Vec<ColumnMetadata>,
    name_to_col_index: HashMap<String, usize>,
    /// Column id set, used to validate column id uniqueness.
    column_ids: HashSet<ColumnId>,

    // Row key metadata:
    row_key_end: usize,
    timestamp_key_index: Option<usize>,
}

impl ColumnsMetadataBuilder {
    pub fn row_key(&mut self, key: RowKeyDescriptor) -> Result<&mut Self> {
        for col in key.columns {
            let _ = self.push_row_key_column(col)?;
        }

        // TODO(yingwen): Validate this is a timestamp column.
        self.timestamp_key_index = Some(self.columns.len());
        let _ = self.push_row_key_column(key.timestamp)?;
        self.row_key_end = self.columns.len();

        Ok(self)
    }

    fn push_row_key_column(&mut self, desc: ColumnDescriptor) -> Result<&mut Self> {
        self.push_field_column(desc)
    }

    pub fn push_field_column(&mut self, desc: ColumnDescriptor) -> Result<&mut Self> {
        ensure!(
            !is_internal_field_column(&desc.name),
            ReservedColumnSnafu { name: &desc.name }
        );

        self.push_new_column(desc)
    }

    fn push_new_column(&mut self, desc: ColumnDescriptor) -> Result<&mut Self> {
        ensure!(
            !self.name_to_col_index.contains_key(&desc.name),
            ColNameExistsSnafu { name: &desc.name }
        );
        ensure!(
            !self.column_ids.contains(&desc.id),
            ColIdExistsSnafu { id: desc.id }
        );

        let column_name = desc.name.clone();
        let column_id = desc.id;
        let meta = ColumnMetadata { desc };

        let column_index = self.columns.len();
        self.columns.push(meta);
        let _ = self.name_to_col_index.insert(column_name, column_index);
        let _ = self.column_ids.insert(column_id);

        Ok(self)
    }

    pub fn build(mut self) -> Result<ColumnsMetadata> {
        let timestamp_key_index = self.timestamp_key_index.context(MissingTimestampSnafu)?;

        let user_column_end = self.columns.len();
        // Setup internal columns.
        for internal_desc in internal_column_descs() {
            let _ = self.push_new_column(internal_desc)?;
        }

        Ok(ColumnsMetadata {
            columns: self.columns,
            name_to_col_index: self.name_to_col_index,
            row_key_end: self.row_key_end,
            timestamp_key_index,
            user_column_end,
        })
    }
}
