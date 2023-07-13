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

//! Metadata of mito regions.

// rewrite note:
// Structs related to column (ColumnsMetadata etc.) still reference to storage/metadata
//   other structs are ported to this file
// Those builder/descriptor can be simplified
// Structs related to ColumnFamilies are removed

use std::sync::Arc;

use snafu::ensure;
use storage::metadata::{ColumnsMetadata, ColumnsMetadataBuilder, ColumnsMetadataRef};
use storage::schema::{RegionSchema, RegionSchemaRef};
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, RegionDescriptor, RegionDescriptorBuilder, RegionId,
    RowKeyDescriptor, Schema, SchemaRef,
};

use crate::error::{Error, InvalidAlterOperationSnafu, InvalidAlterVersionSnafu, Result};
use crate::manifest::action::{RawColumnsMetadata, RawRegionMetadata};

pub type VersionNumber = u32;

/// Static metadata of region.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionMetadata {
    // The following fields are immutable.
    id: RegionId,
    name: String,

    // The following fields are mutable.
    /// Latest schema of the region.
    schema: RegionSchemaRef,
    pub columns: ColumnsMetadataRef,
    version: VersionNumber,
}

impl RegionMetadata {
    #[inline]
    pub fn id(&self) -> RegionId {
        self.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn schema(&self) -> &RegionSchemaRef {
        &self.schema
    }

    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        self.schema.user_schema()
    }

    #[inline]
    pub fn version(&self) -> u32 {
        self.schema.version()
    }

    /// Checks whether the `req` is valid, returns `Err` if it is invalid.
    pub fn validate_alter(&self, req: &AlterRequest) -> Result<()> {
        ensure!(
            req.version == self.version,
            InvalidAlterVersionSnafu {
                expect: req.version,
                given: self.version,
            }
        );

        match &req.operation {
            AlterOperation::AddColumns { columns } => {
                for col in columns {
                    self.validate_add_column(col)?;
                }
            }
            AlterOperation::DropColumns { names } => {
                for name in names {
                    self.validate_drop_column(name)?;
                }
            }
        }

        Ok(())
    }

    /// Returns a new [RegionMetadata] after alteration, leave `self` unchanged.
    ///
    /// Caller should use [RegionMetadata::validate_alter] to validate the `req` and
    /// ensure the version of the `req` is equal to the version of the metadata.
    ///
    /// # Panics
    /// Panics if `req.version != self.version`.
    pub fn alter(&self, req: &AlterRequest) -> Result<RegionMetadata> {
        // The req should have been validated before.
        assert_eq!(req.version, self.version);

        let mut desc = self.to_descriptor();
        // Apply the alter operation to the descriptor.
        req.operation.apply(&mut desc);

        RegionMetadataBuilder::try_from(desc)?
            .version(self.version + 1) // Bump the metadata version.
            .build()
    }

    fn validate_add_column(&self, add_column: &AddColumn) -> Result<()> {
        // We don't check the case that the column is not nullable but default constraint is null. The
        // caller should guarantee this.
        ensure!(
            add_column.desc.is_nullable() || add_column.desc.default_constraint().is_some(),
            InvalidAlterOperationSnafu {
                name: &add_column.desc.name,
                reason: "add a non null column without default value"
            }
        );

        // Use the store schema to check the column as it contains all internal columns.
        let store_schema = self.schema.store_schema();
        ensure!(
            !store_schema.contains_column(&add_column.desc.name),
            InvalidAlterOperationSnafu {
                name: &add_column.desc.name,
                reason: "duplicate column name"
            }
        );

        Ok(())
    }

    fn validate_drop_column(&self, name: &str) -> Result<()> {
        let store_schema = self.schema.store_schema();
        ensure!(
            store_schema.contains_column(name),
            InvalidAlterOperationSnafu {
                name,
                reason: "no such column"
            }
        );
        ensure!(
            !store_schema.is_key_column(name),
            InvalidAlterOperationSnafu {
                name,
                reason: "it's part of the key columns"
            }
        );
        ensure!(
            store_schema.is_user_column(name),
            InvalidAlterOperationSnafu {
                name,
                reason: "it's an internal column"
            }
        );

        Ok(())
    }

    fn to_descriptor(&self) -> RegionDescriptor {
        let row_key = self.columns.to_row_key_descriptor();
        let mut builder = RegionDescriptorBuilder::default()
            .id(self.id)
            .name(&self.name)
            .row_key(row_key);

        // We could ensure all fields are set here.
        builder.build().unwrap()
    }
}

pub type RegionMetadataRef = Arc<RegionMetadata>;

impl From<&RegionMetadata> for RawRegionMetadata {
    fn from(data: &RegionMetadata) -> RawRegionMetadata {
        RawRegionMetadata {
            id: data.id,
            name: data.name.clone(),
            columns: RawColumnsMetadata::from(&*data.columns),
            version: data.version,
        }
    }
}

impl TryFrom<RawRegionMetadata> for RegionMetadata {
    type Error = Error;

    fn try_from(raw: RawRegionMetadata) -> Result<RegionMetadata> {
        let columns = Arc::new(ColumnsMetadata::from(raw.columns));
        let schema = Arc::new(RegionSchema::new(columns.clone(), raw.version)?);

        Ok(RegionMetadata {
            id: raw.id,
            name: raw.name,
            schema,
            columns,
            version: raw.version,
        })
    }
}

struct RegionMetadataBuilder {
    id: RegionId,
    name: String,
    columns_meta_builder: ColumnsMetadataBuilder,
    version: VersionNumber,
}

impl Default for RegionMetadataBuilder {
    fn default() -> RegionMetadataBuilder {
        RegionMetadataBuilder::new()
    }
}

impl RegionMetadataBuilder {
    fn new() -> RegionMetadataBuilder {
        RegionMetadataBuilder {
            id: 0.into(),
            name: String::new(),
            columns_meta_builder: ColumnsMetadataBuilder::default(),
            version: Schema::INITIAL_VERSION,
        }
    }

    fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    fn id(mut self, id: RegionId) -> Self {
        self.id = id;
        self
    }

    fn version(mut self, version: VersionNumber) -> Self {
        self.version = version;
        self
    }

    fn row_key(mut self, key: RowKeyDescriptor) -> Result<Self> {
        let _ = self.columns_meta_builder.row_key(key)?;

        Ok(self)
    }

    fn build(self) -> Result<RegionMetadata> {
        let columns = Arc::new(self.columns_meta_builder.build()?);
        let schema = Arc::new(RegionSchema::new(columns.clone(), self.version)?);

        Ok(RegionMetadata {
            id: self.id,
            name: self.name,
            schema,
            columns,
            version: self.version,
        })
    }
}
