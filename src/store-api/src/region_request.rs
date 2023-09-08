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

use std::collections::HashMap;

use api::helper::ColumnDataTypeWrapper;
use api::v1;
use api::v1::add_column_location::LocationType;
use api::v1::region::{alter_request, region_request, AlterRequest};
use api::v1::Rows;
use datatypes::schema::ColumnSchema;
use snafu::{OptionExt, ResultExt};

use crate::metadata::{
    ColumnMetadata, ConvertDatatypesSnafu, InvalidRawRegionRequestSnafu, MetadataError, Result,
};
use crate::path_utils::region_dir;
use crate::storage::{ColumnId, RegionId, ScanRequest};

#[derive(Debug)]
pub enum RegionRequest {
    // TODO: rename to InsertRequest
    Put(RegionPutRequest),
    Delete(RegionDeleteRequest),
    Create(RegionCreateRequest),
    Drop(RegionDropRequest),
    Open(RegionOpenRequest),
    Close(RegionCloseRequest),
    Alter(RegionAlterRequest),
    Flush(RegionFlushRequest),
    Compact(RegionCompactRequest),
}

impl RegionRequest {
    /// Convert [Body](region_request::Body) to a group of [RegionRequest] with region id.
    /// Inserts/Deletes request might become multiple requests. Others are one-to-one.
    pub fn try_from_request_body(body: region_request::Body) -> Result<Vec<(RegionId, Self)>> {
        match body {
            region_request::Body::Inserts(inserts) => Ok(inserts
                .requests
                .into_iter()
                .filter_map(|r| {
                    let region_id = r.region_id.into();
                    r.rows
                        .map(|rows| (region_id, Self::Put(RegionPutRequest { rows })))
                })
                .collect()),
            region_request::Body::Deletes(deletes) => Ok(deletes
                .requests
                .into_iter()
                .filter_map(|r| {
                    let region_id = r.region_id.into();
                    r.rows
                        .map(|rows| (region_id, Self::Delete(RegionDeleteRequest { rows })))
                })
                .collect()),
            region_request::Body::Create(create) => {
                let column_metadatas = create
                    .column_defs
                    .into_iter()
                    .map(ColumnMetadata::try_from_column_def)
                    .collect::<Result<Vec<_>>>()?;
                let region_id = create.region_id.into();
                let region_dir = region_dir(&create.catalog, &create.schema, region_id);
                Ok(vec![(
                    region_id,
                    Self::Create(RegionCreateRequest {
                        engine: create.engine,
                        column_metadatas,
                        primary_key: create.primary_key,
                        create_if_not_exists: create.create_if_not_exists,
                        options: create.options,
                        region_dir,
                    }),
                )])
            }
            region_request::Body::Drop(drop) => Ok(vec![(
                drop.region_id.into(),
                Self::Drop(RegionDropRequest {}),
            )]),
            region_request::Body::Open(open) => {
                let region_id = open.region_id.into();
                let region_dir = region_dir(&open.catalog, &open.schema, region_id);
                Ok(vec![(
                    region_id,
                    Self::Open(RegionOpenRequest {
                        engine: open.engine,
                        region_dir,
                        options: open.options,
                    }),
                )])
            }
            region_request::Body::Close(close) => Ok(vec![(
                close.region_id.into(),
                Self::Close(RegionCloseRequest {}),
            )]),
            region_request::Body::Alter(alter) => Ok(vec![(
                alter.region_id.into(),
                Self::Alter(RegionAlterRequest::try_from(alter)?),
            )]),
            region_request::Body::Flush(flush) => Ok(vec![(
                flush.region_id.into(),
                Self::Flush(RegionFlushRequest {}),
            )]),
            region_request::Body::Compact(compact) => Ok(vec![(
                compact.region_id.into(),
                Self::Compact(RegionCompactRequest {}),
            )]),
        }
    }
}

/// Request to put data into a region.
#[derive(Debug)]
pub struct RegionPutRequest {
    /// Rows to put.
    pub rows: Rows,
}

#[derive(Debug)]
pub struct RegionReadRequest {
    pub request: ScanRequest,
}

/// Request to delete data from a region.
#[derive(Debug)]
pub struct RegionDeleteRequest {
    /// Keys to rows to delete.
    ///
    /// Each row only contains primary key columns and a time index column.
    pub rows: Rows,
}

#[derive(Debug)]
pub struct RegionCreateRequest {
    /// Region engine name
    pub engine: String,
    /// Columns in this region.
    pub column_metadatas: Vec<ColumnMetadata>,
    /// Columns in the primary key.
    pub primary_key: Vec<ColumnId>,
    /// Create region if not exists.
    pub create_if_not_exists: bool,
    /// Options of the created region.
    pub options: HashMap<String, String>,
    /// Directory for region's data home. Usually is composed by catalog and table id
    pub region_dir: String,
}

#[derive(Debug)]
pub struct RegionDropRequest {}

/// Open region request.
#[derive(Debug)]
pub struct RegionOpenRequest {
    /// Region engine name
    pub engine: String,
    /// Data directory of the region.
    pub region_dir: String,
    /// Options of the opened region.
    pub options: HashMap<String, String>,
}

/// Close region request.
#[derive(Debug)]
pub struct RegionCloseRequest {}

/// Alter metadata of a region.
#[derive(Debug)]
pub struct RegionAlterRequest {
    /// The version of the schema before applying the alteration.
    pub schema_version: u64,
    /// Kind of alteration to do.
    pub kind: AlterKind,
}

impl TryFrom<AlterRequest> for RegionAlterRequest {
    type Error = MetadataError;

    fn try_from(value: AlterRequest) -> Result<Self> {
        let kind = value.kind.context(InvalidRawRegionRequestSnafu {
            err: "missing kind in AlterRequest",
        })?;

        let kind = AlterKind::try_from(kind)?;
        Ok(RegionAlterRequest {
            schema_version: value.schema_version,
            kind,
        })
    }
}

/// Kind of the alteration.
#[derive(Debug)]
pub enum AlterKind {
    /// Add columns to the region.
    AddColumns {
        /// Columns to add.
        columns: Vec<AddColumn>,
    },
    /// Drop columns from the region, only value columns are allowed to drop.
    DropColumns {
        /// Name of columns to drop.
        names: Vec<String>,
    },
}

impl TryFrom<alter_request::Kind> for AlterKind {
    type Error = MetadataError;

    fn try_from(kind: alter_request::Kind) -> Result<Self> {
        let alter_kind = match kind {
            alter_request::Kind::AddColumns(x) => {
                let columns = x
                    .add_columns
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>>>()?;
                AlterKind::AddColumns { columns }
            }
            alter_request::Kind::DropColumns(x) => {
                let names = x.drop_columns.into_iter().map(|x| x.name).collect();
                AlterKind::DropColumns { names }
            }
        };

        Ok(alter_kind)
    }
}

/// Add a column.
#[derive(Debug)]
pub struct AddColumn {
    /// Metadata of the column to add.
    pub column_metadata: ColumnMetadata,
    /// Location to add the column.
    pub location: AddColumnLocation,
}

impl TryFrom<v1::region::AddColumn> for AddColumn {
    type Error = MetadataError;

    fn try_from(add_column: v1::region::AddColumn) -> Result<Self> {
        let column_def = add_column
            .column_def
            .context(InvalidRawRegionRequestSnafu {
                err: "missing column_def in AddColumn",
            })?;

        let column_metadata = ColumnMetadata::try_from_column_def(column_def)?;
        let location = add_column.location.context(InvalidRawRegionRequestSnafu {
            err: "missing location in AddColumn",
        })?;
        let location = AddColumnLocation::try_from(location)?;

        Ok(AddColumn {
            column_metadata,
            location,
        })
    }
}

/// Location to add a column.
#[derive(Debug)]
pub enum AddColumnLocation {
    /// Add the column to the first position of columns.
    First,
    /// Add the column after specific column.
    After {
        /// Add the column after this column.
        column_name: String,
    },
}

impl TryFrom<v1::AddColumnLocation> for AddColumnLocation {
    type Error = MetadataError;

    fn try_from(location: v1::AddColumnLocation) -> Result<Self> {
        let location_type = LocationType::from_i32(location.location_type).context(
            InvalidRawRegionRequestSnafu {
                err: format!("unknown location type {}", location.location_type),
            },
        )?;
        let add_column_location = match location_type {
            LocationType::First => AddColumnLocation::First,
            LocationType::After => AddColumnLocation::After {
                column_name: location.after_column_name,
            },
        };

        Ok(add_column_location)
    }
}

#[derive(Debug)]
pub struct RegionFlushRequest {}

#[derive(Debug)]
pub struct RegionCompactRequest {}
