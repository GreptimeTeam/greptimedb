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
use std::fmt::{self};

use api::v1::add_column_location::LocationType;
use api::v1::region::{alter_request, region_request, AlterRequest};
use api::v1::{self, Rows, SemanticType};
use snafu::{ensure, OptionExt};
use strum::IntoStaticStr;

use crate::logstore::entry;
use crate::metadata::{
    ColumnMetadata, InvalidRawRegionRequestSnafu, InvalidRegionRequestSnafu, MetadataError,
    RegionMetadata, Result,
};
use crate::path_utils::region_dir;
use crate::storage::{ColumnId, RegionId, ScanRequest};

pub type AffectedRows = usize;

#[derive(Debug, IntoStaticStr)]
pub enum RegionRequest {
    Put(RegionPutRequest),
    Delete(RegionDeleteRequest),
    Create(RegionCreateRequest),
    Drop(RegionDropRequest),
    Open(RegionOpenRequest),
    Close(RegionCloseRequest),
    Alter(RegionAlterRequest),
    Flush(RegionFlushRequest),
    Compact(RegionCompactRequest),
    Truncate(RegionTruncateRequest),
    Catchup(RegionCatchupRequest),
}

impl RegionRequest {
    /// Returns the type name of the [RegionRequest].
    #[inline]
    pub fn request_type(&self) -> &'static str {
        match &self {
            RegionRequest::Put(_) => "put",
            RegionRequest::Delete(_) => "delete",
            RegionRequest::Create(_) => "create",
            RegionRequest::Drop(_) => "drop",
            RegionRequest::Open(_) => "open",
            RegionRequest::Close(_) => "close",
            RegionRequest::Alter(_) => "alter",
            RegionRequest::Flush(_) => "flush",
            RegionRequest::Compact(_) => "compact",
            RegionRequest::Truncate(_) => "truncate",
            RegionRequest::Catchup(_) => "catchup",
        }
    }

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
                let region_dir = region_dir(&create.path, region_id);
                Ok(vec![(
                    region_id,
                    Self::Create(RegionCreateRequest {
                        engine: create.engine,
                        column_metadatas,
                        primary_key: create.primary_key,
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
                let region_dir = region_dir(&open.path, region_id);
                Ok(vec![(
                    region_id,
                    Self::Open(RegionOpenRequest {
                        engine: open.engine,
                        region_dir,
                        options: open.options,
                        skip_wal_replay: false,
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
                Self::Flush(RegionFlushRequest {
                    row_group_size: None,
                }),
            )]),
            region_request::Body::Compact(compact) => Ok(vec![(
                compact.region_id.into(),
                Self::Compact(RegionCompactRequest {}),
            )]),
            region_request::Body::Truncate(truncate) => Ok(vec![(
                truncate.region_id.into(),
                Self::Truncate(RegionTruncateRequest {}),
            )]),
        }
    }

    /// Returns the type name of the request.
    pub fn type_name(&self) -> &'static str {
        self.into()
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

#[derive(Debug, Clone)]
pub struct RegionCreateRequest {
    /// Region engine name
    pub engine: String,
    /// Columns in this region.
    pub column_metadatas: Vec<ColumnMetadata>,
    /// Columns in the primary key.
    pub primary_key: Vec<ColumnId>,
    /// Options of the created region.
    pub options: HashMap<String, String>,
    /// Directory for region's data home. Usually is composed by catalog and table id
    pub region_dir: String,
}

#[derive(Debug, Clone, Default)]
pub struct RegionDropRequest {}

/// Open region request.
#[derive(Debug, Clone)]
pub struct RegionOpenRequest {
    /// Region engine name
    pub engine: String,
    /// Data directory of the region.
    pub region_dir: String,
    /// Options of the opened region.
    pub options: HashMap<String, String>,
    /// To skip replaying the WAL.
    pub skip_wal_replay: bool,
}

/// Close region request.
#[derive(Debug)]
pub struct RegionCloseRequest {}

/// Alter metadata of a region.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RegionAlterRequest {
    /// The version of the schema before applying the alteration.
    pub schema_version: u64,
    /// Kind of alteration to do.
    pub kind: AlterKind,
}

impl RegionAlterRequest {
    /// Checks whether the request is valid, returns an error if it is invalid.
    pub fn validate(&self, metadata: &RegionMetadata) -> Result<()> {
        ensure!(
            metadata.schema_version == self.schema_version,
            InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!(
                    "region schema version {} is not equal to request schema version {}",
                    metadata.schema_version, self.schema_version
                ),
            }
        );

        self.kind.validate(metadata)?;

        Ok(())
    }

    /// Returns true if we need to apply the request to the region.
    ///
    /// The `request` should be valid.
    pub fn need_alter(&self, metadata: &RegionMetadata) -> bool {
        debug_assert!(self.validate(metadata).is_ok());
        self.kind.need_alter(metadata)
    }
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
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AlterKind {
    /// Add columns to the region.
    AddColumns {
        /// Columns to add.
        columns: Vec<AddColumn>,
    },
    /// Drop columns from the region, only fields are allowed to drop.
    DropColumns {
        /// Name of columns to drop.
        names: Vec<String>,
    },
}

impl AlterKind {
    /// Returns an error if the the alter kind is invalid.
    ///
    /// It allows adding column if not exists and dropping column if exists.
    pub fn validate(&self, metadata: &RegionMetadata) -> Result<()> {
        match self {
            AlterKind::AddColumns { columns } => {
                for col_to_add in columns {
                    col_to_add.validate(metadata)?;
                }
            }
            AlterKind::DropColumns { names } => {
                for name in names {
                    Self::validate_column_to_drop(name, metadata)?;
                }
            }
        }
        Ok(())
    }

    /// Returns true if we need to apply the alteration to the region.
    pub fn need_alter(&self, metadata: &RegionMetadata) -> bool {
        debug_assert!(self.validate(metadata).is_ok());
        match self {
            AlterKind::AddColumns { columns } => columns
                .iter()
                .any(|col_to_add| col_to_add.need_alter(metadata)),
            AlterKind::DropColumns { names } => names
                .iter()
                .any(|name| metadata.column_by_name(name).is_some()),
        }
    }

    /// Returns an error if the column to drop is invalid.
    fn validate_column_to_drop(name: &str, metadata: &RegionMetadata) -> Result<()> {
        let Some(column) = metadata.column_by_name(name) else {
            return Ok(());
        };
        ensure!(
            column.semantic_type == SemanticType::Field,
            InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!("column {} is not a field and could not be dropped", name),
            }
        );
        Ok(())
    }
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

/// Adds a column.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AddColumn {
    /// Metadata of the column to add.
    pub column_metadata: ColumnMetadata,
    /// Location to add the column. If location is None, the region adds
    /// the column to the last.
    pub location: Option<AddColumnLocation>,
}

impl AddColumn {
    /// Returns an error if the column to add is invalid.
    ///
    /// It allows adding existing columns.
    pub fn validate(&self, metadata: &RegionMetadata) -> Result<()> {
        ensure!(
            self.column_metadata.column_schema.is_nullable()
                || self
                    .column_metadata
                    .column_schema
                    .default_constraint()
                    .is_some(),
            InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!(
                    "no default value for column {}",
                    self.column_metadata.column_schema.name
                ),
            }
        );

        Ok(())
    }

    /// Returns true if no column to add to the region.
    pub fn need_alter(&self, metadata: &RegionMetadata) -> bool {
        debug_assert!(self.validate(metadata).is_ok());
        metadata
            .column_by_name(&self.column_metadata.column_schema.name)
            .is_none()
    }
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
        let location = add_column
            .location
            .map(AddColumnLocation::try_from)
            .transpose()?;

        Ok(AddColumn {
            column_metadata,
            location,
        })
    }
}

/// Location to add a column.
#[derive(Debug, PartialEq, Eq, Clone)]
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
        let location_type = LocationType::try_from(location.location_type)
            .map_err(|e| InvalidRawRegionRequestSnafu { err: e.to_string() }.build())?;
        let add_column_location = match location_type {
            LocationType::First => AddColumnLocation::First,
            LocationType::After => AddColumnLocation::After {
                column_name: location.after_column_name,
            },
        };

        Ok(add_column_location)
    }
}

#[derive(Debug, Default)]
pub struct RegionFlushRequest {
    pub row_group_size: Option<usize>,
}

#[derive(Debug)]
pub struct RegionCompactRequest {}

/// Truncate region request.
#[derive(Debug)]
pub struct RegionTruncateRequest {}

/// Catchup region request.
///
/// Makes a readonly region to catch up to leader region changes.
/// There is no effect if it operating on a leader region.
#[derive(Debug)]
pub struct RegionCatchupRequest {
    /// Sets it to writable if it's available after it has caught up with all changes.
    pub set_writable: bool,
    /// The `entry_id` that was expected to reply to.
    /// `None` stands replaying to latest.
    pub entry_id: Option<entry::Id>,
}

impl fmt::Display for RegionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegionRequest::Put(_) => write!(f, "Put"),
            RegionRequest::Delete(_) => write!(f, "Delete"),
            RegionRequest::Create(_) => write!(f, "Create"),
            RegionRequest::Drop(_) => write!(f, "Drop"),
            RegionRequest::Open(_) => write!(f, "Open"),
            RegionRequest::Close(_) => write!(f, "Close"),
            RegionRequest::Alter(_) => write!(f, "Alter"),
            RegionRequest::Flush(_) => write!(f, "Flush"),
            RegionRequest::Compact(_) => write!(f, "Compact"),
            RegionRequest::Truncate(_) => write!(f, "Truncate"),
            RegionRequest::Catchup(_) => write!(f, "Catchup"),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::region::RegionColumnDef;
    use api::v1::{ColumnDataType, ColumnDef};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

    use super::*;
    use crate::metadata::RegionMetadataBuilder;

    #[test]
    fn test_from_proto_location() {
        let proto_location = v1::AddColumnLocation {
            location_type: LocationType::First as i32,
            after_column_name: "".to_string(),
        };
        let location = AddColumnLocation::try_from(proto_location).unwrap();
        assert_eq!(location, AddColumnLocation::First);

        let proto_location = v1::AddColumnLocation {
            location_type: 10,
            after_column_name: "".to_string(),
        };
        AddColumnLocation::try_from(proto_location).unwrap_err();

        let proto_location = v1::AddColumnLocation {
            location_type: LocationType::After as i32,
            after_column_name: "a".to_string(),
        };
        let location = AddColumnLocation::try_from(proto_location).unwrap();
        assert_eq!(
            location,
            AddColumnLocation::After {
                column_name: "a".to_string()
            }
        );
    }

    #[test]
    fn test_from_none_proto_add_column() {
        AddColumn::try_from(v1::region::AddColumn {
            column_def: None,
            location: None,
        })
        .unwrap_err();
    }

    #[test]
    fn test_from_proto_alter_request() {
        RegionAlterRequest::try_from(AlterRequest {
            region_id: 0,
            schema_version: 1,
            kind: None,
        })
        .unwrap_err();

        let request = RegionAlterRequest::try_from(AlterRequest {
            region_id: 0,
            schema_version: 1,
            kind: Some(alter_request::Kind::AddColumns(v1::region::AddColumns {
                add_columns: vec![v1::region::AddColumn {
                    column_def: Some(RegionColumnDef {
                        column_def: Some(ColumnDef {
                            name: "a".to_string(),
                            data_type: ColumnDataType::String as i32,
                            is_nullable: true,
                            default_constraint: vec![],
                            semantic_type: SemanticType::Field as i32,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        column_id: 1,
                    }),
                    location: Some(v1::AddColumnLocation {
                        location_type: LocationType::First as i32,
                        after_column_name: "".to_string(),
                    }),
                }],
            })),
        })
        .unwrap();

        assert_eq!(
            request,
            RegionAlterRequest {
                schema_version: 1,
                kind: AlterKind::AddColumns {
                    columns: vec![AddColumn {
                        column_metadata: ColumnMetadata {
                            column_schema: ColumnSchema::new(
                                "a",
                                ConcreteDataType::string_datatype(),
                                true,
                            ),
                            semantic_type: SemanticType::Field,
                            column_id: 1,
                        },
                        location: Some(AddColumnLocation::First),
                    }]
                },
            }
        );
    }

    fn new_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![2]);
        builder.build().unwrap()
    }

    #[test]
    fn test_add_column_validate() {
        let metadata = new_metadata();
        let add_column = AddColumn {
            column_metadata: ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_1",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 4,
            },
            location: None,
        };
        add_column.validate(&metadata).unwrap();
        assert!(add_column.need_alter(&metadata));

        // Add not null column.
        AddColumn {
            column_metadata: ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_1",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 4,
            },
            location: None,
        }
        .validate(&metadata)
        .unwrap_err();

        // Add existing column.
        let add_column = AddColumn {
            column_metadata: ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 4,
            },
            location: None,
        };
        add_column.validate(&metadata).unwrap();
        assert!(!add_column.need_alter(&metadata));
    }

    #[test]
    fn test_add_duplicate_columns() {
        let kind = AlterKind::AddColumns {
            columns: vec![
                AddColumn {
                    column_metadata: ColumnMetadata {
                        column_schema: ColumnSchema::new(
                            "tag_1",
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        semantic_type: SemanticType::Tag,
                        column_id: 4,
                    },
                    location: None,
                },
                AddColumn {
                    column_metadata: ColumnMetadata {
                        column_schema: ColumnSchema::new(
                            "tag_1",
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        semantic_type: SemanticType::Field,
                        column_id: 5,
                    },
                    location: None,
                },
            ],
        };
        let metadata = new_metadata();
        kind.validate(&metadata).unwrap();
        assert!(kind.need_alter(&metadata));
    }

    #[test]
    fn test_validate_drop_column() {
        let metadata = new_metadata();
        let kind = AlterKind::DropColumns {
            names: vec!["xxxx".to_string()],
        };
        kind.validate(&metadata).unwrap();
        assert!(!kind.need_alter(&metadata));

        AlterKind::DropColumns {
            names: vec!["tag_0".to_string()],
        }
        .validate(&metadata)
        .unwrap_err();

        let kind = AlterKind::DropColumns {
            names: vec!["field_0".to_string()],
        };
        kind.validate(&metadata).unwrap();
        assert!(kind.need_alter(&metadata));
    }

    #[test]
    fn test_validate_schema_version() {
        let mut metadata = new_metadata();
        metadata.schema_version = 2;

        RegionAlterRequest {
            schema_version: 1,
            kind: AlterKind::DropColumns {
                names: vec!["field_0".to_string()],
            },
        }
        .validate(&metadata)
        .unwrap_err();
    }

    #[test]
    fn test_validate_add_columns() {
        let kind = AlterKind::AddColumns {
            columns: vec![
                AddColumn {
                    column_metadata: ColumnMetadata {
                        column_schema: ColumnSchema::new(
                            "tag_1",
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        semantic_type: SemanticType::Tag,
                        column_id: 4,
                    },
                    location: None,
                },
                AddColumn {
                    column_metadata: ColumnMetadata {
                        column_schema: ColumnSchema::new(
                            "field_1",
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        semantic_type: SemanticType::Field,
                        column_id: 5,
                    },
                    location: None,
                },
            ],
        };
        let request = RegionAlterRequest {
            schema_version: 1,
            kind,
        };
        let mut metadata = new_metadata();
        metadata.schema_version = 1;
        request.validate(&metadata).unwrap();
    }
}
