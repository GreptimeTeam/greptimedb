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
use std::fmt;
use std::time::Duration;

use api::helper::ColumnDataTypeWrapper;
use api::v1::add_column_location::LocationType;
use api::v1::column_def::as_fulltext_option;
use api::v1::region::{
    alter_request, compact_request, region_request, AlterRequest, AlterRequests, CloseRequest,
    CompactRequest, CreateRequest, CreateRequests, DeleteRequests, DropRequest, DropRequests,
    FlushRequest, InsertRequests, OpenRequest, TruncateRequest,
};
use api::v1::{self, Analyzer, ChangeTableOption, Rows, SemanticType};
pub use common_base::AffectedRows;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::FulltextOptions;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use strum::IntoStaticStr;

use crate::logstore::entry;
use crate::metadata::{
    ColumnMetadata, DecodeProtoSnafu, InvalidRawRegionRequestSnafu,
    InvalidRegionOptionChangeRequestSnafu, InvalidRegionRequestSnafu, MetadataError,
    RegionMetadata, Result,
};
use crate::mito_engine_options::{
    TTL_KEY, TWCS_MAX_ACTIVE_WINDOW_FILES, TWCS_MAX_ACTIVE_WINDOW_RUNS,
    TWCS_MAX_INACTIVE_WINDOW_FILES, TWCS_MAX_INACTIVE_WINDOW_RUNS, TWCS_MAX_OUTPUT_FILE_SIZE,
    TWCS_TIME_WINDOW,
};
use crate::path_utils::region_dir;
use crate::storage::{ColumnId, RegionId, ScanRequest};

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
    /// Convert [Body](region_request::Body) to a group of [RegionRequest] with region id.
    /// Inserts/Deletes request might become multiple requests. Others are one-to-one.
    pub fn try_from_request_body(body: region_request::Body) -> Result<Vec<(RegionId, Self)>> {
        match body {
            region_request::Body::Inserts(inserts) => make_region_puts(inserts),
            region_request::Body::Deletes(deletes) => make_region_deletes(deletes),
            region_request::Body::Create(create) => make_region_create(create),
            region_request::Body::Drop(drop) => make_region_drop(drop),
            region_request::Body::Open(open) => make_region_open(open),
            region_request::Body::Close(close) => make_region_close(close),
            region_request::Body::Alter(alter) => make_region_alter(alter),
            region_request::Body::Flush(flush) => make_region_flush(flush),
            region_request::Body::Compact(compact) => make_region_compact(compact),
            region_request::Body::Truncate(truncate) => make_region_truncate(truncate),
            region_request::Body::Creates(creates) => make_region_creates(creates),
            region_request::Body::Drops(drops) => make_region_drops(drops),
            region_request::Body::Alters(alters) => make_region_alters(alters),
        }
    }

    /// Returns the type name of the request.
    pub fn request_type(&self) -> &'static str {
        self.into()
    }
}

fn make_region_puts(inserts: InsertRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let requests = inserts
        .requests
        .into_iter()
        .filter_map(|r| {
            let region_id = r.region_id.into();
            r.rows
                .map(|rows| (region_id, RegionRequest::Put(RegionPutRequest { rows })))
        })
        .collect();
    Ok(requests)
}

fn make_region_deletes(deletes: DeleteRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let requests = deletes
        .requests
        .into_iter()
        .filter_map(|r| {
            let region_id = r.region_id.into();
            r.rows.map(|rows| {
                (
                    region_id,
                    RegionRequest::Delete(RegionDeleteRequest { rows }),
                )
            })
        })
        .collect();
    Ok(requests)
}

fn make_region_create(create: CreateRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let column_metadatas = create
        .column_defs
        .into_iter()
        .map(ColumnMetadata::try_from_column_def)
        .collect::<Result<Vec<_>>>()?;
    let region_id = create.region_id.into();
    let region_dir = region_dir(&create.path, region_id);
    Ok(vec![(
        region_id,
        RegionRequest::Create(RegionCreateRequest {
            engine: create.engine,
            column_metadatas,
            primary_key: create.primary_key,
            options: create.options,
            region_dir,
        }),
    )])
}

fn make_region_creates(creates: CreateRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let mut requests = Vec::with_capacity(creates.requests.len());
    for create in creates.requests {
        requests.extend(make_region_create(create)?);
    }
    Ok(requests)
}

fn make_region_drop(drop: DropRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = drop.region_id.into();
    Ok(vec![(region_id, RegionRequest::Drop(RegionDropRequest {}))])
}

fn make_region_drops(drops: DropRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let mut requests = Vec::with_capacity(drops.requests.len());
    for drop in drops.requests {
        requests.extend(make_region_drop(drop)?);
    }
    Ok(requests)
}

fn make_region_open(open: OpenRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = open.region_id.into();
    let region_dir = region_dir(&open.path, region_id);
    Ok(vec![(
        region_id,
        RegionRequest::Open(RegionOpenRequest {
            engine: open.engine,
            region_dir,
            options: open.options,
            skip_wal_replay: false,
        }),
    )])
}

fn make_region_close(close: CloseRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = close.region_id.into();
    Ok(vec![(
        region_id,
        RegionRequest::Close(RegionCloseRequest {}),
    )])
}

fn make_region_alter(alter: AlterRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = alter.region_id.into();
    Ok(vec![(
        region_id,
        RegionRequest::Alter(RegionAlterRequest::try_from(alter)?),
    )])
}

fn make_region_alters(alters: AlterRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let mut requests = Vec::with_capacity(alters.requests.len());
    for alter in alters.requests {
        requests.extend(make_region_alter(alter)?);
    }
    Ok(requests)
}

fn make_region_flush(flush: FlushRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = flush.region_id.into();
    Ok(vec![(
        region_id,
        RegionRequest::Flush(RegionFlushRequest {
            row_group_size: None,
        }),
    )])
}

fn make_region_compact(compact: CompactRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = compact.region_id.into();
    let options = compact
        .options
        .unwrap_or(compact_request::Options::Regular(Default::default()));
    Ok(vec![(
        region_id,
        RegionRequest::Compact(RegionCompactRequest { options }),
    )])
}

fn make_region_truncate(truncate: TruncateRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = truncate.region_id.into();
    Ok(vec![(
        region_id,
        RegionRequest::Truncate(RegionTruncateRequest {}),
    )])
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

impl RegionCreateRequest {
    /// Checks whether the request is valid, returns an error if it is invalid.
    pub fn validate(&self) -> Result<()> {
        // time index must exist
        ensure!(
            self.column_metadatas
                .iter()
                .any(|x| x.semantic_type == SemanticType::Timestamp),
            InvalidRegionRequestSnafu {
                region_id: RegionId::new(0, 0),
                err: "missing timestamp column in create region request".to_string(),
            }
        );

        // build column id to indices
        let mut column_id_to_indices = HashMap::with_capacity(self.column_metadatas.len());
        for (i, c) in self.column_metadatas.iter().enumerate() {
            if let Some(previous) = column_id_to_indices.insert(c.column_id, i) {
                return InvalidRegionRequestSnafu {
                    region_id: RegionId::new(0, 0),
                    err: format!(
                        "duplicate column id {} (at position {} and {}) in create region request",
                        c.column_id, previous, i
                    ),
                }
                .fail();
            }
        }

        // primary key must exist
        for column_id in &self.primary_key {
            ensure!(
                column_id_to_indices.contains_key(column_id),
                InvalidRegionRequestSnafu {
                    region_id: RegionId::new(0, 0),
                    err: format!(
                        "missing primary key column {} in create region request",
                        column_id
                    ),
                }
            );
        }

        Ok(())
    }
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
    /// Change columns datatype form the region, only fields are allowed to change.
    ChangeColumnTypes {
        /// Columns to change.
        columns: Vec<ChangeColumnType>,
    },
    /// Change region options.
    ChangeRegionOptions { options: Vec<ChangeOption> },
    /// Change fulltext index options.
    ChangeColumnFulltext {
        column_name: String,
        options: FulltextOptions,
    },
}

impl AlterKind {
    /// Returns an error if the alter kind is invalid.
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
            AlterKind::ChangeColumnTypes { columns } => {
                for col_to_change in columns {
                    col_to_change.validate(metadata)?;
                }
            }
            AlterKind::ChangeRegionOptions { .. } => {}
            AlterKind::ChangeColumnFulltext { column_name, .. } => {
                Self::validate_column_fulltext_option(column_name, metadata)?;
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
            AlterKind::ChangeColumnTypes { columns } => columns
                .iter()
                .any(|col_to_change| col_to_change.need_alter(metadata)),
            AlterKind::ChangeRegionOptions { .. } => {
                // we need to update region options for `ChangeTableOptions`.
                // todo: we need to check if ttl has ever changed.
                true
            }
            AlterKind::ChangeColumnFulltext { column_name, .. } => {
                metadata.column_by_name(column_name).is_some()
            }
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

    /// Returns an error if the column to change fulltext index option is invalid.
    fn validate_column_fulltext_option(
        column_name: &String,
        metadata: &RegionMetadata,
    ) -> Result<()> {
        let column = metadata
            .column_by_name(column_name)
            .context(InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!("column {} not found", column_name),
            })?;

        ensure!(
            column.column_schema.data_type.is_string(),
            InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!(
                    "cannot change fulltext index options for non-string column {}",
                    column_name
                ),
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
            alter_request::Kind::ChangeColumnTypes(x) => {
                let columns = x
                    .change_column_types
                    .into_iter()
                    .map(|x| x.into())
                    .collect::<Vec<_>>();
                AlterKind::ChangeColumnTypes { columns }
            }
            alter_request::Kind::DropColumns(x) => {
                let names = x.drop_columns.into_iter().map(|x| x.name).collect();
                AlterKind::DropColumns { names }
            }
            alter_request::Kind::ChangeTableOptions(change_options) => {
                AlterKind::ChangeRegionOptions {
                    options: change_options
                        .change_table_options
                        .iter()
                        .map(TryFrom::try_from)
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            alter_request::Kind::ChangeColumnFulltext(x) => AlterKind::ChangeColumnFulltext {
                column_name: x.column_name.clone(),
                options: FulltextOptions {
                    enable: x.enable,
                    analyzer: as_fulltext_option(
                        Analyzer::try_from(x.analyzer).context(DecodeProtoSnafu)?,
                    ),
                    case_sensitive: x.case_sensitive,
                },
            },
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

/// Change a column's datatype.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ChangeColumnType {
    /// Schema of the column to modify.
    pub column_name: String,
    /// Column will be changed to this type.
    pub target_type: ConcreteDataType,
}

impl ChangeColumnType {
    /// Returns an error if the column's datatype to change is invalid.
    pub fn validate(&self, metadata: &RegionMetadata) -> Result<()> {
        let column_meta = metadata
            .column_by_name(&self.column_name)
            .with_context(|| InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!("column {} not found", self.column_name),
            })?;

        ensure!(
            matches!(column_meta.semantic_type, SemanticType::Field),
            InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: "'timestamp' or 'tag' column cannot change type".to_string()
            }
        );
        ensure!(
            column_meta
                .column_schema
                .data_type
                .can_arrow_type_cast_to(&self.target_type),
            InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!(
                    "column '{}' cannot be cast automatically to type '{}'",
                    self.column_name, self.target_type
                ),
            }
        );

        Ok(())
    }

    /// Returns true if no column's datatype to change to the region.
    pub fn need_alter(&self, metadata: &RegionMetadata) -> bool {
        debug_assert!(self.validate(metadata).is_ok());
        metadata.column_by_name(&self.column_name).is_some()
    }
}

impl From<v1::ChangeColumnType> for ChangeColumnType {
    fn from(change_column_type: v1::ChangeColumnType) -> Self {
        let target_type = ColumnDataTypeWrapper::new(
            change_column_type.target_type(),
            change_column_type.target_type_extension,
        )
        .into();

        ChangeColumnType {
            column_name: change_column_type.column_name,
            target_type,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum ChangeOption {
    TTL(Duration),
    // Modifying TwscOptions with values as (option name, new value).
    Twsc(String, String),
}

impl TryFrom<&ChangeTableOption> for ChangeOption {
    type Error = MetadataError;

    fn try_from(value: &ChangeTableOption) -> std::result::Result<Self, Self::Error> {
        let ChangeTableOption { key, value } = value;

        match key.as_str() {
            TTL_KEY => {
                let ttl = if value.is_empty() {
                    Duration::from_secs(0)
                } else {
                    humantime::parse_duration(value)
                        .map_err(|_| InvalidRegionOptionChangeRequestSnafu { key, value }.build())?
                };
                Ok(Self::TTL(ttl))
            }
            TWCS_MAX_ACTIVE_WINDOW_RUNS
            | TWCS_MAX_ACTIVE_WINDOW_FILES
            | TWCS_MAX_INACTIVE_WINDOW_FILES
            | TWCS_MAX_INACTIVE_WINDOW_RUNS
            | TWCS_MAX_OUTPUT_FILE_SIZE
            | TWCS_TIME_WINDOW => Ok(Self::Twsc(key.to_string(), value.to_string())),
            _ => InvalidRegionOptionChangeRequestSnafu { key, value }.fail(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegionFlushRequest {
    pub row_group_size: Option<usize>,
}

#[derive(Debug)]
pub struct RegionCompactRequest {
    pub options: compact_request::Options,
}

impl Default for RegionCompactRequest {
    fn default() -> Self {
        Self {
            // Default to regular compaction.
            options: compact_request::Options::Regular(Default::default()),
        }
    }
}

/// Truncate region request.
#[derive(Debug)]
pub struct RegionTruncateRequest {}

/// Catchup region request.
///
/// Makes a readonly region to catch up to leader region changes.
/// There is no effect if it operating on a leader region.
#[derive(Debug, Clone, Copy)]
pub struct RegionCatchupRequest {
    /// Sets it to writable if it's available after it has caught up with all changes.
    pub set_writable: bool,
    /// The `entry_id` that was expected to reply to.
    /// `None` stands replaying to latest.
    pub entry_id: Option<entry::Id>,
    /// The hint for replaying memtable.
    pub location_id: Option<u64>,
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
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer};

    use super::*;
    use crate::metadata::RegionMetadataBuilder;

    #[test]
    fn test_from_proto_location() {
        let proto_location = v1::AddColumnLocation {
            location_type: LocationType::First as i32,
            after_column_name: String::default(),
        };
        let location = AddColumnLocation::try_from(proto_location).unwrap();
        assert_eq!(location, AddColumnLocation::First);

        let proto_location = v1::AddColumnLocation {
            location_type: 10,
            after_column_name: String::default(),
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
                        after_column_name: String::default(),
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
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_1",
                    ConcreteDataType::boolean_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
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
    fn test_validate_change_column_type() {
        let metadata = new_metadata();
        AlterKind::ChangeColumnTypes {
            columns: vec![ChangeColumnType {
                column_name: "xxxx".to_string(),
                target_type: ConcreteDataType::string_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        AlterKind::ChangeColumnTypes {
            columns: vec![ChangeColumnType {
                column_name: "field_1".to_string(),
                target_type: ConcreteDataType::date_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        AlterKind::ChangeColumnTypes {
            columns: vec![ChangeColumnType {
                column_name: "ts".to_string(),
                target_type: ConcreteDataType::date_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        AlterKind::ChangeColumnTypes {
            columns: vec![ChangeColumnType {
                column_name: "tag_0".to_string(),
                target_type: ConcreteDataType::date_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        let kind = AlterKind::ChangeColumnTypes {
            columns: vec![ChangeColumnType {
                column_name: "field_0".to_string(),
                target_type: ConcreteDataType::int32_datatype(),
            }],
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

    #[test]
    fn test_validate_create_region() {
        let column_metadatas = vec![
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            },
        ];
        let create = RegionCreateRequest {
            engine: "mito".to_string(),
            column_metadatas,
            primary_key: vec![3, 4],
            options: HashMap::new(),
            region_dir: "path".to_string(),
        };

        assert!(create.validate().is_err());
    }

    #[test]
    fn test_validate_change_column_fulltext_options() {
        let kind = AlterKind::ChangeColumnFulltext {
            column_name: "tag_0".to_string(),
            options: FulltextOptions {
                enable: true,
                analyzer: FulltextAnalyzer::Chinese,
                case_sensitive: false,
            },
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
