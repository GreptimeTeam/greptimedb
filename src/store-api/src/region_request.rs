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
use std::fmt::{self, Display};

use api::helper::{ColumnDataTypeWrapper, from_pb_time_ranges};
use api::v1::add_column_location::LocationType;
use api::v1::column_def::{
    as_fulltext_option_analyzer, as_fulltext_option_backend, as_skipping_index_type,
};
use api::v1::region::bulk_insert_request::Body;
use api::v1::region::{
    AlterRequest, AlterRequests, BuildIndexRequest, BulkInsertRequest, CloseRequest,
    CompactRequest, CreateRequest, CreateRequests, DeleteRequests, DropRequest, DropRequests,
    FlushRequest, InsertRequests, OpenRequest, TruncateRequest, alter_request, compact_request,
    region_request, truncate_request,
};
use api::v1::{
    self, Analyzer, ArrowIpc, FulltextBackend as PbFulltextBackend, Option as PbOption, Rows,
    SemanticType, SkippingIndexType as PbSkippingIndexType, WriteHint,
};
pub use common_base::AffectedRows;
use common_grpc::flight::FlightDecoder;
use common_recordbatch::DfRecordBatch;
use common_time::{TimeToLive, Timestamp};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{FulltextOptions, SkippingIndexOptions};
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use strum::{AsRefStr, IntoStaticStr};

use crate::logstore::entry;
use crate::metadata::{
    ColumnMetadata, ConvertTimeRangesSnafu, DecodeProtoSnafu, FlightCodecSnafu,
    InvalidIndexOptionSnafu, InvalidRawRegionRequestSnafu, InvalidRegionRequestSnafu,
    InvalidSetRegionOptionRequestSnafu, InvalidUnsetRegionOptionRequestSnafu, MetadataError,
    RegionMetadata, Result, UnexpectedSnafu,
};
use crate::metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY;
use crate::metrics;
use crate::mito_engine_options::{
    SST_FORMAT_KEY, TTL_KEY, TWCS_MAX_OUTPUT_FILE_SIZE, TWCS_TIME_WINDOW, TWCS_TRIGGER_FILE_NUM,
};
use crate::path_utils::table_dir;
use crate::storage::{ColumnId, RegionId, ScanRequest};

/// The type of path to generate.
#[derive(Debug, Clone, Copy, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum PathType {
    /// A bare path - the original path of an engine.
    ///
    /// The path prefix is `{table_dir}/{table_id}_{region_sequence}/`.
    Bare,
    /// A path for the data region of a metric engine table.
    ///
    /// The path prefix is `{table_dir}/{table_id}_{region_sequence}/data/`.
    Data,
    /// A path for the metadata region of a metric engine table.
    ///
    /// The path prefix is `{table_dir}/{table_id}_{region_sequence}/metadata/`.
    Metadata,
}

#[derive(Debug, IntoStaticStr)]
pub enum BatchRegionDdlRequest {
    Create(Vec<(RegionId, RegionCreateRequest)>),
    Drop(Vec<(RegionId, RegionDropRequest)>),
    Alter(Vec<(RegionId, RegionAlterRequest)>),
}

impl BatchRegionDdlRequest {
    /// Converts [Body](region_request::Body) to [`BatchRegionDdlRequest`].
    pub fn try_from_request_body(body: region_request::Body) -> Result<Option<Self>> {
        match body {
            region_request::Body::Creates(creates) => {
                let requests = creates
                    .requests
                    .into_iter()
                    .map(parse_region_create)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(Self::Create(requests)))
            }
            region_request::Body::Drops(drops) => {
                let requests = drops
                    .requests
                    .into_iter()
                    .map(parse_region_drop)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(Self::Drop(requests)))
            }
            region_request::Body::Alters(alters) => {
                let requests = alters
                    .requests
                    .into_iter()
                    .map(parse_region_alter)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(Self::Alter(requests)))
            }
            _ => Ok(None),
        }
    }

    pub fn request_type(&self) -> &'static str {
        self.into()
    }

    pub fn into_region_requests(self) -> Vec<(RegionId, RegionRequest)> {
        match self {
            Self::Create(requests) => requests
                .into_iter()
                .map(|(region_id, request)| (region_id, RegionRequest::Create(request)))
                .collect(),
            Self::Drop(requests) => requests
                .into_iter()
                .map(|(region_id, request)| (region_id, RegionRequest::Drop(request)))
                .collect(),
            Self::Alter(requests) => requests
                .into_iter()
                .map(|(region_id, request)| (region_id, RegionRequest::Alter(request)))
                .collect(),
        }
    }
}

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
    BuildIndex(RegionBuildIndexRequest),
    Truncate(RegionTruncateRequest),
    Catchup(RegionCatchupRequest),
    BulkInserts(RegionBulkInsertsRequest),
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
            region_request::Body::BuildIndex(index) => make_region_build_index(index),
            region_request::Body::Truncate(truncate) => make_region_truncate(truncate),
            region_request::Body::Creates(creates) => make_region_creates(creates),
            region_request::Body::Drops(drops) => make_region_drops(drops),
            region_request::Body::Alters(alters) => make_region_alters(alters),
            region_request::Body::BulkInsert(bulk) => make_region_bulk_inserts(bulk),
            region_request::Body::Sync(_) => UnexpectedSnafu {
                reason: "Sync request should be handled separately by RegionServer",
            }
            .fail(),
            region_request::Body::ListMetadata(_) => UnexpectedSnafu {
                reason: "ListMetadata request should be handled separately by RegionServer",
            }
            .fail(),
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
            r.rows.map(|rows| {
                (
                    region_id,
                    RegionRequest::Put(RegionPutRequest { rows, hint: None }),
                )
            })
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
                    RegionRequest::Delete(RegionDeleteRequest { rows, hint: None }),
                )
            })
        })
        .collect();
    Ok(requests)
}

fn parse_region_create(create: CreateRequest) -> Result<(RegionId, RegionCreateRequest)> {
    let column_metadatas = create
        .column_defs
        .into_iter()
        .map(ColumnMetadata::try_from_column_def)
        .collect::<Result<Vec<_>>>()?;
    let region_id = RegionId::from(create.region_id);
    let table_dir = table_dir(&create.path, region_id.table_id());
    let partition_expr_json = create.partition.as_ref().map(|p| p.expression.clone());
    Ok((
        region_id,
        RegionCreateRequest {
            engine: create.engine,
            column_metadatas,
            primary_key: create.primary_key,
            options: create.options,
            table_dir,
            path_type: PathType::Bare,
            partition_expr_json,
        },
    ))
}

fn make_region_create(create: CreateRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let (region_id, request) = parse_region_create(create)?;
    Ok(vec![(region_id, RegionRequest::Create(request))])
}

fn make_region_creates(creates: CreateRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let mut requests = Vec::with_capacity(creates.requests.len());
    for create in creates.requests {
        requests.extend(make_region_create(create)?);
    }
    Ok(requests)
}

fn parse_region_drop(drop: DropRequest) -> Result<(RegionId, RegionDropRequest)> {
    let region_id = drop.region_id.into();
    Ok((
        region_id,
        RegionDropRequest {
            fast_path: drop.fast_path,
        },
    ))
}

fn make_region_drop(drop: DropRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let (region_id, request) = parse_region_drop(drop)?;
    Ok(vec![(region_id, RegionRequest::Drop(request))])
}

fn make_region_drops(drops: DropRequests) -> Result<Vec<(RegionId, RegionRequest)>> {
    let mut requests = Vec::with_capacity(drops.requests.len());
    for drop in drops.requests {
        requests.extend(make_region_drop(drop)?);
    }
    Ok(requests)
}

fn make_region_open(open: OpenRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = RegionId::from(open.region_id);
    let table_dir = table_dir(&open.path, region_id.table_id());
    Ok(vec![(
        region_id,
        RegionRequest::Open(RegionOpenRequest {
            engine: open.engine,
            table_dir,
            path_type: PathType::Bare,
            options: open.options,
            skip_wal_replay: false,
            checkpoint: None,
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

fn parse_region_alter(alter: AlterRequest) -> Result<(RegionId, RegionAlterRequest)> {
    let region_id = alter.region_id.into();
    let request = RegionAlterRequest::try_from(alter)?;
    Ok((region_id, request))
}

fn make_region_alter(alter: AlterRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let (region_id, request) = parse_region_alter(alter)?;
    Ok(vec![(region_id, RegionRequest::Alter(request))])
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
    // Convert parallelism: a value of 0 indicates no specific parallelism requested (None)
    let parallelism = if compact.parallelism == 0 {
        None
    } else {
        Some(compact.parallelism)
    };
    Ok(vec![(
        region_id,
        RegionRequest::Compact(RegionCompactRequest {
            options,
            parallelism,
        }),
    )])
}

fn make_region_build_index(index: BuildIndexRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = index.region_id.into();
    Ok(vec![(
        region_id,
        RegionRequest::BuildIndex(RegionBuildIndexRequest {}),
    )])
}

fn make_region_truncate(truncate: TruncateRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = truncate.region_id.into();
    match truncate.kind {
        None => InvalidRawRegionRequestSnafu {
            err: "missing kind in TruncateRequest".to_string(),
        }
        .fail(),
        Some(truncate_request::Kind::All(_)) => Ok(vec![(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )]),
        Some(truncate_request::Kind::TimeRanges(time_ranges)) => {
            let time_ranges = from_pb_time_ranges(time_ranges).context(ConvertTimeRangesSnafu)?;

            Ok(vec![(
                region_id,
                RegionRequest::Truncate(RegionTruncateRequest::ByTimeRanges { time_ranges }),
            )])
        }
    }
}

/// Convert [BulkInsertRequest] to [RegionRequest] and group by [RegionId].
fn make_region_bulk_inserts(request: BulkInsertRequest) -> Result<Vec<(RegionId, RegionRequest)>> {
    let region_id = request.region_id.into();
    let Some(Body::ArrowIpc(request)) = request.body else {
        return Ok(vec![]);
    };

    let decoder_timer = metrics::CONVERT_REGION_BULK_REQUEST
        .with_label_values(&["decode"])
        .start_timer();
    let mut decoder =
        FlightDecoder::try_from_schema_bytes(&request.schema).context(FlightCodecSnafu)?;
    let payload = decoder
        .try_decode_record_batch(&request.data_header, &request.payload)
        .context(FlightCodecSnafu)?;
    decoder_timer.observe_duration();
    Ok(vec![(
        region_id,
        RegionRequest::BulkInserts(RegionBulkInsertsRequest {
            region_id,
            payload,
            raw_data: request,
        }),
    )])
}

/// Request to put data into a region.
#[derive(Debug)]
pub struct RegionPutRequest {
    /// Rows to put.
    pub rows: Rows,
    /// Write hint.
    pub hint: Option<WriteHint>,
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
    /// Write hint.
    pub hint: Option<WriteHint>,
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
    /// Directory for table's data home. Usually is composed by catalog and table id
    pub table_dir: String,
    /// Path type for generating paths
    pub path_type: PathType,
    /// Partition expression JSON from table metadata. Set to empty string for a region without partition.
    /// `Option` to keep compatibility with old clients.
    pub partition_expr_json: Option<String>,
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

    /// Returns true when the region belongs to the metric engine's physical table.
    pub fn is_physical_table(&self) -> bool {
        self.options.contains_key(PHYSICAL_TABLE_METADATA_KEY)
    }
}

#[derive(Debug, Clone)]
pub struct RegionDropRequest {
    pub fast_path: bool,
}

/// Open region request.
#[derive(Debug, Clone)]
pub struct RegionOpenRequest {
    /// Region engine name
    pub engine: String,
    /// Directory for table's data home. Usually is composed by catalog and table id
    pub table_dir: String,
    /// Path type for generating paths
    pub path_type: PathType,
    /// Options of the opened region.
    pub options: HashMap<String, String>,
    /// To skip replaying the WAL.
    pub skip_wal_replay: bool,
    /// Replay checkpoint.
    pub checkpoint: Option<ReplayCheckpoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayCheckpoint {
    pub entry_id: u64,
    pub metadata_entry_id: Option<u64>,
}

impl RegionOpenRequest {
    /// Returns true when the region belongs to the metric engine's physical table.
    pub fn is_physical_table(&self) -> bool {
        self.options.contains_key(PHYSICAL_TABLE_METADATA_KEY)
    }
}

/// Close region request.
#[derive(Debug)]
pub struct RegionCloseRequest {}

/// Alter metadata of a region.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RegionAlterRequest {
    /// Kind of alteration to do.
    pub kind: AlterKind,
}

impl RegionAlterRequest {
    /// Checks whether the request is valid, returns an error if it is invalid.
    pub fn validate(&self, metadata: &RegionMetadata) -> Result<()> {
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
        Ok(RegionAlterRequest { kind })
    }
}

/// Kind of the alteration.
#[derive(Debug, PartialEq, Eq, Clone, AsRefStr)]
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
    ModifyColumnTypes {
        /// Columns to change.
        columns: Vec<ModifyColumnType>,
    },
    /// Set region options.
    SetRegionOptions { options: Vec<SetRegionOption> },
    /// Unset region options.
    UnsetRegionOptions { keys: Vec<UnsetRegionOption> },
    /// Set index options.
    SetIndexes { options: Vec<SetIndexOption> },
    /// Unset index options.
    UnsetIndexes { options: Vec<UnsetIndexOption> },
    /// Drop column default value.
    DropDefaults {
        /// Name of columns to drop.
        names: Vec<String>,
    },
    /// Set column default value.
    SetDefaults {
        /// Columns to change.
        columns: Vec<SetDefault>,
    },
    /// Sync column metadatas.
    SyncColumns {
        column_metadatas: Vec<ColumnMetadata>,
    },
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetDefault {
    pub name: String,
    pub default_constraint: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SetIndexOption {
    Fulltext {
        column_name: String,
        options: FulltextOptions,
    },
    Inverted {
        column_name: String,
    },
    Skipping {
        column_name: String,
        options: SkippingIndexOptions,
    },
}

impl SetIndexOption {
    /// Returns the column name of the index option.
    pub fn column_name(&self) -> &String {
        match self {
            SetIndexOption::Fulltext { column_name, .. } => column_name,
            SetIndexOption::Inverted { column_name } => column_name,
            SetIndexOption::Skipping { column_name, .. } => column_name,
        }
    }

    /// Returns true if the index option is fulltext.
    pub fn is_fulltext(&self) -> bool {
        match self {
            SetIndexOption::Fulltext { .. } => true,
            SetIndexOption::Inverted { .. } => false,
            SetIndexOption::Skipping { .. } => false,
        }
    }
}

impl TryFrom<v1::SetIndex> for SetIndexOption {
    type Error = MetadataError;

    fn try_from(value: v1::SetIndex) -> Result<Self> {
        let option = value.options.context(InvalidRawRegionRequestSnafu {
            err: "missing options in SetIndex",
        })?;

        let opt = match option {
            v1::set_index::Options::Fulltext(x) => SetIndexOption::Fulltext {
                column_name: x.column_name.clone(),
                options: FulltextOptions::new(
                    x.enable,
                    as_fulltext_option_analyzer(
                        Analyzer::try_from(x.analyzer).context(DecodeProtoSnafu)?,
                    ),
                    x.case_sensitive,
                    as_fulltext_option_backend(
                        PbFulltextBackend::try_from(x.backend).context(DecodeProtoSnafu)?,
                    ),
                    x.granularity as u32,
                    x.false_positive_rate,
                )
                .context(InvalidIndexOptionSnafu)?,
            },
            v1::set_index::Options::Inverted(i) => SetIndexOption::Inverted {
                column_name: i.column_name,
            },
            v1::set_index::Options::Skipping(s) => SetIndexOption::Skipping {
                column_name: s.column_name,
                options: SkippingIndexOptions::new(
                    s.granularity as u32,
                    s.false_positive_rate,
                    as_skipping_index_type(
                        PbSkippingIndexType::try_from(s.skipping_index_type)
                            .context(DecodeProtoSnafu)?,
                    ),
                )
                .context(InvalidIndexOptionSnafu)?,
            },
        };

        Ok(opt)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UnsetIndexOption {
    Fulltext { column_name: String },
    Inverted { column_name: String },
    Skipping { column_name: String },
}

impl UnsetIndexOption {
    pub fn column_name(&self) -> &String {
        match self {
            UnsetIndexOption::Fulltext { column_name } => column_name,
            UnsetIndexOption::Inverted { column_name } => column_name,
            UnsetIndexOption::Skipping { column_name } => column_name,
        }
    }

    pub fn is_fulltext(&self) -> bool {
        match self {
            UnsetIndexOption::Fulltext { .. } => true,
            UnsetIndexOption::Inverted { .. } => false,
            UnsetIndexOption::Skipping { .. } => false,
        }
    }
}

impl TryFrom<v1::UnsetIndex> for UnsetIndexOption {
    type Error = MetadataError;

    fn try_from(value: v1::UnsetIndex) -> Result<Self> {
        let option = value.options.context(InvalidRawRegionRequestSnafu {
            err: "missing options in UnsetIndex",
        })?;

        let opt = match option {
            v1::unset_index::Options::Fulltext(f) => UnsetIndexOption::Fulltext {
                column_name: f.column_name,
            },
            v1::unset_index::Options::Inverted(i) => UnsetIndexOption::Inverted {
                column_name: i.column_name,
            },
            v1::unset_index::Options::Skipping(s) => UnsetIndexOption::Skipping {
                column_name: s.column_name,
            },
        };

        Ok(opt)
    }
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
            AlterKind::ModifyColumnTypes { columns } => {
                for col_to_change in columns {
                    col_to_change.validate(metadata)?;
                }
            }
            AlterKind::SetRegionOptions { .. } => {}
            AlterKind::UnsetRegionOptions { .. } => {}
            AlterKind::SetIndexes { options } => {
                for option in options {
                    Self::validate_column_alter_index_option(
                        option.column_name(),
                        metadata,
                        option.is_fulltext(),
                    )?;
                }
            }
            AlterKind::UnsetIndexes { options } => {
                for option in options {
                    Self::validate_column_alter_index_option(
                        option.column_name(),
                        metadata,
                        option.is_fulltext(),
                    )?;
                }
            }
            AlterKind::DropDefaults { names } => {
                names
                    .iter()
                    .try_for_each(|name| Self::validate_column_existence(name, metadata))?;
            }
            AlterKind::SetDefaults { columns } => {
                columns
                    .iter()
                    .try_for_each(|col| Self::validate_column_existence(&col.name, metadata))?;
            }
            AlterKind::SyncColumns { column_metadatas } => {
                let new_primary_keys = column_metadatas
                    .iter()
                    .filter(|c| c.semantic_type == SemanticType::Tag)
                    .map(|c| (c.column_schema.name.as_str(), c.column_id))
                    .collect::<HashMap<_, _>>();

                let old_primary_keys = metadata
                    .column_metadatas
                    .iter()
                    .filter(|c| c.semantic_type == SemanticType::Tag)
                    .map(|c| (c.column_schema.name.as_str(), c.column_id));

                for (name, id) in old_primary_keys {
                    let primary_key =
                        new_primary_keys
                            .get(name)
                            .with_context(|| InvalidRegionRequestSnafu {
                                region_id: metadata.region_id,
                                err: format!("column {} is not a primary key", name),
                            })?;

                    ensure!(
                        *primary_key == id,
                        InvalidRegionRequestSnafu {
                            region_id: metadata.region_id,
                            err: format!(
                                "column with same name {} has different id, existing: {}, got: {}",
                                name, id, primary_key
                            ),
                        }
                    );
                }

                let new_ts_column = column_metadatas
                    .iter()
                    .find(|c| c.semantic_type == SemanticType::Timestamp)
                    .map(|c| (c.column_schema.name.as_str(), c.column_id))
                    .context(InvalidRegionRequestSnafu {
                        region_id: metadata.region_id,
                        err: "timestamp column not found",
                    })?;

                // Safety: timestamp column must exist.
                let old_ts_column = metadata
                    .column_metadatas
                    .iter()
                    .find(|c| c.semantic_type == SemanticType::Timestamp)
                    .map(|c| (c.column_schema.name.as_str(), c.column_id))
                    .unwrap();

                ensure!(
                    new_ts_column == old_ts_column,
                    InvalidRegionRequestSnafu {
                        region_id: metadata.region_id,
                        err: format!(
                            "timestamp column {} has different id, existing: {}, got: {}",
                            old_ts_column.0, old_ts_column.1, new_ts_column.1
                        ),
                    }
                );
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
            AlterKind::ModifyColumnTypes { columns } => columns
                .iter()
                .any(|col_to_change| col_to_change.need_alter(metadata)),
            AlterKind::SetRegionOptions { .. } => true,
            AlterKind::UnsetRegionOptions { .. } => true,
            AlterKind::SetIndexes { options, .. } => options
                .iter()
                .any(|option| metadata.column_by_name(option.column_name()).is_some()),
            AlterKind::UnsetIndexes { options } => options
                .iter()
                .any(|option| metadata.column_by_name(option.column_name()).is_some()),
            AlterKind::DropDefaults { names } => names
                .iter()
                .any(|name| metadata.column_by_name(name).is_some()),

            AlterKind::SetDefaults { columns } => columns
                .iter()
                .any(|x| metadata.column_by_name(&x.name).is_some()),
            AlterKind::SyncColumns { column_metadatas } => {
                metadata.column_metadatas != *column_metadatas
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

    /// Returns an error if the column's alter index option is invalid.
    fn validate_column_alter_index_option(
        column_name: &String,
        metadata: &RegionMetadata,
        is_fulltext: bool,
    ) -> Result<()> {
        let column = metadata
            .column_by_name(column_name)
            .context(InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!("column {} not found", column_name),
            })?;

        if is_fulltext {
            ensure!(
                column.column_schema.data_type.is_string(),
                InvalidRegionRequestSnafu {
                    region_id: metadata.region_id,
                    err: format!(
                        "cannot change alter index options for non-string column {}",
                        column_name
                    ),
                }
            );
        }

        Ok(())
    }

    /// Returns an error if the column isn't exist.
    fn validate_column_existence(column_name: &String, metadata: &RegionMetadata) -> Result<()> {
        metadata
            .column_by_name(column_name)
            .context(InvalidRegionRequestSnafu {
                region_id: metadata.region_id,
                err: format!("column {} not found", column_name),
            })?;

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
            alter_request::Kind::ModifyColumnTypes(x) => {
                let columns = x
                    .modify_column_types
                    .into_iter()
                    .map(|x| x.into())
                    .collect::<Vec<_>>();
                AlterKind::ModifyColumnTypes { columns }
            }
            alter_request::Kind::DropColumns(x) => {
                let names = x.drop_columns.into_iter().map(|x| x.name).collect();
                AlterKind::DropColumns { names }
            }
            alter_request::Kind::SetTableOptions(options) => AlterKind::SetRegionOptions {
                options: options
                    .table_options
                    .iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<Vec<_>>>()?,
            },
            alter_request::Kind::UnsetTableOptions(options) => AlterKind::UnsetRegionOptions {
                keys: options
                    .keys
                    .iter()
                    .map(|key| UnsetRegionOption::try_from(key.as_str()))
                    .collect::<Result<Vec<_>>>()?,
            },
            alter_request::Kind::SetIndex(o) => AlterKind::SetIndexes {
                options: vec![SetIndexOption::try_from(o)?],
            },
            alter_request::Kind::UnsetIndex(o) => AlterKind::UnsetIndexes {
                options: vec![UnsetIndexOption::try_from(o)?],
            },
            alter_request::Kind::SetIndexes(o) => AlterKind::SetIndexes {
                options: o
                    .set_indexes
                    .into_iter()
                    .map(SetIndexOption::try_from)
                    .collect::<Result<Vec<_>>>()?,
            },
            alter_request::Kind::UnsetIndexes(o) => AlterKind::UnsetIndexes {
                options: o
                    .unset_indexes
                    .into_iter()
                    .map(UnsetIndexOption::try_from)
                    .collect::<Result<Vec<_>>>()?,
            },
            alter_request::Kind::DropDefaults(x) => AlterKind::DropDefaults {
                names: x.drop_defaults.into_iter().map(|x| x.column_name).collect(),
            },
            alter_request::Kind::SetDefaults(x) => AlterKind::SetDefaults {
                columns: x
                    .set_defaults
                    .into_iter()
                    .map(|x| {
                        Ok(SetDefault {
                            name: x.column_name,
                            default_constraint: x.default_constraint.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            },
            alter_request::Kind::SyncColumns(x) => AlterKind::SyncColumns {
                column_metadatas: x
                    .column_defs
                    .into_iter()
                    .map(ColumnMetadata::try_from_column_def)
                    .collect::<Result<Vec<_>>>()?,
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
    /// It allows adding existing columns. However, the existing column must have the same metadata
    /// and the location must be None.
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

        if let Some(existing_column) =
            metadata.column_by_name(&self.column_metadata.column_schema.name)
        {
            // If the column already exists.
            ensure!(
                *existing_column == self.column_metadata,
                InvalidRegionRequestSnafu {
                    region_id: metadata.region_id,
                    err: format!(
                        "column {} already exists with different metadata, existing: {:?}, got: {:?}",
                        self.column_metadata.column_schema.name,
                        existing_column,
                        self.column_metadata,
                    ),
                }
            );
            ensure!(
                self.location.is_none(),
                InvalidRegionRequestSnafu {
                    region_id: metadata.region_id,
                    err: format!(
                        "column {} already exists, but location is specified",
                        self.column_metadata.column_schema.name
                    ),
                }
            );
        }

        if let Some(existing_column) = metadata.column_by_id(self.column_metadata.column_id) {
            // Ensures the existing column has the same name.
            ensure!(
                existing_column.column_schema.name == self.column_metadata.column_schema.name,
                InvalidRegionRequestSnafu {
                    region_id: metadata.region_id,
                    err: format!(
                        "column id {} already exists with different name {}",
                        self.column_metadata.column_id, existing_column.column_schema.name
                    ),
                }
            );
        }

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
pub struct ModifyColumnType {
    /// Schema of the column to modify.
    pub column_name: String,
    /// Column will be changed to this type.
    pub target_type: ConcreteDataType,
}

impl ModifyColumnType {
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

impl From<v1::ModifyColumnType> for ModifyColumnType {
    fn from(modify_column_type: v1::ModifyColumnType) -> Self {
        let target_type = ColumnDataTypeWrapper::new(
            modify_column_type.target_type(),
            modify_column_type.target_type_extension,
        )
        .into();

        ModifyColumnType {
            column_name: modify_column_type.column_name,
            target_type,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum SetRegionOption {
    Ttl(Option<TimeToLive>),
    // Modifying TwscOptions with values as (option name, new value).
    Twsc(String, String),
    // Modifying the SST format.
    Format(String),
}

impl TryFrom<&PbOption> for SetRegionOption {
    type Error = MetadataError;

    fn try_from(value: &PbOption) -> std::result::Result<Self, Self::Error> {
        let PbOption { key, value } = value;
        match key.as_str() {
            TTL_KEY => {
                let ttl = TimeToLive::from_humantime_or_str(value)
                    .map_err(|_| InvalidSetRegionOptionRequestSnafu { key, value }.build())?;

                Ok(Self::Ttl(Some(ttl)))
            }
            TWCS_TRIGGER_FILE_NUM | TWCS_MAX_OUTPUT_FILE_SIZE | TWCS_TIME_WINDOW => {
                Ok(Self::Twsc(key.clone(), value.clone()))
            }
            SST_FORMAT_KEY => Ok(Self::Format(value.clone())),
            _ => InvalidSetRegionOptionRequestSnafu { key, value }.fail(),
        }
    }
}

impl From<&UnsetRegionOption> for SetRegionOption {
    fn from(unset_option: &UnsetRegionOption) -> Self {
        match unset_option {
            UnsetRegionOption::TwcsTriggerFileNum => {
                SetRegionOption::Twsc(unset_option.to_string(), String::new())
            }
            UnsetRegionOption::TwcsMaxOutputFileSize => {
                SetRegionOption::Twsc(unset_option.to_string(), String::new())
            }
            UnsetRegionOption::TwcsTimeWindow => {
                SetRegionOption::Twsc(unset_option.to_string(), String::new())
            }
            UnsetRegionOption::Ttl => SetRegionOption::Ttl(Default::default()),
        }
    }
}

impl TryFrom<&str> for UnsetRegionOption {
    type Error = MetadataError;

    fn try_from(key: &str) -> Result<Self> {
        match key.to_ascii_lowercase().as_str() {
            TTL_KEY => Ok(Self::Ttl),
            TWCS_TRIGGER_FILE_NUM => Ok(Self::TwcsTriggerFileNum),
            TWCS_MAX_OUTPUT_FILE_SIZE => Ok(Self::TwcsMaxOutputFileSize),
            TWCS_TIME_WINDOW => Ok(Self::TwcsTimeWindow),
            _ => InvalidUnsetRegionOptionRequestSnafu { key }.fail(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum UnsetRegionOption {
    TwcsTriggerFileNum,
    TwcsMaxOutputFileSize,
    TwcsTimeWindow,
    Ttl,
}

impl UnsetRegionOption {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ttl => TTL_KEY,
            Self::TwcsTriggerFileNum => TWCS_TRIGGER_FILE_NUM,
            Self::TwcsMaxOutputFileSize => TWCS_MAX_OUTPUT_FILE_SIZE,
            Self::TwcsTimeWindow => TWCS_TIME_WINDOW,
        }
    }
}

impl Display for UnsetRegionOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegionFlushRequest {
    pub row_group_size: Option<usize>,
}

#[derive(Debug)]
pub struct RegionCompactRequest {
    pub options: compact_request::Options,
    pub parallelism: Option<u32>,
}

impl Default for RegionCompactRequest {
    fn default() -> Self {
        Self {
            // Default to regular compaction.
            options: compact_request::Options::Regular(Default::default()),
            parallelism: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegionBuildIndexRequest {}

/// Truncate region request.
#[derive(Debug)]
pub enum RegionTruncateRequest {
    /// Truncate all data in the region.
    All,
    ByTimeRanges {
        /// Time ranges to truncate. Both bound are inclusive.
        /// only files that are fully contained in the time range will be truncated.
        /// so no guarantee that all data in the time range will be truncated.
        time_ranges: Vec<(Timestamp, Timestamp)>,
    },
}

/// Catchup region request.
///
/// Makes a readonly region to catch up to leader region changes.
/// There is no effect if it operating on a leader region.
#[derive(Debug, Clone, Copy, Default)]
pub struct RegionCatchupRequest {
    /// Sets it to writable if it's available after it has caught up with all changes.
    pub set_writable: bool,
    /// The `entry_id` that was expected to reply to.
    /// `None` stands replaying to latest.
    pub entry_id: Option<entry::Id>,
    /// Used for metrics metadata region.
    /// The `entry_id` that was expected to reply to.
    /// `None` stands replaying to latest.
    pub metadata_entry_id: Option<entry::Id>,
    /// The hint for replaying memtable.
    pub location_id: Option<u64>,
    /// Replay checkpoint.
    pub checkpoint: Option<ReplayCheckpoint>,
}

#[derive(Debug, Clone)]
pub struct RegionBulkInsertsRequest {
    pub region_id: RegionId,
    pub payload: DfRecordBatch,
    pub raw_data: ArrowIpc,
}

impl RegionBulkInsertsRequest {
    pub fn estimated_size(&self) -> usize {
        self.payload.get_array_memory_size()
    }
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
            RegionRequest::BuildIndex(_) => write!(f, "BuildIndex"),
            RegionRequest::Truncate(_) => write!(f, "Truncate"),
            RegionRequest::Catchup(_) => write!(f, "Catchup"),
            RegionRequest::BulkInserts(_) => write!(f, "BulkInserts"),
        }
    }
}

#[cfg(test)]
mod tests {

    use api::v1::region::RegionColumnDef;
    use api::v1::{ColumnDataType, ColumnDef};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextBackend};

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

    /// Returns a new region metadata for testing. Metadata:
    /// `[(ts, ms, 1), (tag_0, string, 2), (field_0, string, 3), (field_1, bool, 4)]`
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
                column_id: 5,
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
                column_id: 5,
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
                column_id: 2,
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
                        column_id: 5,
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
                        column_id: 6,
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
    fn test_add_existing_column_different_metadata() {
        let metadata = new_metadata();

        // Add existing column with different id.
        let kind = AlterKind::AddColumns {
            columns: vec![AddColumn {
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
            }],
        };
        kind.validate(&metadata).unwrap_err();

        // Add existing column with different type.
        let kind = AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_0",
                        ConcreteDataType::int64_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 2,
                },
                location: None,
            }],
        };
        kind.validate(&metadata).unwrap_err();

        // Add existing column with different name.
        let kind = AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_1",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 2,
                },
                location: None,
            }],
        };
        kind.validate(&metadata).unwrap_err();
    }

    #[test]
    fn test_add_existing_column_with_location() {
        let metadata = new_metadata();
        let kind = AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_0",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 2,
                },
                location: Some(AddColumnLocation::First),
            }],
        };
        kind.validate(&metadata).unwrap_err();
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
    fn test_validate_modify_column_type() {
        let metadata = new_metadata();
        AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnType {
                column_name: "xxxx".to_string(),
                target_type: ConcreteDataType::string_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnType {
                column_name: "field_1".to_string(),
                target_type: ConcreteDataType::date_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnType {
                column_name: "ts".to_string(),
                target_type: ConcreteDataType::date_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnType {
                column_name: "tag_0".to_string(),
                target_type: ConcreteDataType::date_datatype(),
            }],
        }
        .validate(&metadata)
        .unwrap_err();

        let kind = AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnType {
                column_name: "field_0".to_string(),
                target_type: ConcreteDataType::int32_datatype(),
            }],
        };
        kind.validate(&metadata).unwrap();
        assert!(kind.need_alter(&metadata));
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
                        column_id: 5,
                    },
                    location: None,
                },
                AddColumn {
                    column_metadata: ColumnMetadata {
                        column_schema: ColumnSchema::new(
                            "field_2",
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        semantic_type: SemanticType::Field,
                        column_id: 6,
                    },
                    location: None,
                },
            ],
        };
        let request = RegionAlterRequest { kind };
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
            table_dir: "path".to_string(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
        };

        assert!(create.validate().is_err());
    }

    #[test]
    fn test_validate_modify_column_fulltext_options() {
        let kind = AlterKind::SetIndexes {
            options: vec![SetIndexOption::Fulltext {
                column_name: "tag_0".to_string(),
                options: FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::Chinese,
                    false,
                    FulltextBackend::Bloom,
                    1000,
                    0.01,
                ),
            }],
        };
        let request = RegionAlterRequest { kind };
        let mut metadata = new_metadata();
        metadata.schema_version = 1;
        request.validate(&metadata).unwrap();

        let kind = AlterKind::UnsetIndexes {
            options: vec![UnsetIndexOption::Fulltext {
                column_name: "tag_0".to_string(),
            }],
        };
        let request = RegionAlterRequest { kind };
        let mut metadata = new_metadata();
        metadata.schema_version = 1;
        request.validate(&metadata).unwrap();
    }

    #[test]
    fn test_validate_sync_columns() {
        let metadata = new_metadata();
        let kind = AlterKind::SyncColumns {
            column_metadatas: vec![
                ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_1",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 5,
                },
                ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "field_2",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Field,
                    column_id: 6,
                },
            ],
        };
        let err = kind.validate(&metadata).unwrap_err();
        assert!(err.to_string().contains("not a primary key"));

        // Change the timestamp column name.
        let mut column_metadatas_with_different_ts_column = metadata.column_metadatas.clone();
        let ts_column = column_metadatas_with_different_ts_column
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Timestamp)
            .unwrap();
        ts_column.column_schema.name = "ts1".to_string();

        let kind = AlterKind::SyncColumns {
            column_metadatas: column_metadatas_with_different_ts_column,
        };
        let err = kind.validate(&metadata).unwrap_err();
        assert!(
            err.to_string()
                .contains("timestamp column ts has different id")
        );

        // Change the primary key column name.
        let mut column_metadatas_with_different_pk_column = metadata.column_metadatas.clone();
        let pk_column = column_metadatas_with_different_pk_column
            .iter_mut()
            .find(|c| c.column_schema.name == "tag_0")
            .unwrap();
        pk_column.column_id = 100;
        let kind = AlterKind::SyncColumns {
            column_metadatas: column_metadatas_with_different_pk_column,
        };
        let err = kind.validate(&metadata).unwrap_err();
        assert!(
            err.to_string()
                .contains("column with same name tag_0 has different id")
        );

        // Add a new field column.
        let mut column_metadatas_with_new_field_column = metadata.column_metadatas.clone();
        column_metadatas_with_new_field_column.push(ColumnMetadata {
            column_schema: ColumnSchema::new("field_2", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 4,
        });
        let kind = AlterKind::SyncColumns {
            column_metadatas: column_metadatas_with_new_field_column,
        };
        kind.validate(&metadata).unwrap();
    }

    #[test]
    fn test_cast_path_type_to_primitive() {
        assert_eq!(PathType::Bare as u8, 0);
        assert_eq!(PathType::Data as u8, 1);
        assert_eq!(PathType::Metadata as u8, 2);
        assert_eq!(
            PathType::try_from(PathType::Bare as u8).unwrap(),
            PathType::Bare
        );
        assert_eq!(
            PathType::try_from(PathType::Data as u8).unwrap(),
            PathType::Data
        );
        assert_eq!(
            PathType::try_from(PathType::Metadata as u8).unwrap(),
            PathType::Metadata
        );
    }
}
