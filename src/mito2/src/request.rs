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

//! Worker requests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use api::helper::{
    is_column_type_value_eq, is_semantic_type_eq, proto_value_type, to_proto_value,
    ColumnDataTypeWrapper,
};
use api::v1::column_def::options_from_column_schema;
use api::v1::{ColumnDataType, ColumnSchema, OpType, Rows, SemanticType, Value, WriteHint};
use common_telemetry::info;
use datatypes::prelude::DataType;
use prometheus::HistogramTimer;
use prost::Message;
use smallvec::SmallVec;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::codec::{infer_primary_key_encoding_from_hint, PrimaryKeyEncoding};
use store_api::manifest::ManifestVersion;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::region_engine::{SetRegionRoleStateResponse, SettableRegionRoleState};
use store_api::region_request::{
    AffectedRows, RegionAlterRequest, RegionBulkInsertsRequest, RegionCatchupRequest,
    RegionCloseRequest, RegionCompactRequest, RegionCreateRequest, RegionFlushRequest,
    RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::error::{
    CompactRegionSnafu, ConvertColumnDataTypeSnafu, CreateDefaultSnafu, Error, FillDefaultSnafu,
    FlushRegionSnafu, InvalidRequestSnafu, Result, UnexpectedImpureDefaultSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::MemtableId;
use crate::metrics::COMPACTION_ELAPSED_TOTAL;
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::wal::EntryId;

/// Request to write a region.
#[derive(Debug)]
pub struct WriteRequest {
    /// Region to write.
    pub region_id: RegionId,
    /// Type of the write request.
    pub op_type: OpType,
    /// Rows to write.
    pub rows: Rows,
    /// Map column name to column index in `rows`.
    pub name_to_index: HashMap<String, usize>,
    /// Whether each column has null.
    pub has_null: Vec<bool>,
    /// Write hint.
    pub hint: Option<WriteHint>,
    /// Region metadata on the time of this request is created.
    pub(crate) region_metadata: Option<RegionMetadataRef>,
}

impl WriteRequest {
    /// Creates a new request.
    ///
    /// Returns `Err` if `rows` are invalid.
    pub fn new(
        region_id: RegionId,
        op_type: OpType,
        rows: Rows,
        region_metadata: Option<RegionMetadataRef>,
    ) -> Result<WriteRequest> {
        let mut name_to_index = HashMap::with_capacity(rows.schema.len());
        for (index, column) in rows.schema.iter().enumerate() {
            ensure!(
                name_to_index
                    .insert(column.column_name.clone(), index)
                    .is_none(),
                InvalidRequestSnafu {
                    region_id,
                    reason: format!("duplicate column {}", column.column_name),
                }
            );
        }

        let mut has_null = vec![false; rows.schema.len()];
        for row in &rows.rows {
            ensure!(
                row.values.len() == rows.schema.len(),
                InvalidRequestSnafu {
                    region_id,
                    reason: format!(
                        "row has {} columns but schema has {}",
                        row.values.len(),
                        rows.schema.len()
                    ),
                }
            );

            for (i, (value, column_schema)) in row.values.iter().zip(&rows.schema).enumerate() {
                validate_proto_value(region_id, value, column_schema)?;

                if value.value_data.is_none() {
                    has_null[i] = true;
                }
            }
        }

        Ok(WriteRequest {
            region_id,
            op_type,
            rows,
            name_to_index,
            has_null,
            hint: None,
            region_metadata,
        })
    }

    /// Sets the write hint.
    pub fn with_hint(mut self, hint: Option<WriteHint>) -> Self {
        self.hint = hint;
        self
    }

    /// Returns the encoding hint.
    pub fn primary_key_encoding(&self) -> PrimaryKeyEncoding {
        infer_primary_key_encoding_from_hint(self.hint.as_ref())
    }

    /// Returns estimated size of the request.
    pub(crate) fn estimated_size(&self) -> usize {
        let row_size = self
            .rows
            .rows
            .first()
            .map(|row| row.encoded_len())
            .unwrap_or(0);
        row_size * self.rows.rows.len()
    }

    /// Gets column index by name.
    pub fn column_index_by_name(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Checks schema of rows is compatible with schema of the region.
    ///
    /// If column with default value is missing, it returns a special [FillDefault](crate::error::Error::FillDefault)
    /// error.
    pub(crate) fn check_schema(&self, metadata: &RegionMetadata) -> Result<()> {
        debug_assert_eq!(self.region_id, metadata.region_id);

        let region_id = self.region_id;
        // Index all columns in rows.
        let mut rows_columns: HashMap<_, _> = self
            .rows
            .schema
            .iter()
            .map(|column| (&column.column_name, column))
            .collect();

        let mut need_fill_default = false;
        // Checks all columns in this region.
        for column in &metadata.column_metadatas {
            if let Some(input_col) = rows_columns.remove(&column.column_schema.name) {
                // Check data type.
                ensure!(
                    is_column_type_value_eq(
                        input_col.datatype,
                        input_col.datatype_extension,
                        &column.column_schema.data_type
                    ),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!(
                            "column {} expect type {:?}, given: {}({})",
                            column.column_schema.name,
                            column.column_schema.data_type,
                            ColumnDataType::try_from(input_col.datatype)
                                .map(|v| v.as_str_name())
                                .unwrap_or("Unknown"),
                            input_col.datatype,
                        )
                    }
                );

                // Check semantic type.
                ensure!(
                    is_semantic_type_eq(input_col.semantic_type, column.semantic_type),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!(
                            "column {} has semantic type {:?}, given: {}({})",
                            column.column_schema.name,
                            column.semantic_type,
                            api::v1::SemanticType::try_from(input_col.semantic_type)
                                .map(|v| v.as_str_name())
                                .unwrap_or("Unknown"),
                            input_col.semantic_type
                        ),
                    }
                );

                // Check nullable.
                // Safety: `rows_columns` ensures this column exists.
                let has_null = self.has_null[self.name_to_index[&column.column_schema.name]];
                ensure!(
                    !has_null || column.column_schema.is_nullable(),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!(
                            "column {} is not null but input has null",
                            column.column_schema.name
                        ),
                    }
                );
            } else {
                // Rows don't have this column.
                self.check_missing_column(column)?;

                need_fill_default = true;
            }
        }

        // Checks all columns in rows exist in the region.
        if !rows_columns.is_empty() {
            let names: Vec<_> = rows_columns.into_keys().collect();
            return InvalidRequestSnafu {
                region_id,
                reason: format!("unknown columns: {:?}", names),
            }
            .fail();
        }

        // If we need to fill default values, return a special error.
        ensure!(!need_fill_default, FillDefaultSnafu { region_id });

        Ok(())
    }

    /// Tries to fill missing columns.
    ///
    /// Currently, our protobuf format might be inefficient when we need to fill lots of null
    /// values.
    pub(crate) fn fill_missing_columns(&mut self, metadata: &RegionMetadata) -> Result<()> {
        debug_assert_eq!(self.region_id, metadata.region_id);

        let mut columns_to_fill = vec![];
        for column in &metadata.column_metadatas {
            if !self.name_to_index.contains_key(&column.column_schema.name) {
                columns_to_fill.push(column);
            }
        }
        self.fill_columns(columns_to_fill)?;

        Ok(())
    }

    /// Checks the schema and fill missing columns.
    pub(crate) fn maybe_fill_missing_columns(&mut self, metadata: &RegionMetadata) -> Result<()> {
        if let Err(e) = self.check_schema(metadata) {
            if e.is_fill_default() {
                // TODO(yingwen): Add metrics for this case.
                // We need to fill default value. The write request may be a request
                // sent before changing the schema.
                self.fill_missing_columns(metadata)?;
            } else {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Fills default value for specific `columns`.
    fn fill_columns(&mut self, columns: Vec<&ColumnMetadata>) -> Result<()> {
        let mut default_values = Vec::with_capacity(columns.len());
        let mut columns_to_fill = Vec::with_capacity(columns.len());
        for column in columns {
            let default_value = self.column_default_value(column)?;
            if default_value.value_data.is_some() {
                default_values.push(default_value);
                columns_to_fill.push(column);
            }
        }

        for row in &mut self.rows.rows {
            row.values.extend(default_values.iter().cloned());
        }

        for column in columns_to_fill {
            let (datatype, datatype_ext) =
                ColumnDataTypeWrapper::try_from(column.column_schema.data_type.clone())
                    .with_context(|_| ConvertColumnDataTypeSnafu {
                        reason: format!(
                            "no protobuf type for column {} ({:?})",
                            column.column_schema.name, column.column_schema.data_type
                        ),
                    })?
                    .to_parts();
            self.rows.schema.push(ColumnSchema {
                column_name: column.column_schema.name.clone(),
                datatype: datatype as i32,
                semantic_type: column.semantic_type as i32,
                datatype_extension: datatype_ext,
                options: options_from_column_schema(&column.column_schema),
            });
        }

        Ok(())
    }

    /// Checks whether we should allow a row doesn't provide this column.
    fn check_missing_column(&self, column: &ColumnMetadata) -> Result<()> {
        if self.op_type == OpType::Delete {
            if column.semantic_type == SemanticType::Field {
                // For delete request, all tags and timestamp is required. We don't fill default
                // tag or timestamp while deleting rows.
                return Ok(());
            } else {
                return InvalidRequestSnafu {
                    region_id: self.region_id,
                    reason: format!("delete requests need column {}", column.column_schema.name),
                }
                .fail();
            }
        }

        // Not a delete request. Checks whether they have default value.
        ensure!(
            column.column_schema.is_nullable()
                || column.column_schema.default_constraint().is_some(),
            InvalidRequestSnafu {
                region_id: self.region_id,
                reason: format!("missing column {}", column.column_schema.name),
            }
        );

        Ok(())
    }

    /// Returns the default value for specific column.
    fn column_default_value(&self, column: &ColumnMetadata) -> Result<Value> {
        let default_value = match self.op_type {
            OpType::Delete => {
                ensure!(
                    column.semantic_type == SemanticType::Field,
                    InvalidRequestSnafu {
                        region_id: self.region_id,
                        reason: format!(
                            "delete requests need column {}",
                            column.column_schema.name
                        ),
                    }
                );

                // For delete request, we need a default value for padding so we
                // can delete a row even a field doesn't have a default value. So the
                // value doesn't need to following the default value constraint of the
                // column.
                if column.column_schema.is_nullable() {
                    datatypes::value::Value::Null
                } else {
                    column.column_schema.data_type.default_value()
                }
            }
            OpType::Put => {
                // For put requests, we use the default value from column schema.
                if column.column_schema.is_default_impure() {
                    UnexpectedImpureDefaultSnafu {
                        region_id: self.region_id,
                        column: &column.column_schema.name,
                        default_value: format!("{:?}", column.column_schema.default_constraint()),
                    }
                    .fail()?
                }
                column
                    .column_schema
                    .create_default()
                    .context(CreateDefaultSnafu {
                        region_id: self.region_id,
                        column: &column.column_schema.name,
                    })?
                    // This column doesn't have default value.
                    .with_context(|| InvalidRequestSnafu {
                        region_id: self.region_id,
                        reason: format!(
                            "column {} does not have default value",
                            column.column_schema.name
                        ),
                    })?
            }
        };

        // Convert default value into proto's value.
        to_proto_value(default_value).with_context(|| InvalidRequestSnafu {
            region_id: self.region_id,
            reason: format!(
                "no protobuf type for default value of column {} ({:?})",
                column.column_schema.name, column.column_schema.data_type
            ),
        })
    }
}

/// Validate proto value schema.
pub(crate) fn validate_proto_value(
    region_id: RegionId,
    value: &Value,
    column_schema: &ColumnSchema,
) -> Result<()> {
    if let Some(value_type) = proto_value_type(value) {
        let column_type = ColumnDataType::try_from(column_schema.datatype).map_err(|_| {
            InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "column {} has unknown type {}",
                    column_schema.column_name, column_schema.datatype
                ),
            }
            .build()
        })?;
        ensure!(
            proto_value_type_match(column_type, value_type),
            InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "value has type {:?}, but column {} has type {:?}({})",
                    value_type, column_schema.column_name, column_type, column_schema.datatype,
                ),
            }
        );
    }

    Ok(())
}

fn proto_value_type_match(column_type: ColumnDataType, value_type: ColumnDataType) -> bool {
    match (column_type, value_type) {
        (ct, vt) if ct == vt => true,
        (ColumnDataType::Vector, ColumnDataType::Binary) => true,
        (ColumnDataType::Json, ColumnDataType::Binary) => true,
        _ => false,
    }
}

/// Oneshot output result sender.
#[derive(Debug)]
pub struct OutputTx(Sender<Result<AffectedRows>>);

impl OutputTx {
    /// Creates a new output sender.
    pub(crate) fn new(sender: Sender<Result<AffectedRows>>) -> OutputTx {
        OutputTx(sender)
    }

    /// Sends the `result`.
    pub(crate) fn send(self, result: Result<AffectedRows>) {
        // Ignores send result.
        let _ = self.0.send(result);
    }
}

/// Optional output result sender.
#[derive(Debug)]
pub(crate) struct OptionOutputTx(Option<OutputTx>);

impl OptionOutputTx {
    /// Creates a sender.
    pub(crate) fn new(sender: Option<OutputTx>) -> OptionOutputTx {
        OptionOutputTx(sender)
    }

    /// Creates an empty sender.
    pub(crate) fn none() -> OptionOutputTx {
        OptionOutputTx(None)
    }

    /// Sends the `result` and consumes the inner sender.
    pub(crate) fn send_mut(&mut self, result: Result<AffectedRows>) {
        if let Some(sender) = self.0.take() {
            sender.send(result);
        }
    }

    /// Sends the `result` and consumes the sender.
    pub(crate) fn send(mut self, result: Result<AffectedRows>) {
        if let Some(sender) = self.0.take() {
            sender.send(result);
        }
    }

    /// Takes the inner sender.
    pub(crate) fn take_inner(&mut self) -> Option<OutputTx> {
        self.0.take()
    }
}

impl From<Sender<Result<AffectedRows>>> for OptionOutputTx {
    fn from(sender: Sender<Result<AffectedRows>>) -> Self {
        Self::new(Some(OutputTx::new(sender)))
    }
}

impl OnFailure for OptionOutputTx {
    fn on_failure(&mut self, err: Error) {
        self.send_mut(Err(err));
    }
}

/// Callback on failure.
pub(crate) trait OnFailure {
    /// Handles `err` on failure.
    fn on_failure(&mut self, err: Error);
}

/// Sender and write request.
#[derive(Debug)]
pub(crate) struct SenderWriteRequest {
    /// Result sender.
    pub(crate) sender: OptionOutputTx,
    pub(crate) request: WriteRequest,
}

pub(crate) struct SenderBulkRequest {
    pub(crate) sender: OptionOutputTx,
    pub(crate) region_id: RegionId,
    pub(crate) request: BulkPart,
    pub(crate) region_metadata: RegionMetadataRef,
}

/// Request sent to a worker
#[derive(Debug)]
pub(crate) enum WorkerRequest {
    /// Write to a region.
    Write(SenderWriteRequest),

    /// Ddl request to a region.
    Ddl(SenderDdlRequest),

    /// Notifications from internal background jobs.
    Background {
        /// Id of the region to send.
        region_id: RegionId,
        /// Internal notification.
        notify: BackgroundNotify,
    },

    /// The internal commands.
    SetRegionRoleStateGracefully {
        /// Id of the region to send.
        region_id: RegionId,
        /// The [SettableRegionRoleState].
        region_role_state: SettableRegionRoleState,
        /// The sender of [SetReadonlyResponse].
        sender: Sender<SetRegionRoleStateResponse>,
    },

    /// Notify a worker to stop.
    Stop,

    /// Use [RegionEdit] to edit a region directly.
    EditRegion(RegionEditRequest),

    /// Keep the manifest of a region up to date.
    SyncRegion(RegionSyncRequest),

    /// Bulk inserts request and region metadata.
    BulkInserts {
        metadata: Option<RegionMetadataRef>,
        request: RegionBulkInsertsRequest,
        sender: OptionOutputTx,
    },
}

impl WorkerRequest {
    pub(crate) fn new_open_region_request(
        region_id: RegionId,
        request: RegionOpenRequest,
        entry_receiver: Option<WalEntryReceiver>,
    ) -> (WorkerRequest, Receiver<Result<AffectedRows>>) {
        let (sender, receiver) = oneshot::channel();

        let worker_request = WorkerRequest::Ddl(SenderDdlRequest {
            region_id,
            sender: sender.into(),
            request: DdlRequest::Open((request, entry_receiver)),
        });

        (worker_request, receiver)
    }

    /// Converts request from a [RegionRequest].
    pub(crate) fn try_from_region_request(
        region_id: RegionId,
        value: RegionRequest,
        region_metadata: Option<RegionMetadataRef>,
    ) -> Result<(WorkerRequest, Receiver<Result<AffectedRows>>)> {
        let (sender, receiver) = oneshot::channel();
        let worker_request = match value {
            RegionRequest::Put(v) => {
                let mut write_request =
                    WriteRequest::new(region_id, OpType::Put, v.rows, region_metadata.clone())?
                        .with_hint(v.hint);
                if write_request.primary_key_encoding() == PrimaryKeyEncoding::Dense
                    && let Some(region_metadata) = &region_metadata
                {
                    write_request.maybe_fill_missing_columns(region_metadata)?;
                }
                WorkerRequest::Write(SenderWriteRequest {
                    sender: sender.into(),
                    request: write_request,
                })
            }
            RegionRequest::Delete(v) => {
                let mut write_request =
                    WriteRequest::new(region_id, OpType::Delete, v.rows, region_metadata.clone())?;
                if write_request.primary_key_encoding() == PrimaryKeyEncoding::Dense
                    && let Some(region_metadata) = &region_metadata
                {
                    write_request.maybe_fill_missing_columns(region_metadata)?;
                }
                WorkerRequest::Write(SenderWriteRequest {
                    sender: sender.into(),
                    request: write_request,
                })
            }
            RegionRequest::Create(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Create(v),
            }),
            RegionRequest::Drop(_) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Drop,
            }),
            RegionRequest::Open(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Open((v, None)),
            }),
            RegionRequest::Close(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Close(v),
            }),
            RegionRequest::Alter(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Alter(v),
            }),
            RegionRequest::Flush(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Flush(v),
            }),
            RegionRequest::Compact(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Compact(v),
            }),
            RegionRequest::Truncate(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Truncate(v),
            }),
            RegionRequest::Catchup(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: sender.into(),
                request: DdlRequest::Catchup(v),
            }),
            RegionRequest::BulkInserts(region_bulk_inserts_request) => WorkerRequest::BulkInserts {
                metadata: region_metadata,
                sender: sender.into(),
                request: region_bulk_inserts_request,
            },
        };

        Ok((worker_request, receiver))
    }

    pub(crate) fn new_set_readonly_gracefully(
        region_id: RegionId,
        region_role_state: SettableRegionRoleState,
    ) -> (WorkerRequest, Receiver<SetRegionRoleStateResponse>) {
        let (sender, receiver) = oneshot::channel();

        (
            WorkerRequest::SetRegionRoleStateGracefully {
                region_id,
                region_role_state,
                sender,
            },
            receiver,
        )
    }

    pub(crate) fn new_sync_region_request(
        region_id: RegionId,
        manifest_version: ManifestVersion,
    ) -> (WorkerRequest, Receiver<Result<(ManifestVersion, bool)>>) {
        let (sender, receiver) = oneshot::channel();
        (
            WorkerRequest::SyncRegion(RegionSyncRequest {
                region_id,
                manifest_version,
                sender,
            }),
            receiver,
        )
    }
}

/// DDL request to a region.
#[derive(Debug)]
pub(crate) enum DdlRequest {
    Create(RegionCreateRequest),
    Drop,
    Open((RegionOpenRequest, Option<WalEntryReceiver>)),
    Close(RegionCloseRequest),
    Alter(RegionAlterRequest),
    Flush(RegionFlushRequest),
    Compact(RegionCompactRequest),
    Truncate(RegionTruncateRequest),
    Catchup(RegionCatchupRequest),
}

/// Sender and Ddl request.
#[derive(Debug)]
pub(crate) struct SenderDdlRequest {
    /// Region id of the request.
    pub(crate) region_id: RegionId,
    /// Result sender.
    pub(crate) sender: OptionOutputTx,
    /// Ddl request.
    pub(crate) request: DdlRequest,
}

/// Notification from a background job.
#[derive(Debug)]
pub(crate) enum BackgroundNotify {
    /// Flush has finished.
    FlushFinished(FlushFinished),
    /// Flush has failed.
    FlushFailed(FlushFailed),
    /// Compaction has finished.
    CompactionFinished(CompactionFinished),
    /// Compaction has failed.
    CompactionFailed(CompactionFailed),
    /// Truncate result.
    Truncate(TruncateResult),
    /// Region change result.
    RegionChange(RegionChangeResult),
    /// Region edit result.
    RegionEdit(RegionEditResult),
}

/// Notifies a flush job is finished.
#[derive(Debug)]
pub(crate) struct FlushFinished {
    /// Region id.
    pub(crate) region_id: RegionId,
    /// Entry id of flushed data.
    pub(crate) flushed_entry_id: EntryId,
    /// Flush result senders.
    pub(crate) senders: Vec<OutputTx>,
    /// Flush timer.
    pub(crate) _timer: HistogramTimer,
    /// Region edit to apply.
    pub(crate) edit: RegionEdit,
    /// Memtables to remove.
    pub(crate) memtables_to_remove: SmallVec<[MemtableId; 2]>,
}

impl FlushFinished {
    /// Marks the flush job as successful and observes the timer.
    pub(crate) fn on_success(self) {
        for sender in self.senders {
            sender.send(Ok(0));
        }
    }
}

impl OnFailure for FlushFinished {
    fn on_failure(&mut self, err: Error) {
        let err = Arc::new(err);
        for sender in self.senders.drain(..) {
            sender.send(Err(err.clone()).context(FlushRegionSnafu {
                region_id: self.region_id,
            }));
        }
    }
}

/// Notifies a flush job is failed.
#[derive(Debug)]
pub(crate) struct FlushFailed {
    /// The error source of the failure.
    pub(crate) err: Arc<Error>,
}

/// Notifies a compaction job has finished.
#[derive(Debug)]
pub(crate) struct CompactionFinished {
    /// Region id.
    pub(crate) region_id: RegionId,
    /// Compaction result senders.
    pub(crate) senders: Vec<OutputTx>,
    /// Start time of compaction task.
    pub(crate) start_time: Instant,
    /// Region edit to apply.
    pub(crate) edit: RegionEdit,
}

impl CompactionFinished {
    pub fn on_success(self) {
        // only update compaction time on success
        COMPACTION_ELAPSED_TOTAL.observe(self.start_time.elapsed().as_secs_f64());

        for sender in self.senders {
            sender.send(Ok(0));
        }
        info!("Successfully compacted region: {}", self.region_id);
    }
}

impl OnFailure for CompactionFinished {
    /// Compaction succeeded but failed to update manifest or region's already been dropped.
    fn on_failure(&mut self, err: Error) {
        let err = Arc::new(err);
        for sender in self.senders.drain(..) {
            sender.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.region_id,
            }));
        }
    }
}

/// A failing compaction result.
#[derive(Debug)]
pub(crate) struct CompactionFailed {
    pub(crate) region_id: RegionId,
    /// The error source of the failure.
    pub(crate) err: Arc<Error>,
}

/// Notifies the truncate result of a region.
#[derive(Debug)]
pub(crate) struct TruncateResult {
    /// Region id.
    pub(crate) region_id: RegionId,
    /// Result sender.
    pub(crate) sender: OptionOutputTx,
    /// Truncate result.
    pub(crate) result: Result<()>,
    /// Truncated entry id.
    pub(crate) truncated_entry_id: EntryId,
    /// Truncated sequence.
    pub(crate) truncated_sequence: SequenceNumber,
}

/// Notifies the region the result of writing region change action.
#[derive(Debug)]
pub(crate) struct RegionChangeResult {
    /// Region id.
    pub(crate) region_id: RegionId,
    /// The new region metadata to apply.
    pub(crate) new_meta: RegionMetadataRef,
    /// Result sender.
    pub(crate) sender: OptionOutputTx,
    /// Result from the manifest manager.
    pub(crate) result: Result<()>,
}

/// Request to edit a region directly.
#[derive(Debug)]
pub(crate) struct RegionEditRequest {
    pub(crate) region_id: RegionId,
    pub(crate) edit: RegionEdit,
    /// The sender to notify the result to the region engine.
    pub(crate) tx: Sender<Result<()>>,
}

/// Notifies the regin the result of editing region.
#[derive(Debug)]
pub(crate) struct RegionEditResult {
    /// Region id.
    pub(crate) region_id: RegionId,
    /// Result sender.
    pub(crate) sender: Sender<Result<()>>,
    /// Region edit to apply.
    pub(crate) edit: RegionEdit,
    /// Result from the manifest manager.
    pub(crate) result: Result<()>,
}

#[derive(Debug)]
pub(crate) struct RegionSyncRequest {
    pub(crate) region_id: RegionId,
    pub(crate) manifest_version: ManifestVersion,
    /// Returns the latest manifest version and a boolean indicating whether new maniefst is installed.
    pub(crate) sender: Sender<Result<(ManifestVersion, bool)>>,
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{Row, SemanticType};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnDefaultConstraint;
    use store_api::metadata::RegionMetadataBuilder;

    use super::*;
    use crate::error::Error;
    use crate::test_util::{i64_value, ts_ms_value};

    fn new_column_schema(
        name: &str,
        data_type: ColumnDataType,
        semantic_type: SemanticType,
    ) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            datatype: data_type as i32,
            semantic_type: semantic_type as i32,
            ..Default::default()
        }
    }

    fn check_invalid_request(err: &Error, expect: &str) {
        if let Error::InvalidRequest {
            region_id: _,
            reason,
            location: _,
        } = err
        {
            assert_eq!(reason, expect);
        } else {
            panic!("Unexpected error {err}")
        }
    }

    #[test]
    fn test_write_request_duplicate_column() {
        let rows = Rows {
            schema: vec![
                new_column_schema("c0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema("c0", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![],
        };

        let err = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap_err();
        check_invalid_request(&err, "duplicate column c0");
    }

    #[test]
    fn test_valid_write_request() {
        let rows = Rows {
            schema: vec![
                new_column_schema("c0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema("c1", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![i64_value(1), i64_value(2)],
            }],
        };

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        assert_eq!(0, request.column_index_by_name("c0").unwrap());
        assert_eq!(1, request.column_index_by_name("c1").unwrap());
        assert_eq!(None, request.column_index_by_name("c2"));
    }

    #[test]
    fn test_write_request_column_num() {
        let rows = Rows {
            schema: vec![
                new_column_schema("c0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema("c1", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![i64_value(1), i64_value(2), i64_value(3)],
            }],
        };

        let err = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap_err();
        check_invalid_request(&err, "row has 3 columns but schema has 2");
    }

    fn new_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "k0",
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .primary_key(vec![2]);
        builder.build().unwrap()
    }

    #[test]
    fn test_check_schema() {
        let rows = Rows {
            schema: vec![
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![ts_ms_value(1), i64_value(2)],
            }],
        };
        let metadata = new_region_metadata();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        request.check_schema(&metadata).unwrap();
    }

    #[test]
    fn test_column_type() {
        let rows = Rows {
            schema: vec![
                new_column_schema("ts", ColumnDataType::Int64, SemanticType::Timestamp),
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![i64_value(1), i64_value(2)],
            }],
        };
        let metadata = new_region_metadata();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, "column ts expect type Timestamp(Millisecond(TimestampMillisecondType)), given: INT64(4)");
    }

    #[test]
    fn test_semantic_type() {
        let rows = Rows {
            schema: vec![
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Tag,
                ),
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![ts_ms_value(1), i64_value(2)],
            }],
        };
        let metadata = new_region_metadata();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, "column ts has semantic type Timestamp, given: TAG(0)");
    }

    #[test]
    fn test_column_nullable() {
        let rows = Rows {
            schema: vec![
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![Value { value_data: None }, i64_value(2)],
            }],
        };
        let metadata = new_region_metadata();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, "column ts is not null but input has null");
    }

    #[test]
    fn test_column_default() {
        let rows = Rows {
            schema: vec![new_column_schema(
                "k0",
                ColumnDataType::Int64,
                SemanticType::Tag,
            )],
            rows: vec![Row {
                values: vec![i64_value(1)],
            }],
        };
        let metadata = new_region_metadata();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, "missing column ts");
    }

    #[test]
    fn test_unknown_column() {
        let rows = Rows {
            schema: vec![
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema("k1", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![ts_ms_value(1), i64_value(2), i64_value(3)],
            }],
        };
        let metadata = new_region_metadata();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, r#"unknown columns: ["k1"]"#);
    }

    #[test]
    fn test_fill_impure_columns_err() {
        let rows = Rows {
            schema: vec![new_column_schema(
                "k0",
                ColumnDataType::Int64,
                SemanticType::Tag,
            )],
            rows: vec![Row {
                values: vec![i64_value(1)],
            }],
        };
        let metadata = {
            let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
            builder
                .push_column_metadata(ColumnMetadata {
                    column_schema: datatypes::schema::ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                        "now()".to_string(),
                    )))
                    .unwrap(),
                    semantic_type: SemanticType::Timestamp,
                    column_id: 1,
                })
                .push_column_metadata(ColumnMetadata {
                    column_schema: datatypes::schema::ColumnSchema::new(
                        "k0",
                        ConcreteDataType::int64_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 2,
                })
                .primary_key(vec![2]);
            builder.build().unwrap()
        };

        let mut request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        assert!(err.is_fill_default());
        assert!(request
            .fill_missing_columns(&metadata)
            .unwrap_err()
            .to_string()
            .contains("Unexpected impure default value with region_id"));
    }

    #[test]
    fn test_fill_missing_columns() {
        let rows = Rows {
            schema: vec![new_column_schema(
                "ts",
                ColumnDataType::TimestampMillisecond,
                SemanticType::Timestamp,
            )],
            rows: vec![Row {
                values: vec![ts_ms_value(1)],
            }],
        };
        let metadata = new_region_metadata();

        let mut request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        assert!(err.is_fill_default());
        request.fill_missing_columns(&metadata).unwrap();

        let expect_rows = Rows {
            schema: vec![new_column_schema(
                "ts",
                ColumnDataType::TimestampMillisecond,
                SemanticType::Timestamp,
            )],
            rows: vec![Row {
                values: vec![ts_ms_value(1)],
            }],
        };
        assert_eq!(expect_rows, request.rows);
    }

    fn builder_with_ts_tag() -> RegionMetadataBuilder {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "k0",
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .primary_key(vec![2]);
        builder
    }

    fn region_metadata_two_fields() -> RegionMetadata {
        let mut builder = builder_with_ts_tag();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "f0",
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            // Column is not nullable.
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "f1",
                    ConcreteDataType::int64_datatype(),
                    false,
                )
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(
                    datatypes::value::Value::Int64(100),
                )))
                .unwrap(),
                semantic_type: SemanticType::Field,
                column_id: 4,
            });
        builder.build().unwrap()
    }

    #[test]
    fn test_fill_missing_for_delete() {
        let rows = Rows {
            schema: vec![new_column_schema(
                "ts",
                ColumnDataType::TimestampMillisecond,
                SemanticType::Timestamp,
            )],
            rows: vec![Row {
                values: vec![ts_ms_value(1)],
            }],
        };
        let metadata = region_metadata_two_fields();

        let mut request =
            WriteRequest::new(RegionId::new(1, 1), OpType::Delete, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, "delete requests need column k0");
        let err = request.fill_missing_columns(&metadata).unwrap_err();
        check_invalid_request(&err, "delete requests need column k0");

        let rows = Rows {
            schema: vec![
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
            ],
            rows: vec![Row {
                values: vec![i64_value(100), ts_ms_value(1)],
            }],
        };
        let mut request =
            WriteRequest::new(RegionId::new(1, 1), OpType::Delete, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        assert!(err.is_fill_default());
        request.fill_missing_columns(&metadata).unwrap();

        let expect_rows = Rows {
            schema: vec![
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("f1", ColumnDataType::Int64, SemanticType::Field),
            ],
            // Column f1 is not nullable and we use 0 for padding.
            rows: vec![Row {
                values: vec![i64_value(100), ts_ms_value(1), i64_value(0)],
            }],
        };
        assert_eq!(expect_rows, request.rows);
    }

    #[test]
    fn test_fill_missing_without_default_in_delete() {
        let mut builder = builder_with_ts_tag();
        builder
            // f0 is nullable.
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "f0",
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            // f1 is not nullable and don't has default.
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "f1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
            });
        let metadata = builder.build().unwrap();

        let rows = Rows {
            schema: vec![
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
            ],
            // Missing f0 (nullable), f1 (not nullable).
            rows: vec![Row {
                values: vec![i64_value(100), ts_ms_value(1)],
            }],
        };
        let mut request =
            WriteRequest::new(RegionId::new(1, 1), OpType::Delete, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        assert!(err.is_fill_default());
        request.fill_missing_columns(&metadata).unwrap();

        let expect_rows = Rows {
            schema: vec![
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("f1", ColumnDataType::Int64, SemanticType::Field),
            ],
            // Column f1 is not nullable and we use 0 for padding.
            rows: vec![Row {
                values: vec![i64_value(100), ts_ms_value(1), i64_value(0)],
            }],
        };
        assert_eq!(expect_rows, request.rows);
    }

    #[test]
    fn test_no_default() {
        let rows = Rows {
            schema: vec![new_column_schema(
                "k0",
                ColumnDataType::Int64,
                SemanticType::Tag,
            )],
            rows: vec![Row {
                values: vec![i64_value(1)],
            }],
        };
        let metadata = new_region_metadata();

        let mut request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.fill_missing_columns(&metadata).unwrap_err();
        check_invalid_request(&err, "column ts does not have default value");
    }

    #[test]
    fn test_missing_and_invalid() {
        // Missing f0 and f1 has invalid type (string).
        let rows = Rows {
            schema: vec![
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("f1", ColumnDataType::String, SemanticType::Field),
            ],
            rows: vec![Row {
                values: vec![
                    i64_value(100),
                    ts_ms_value(1),
                    Value {
                        value_data: Some(ValueData::StringValue("xxxxx".to_string())),
                    },
                ],
            }],
        };
        let metadata = region_metadata_two_fields();

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows, None).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(
            &err,
            "column f1 expect type Int64(Int64Type), given: STRING(12)",
        );
    }

    #[test]
    fn test_write_request_metadata() {
        let rows = Rows {
            schema: vec![
                new_column_schema("c0", ColumnDataType::Int64, SemanticType::Tag),
                new_column_schema("c1", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![i64_value(1), i64_value(2)],
            }],
        };

        let metadata = Arc::new(new_region_metadata());
        let request = WriteRequest::new(
            RegionId::new(1, 1),
            OpType::Put,
            rows,
            Some(metadata.clone()),
        )
        .unwrap();

        assert!(request.region_metadata.is_some());
        assert_eq!(
            request.region_metadata.unwrap().region_id,
            RegionId::new(1, 1)
        );
    }
}
