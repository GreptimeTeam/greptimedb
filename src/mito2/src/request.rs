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
use std::time::Duration;

use api::helper::{
    is_column_type_value_eq, is_semantic_type_eq, proto_value_type, to_column_data_type,
    to_proto_value,
};
use api::v1::{ColumnDataType, ColumnSchema, OpType, Rows, Value};
use common_base::readable_size::ReadableSize;
use common_query::Output;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::region_request::{
    RegionAlterRequest, RegionCloseRequest, RegionCompactRequest, RegionCreateRequest,
    RegionDropRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest,
};
use store_api::storage::{CompactionStrategy, RegionId};
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::config::DEFAULT_WRITE_BUFFER_SIZE;
use crate::error::{CreateDefaultSnafu, Error, FillDefaultSnafu, InvalidRequestSnafu, Result};
use crate::sst::file::FileMeta;

/// Options that affect the entire region.
///
/// Users need to specify the options while creating/opening a region.
#[derive(Debug)]
pub struct RegionOptions {
    /// Region memtable max size in bytes.
    pub write_buffer_size: Option<ReadableSize>,
    /// Region SST files TTL.
    pub ttl: Option<Duration>,
    /// Compaction strategy.
    pub compaction_strategy: CompactionStrategy,
}

impl Default for RegionOptions {
    fn default() -> Self {
        RegionOptions {
            write_buffer_size: Some(DEFAULT_WRITE_BUFFER_SIZE),
            ttl: None,
            compaction_strategy: CompactionStrategy::LeveledTimeWindow,
        }
    }
}

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
    name_to_index: HashMap<String, usize>,
    /// Whether each column has null.
    has_null: Vec<bool>,
}

impl WriteRequest {
    /// Creates a new request.
    ///
    /// Returns `Err` if `rows` are invalid.
    pub fn new(region_id: RegionId, op_type: OpType, rows: Rows) -> Result<WriteRequest> {
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
        })
    }

    /// Get column index by name.
    pub(crate) fn column_index_by_name(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    // TODO(yingwen): Check delete schema.
    /// Checks schema of rows is compatible with schema of the region.
    ///
    /// If column with default value is missing, it returns a special [FillDefault](crate::error::Error::FillDefault)
    /// error.
    pub(crate) fn check_schema(&self, metadata: &RegionMetadata) -> Result<()> {
        let region_id = self.region_id;
        // Index all columns in rows.
        let mut rows_columns: HashMap<_, _> = self
            .rows
            .schema
            .iter()
            .map(|column| (&column.column_name, column))
            .collect();

        // Checks all columns in this region.
        for column in &metadata.column_metadatas {
            if let Some(input_col) = rows_columns.remove(&column.column_schema.name) {
                // Check data type.
                ensure!(
                    is_column_type_value_eq(input_col.datatype, &column.column_schema.data_type),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!(
                            "column {} expect type {:?}, given: {}({})",
                            column.column_schema.name,
                            column.column_schema.data_type,
                            ColumnDataType::from_i32(input_col.datatype)
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
                            api::v1::SemanticType::from_i32(input_col.semantic_type)
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
                        reason: format!("column {} is not null", column.column_schema.name),
                    }
                );
            } else {
                // For columns not in rows, checks whether they have default value.
                ensure!(
                    column.column_schema.is_nullable()
                        || column.column_schema.default_constraint().is_some(),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!("missing column {}", column.column_schema.name),
                    }
                );

                return FillDefaultSnafu { region_id }.fail();
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

        Ok(())
    }

    /// Try to fill missing columns.
    ///
    /// Currently, our protobuf format might be inefficient when we need to fill lots of null
    /// values.
    pub(crate) fn fill_missing_columns(&mut self, metadata: &RegionMetadata) -> Result<()> {
        for column in &metadata.column_metadatas {
            if !self.name_to_index.contains_key(&column.column_schema.name) {
                self.fill_column(metadata.region_id, column)?;
            }
        }

        Ok(())
    }

    /// Fill default value for specific `column`.
    fn fill_column(&mut self, region_id: RegionId, column: &ColumnMetadata) -> Result<()> {
        // Need to add a default value for this column.
        let default_value = column
            .column_schema
            .create_default()
            .context(CreateDefaultSnafu {
                region_id,
                column: &column.column_schema.name,
            })?
            // This column doesn't have default value.
            .with_context(|| InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "column {} does not have default value",
                    column.column_schema.name
                ),
            })?;

        // Convert default value into proto's value.

        let proto_value = to_proto_value(default_value).with_context(|| InvalidRequestSnafu {
            region_id,
            reason: format!(
                "no protobuf type for default value of column {} ({:?})",
                column.column_schema.name, column.column_schema.data_type
            ),
        })?;

        // Insert default value to each row.
        for row in &mut self.rows.rows {
            row.values.push(proto_value.clone());
        }

        // Insert column schema.
        let datatype = to_column_data_type(&column.column_schema.data_type).with_context(|| {
            InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "no protobuf type for column {} ({:?})",
                    column.column_schema.name, column.column_schema.data_type
                ),
            }
        })?;
        self.rows.schema.push(ColumnSchema {
            column_name: column.column_schema.name.clone(),
            datatype: datatype as i32,
            semantic_type: column.semantic_type as i32,
        });

        Ok(())
    }
}

/// Validate proto value schema.
pub(crate) fn validate_proto_value(
    region_id: RegionId,
    value: &Value,
    column_schema: &ColumnSchema,
) -> Result<()> {
    if let Some(value_type) = proto_value_type(value) {
        ensure!(
            value_type as i32 == column_schema.datatype,
            InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "column {} has type {:?}, but schema has type {:?}",
                    column_schema.column_name,
                    value_type,
                    ColumnDataType::from_i32(column_schema.datatype)
                ),
            }
        );
    }

    Ok(())
}

/// Sender and write request.
#[derive(Debug)]
pub(crate) struct SenderWriteRequest {
    /// Result sender.
    pub(crate) sender: Option<Sender<Result<Output>>>,
    pub(crate) request: WriteRequest,
}

/// Request sent to a worker
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

    /// Notify a worker to stop.
    Stop,
}

impl WorkerRequest {
    /// Converts request from a [RegionRequest].
    pub(crate) fn try_from_region_request(
        region_id: RegionId,
        value: RegionRequest,
    ) -> Result<(WorkerRequest, Receiver<Result<Output>>)> {
        let (sender, receiver) = oneshot::channel();
        let worker_request = match value {
            RegionRequest::Put(v) => {
                let write_request = WriteRequest::new(region_id, OpType::Put, v.rows)?;
                WorkerRequest::Write(SenderWriteRequest {
                    sender: Some(sender),
                    request: write_request,
                })
            }
            RegionRequest::Delete(v) => {
                let write_request = WriteRequest::new(region_id, OpType::Delete, v.rows)?;
                WorkerRequest::Write(SenderWriteRequest {
                    sender: Some(sender),
                    request: write_request,
                })
            }
            RegionRequest::Create(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Create(v),
            }),
            RegionRequest::Drop(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Drop(v),
            }),
            RegionRequest::Open(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Open(v),
            }),
            RegionRequest::Close(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Close(v),
            }),
            RegionRequest::Alter(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Alter(v),
            }),
            RegionRequest::Flush(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Flush(v),
            }),
            RegionRequest::Compact(v) => WorkerRequest::Ddl(SenderDdlRequest {
                region_id,
                sender: Some(sender),
                request: DdlRequest::Compact(v),
            }),
        };

        Ok((worker_request, receiver))
    }
}

/// DDL request to a region.
#[derive(Debug)]
pub(crate) enum DdlRequest {
    Create(RegionCreateRequest),
    Drop(RegionDropRequest),
    Open(RegionOpenRequest),
    Close(RegionCloseRequest),
    Alter(RegionAlterRequest),
    Flush(RegionFlushRequest),
    Compact(RegionCompactRequest),
}

/// Sender and Ddl request.
#[derive(Debug)]
pub(crate) struct SenderDdlRequest {
    /// Region id of the request.
    pub(crate) region_id: RegionId,
    /// Result sender.
    pub(crate) sender: Option<Sender<Result<Output>>>,
    /// Ddl request.
    pub(crate) request: DdlRequest,
}

/// Notification from a background job.
#[derive(Debug)]
pub(crate) enum BackgroundNotify {
    /// Flush is finished.
    FlushFinished(FlushFinished),
    /// Flush is failed.
    FlushFailed(FlushFailed),
}

/// Notifies a flush job is finished.
#[derive(Debug)]
pub(crate) struct FlushFinished {
    /// Meta of the flushed SST.
    pub(crate) file_meta: FileMeta,
}

/// Notifies a flush job is failed.
#[derive(Debug)]
pub(crate) struct FlushFailed {
    /// The reason of a failed flush job.
    pub(crate) error: Error,
}

#[cfg(test)]
mod tests {
    use api::v1::{Row, SemanticType};
    use datatypes::prelude::ConcreteDataType;
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

        let err = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap_err();
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
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

        let err = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap_err();
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, "column ts is not null");
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
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

        let request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        check_invalid_request(&err, r#"unknown columns: ["k1"]"#);
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

        let mut request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
        let err = request.check_schema(&metadata).unwrap_err();
        assert!(err.is_fill_default());
        request.fill_missing_columns(&metadata).unwrap();

        let expect_rows = Rows {
            schema: vec![
                new_column_schema(
                    "ts",
                    ColumnDataType::TimestampMillisecond,
                    SemanticType::Timestamp,
                ),
                new_column_schema("k0", ColumnDataType::Int64, SemanticType::Tag),
            ],
            rows: vec![Row {
                values: vec![ts_ms_value(1), Value { value_data: None }],
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

        let mut request = WriteRequest::new(RegionId::new(1, 1), OpType::Put, rows).unwrap();
        let err = request.fill_missing_columns(&metadata).unwrap_err();
        check_invalid_request(&err, "column ts does not have default value");
    }
}
