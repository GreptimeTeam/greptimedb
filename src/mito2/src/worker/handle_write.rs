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

//! Handling write requests.

use std::collections::HashMap;

use datatypes::prelude::ConcreteDataType;
use datatypes::types::{TimeType, TimestampType};
use greptime_proto::v1::mito::Mutation;
use greptime_proto::v1::{ColumnDataType, Rows};
use snafu::ensure;
use tokio::sync::oneshot::Sender;

use crate::error::{InvalidRequestSnafu, RegionNotFoundSnafu, Result};
use crate::metadata::SemanticType;
use crate::region::version::VersionRef;
use crate::region::MitoRegionRef;
use crate::request::SenderWriteRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Takes and handles all write requests.
    pub(crate) async fn handle_write_requests(&mut self, write_requests: Vec<SenderWriteRequest>) {
        if write_requests.is_empty() {
            return;
        }

        let mut region_ctxs = HashMap::new();
        for sender_req in write_requests {
            let region_id = sender_req.request.region_id;
            // Checks whether the region exists.
            if !region_ctxs.contains_key(&region_id) {
                let Some(region) = self.regions.get_region(region_id) else {
                    // No such region.
                    send_result(sender_req.sender, RegionNotFoundSnafu {
                        region_id,
                    }.fail());

                    continue;
                };

                // Initialize the context.
                region_ctxs.insert(region_id, RegionWriteCtx::new(region));
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            // Checks request schema.
            if let Err(e) = region_ctx.check_schema(&sender_req.request.rows) {
                send_result(sender_req.sender, Err(e));

                continue;
            }

            // Push request.
        }
        // We need to check:
        // - region exists, if not, return error
        // - check whether the schema is compatible with region schema
        // - collect rows by region
        // - get sequence for each row

        // problem:
        // - column order in request may be different from table column order
        // - need to add missing column
        // - memtable may need a new struct for sequence and op type.

        todo!()
    }
}

/// Send result to the request.
fn send_result(sender: Option<Sender<Result<()>>>, res: Result<()>) {
    if let Some(sender) = sender {
        // Ignore send result.
        let _ = sender.send(res);
    }
}

/// Context to write to a region.
struct RegionWriteCtx {
    /// Region to write.
    region: MitoRegionRef,
    /// Version of the region while creating the context.
    version: VersionRef,
    /// Valid mutations.
    mutations: Vec<Mutation>,
    /// Result senders.
    ///
    /// The sender is 1:1 map to the mutation in `mutations`.
    senders: Vec<Option<Sender<Result<()>>>>,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    fn new(region: MitoRegionRef) -> RegionWriteCtx {
        let version = region.version();
        RegionWriteCtx {
            region,
            version,
            mutations: Vec::new(),
            senders: Vec::new(),
        }
    }

    /// Checks schema of rows.
    fn check_schema(&self, rows: &Rows) -> Result<()> {
        let region_id = self.region.region_id;
        // Index all columns in rows.
        let mut rows_columns: HashMap<_, _> = rows
            .schema
            .iter()
            .map(|column| (&column.column_name, column))
            .collect();

        // Checks all columns in this region.
        for column in &self.version.metadata.column_metadatas {
            if let Some(input_col) = rows_columns.remove(&column.column_schema.name) {
                // Check data type.
                ensure!(
                    check_column_type(input_col.datatype, &column.column_schema.data_type),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!(
                            "Column {} expect type {:?}, given: {:?}({})",
                            column.column_schema.name,
                            column.column_schema.data_type,
                            ColumnDataType::from_i32(input_col.datatype),
                            input_col.datatype,
                        )
                    }
                );

                // Check semantic type.
                ensure!(
                    check_semantic_type(input_col.semantic_type, column.semantic_type),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!(
                            "Column {} has semantic type {:?}, given: {:?}({})",
                            column.column_schema.name,
                            column.semantic_type,
                            greptime_proto::v1::SemanticType::from_i32(input_col.semantic_type),
                            input_col.semantic_type
                        ),
                    }
                );
            } else {
                // For columns not in rows, checks whether they are nullable.
                ensure!(
                    column.column_schema.is_nullable()
                        || column.column_schema.default_constraint().is_some(),
                    InvalidRequestSnafu {
                        region_id,
                        reason: format!("Missing column {}", column.column_schema.name),
                    }
                );
            }
        }

        // Checks all columns in rows exist in the regino.
        if !rows_columns.is_empty() {
            let names: Vec<_> = rows_columns.into_keys().collect();
            return InvalidRequestSnafu {
                region_id,
                reason: format!("Unknown columns: {:?}", names),
            }
            .fail();
        }

        Ok(())
    }
}

/// Returns true if the pb semantic type is valid.
fn check_semantic_type(type_value: i32, semantic_type: SemanticType) -> bool {
    type_value == semantic_type as i32
}

/// Returns true if the pb type value is valid.
fn check_column_type(type_value: i32, expect_type: &ConcreteDataType) -> bool {
    let Some(column_type) = ColumnDataType::from_i32(type_value) else {
        return false;
    };

    is_column_type_eq(column_type, expect_type)
}

/// Returns true if the column type is equal to exepcted type.
fn is_column_type_eq(column_type: ColumnDataType, expect_type: &ConcreteDataType) -> bool {
    match (column_type, expect_type) {
        (ColumnDataType::Boolean, ConcreteDataType::Boolean(_))
        | (ColumnDataType::Int8, ConcreteDataType::Int8(_))
        | (ColumnDataType::Int16, ConcreteDataType::Int16(_))
        | (ColumnDataType::Int32, ConcreteDataType::Int32(_))
        | (ColumnDataType::Int64, ConcreteDataType::Int64(_))
        | (ColumnDataType::Uint8, ConcreteDataType::UInt8(_))
        | (ColumnDataType::Uint16, ConcreteDataType::UInt16(_))
        | (ColumnDataType::Uint32, ConcreteDataType::UInt32(_))
        | (ColumnDataType::Uint64, ConcreteDataType::UInt64(_))
        | (ColumnDataType::Float32, ConcreteDataType::Float32(_))
        | (ColumnDataType::Float64, ConcreteDataType::Float64(_))
        | (ColumnDataType::Binary, ConcreteDataType::Binary(_))
        | (ColumnDataType::String, ConcreteDataType::String(_))
        | (ColumnDataType::Date, ConcreteDataType::Date(_))
        | (ColumnDataType::Datetime, ConcreteDataType::DateTime(_))
        | (
            ColumnDataType::TimestampSecond,
            ConcreteDataType::Timestamp(TimestampType::Second(_)),
        )
        | (
            ColumnDataType::TimestampMillisecond,
            ConcreteDataType::Timestamp(TimestampType::Millisecond(_)),
        )
        | (
            ColumnDataType::TimestampMicrosecond,
            ConcreteDataType::Timestamp(TimestampType::Microsecond(_)),
        )
        | (
            ColumnDataType::TimestampNanosecond,
            ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)),
        )
        | (ColumnDataType::TimeSecond, ConcreteDataType::Time(TimeType::Second(_)))
        | (ColumnDataType::TimeMillisecond, ConcreteDataType::Time(TimeType::Millisecond(_)))
        | (ColumnDataType::TimeMicrosecond, ConcreteDataType::Time(TimeType::Microsecond(_)))
        | (ColumnDataType::TimeNanosecond, ConcreteDataType::Time(TimeType::Nanosecond(_))) => true,
        _ => false,
    }
}

// sender
// pb write message
// region id
// rows
// sequence
// op type

// /// Entry for a write request in [WriteRequestBatch].
// #[derive(Debug)]
// pub(crate) struct BatchEntry {
//     /// Result sender.
//     pub(crate) sender: Option<Sender<Result<()>>>,
//     /// A region write request.
//     pub(crate) request: WriteRequest,
// }

// /// Batch of write requests.
// #[derive(Debug, Default)]
// pub(crate) struct WriteRequestBatch {
//     /// Batched requests for each region.
//     pub(crate) requests: HashMap<RegionId, Vec<BatchEntry>>,
// }

// impl WriteRequestBatch {
//     /// Push a write request into the batch.
//     ///
//     /// # Panics
//     /// Panics if the request body isn't a [WriteRequest].
//     pub(crate) fn push(&mut self, request: RegionRequest) {
//         match request.body {
//             RequestBody::Write(write_req) => {
//                 self.requests.entry(write_req.region_id)
//                     .or_default()
//                     .push(BatchEntry { sender: request.sender, request: write_req, })
//             },
//             other => panic!("request is not a write request: {:?}", other),
//         }
//     }

//     /// Returns true if the batch is empty.
//     pub(crate) fn is_empty(&self) -> bool {
//         self.requests.is_empty()
//     }
// }
