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

//! Handles bulk insert requests.

use api::helper::{value_to_grpc_value, ColumnDataTypeWrapper};
use api::v1::{ColumnSchema, OpType, Row, Rows};
use common_recordbatch::DfRecordBatch;
use datatypes::prelude::VectorRef;
use datatypes::vectors::Helper;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{BulkInsertPayload, RegionBulkInsertsRequest};

use crate::error;
use crate::request::{OptionOutputTx, SenderWriteRequest, WriteRequest};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_bulk_inserts(
        &mut self,
        request: RegionBulkInsertsRequest,
        region_metadata: RegionMetadataRef,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        sender: OptionOutputTx,
    ) {
        let schema = match region_metadata_to_column_schema(&region_metadata) {
            Ok(schema) => schema,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };
        let mut pending_tasks = Vec::with_capacity(request.payloads.len());
        for req in request.payloads {
            match req {
                BulkInsertPayload::ArrowIpc(df_record_batch) => {
                    let rows = Rows {
                        schema: schema.clone(),
                        rows: record_batch_to_rows(&region_metadata, &df_record_batch),
                    };

                    let write_request = match WriteRequest::new(
                        region_metadata.region_id,
                        OpType::Put,
                        rows,
                        Some(region_metadata.clone()),
                    ) {
                        Ok(write_request) => write_request,
                        Err(e) => {
                            sender.send(Err(e));
                            return;
                        }
                    };

                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let sender = OptionOutputTx::from(tx);
                    let req = SenderWriteRequest {
                        sender,
                        request: write_request,
                    };
                    pending_tasks.push(rx);
                    pending_write_requests.push(req);
                }
            }
        }

        common_runtime::spawn_global(async move {
            let results = match futures::future::try_join_all(pending_tasks).await {
                Ok(results) => results,
                Err(e) => {
                    sender.send(Err(e).context(error::RecvSnafu));
                    return;
                }
            };
            let result1 = match results.into_iter().collect::<error::Result<Vec<_>>>() {
                Ok(results) => Ok(results.into_iter().sum()),
                Err(e) => Err(e),
            };
            sender.send(result1);
        });
    }
}

fn region_metadata_to_column_schema(
    region_meta: &RegionMetadataRef,
) -> error::Result<Vec<ColumnSchema>> {
    region_meta
        .column_metadatas
        .iter()
        .map(|c| {
            let wrapper = ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
                .with_context(|_| error::ConvertDataTypeSnafu {
                    data_type: c.column_schema.data_type.clone(),
                })?;

            Ok(api::v1::ColumnSchema {
                column_name: c.column_schema.name.clone(),
                datatype: wrapper.datatype() as i32,
                semantic_type: c.semantic_type as i32,
                ..Default::default()
            })
        })
        .collect::<error::Result<_>>()
}

fn record_batch_to_rows(region_metadata: &RegionMetadataRef, rb: &DfRecordBatch) -> Vec<Row> {
    let num_rows = rb.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    let vectors: Vec<_> = region_metadata
        .column_metadatas
        .iter()
        .map(|c| {
            rb.column_by_name(&c.column_schema.name)
                .map(|arr| Helper::try_into_vector(arr).unwrap())
        })
        .collect();

    let mut current_row = Vec::with_capacity(region_metadata.column_metadatas.len());
    for row_idx in 0..num_rows {
        row_at(&vectors, row_idx, &mut current_row);
        let row = Row {
            values: std::mem::take(&mut current_row),
        };
        rows.push(row);
    }
    rows
}

fn row_at(vectors: &[Option<VectorRef>], row_idx: usize, row: &mut Vec<api::v1::Value>) {
    for a in vectors {
        let value = if let Some(a) = a {
            value_to_grpc_value(a.get(row_idx))
        } else {
            api::v1::Value { value_data: None }
        };
        row.push(value)
    }
}
