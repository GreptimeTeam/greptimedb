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

use std::collections::HashMap;

use api::helper::{value_to_grpc_value, ColumnDataTypeWrapper};
use api::v1::{ColumnSchema, OpType, Row, Rows};
use common_base::AffectedRows;
use common_recordbatch::DfRecordBatch;
use datatypes::prelude::VectorRef;
use datatypes::vectors::Helper;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{BulkInsertPayload, RegionBulkInsertsRequest};
use tokio::sync::oneshot::Receiver;

use crate::error;
use crate::request::{OptionOutputTx, SenderWriteRequest, WriteRequest};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_bulk_insert(
        &mut self,
        mut request: RegionBulkInsertsRequest,
        region_metadata: RegionMetadataRef,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        sender: OptionOutputTx,
    ) {
        let (column_schemas, name_to_index) =
            match region_metadata_to_column_schema(&region_metadata) {
                Ok(schema) => schema,
                Err(e) => {
                    sender.send(Err(e));
                    return;
                }
            };

        // fast path: only one payload.
        if request.payloads.len() == 1 {
            match Self::handle_payload(
                &region_metadata,
                request.payloads.swap_remove(0),
                pending_write_requests,
                column_schemas,
                name_to_index,
            ) {
                Ok(task_future) => common_runtime::spawn_global(async move {
                    sender.send(task_future.await.context(error::RecvSnafu).flatten());
                }),
                Err(e) => {
                    sender.send(Err(e));
                    return;
                }
            };
            return;
        }

        let mut pending_tasks = Vec::with_capacity(request.payloads.len());
        for req in request.payloads {
            match Self::handle_payload(
                &region_metadata,
                req,
                pending_write_requests,
                column_schemas.clone(),
                name_to_index.clone(),
            ) {
                Ok(task_future) => {
                    pending_tasks.push(task_future);
                }
                Err(e) => {
                    sender.send(Err(e));
                    return;
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
            sender.send(
                match results.into_iter().collect::<error::Result<Vec<_>>>() {
                    Ok(results) => Ok(results.into_iter().sum()),
                    Err(e) => Err(e),
                },
            );
        });
    }

    fn handle_payload(
        region_metadata: &RegionMetadataRef,
        payload: BulkInsertPayload,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        column_schemas: Vec<ColumnSchema>,
        name_to_index: HashMap<String, usize>,
    ) -> error::Result<Receiver<error::Result<AffectedRows>>> {
        let rx = match payload {
            BulkInsertPayload::ArrowIpc(rb) => Self::handle_arrow_ipc(
                region_metadata,
                rb,
                pending_write_requests,
                column_schemas,
                name_to_index,
            ),
            BulkInsertPayload::Rows { data, has_null } => Self::handle_rows(
                region_metadata,
                data,
                column_schemas,
                has_null,
                pending_write_requests,
                name_to_index,
            ),
        }?;

        Ok(rx)
    }

    fn handle_arrow_ipc(
        region_metadata: &RegionMetadataRef,
        df_record_batch: DfRecordBatch,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        column_schemas: Vec<ColumnSchema>,
        name_to_index: HashMap<String, usize>,
    ) -> error::Result<Receiver<error::Result<AffectedRows>>> {
        let has_null: Vec<_> = df_record_batch
            .columns()
            .iter()
            .map(|c| c.null_count() > 0)
            .collect();

        let rows = record_batch_to_rows(region_metadata, &df_record_batch)?;

        let write_request = WriteRequest {
            region_id: region_metadata.region_id,
            op_type: OpType::Put,
            rows: Rows {
                schema: column_schemas,
                rows,
            },
            name_to_index,
            has_null,
            hint: None,
            region_metadata: Some(region_metadata.clone()),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let sender = OptionOutputTx::from(tx);
        let req = SenderWriteRequest {
            sender,
            request: write_request,
        };
        pending_write_requests.push(req);
        Ok(rx)
    }

    fn handle_rows(
        region_metadata: &RegionMetadataRef,
        rows: Vec<Row>,
        column_schemas: Vec<ColumnSchema>,
        has_null: Vec<bool>,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        name_to_index: HashMap<String, usize>,
    ) -> error::Result<Receiver<error::Result<AffectedRows>>> {
        let write_request = WriteRequest {
            region_id: region_metadata.region_id,
            op_type: OpType::Put,
            rows: Rows {
                schema: column_schemas,
                rows,
            },
            name_to_index,
            has_null,
            hint: None,
            region_metadata: Some(region_metadata.clone()),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let sender = OptionOutputTx::from(tx);
        let req = SenderWriteRequest {
            sender,
            request: write_request,
        };
        pending_write_requests.push(req);
        Ok(rx)
    }
}

fn region_metadata_to_column_schema(
    region_meta: &RegionMetadataRef,
) -> error::Result<(Vec<ColumnSchema>, HashMap<String, usize>)> {
    let mut column_schemas = Vec::with_capacity(region_meta.column_metadatas.len());
    let mut name_to_index = HashMap::with_capacity(region_meta.column_metadatas.len());

    for (idx, c) in region_meta.column_metadatas.iter().enumerate() {
        let wrapper = ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
            .with_context(|_| error::ConvertDataTypeSnafu {
                data_type: c.column_schema.data_type.clone(),
            })?;
        column_schemas.push(ColumnSchema {
            column_name: c.column_schema.name.clone(),
            datatype: wrapper.datatype() as i32,
            semantic_type: c.semantic_type as i32,
            ..Default::default()
        });

        name_to_index.insert(c.column_schema.name.clone(), idx);
    }

    Ok((column_schemas, name_to_index))
}

/// Convert [DfRecordBatch] to gRPC rows.
fn record_batch_to_rows(
    region_metadata: &RegionMetadataRef,
    rb: &DfRecordBatch,
) -> error::Result<Vec<Row>> {
    let num_rows = rb.num_rows();
    let mut rows = Vec::with_capacity(num_rows);
    if num_rows == 0 {
        return Ok(rows);
    }
    let vectors: Vec<Option<VectorRef>> = region_metadata
        .column_metadatas
        .iter()
        .map(|c| {
            rb.column_by_name(&c.column_schema.name)
                .map(|column| Helper::try_into_vector(column).context(error::ConvertVectorSnafu))
                .transpose()
        })
        .collect::<error::Result<_>>()?;

    for row_idx in 0..num_rows {
        let row = Row {
            values: row_at(&vectors, row_idx),
        };
        rows.push(row);
    }
    Ok(rows)
}

fn row_at(vectors: &[Option<VectorRef>], row_idx: usize) -> Vec<api::v1::Value> {
    let mut row = Vec::with_capacity(vectors.len());
    for a in vectors {
        let value = if let Some(a) = a {
            value_to_grpc_value(a.get(row_idx))
        } else {
            api::v1::Value { value_data: None }
        };
        row.push(value)
    }
    row
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray};

    use super::*;
    use crate::test_util::meta_util::TestRegionMetadataBuilder;

    fn build_record_batch(num_rows: usize) -> DfRecordBatch {
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().build());
        let schema = region_metadata.schema.arrow_schema().clone();
        let values = (0..num_rows).map(|v| v as i64).collect::<Vec<_>>();
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter_values(values.clone()));
        let k0_array = Arc::new(Int64Array::from_iter_values(values.clone()));
        let v0_array = Arc::new(Int64Array::from_iter_values(values));
        DfRecordBatch::try_new(schema, vec![ts_array, k0_array, v0_array]).unwrap()
    }

    #[test]
    fn test_region_metadata_to_column_schema() {
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().build());
        let (result, _) = region_metadata_to_column_schema(&region_metadata).unwrap();
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].column_name, "ts");
        assert_eq!(result[0].semantic_type, SemanticType::Timestamp as i32);

        assert_eq!(result[1].column_name, "k0");
        assert_eq!(result[1].semantic_type, SemanticType::Tag as i32);

        assert_eq!(result[2].column_name, "v0");
        assert_eq!(result[2].semantic_type, SemanticType::Field as i32);
    }

    #[test]
    fn test_record_batch_to_rows() {
        // Create record batch
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().build());
        let record_batch = build_record_batch(10);
        let rows = record_batch_to_rows(&region_metadata, &record_batch).unwrap();

        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].values.len(), 3);

        for (row_idx, row) in rows.iter().enumerate().take(10) {
            assert_eq!(
                row.values[0].value_data.as_ref().unwrap(),
                &api::v1::value::ValueData::TimestampMillisecondValue(row_idx as i64)
            );
        }
    }

    #[test]
    fn test_record_batch_to_rows_schema_mismatch() {
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().num_fields(2).build());
        let record_batch = build_record_batch(1);

        let rows = record_batch_to_rows(&region_metadata, &record_batch).unwrap();
        assert_eq!(rows.len(), 1);

        // Check first row
        let row1 = &rows[0];
        assert_eq!(row1.values.len(), 4);
        assert_eq!(
            row1.values[0].value_data.as_ref().unwrap(),
            &api::v1::value::ValueData::TimestampMillisecondValue(0)
        );
        assert_eq!(
            row1.values[1].value_data.as_ref().unwrap(),
            &api::v1::value::ValueData::I64Value(0)
        );
        assert_eq!(
            row1.values[2].value_data.as_ref().unwrap(),
            &api::v1::value::ValueData::I64Value(0)
        );

        assert!(row1.values[3].value_data.is_none());
    }
}
