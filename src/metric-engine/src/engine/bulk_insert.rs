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

use std::collections::HashSet;

use api::helper::{ColumnDataTypeWrapper, to_grpc_value};
use api::v1::{ArrowIpc, SemanticType};
use bytes::Bytes;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::Helper;
use snafu::{OptionExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{
    AffectedRows, RegionBulkInsertsRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::RegionId;

use crate::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use crate::engine::MetricEngineInner;
use crate::error::{
    ColumnNotFoundSnafu, Error as MetricError, ForbiddenPhysicalAlterSnafu,
    PhysicalRegionNotFoundSnafu, Result, UnexpectedRequestSnafu, UnsupportedRegionRequestSnafu,
};

impl MetricEngineInner {
    pub async fn bulk_insert_region(
        &self,
        region_id: RegionId,
        request: RegionBulkInsertsRequest,
    ) -> Result<AffectedRows> {
        if self.is_physical_region(region_id) {
            return ForbiddenPhysicalAlterSnafu.fail();
        }

        let (physical_region_id, data_region_id, primary_key_encoding) =
            self.find_data_region_meta(region_id)?;

        if primary_key_encoding != PrimaryKeyEncoding::Sparse {
            return UnsupportedRegionRequestSnafu {
                request: RegionRequest::BulkInserts(request),
            }
            .fail();
        }

        let batch = request.payload;
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let logical_metadata = self
            .logical_region_metadata(physical_region_id, region_id)
            .await?;
        let (tag_columns, non_tag_indices) = self
            .resolve_tag_columns(region_id, data_region_id, &batch, &logical_metadata)
            .await?;
        let modified_batch = modify_batch_sparse(
            batch.clone(),
            region_id.table_id(),
            &tag_columns,
            &non_tag_indices,
        )?;
        let (schema, data_header, payload) = record_batch_to_ipc(&modified_batch)?;

        let request = RegionBulkInsertsRequest {
            region_id: data_region_id,
            payload: modified_batch,
            raw_data: ArrowIpc {
                schema,
                data_header,
                payload,
            },
            partition_expr_version: None,
        };
        match self
            .data_region
            .write_data(data_region_id, RegionRequest::BulkInserts(request))
            .await
        {
            Ok(affected_rows) => Ok(affected_rows),
            Err(err) if is_unsupported_bulk_write(&err) => {
                let rows = record_batch_to_rows(&batch, &logical_metadata, region_id)?;
                self.put_region(region_id, RegionPutRequest { rows, hint: None, partition_expr_version: None })
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn resolve_tag_columns(
        &self,
        logical_region_id: RegionId,
        data_region_id: RegionId,
        batch: &RecordBatch,
        logical_metadata: &RegionMetadataRef,
    ) -> Result<(Vec<TagColumnInfo>, Vec<usize>)> {
        resolve_tag_columns_from_metadata(
            logical_region_id,
            data_region_id,
            batch,
            logical_metadata,
            self,
        )
    }
}

fn resolve_tag_columns_from_metadata(
    logical_region_id: RegionId,
    data_region_id: RegionId,
    batch: &RecordBatch,
    logical_metadata: &RegionMetadataRef,
    engine: &MetricEngineInner,
) -> Result<(Vec<TagColumnInfo>, Vec<usize>)> {
    let tag_names: HashSet<&str> = logical_metadata
        .column_metadatas
        .iter()
        .filter_map(|column| {
            if column.semantic_type == SemanticType::Tag {
                Some(column.column_schema.name.as_str())
            } else {
                None
            }
        })
        .collect();

    let state = engine.state.read().unwrap();
    let physical_columns = state
        .physical_region_states()
        .get(&data_region_id)
        .context(PhysicalRegionNotFoundSnafu {
            region_id: data_region_id,
        })?
        .physical_columns();

    let mut tag_columns = Vec::new();
    let mut non_tag_indices = Vec::new();

    for (index, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name();
        let column_id = *physical_columns.get(name).context(ColumnNotFoundSnafu {
            name: name.clone(),
            region_id: logical_region_id,
        })?;
        if tag_names.contains(name.as_str()) {
            tag_columns.push(TagColumnInfo {
                name: name.clone(),
                index,
                column_id,
            });
        } else {
            non_tag_indices.push(index);
        }
    }

    tag_columns.sort_by(|a, b| a.name.cmp(&b.name));
    Ok((tag_columns, non_tag_indices))
}

fn is_unsupported_bulk_write(err: &MetricError) -> bool {
    err.status_code() == StatusCode::Unsupported
}

fn record_batch_to_rows(
    batch: &RecordBatch,
    logical_metadata: &RegionMetadataRef,
    logical_region_id: RegionId,
) -> Result<api::v1::Rows> {
    let metadata_by_name: std::collections::HashMap<_, _> = logical_metadata
        .column_metadatas
        .iter()
        .map(|meta| (meta.column_schema.name.as_str(), meta))
        .collect();

    let mut schema = Vec::with_capacity(batch.num_columns());
    let mut vectors = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let metadata = metadata_by_name
            .get(field.name().as_str())
            .copied()
            .context(ColumnNotFoundSnafu {
                name: field.name().clone(),
                region_id: logical_region_id,
            })?;

        let data_type_wrapper = ColumnDataTypeWrapper::try_from(
            metadata.column_schema.data_type.clone(),
        )
        .map_err(|e| {
            UnexpectedRequestSnafu {
                reason: format!(
                    "Failed to convert column '{}' datatype: {}",
                    field.name(),
                    e
                ),
            }
            .build()
        })?;
        schema.push(api::v1::ColumnSchema {
            column_name: field.name().clone(),
            datatype: data_type_wrapper.datatype() as i32,
            semantic_type: metadata.semantic_type as i32,
            datatype_extension: None,
            options: None,
        });
        vectors.push(Helper::try_into_vector(batch.column(idx)).map_err(|e| {
            UnexpectedRequestSnafu {
                reason: format!(
                    "Failed to convert column '{}' to vector: {}",
                    field.name(),
                    e
                ),
            }
            .build()
        })?);
    }

    let rows = (0..batch.num_rows())
        .map(|row_idx| api::v1::Row {
            values: vectors
                .iter()
                .map(|vector| to_grpc_value(vector.get(row_idx)))
                .collect(),
        })
        .collect();

    Ok(api::v1::Rows { schema, rows })
}

fn record_batch_to_ipc(record_batch: &RecordBatch) -> Result<(Bytes, Bytes, Bytes)> {
    let mut encoder = FlightEncoder::default();
    let schema = encoder.encode_schema(record_batch.schema().as_ref());
    let mut iter = encoder
        .encode(FlightMessage::RecordBatch(record_batch.clone()))
        .into_iter();

    let Some(flight_data) = iter.next() else {
        return UnexpectedRequestSnafu {
            reason: "Failed to encode empty flight data".to_string(),
        }
        .fail();
    };
    ensure!(
        iter.next().is_none(),
        UnexpectedRequestSnafu {
            reason: "Bulk insert RecordBatch with dictionary arrays is unsupported".to_string(),
        }
    );

    Ok((
        schema.data_header,
        flight_data.data_header,
        flight_data.data_body,
    ))
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use api::v1::ArrowIpc;
    use common_error::ext::ErrorExt;
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use common_recordbatch::RecordBatches;
    use datatypes::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use datatypes::arrow::record_batch::RecordBatch;
    use store_api::metric_engine_consts::MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING;
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{RegionBulkInsertsRequest, RegionPutRequest, RegionRequest};
    use store_api::storage::{RegionId, ScanRequest};

    use super::record_batch_to_ipc;
    use crate::error::Error;
    use crate::test_util::{self, TestEnv};

    fn build_logical_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                greptime_timestamp(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(greptime_value(), DataType::Float64, true),
            Field::new("job", DataType::Utf8, true),
        ]));

        let mut ts = Vec::with_capacity(rows);
        let mut values = Vec::with_capacity(rows);
        let mut tags = Vec::with_capacity(rows);
        for i in 0..rows {
            ts.push(i as i64);
            values.push(i as f64);
            tags.push("tag_0".to_string());
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(tags)),
            ],
        )
        .unwrap()
    }

    fn build_bulk_request(logical_region_id: RegionId, batch: RecordBatch) -> RegionRequest {
        let (schema, data_header, payload) = record_batch_to_ipc(&batch).unwrap();
        RegionRequest::BulkInserts(RegionBulkInsertsRequest {
            region_id: logical_region_id,
            payload: batch,
            raw_data: ArrowIpc {
                schema,
                data_header,
                payload,
            },
            partition_expr_version: None,
        })
    }

    async fn init_dense_metric_region(env: &TestEnv) -> RegionId {
        let physical_region_id = env.default_physical_region_id();
        env.create_physical_region(
            physical_region_id,
            &TestEnv::default_table_dir(),
            vec![(
                MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING.to_string(),
                "dense".to_string(),
            )],
        )
        .await;

        let logical_region_id = env.default_logical_region_id();
        let request = test_util::create_logical_region_request(
            &["job"],
            physical_region_id,
            &table_dir("test", logical_region_id.table_id()),
        );
        env.metric()
            .handle_request(logical_region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        logical_region_id
    }

    #[tokio::test]
    async fn test_bulk_insert_sparse_encoding() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let logical_region_id = env.default_logical_region_id();

        let request = build_bulk_request(logical_region_id, build_logical_batch(4));
        let response = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 4);

        let stream = env
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
    }

    #[tokio::test]
    async fn test_bulk_insert_dense_encoding_rejected() {
        let env = TestEnv::new().await;
        let logical_region_id = init_dense_metric_region(&env).await;

        let request = build_bulk_request(logical_region_id, build_logical_batch(2));
        let err = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap_err();
        let Some(err) = err.as_any().downcast_ref::<Error>() else {
            panic!("unexpected error type");
        };
        assert_matches!(err, Error::UnsupportedRegionRequest { .. });
    }

    #[tokio::test]
    async fn test_bulk_insert_matches_put() {
        let env_put = TestEnv::new().await;
        env_put.init_metric_region().await;
        let logical_region_id = env_put.default_logical_region_id();
        let schema = test_util::row_schema_with_tags(&["job"]);
        let rows = test_util::build_rows(1, 5);
        env_put
            .metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: api::v1::Rows { schema, rows },
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap();
        let put_stream = env_put
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let put_batches = RecordBatches::try_collect(put_stream).await.unwrap();
        let put_output = put_batches.pretty_print().unwrap();

        let env_bulk = TestEnv::new().await;
        env_bulk.init_metric_region().await;
        let request = build_bulk_request(logical_region_id, build_logical_batch(5));
        env_bulk
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        let bulk_stream = env_bulk
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let bulk_batches = RecordBatches::try_collect(bulk_stream).await.unwrap();
        let bulk_output = bulk_batches.pretty_print().unwrap();

        assert_eq!(put_output, bulk_output);
    }
}
