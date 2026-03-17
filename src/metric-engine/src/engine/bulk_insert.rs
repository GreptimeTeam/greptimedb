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

use api::v1::{ArrowIpc, ColumnDataType, SemanticType};
use bytes::Bytes;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_query::prelude::{greptime_timestamp, greptime_value};
use datatypes::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{
    AffectedRows, RegionBulkInsertsRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::RegionId;

use crate::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use crate::engine::MetricEngineInner;
use crate::error;
use crate::error::Result;

impl MetricEngineInner {
    /// Bulk-inserts logical rows into a metric region.
    ///
    /// This method accepts a `RegionBulkInsertsRequest` whose payload is a logical
    /// `RecordBatch` (timestamp, value and tag columns) for the given logical `region_id`.
    ///
    /// The transformed batch is encoded to Arrow IPC and forwarded as a `BulkInserts`
    /// request to the data region, along with the original `partition_expr_version`.
    /// If the data region reports `StatusCode::Unsupported` for bulk inserts, the request
    /// is transparently retried as a `Put` by converting the original logical batch into
    /// `api::v1::Rows`, so callers observe the same semantics as `put_region`.
    ///
    /// Returns the number of affected rows, or `0` if the input batch is empty.
    pub async fn bulk_insert_region(
        &self,
        region_id: RegionId,
        request: RegionBulkInsertsRequest,
    ) -> Result<AffectedRows> {
        ensure!(
            !self.is_physical_region(region_id),
            error::UnsupportedRegionRequestSnafu {
                request: RegionRequest::BulkInserts(request),
            }
        );

        let (physical_region_id, data_region_id, primary_key_encoding) =
            self.find_data_region_meta(region_id)?;

        if primary_key_encoding != PrimaryKeyEncoding::Sparse {
            return error::UnsupportedRegionRequestSnafu {
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
        let (tag_columns, non_tag_indices) = self.resolve_tag_columns_from_metadata(
            region_id,
            data_region_id,
            &batch,
            &logical_metadata,
        )?;
        let modified_batch = modify_batch_sparse(
            batch.clone(),
            region_id.table_id(),
            &tag_columns,
            &non_tag_indices,
        )?;
        let (schema, data_header, payload) = record_batch_to_ipc(&modified_batch)?;

        let partition_expr_version = request.partition_expr_version;
        let request = RegionBulkInsertsRequest {
            region_id: data_region_id,
            payload: modified_batch,
            raw_data: ArrowIpc {
                schema,
                data_header,
                payload,
            },
            partition_expr_version,
        };
        match self
            .data_region
            .write_data(data_region_id, RegionRequest::BulkInserts(request))
            .await
        {
            Ok(affected_rows) => Ok(affected_rows),
            Err(err) if err.status_code() == StatusCode::Unsupported => {
                let rows = record_batch_to_rows(&batch, region_id)?;
                self.put_region(
                    region_id,
                    RegionPutRequest {
                        rows,
                        hint: None,
                        partition_expr_version,
                    },
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    fn resolve_tag_columns_from_metadata(
        &self,
        logical_region_id: RegionId,
        data_region_id: RegionId,
        batch: &RecordBatch,
        logical_metadata: &RegionMetadataRef,
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

        let mut tag_columns = Vec::new();
        let mut non_tag_indices = Vec::new();
        {
            let state = self.state.read().unwrap();
            let physical_columns = state
                .physical_region_states()
                .get(&data_region_id)
                .context(error::PhysicalRegionNotFoundSnafu {
                    region_id: data_region_id,
                })?
                .physical_columns();

            for (index, field) in batch.schema().fields().iter().enumerate() {
                let name = field.name();
                let column_id =
                    *physical_columns
                        .get(name)
                        .with_context(|| error::ColumnNotFoundSnafu {
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
        }

        tag_columns.sort_by(|a, b| a.name.cmp(&b.name));
        Ok((tag_columns, non_tag_indices))
    }
}

fn record_batch_to_rows(batch: &RecordBatch, logical_region_id: RegionId) -> Result<api::v1::Rows> {
    let schema_ref = batch.schema();
    let fields = schema_ref.fields();

    let mut ts_idx = None;
    let mut val_idx = None;
    let mut tag_indices = Vec::new();

    for (idx, field) in fields.iter().enumerate() {
        if field.name() == greptime_timestamp() {
            ts_idx = Some(idx);
            if !matches!(
                field.data_type(),
                datatypes::arrow::datatypes::DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    _
                )
            ) {
                return error::UnexpectedRequestSnafu {
                    reason: format!(
                        "Timestamp column '{}' in region {:?} has incompatible type: {:?}",
                        field.name(),
                        logical_region_id,
                        field.data_type()
                    ),
                }
                .fail();
            }
        } else if field.name() == greptime_value() {
            val_idx = Some(idx);
            if !matches!(
                field.data_type(),
                datatypes::arrow::datatypes::DataType::Float64
            ) {
                return error::UnexpectedRequestSnafu {
                    reason: format!(
                        "Value column '{}' in region {:?} has incompatible type: {:?}",
                        field.name(),
                        logical_region_id,
                        field.data_type()
                    ),
                }
                .fail();
            }
        } else {
            if !matches!(
                field.data_type(),
                datatypes::arrow::datatypes::DataType::Utf8
            ) {
                return error::UnexpectedRequestSnafu {
                    reason: format!(
                        "Tag column '{}' in region {:?} must be Utf8, found: {:?}",
                        field.name(),
                        logical_region_id,
                        field.data_type()
                    ),
                }
                .fail();
            }
            tag_indices.push(idx);
        }
    }

    let ts_idx = ts_idx.context(error::UnexpectedRequestSnafu {
        reason: format!(
            "Timestamp column '{}' not found in RecordBatch for region {:?}",
            greptime_timestamp(),
            logical_region_id
        ),
    })?;
    let val_idx = val_idx.context(error::UnexpectedRequestSnafu {
        reason: format!(
            "Value column '{}' not found in RecordBatch for region {:?}",
            greptime_value(),
            logical_region_id
        ),
    })?;

    let mut schema = Vec::with_capacity(2 + tag_indices.len());
    schema.push(api::v1::ColumnSchema {
        column_name: greptime_timestamp().to_string(),
        datatype: ColumnDataType::TimestampMillisecond as i32,
        semantic_type: SemanticType::Timestamp as i32,
        datatype_extension: None,
        options: None,
    });
    schema.push(api::v1::ColumnSchema {
        column_name: greptime_value().to_string(),
        datatype: ColumnDataType::Float64 as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension: None,
        options: None,
    });
    for &idx in &tag_indices {
        let field = &fields[idx];
        schema.push(api::v1::ColumnSchema {
            column_name: field.name().clone(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Tag as i32,
            datatype_extension: None,
            options: None,
        });
    }

    let ts_array = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("validated as TimestampMillisecond");
    let val_array = batch
        .column(val_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let tag_arrays: Vec<&StringArray> = tag_indices
        .iter()
        .map(|&idx| {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("validated as Utf8")
        })
        .collect();

    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(2 + tag_arrays.len());

        if ts_array.is_null(row_idx) {
            values.push(api::v1::Value { value_data: None });
        } else {
            values.push(api::v1::Value {
                value_data: Some(api::v1::value::ValueData::TimestampMillisecondValue(
                    ts_array.value(row_idx),
                )),
            });
        }

        if val_array.is_null(row_idx) {
            values.push(api::v1::Value { value_data: None });
        } else {
            values.push(api::v1::Value {
                value_data: Some(api::v1::value::ValueData::F64Value(
                    val_array.value(row_idx),
                )),
            });
        }

        for arr in &tag_arrays {
            if arr.is_null(row_idx) {
                values.push(api::v1::Value { value_data: None });
            } else {
                values.push(api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue(
                        arr.value(row_idx).to_string(),
                    )),
                });
            }
        }

        rows.push(api::v1::Row { values });
    }

    Ok(api::v1::Rows { schema, rows })
}

fn record_batch_to_ipc(record_batch: &RecordBatch) -> Result<(Bytes, Bytes, Bytes)> {
    let mut encoder = FlightEncoder::default();
    let schema = encoder.encode_schema(record_batch.schema().as_ref());
    let mut iter = encoder
        .encode(FlightMessage::RecordBatch(record_batch.clone()))
        .into_iter();

    let Some(flight_data) = iter.next() else {
        return error::UnexpectedRequestSnafu {
            reason: "Failed to encode empty flight data",
        }
        .fail();
    };
    ensure!(
        iter.next().is_none(),
        error::UnexpectedRequestSnafu {
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

    fn build_logical_batch(start: usize, rows: usize) -> RecordBatch {
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
        for i in start..start + rows {
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
    async fn test_bulk_insert_empty_batch_returns_zero() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let logical_region_id = env.default_logical_region_id();

        let batch = build_logical_batch(0, 0);
        let request = RegionRequest::BulkInserts(RegionBulkInsertsRequest {
            region_id: logical_region_id,
            payload: batch,
            raw_data: ArrowIpc::default(),
            partition_expr_version: None,
        });
        let response = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 0);
    }

    #[tokio::test]
    async fn test_bulk_insert_physical_region_rejected() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let physical_region_id = env.default_physical_region_id();
        let batch = build_logical_batch(0, 2);
        let request = build_bulk_request(physical_region_id, batch);

        let err = env
            .metric()
            .handle_request(physical_region_id, request)
            .await
            .unwrap_err();
        let Some(err) = err.as_any().downcast_ref::<Error>() else {
            panic!("unexpected error type");
        };
        assert_matches!(err, Error::UnsupportedRegionRequest { .. });
    }

    #[tokio::test]
    async fn test_bulk_insert_unknown_column_errors() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let logical_region_id = env.default_logical_region_id();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                greptime_timestamp(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(greptime_value(), DataType::Float64, true),
            Field::new("nonexistent_column", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0i64])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["val"])),
            ],
        )
        .unwrap();

        let request = build_bulk_request(logical_region_id, batch);
        let err = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap_err();
        let Some(err) = err.as_any().downcast_ref::<Error>() else {
            panic!("unexpected error type");
        };
        assert_matches!(err, Error::ColumnNotFound { .. });
    }

    #[tokio::test]
    async fn test_bulk_insert_multiple_tag_columns() {
        let env = TestEnv::new().await;
        let physical_region_id = env.default_physical_region_id();
        env.create_physical_region(physical_region_id, &TestEnv::default_table_dir(), vec![])
            .await;
        let logical_region_id = env.default_logical_region_id();
        let request = test_util::create_logical_region_request(
            &["host", "region"],
            physical_region_id,
            &table_dir("test", logical_region_id.table_id()),
        );
        env.metric()
            .handle_request(logical_region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                greptime_timestamp(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(greptime_value(), DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0i64, 1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(StringArray::from(vec!["h1", "h2", "h1"])),
                Arc::new(StringArray::from(vec!["us-east", "us-west", "eu-west"])),
            ],
        )
        .unwrap();

        let request = build_bulk_request(logical_region_id, batch);
        let response = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3);

        let stream = env
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn test_bulk_insert_accumulates_rows() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let logical_region_id = env.default_logical_region_id();

        let request = build_bulk_request(logical_region_id, build_logical_batch(0, 3));
        let response = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3);

        let request = build_bulk_request(logical_region_id, build_logical_batch(3, 5));
        let response = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 5);

        let stream = env
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 8);
    }

    #[tokio::test]
    async fn test_bulk_insert_sparse_encoding() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let logical_region_id = env.default_logical_region_id();

        let request = build_bulk_request(logical_region_id, build_logical_batch(0, 4));
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

        let request = build_bulk_request(logical_region_id, build_logical_batch(0, 2));
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
        let request = build_bulk_request(logical_region_id, build_logical_batch(0, 5));
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
