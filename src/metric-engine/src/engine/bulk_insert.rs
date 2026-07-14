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
use std::sync::Arc;

use api::v1::{ArrowIpc, SemanticType};
use common_grpc::flight::{FlightEncoder, FlightMessage};
use datatypes::arrow::array::new_null_array;
use datatypes::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::metric_engine_consts::is_metric_engine_value_int_column;
use store_api::region_request::{AffectedRows, RegionBulkInsertsRequest, RegionRequest};
use store_api::storage::RegionId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use crate::engine::MetricEngineInner;
use crate::error;
use crate::error::Result;
use crate::metrics::MITO_OPERATION_ELAPSED;

impl MetricEngineInner {
    /// Bulk-inserts rows into a metric region.
    ///
    /// **Logical region path:** The request payload is a logical `RecordBatch`
    /// (timestamp, value and tag columns). It is transformed to physical format
    /// via `modify_batch_sparse`, encoded to Arrow IPC, and forwarded as a
    /// `BulkInserts` request to the data region.
    ///
    /// **Physical region path:** The request payload is already in physical format
    /// (produced by the batcher's `flush_batch_physical`). Missing integer companion
    /// fields are appended before it is forwarded to the data region.
    ///
    /// Returns the number of affected rows, or `0` if the input batch is empty.
    pub async fn bulk_insert_region(
        &self,
        region_id: RegionId,
        request: RegionBulkInsertsRequest,
    ) -> Result<AffectedRows> {
        if request.payload.num_rows() == 0 {
            return Ok(0);
        }
        if self.is_physical_region(region_id) {
            let _timer = MITO_OPERATION_ELAPSED
                .with_label_values(&["bulk_insert_physical"])
                .start_timer();
            return self.bulk_insert_physical_region(region_id, request).await;
        }

        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["bulk_insert_logical"])
            .start_timer();
        self.bulk_insert_logical_region(region_id, request).await
    }

    /// Passthrough for bulk inserts targeting a physical data region.
    ///
    /// The batch is already in physical format (with `__primary_key`, timestamp,
    /// value columns). Missing integer companion fields are appended before forwarding.
    async fn bulk_insert_physical_region(
        &self,
        region_id: RegionId,
        mut request: RegionBulkInsertsRequest,
    ) -> Result<AffectedRows> {
        let metadata = self
            .mito
            .get_physical_metadata(region_id)
            .context(error::MitoReadOperationSnafu)?;
        let original_num_columns = request.payload.num_columns();
        request.payload =
            Self::append_missing_metric_value_int_fields(region_id, &metadata, request.payload)?;
        if request.payload.num_columns() != original_num_columns {
            request.raw_data = record_batch_to_ipc(&request.payload)?;
        }
        // Simply set the aligned schema to the data region schema version to avoid filling missing columns
        // because that schema should be constant and callers have ensured request has the same schema.
        request.aligned_schema_version = Some(metadata.schema_version);
        self.data_region
            .write_data(region_id, RegionRequest::BulkInserts(request))
            .await
    }

    /// Bulk-inserts logical rows, transforming them to physical format first.
    async fn bulk_insert_logical_region(
        &self,
        region_id: RegionId,
        request: RegionBulkInsertsRequest,
    ) -> Result<AffectedRows> {
        let (physical_region_id, data_region_id, primary_key_encoding) =
            self.find_data_region_meta(region_id)?;

        if primary_key_encoding != PrimaryKeyEncoding::Sparse {
            return error::UnsupportedRegionRequestSnafu {
                request: RegionRequest::BulkInserts(request),
            }
            .fail();
        }

        let batch = request.payload;

        let logical_metadata = self
            .logical_region_metadata(physical_region_id, region_id)
            .await?;
        let (tag_columns, non_tag_indices) = self.resolve_tag_columns_from_metadata(
            region_id,
            data_region_id,
            &batch,
            &logical_metadata,
        )?;
        let modified_batch =
            modify_batch_sparse(batch, region_id.table_id(), &tag_columns, &non_tag_indices)?;
        let metadata = self
            .mito
            .get_physical_metadata(data_region_id)
            .context(error::MitoReadOperationSnafu)?;
        let modified_batch = Self::append_missing_metric_value_int_fields(
            data_region_id,
            &metadata,
            modified_batch,
        )?;

        let partition_expr_version = request.partition_expr_version;

        let request = RegionBulkInsertsRequest {
            region_id: data_region_id,
            raw_data: record_batch_to_ipc(&modified_batch)?,
            payload: modified_batch,
            partition_expr_version,
            aligned_schema_version: Some(metadata.schema_version),
        };
        self.data_region
            .write_data(data_region_id, RegionRequest::BulkInserts(request))
            .await
    }

    fn append_missing_metric_value_int_fields(
        data_region_id: RegionId,
        metadata: &RegionMetadata,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        let batch_schema = batch.schema();
        let existing_names = batch_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<HashSet<_>>();

        let missing_columns = metadata
            .field_columns()
            .filter(|column| is_metric_engine_value_int_column(&column.column_schema.name))
            .filter(|column| !existing_names.contains(column.column_schema.name.as_str()))
            .collect::<Vec<_>>();
        if missing_columns.is_empty() {
            return Ok(batch);
        }

        let mut fields = batch_schema.fields().to_vec();
        let insert_index = batch_schema
            .index_of(PRIMARY_KEY_COLUMN_NAME)
            .unwrap_or(fields.len());
        let mut columns = batch.columns().to_vec();
        for (offset, column) in missing_columns.into_iter().enumerate() {
            let field = ArrowField::try_from(&column.column_schema).map_err(|err| {
                error::InvalidRequestSnafu {
                    region_id: data_region_id,
                    reason: format!(
                        "failed to build Arrow field for column {}: {err}",
                        column.column_schema.name
                    ),
                }
                .build()
            })?;
            columns.insert(
                insert_index + offset,
                new_null_array(field.data_type(), batch.num_rows()),
            );
            fields.insert(insert_index + offset, Arc::new(field));
        }

        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).map_err(|err| {
            error::InvalidRequestSnafu {
                region_id: data_region_id,
                reason: format!("failed to append metric value companion columns: {err}"),
            }
            .build()
        })
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
                let column_id = physical_columns
                    .get(name)
                    .map(|info| info.column_id)
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

fn record_batch_to_ipc(record_batch: &RecordBatch) -> Result<ArrowIpc> {
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

    Ok(ArrowIpc {
        schema: schema.data_header,
        data_header: flight_data.data_header,
        payload: flight_data.data_body,
    })
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::Arc;

    use api::v1::ArrowIpc;
    use common_error::ext::ErrorExt;
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use common_recordbatch::RecordBatches;
    use datatypes::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use datatypes::arrow::record_batch::RecordBatch;
    use mito2::config::MitoConfig;
    use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{RegionBulkInsertsRequest, RegionPutRequest, RegionRequest};
    use store_api::storage::{RegionId, ScanRequest};

    use super::record_batch_to_ipc;
    use crate::batch_modifier::{TagColumnInfo, modify_batch_sparse};
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
        RegionRequest::BulkInserts(RegionBulkInsertsRequest {
            region_id: logical_region_id,
            raw_data: record_batch_to_ipc(&batch).unwrap(),
            payload: batch,
            partition_expr_version: None,
            aligned_schema_version: None,
        })
    }

    fn collect_metric_values(batches: &RecordBatches) -> Vec<f64> {
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(greptime_value())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .map(Option::unwrap)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        values
    }

    async fn init_dense_metric_region(env: &TestEnv) -> RegionId {
        let physical_region_id = env.default_physical_region_id();
        env.create_physical_region(
            physical_region_id,
            &TestEnv::default_table_dir(),
            vec![(PRIMARY_KEY_ENCODING.to_string(), "dense".to_string())],
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
            aligned_schema_version: None,
        });
        let response = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 0);
    }

    #[tokio::test]
    async fn test_bulk_insert_physical_region_passthrough() {
        // Use flat format so that BulkMemtable is used (supports write_bulk).
        let mito_config = MitoConfig {
            default_flat_format: true,
            ..Default::default()
        };
        let env = TestEnv::with_mito_config("", mito_config, Default::default()).await;
        env.init_metric_region().await;
        let physical_region_id = env.default_physical_region_id();
        let logical_region_id = env.default_logical_region_id();

        // First, do a normal logical bulk insert so we can compare results.
        let logical_batch = build_logical_batch(0, 3);
        let logical_request = build_bulk_request(logical_region_id, logical_batch.clone());
        let response = env
            .metric()
            .handle_request(logical_region_id, logical_request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3);

        // Now build a physical-format batch using modify_batch_sparse (simulating
        // what the batcher's flush_batch_physical does) and send it directly to
        // the physical region.
        let tag_columns = vec![TagColumnInfo {
            name: "job".to_string(),
            index: 2,
            column_id: 3, // column_id for "job" in the physical table
        }];
        let non_tag_indices = vec![0, 1]; // timestamp, value
        let second_batch = build_logical_batch(3, 3);
        let physical_batch = modify_batch_sparse(
            second_batch,
            logical_region_id.table_id(),
            &tag_columns,
            &non_tag_indices,
        )
        .unwrap();
        let request = build_bulk_request(physical_region_id, physical_batch);
        let response = env
            .metric()
            .handle_request(physical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3);

        // Verify all 6 rows are readable from the logical region.
        let stream = env
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
        assert_eq!(
            collect_metric_values(&batches),
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        );

        env.metric()
            .handle_request(physical_region_id, RegionRequest::Flush(Default::default()))
            .await
            .unwrap();
        let stream = env
            .metric()
            .scan_to_stream(logical_region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
        assert_eq!(
            collect_metric_values(&batches),
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        );
    }

    #[tokio::test]
    async fn test_bulk_insert_physical_region_empty_batch() {
        // Use flat format so that BulkMemtable is used (supports write_bulk).
        let mito_config = MitoConfig {
            default_flat_format: true,
            ..Default::default()
        };
        let env = TestEnv::with_mito_config("", mito_config, Default::default()).await;
        env.init_metric_region().await;
        let physical_region_id = env.default_physical_region_id();

        let batch = build_logical_batch(0, 0);
        let request = build_bulk_request(physical_region_id, batch);
        let response = env
            .metric()
            .handle_request(physical_region_id, request)
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 0);
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
