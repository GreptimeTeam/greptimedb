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

use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use api::v1::region::{
    bulk_insert_request, region_request, ArrowIpc, BulkInsertRequest, RegionRequest,
    RegionRequestHeader,
};
use bytes::Bytes;
use common_base::AffectedRows;
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_grpc::FlightData;
use common_recordbatch::RecordBatch;
use common_telemetry::tracing_context::TracingContext;
use datatypes::schema::Schema;
use prost::Message;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::insert::Inserter;
use crate::{error, metrics};

impl Inserter {
    /// Handle bulk insert request.
    pub async fn handle_bulk_insert(
        &self,
        table_id: TableId,
        decoder: &mut FlightDecoder,
        data: FlightData,
    ) -> error::Result<AffectedRows> {
        let decode_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["decode_request"])
            .start_timer();
        let body_size = data.data_body.len();
        // Build region server requests
        let message = decoder
            .try_decode(&data)
            .context(error::DecodeFlightDataSnafu)?;
        let FlightMessage::Recordbatch(rb) = message else {
            return Ok(0);
        };
        let record_batch = rb.df_record_batch();
        decode_timer.observe_duration();
        metrics::BULK_REQUEST_MESSAGE_SIZE.observe(body_size as f64);
        metrics::BULK_REQUEST_ROWS
            .with_label_values(&["raw"])
            .observe(record_batch.num_rows() as f64);

        // todo(hl): find a way to embed raw FlightData messages in greptimedb proto files so we don't have to encode here.

        // safety: when reach here schema must be present.
        let schema_message = FlightEncoder::default()
            .encode(FlightMessage::Schema(decoder.schema().unwrap().clone()));
        let schema_bytes = Bytes::from(schema_message.encode_to_vec());

        let partition_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["partition"])
            .start_timer();
        let partition_rule = self
            .partition_manager
            .find_table_partition_rule(table_id)
            .await
            .context(error::InvalidPartitionSnafu)?;

        // find partitions for each row in the record batch
        let region_masks = partition_rule
            .split_record_batch(record_batch)
            .context(error::SplitInsertSnafu)?;
        partition_timer.observe_duration();

        // fast path: only one region.
        if region_masks.len() == 1 {
            metrics::BULK_REQUEST_ROWS
                .with_label_values(&["rows_per_region"])
                .observe(record_batch.num_rows() as f64);

            // SAFETY: region masks length checked
            let (region_number, _) = region_masks.into_iter().next().unwrap();
            let region_id = RegionId::new(table_id, region_number);
            let datanode = self
                .partition_manager
                .find_region_leader(region_id)
                .await
                .context(error::FindRegionLeaderSnafu)?;
            let payload = {
                let _encode_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                    .with_label_values(&["encode"])
                    .start_timer();
                Bytes::from(data.encode_to_vec())
            };
            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                    body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                        region_id: region_id.as_u64(),
                        schema: schema_bytes,
                        payload,
                    })),
                })),
            };

            let _datanode_handle_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                .with_label_values(&["datanode_handle"])
                .start_timer();
            let datanode = self.node_manager.datanode(&datanode).await;
            return datanode
                .handle(request)
                .await
                .context(error::RequestRegionSnafu)
                .map(|r| r.affected_rows);
        }

        let mut mask_per_datanode = HashMap::with_capacity(region_masks.len());
        for (region_number, mask) in region_masks {
            let region_id = RegionId::new(table_id, region_number);
            let datanode = self
                .partition_manager
                .find_region_leader(region_id)
                .await
                .context(error::FindRegionLeaderSnafu)?;
            mask_per_datanode
                .entry(datanode)
                .or_insert_with(Vec::new)
                .push((region_id, mask));
        }

        let wait_all_datanode_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["wait_all_datanode"])
            .start_timer();

        let mut handles = Vec::with_capacity(mask_per_datanode.len());
        let record_batch_schema =
            Arc::new(Schema::try_from(record_batch.schema()).context(error::ConvertSchemaSnafu)?);

        let mut raw_data_bytes = None;
        for (peer, masks) in mask_per_datanode {
            for (region_id, mask) in masks {
                let rb = record_batch.clone();
                let schema_bytes = schema_bytes.clone();
                let record_batch_schema = record_batch_schema.clone();
                let node_manager = self.node_manager.clone();
                let peer = peer.clone();
                let raw_data = if mask.select_all() {
                    Some(
                        raw_data_bytes
                            .get_or_insert_with(|| Bytes::from(data.encode_to_vec()))
                            .clone(),
                    )
                } else {
                    None
                };
                let handle: common_runtime::JoinHandle<error::Result<api::region::RegionResponse>> =
                    common_runtime::spawn_global(async move {
                        let payload = if mask.select_all() {
                            // SAFETY: raw data must be present, we can avoid re-encoding.
                            raw_data.unwrap()
                        } else {
                            let filter_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                                .with_label_values(&["filter"])
                                .start_timer();
                            let rb = arrow::compute::filter_record_batch(&rb, mask.array())
                                .context(error::ComputeArrowSnafu)?;
                            filter_timer.observe_duration();
                            metrics::BULK_REQUEST_ROWS
                                .with_label_values(&["rows_per_region"])
                                .observe(rb.num_rows() as f64);

                            let encode_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                                .with_label_values(&["encode"])
                                .start_timer();
                            let batch =
                                RecordBatch::try_from_df_record_batch(record_batch_schema, rb)
                                    .context(error::BuildRecordBatchSnafu)?;
                            let payload = Bytes::from(
                                FlightEncoder::default()
                                    .encode(FlightMessage::Recordbatch(batch))
                                    .encode_to_vec(),
                            );
                            encode_timer.observe_duration();
                            payload
                        };
                        let _datanode_handle_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                            .with_label_values(&["datanode_handle"])
                            .start_timer();
                        let request = RegionRequest {
                            header: Some(RegionRequestHeader {
                                tracing_context: TracingContext::from_current_span().to_w3c(),
                                ..Default::default()
                            }),
                            body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                                body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                                    region_id: region_id.as_u64(),
                                    schema: schema_bytes,
                                    payload,
                                })),
                            })),
                        };

                        let datanode = node_manager.datanode(&peer).await;
                        datanode
                            .handle(request)
                            .await
                            .context(error::RequestRegionSnafu)
                    });
                handles.push(handle);
            }
        }

        let region_responses = futures::future::try_join_all(handles)
            .await
            .context(error::JoinTaskSnafu)?;
        wait_all_datanode_timer.observe_duration();
        let mut rows_inserted: usize = 0;
        for res in region_responses {
            rows_inserted += res?.affected_rows;
        }
        Ok(rows_inserted)
    }
}
