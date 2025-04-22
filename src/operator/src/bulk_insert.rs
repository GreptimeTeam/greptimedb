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

use ahash::{HashMap, HashMapExt};
use api::v1::region::{
    bulk_insert_request, region_request, ArrowIpc, BulkInsertRequest, RegionRequest,
    RegionRequestHeader, RegionSelection,
};
use bytes::Bytes;
use common_base::AffectedRows;
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_grpc::FlightData;
use common_telemetry::tracing_context::TracingContext;
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
        let raw_flight_data = Bytes::from(data.encode_to_vec());
        let body_size = data.data_body.len();
        // Build region server requests
        let message = decoder
            .try_decode(data)
            .context(error::DecodeFlightDataSnafu)?;
        let FlightMessage::Recordbatch(rb) = message else {
            return Ok(0);
        };
        metrics::BULK_REQUEST_MESSAGE_SIZE.observe(body_size as f64);
        decode_timer.observe_duration();

        // todo(hl): find a way to embed raw FlightData messages in greptimedb proto files so we don't have to encode here.
        // safety: when reach here schema must be present.
        let schema_message = FlightEncoder::default()
            .encode(FlightMessage::Schema(decoder.schema().unwrap().clone()));
        let schema_data = Bytes::from(schema_message.encode_to_vec());

        let record_batch = rb.df_record_batch();

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

        let group_request_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["group_request"])
            .start_timer();

        let mut mask_per_datanode = HashMap::with_capacity(region_masks.len());
        for (region_number, mask) in region_masks {
            let region_id = RegionId::new(table_id, region_number);
            let datanode = self
                .partition_manager
                .find_region_leader(region_id)
                .await
                .context(error::FindRegionLeaderSnafu)?;
            let selection = RegionSelection {
                region_id: region_id.as_u64(),
                selection: mask.values().inner().as_slice().to_vec(),
            };
            mask_per_datanode
                .entry(datanode)
                .or_insert_with(Vec::new)
                .push(selection);
        }
        group_request_timer.observe_duration();

        let datanode_handle_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["datanode_handle"])
            .start_timer();
        // fast path: only one datanode
        if mask_per_datanode.len() == 1 {
            let (peer, requests) = mask_per_datanode.into_iter().next().unwrap();
            let datanode = self.node_manager.datanode(&peer).await;
            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                    body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                        schema: schema_data,
                        payload: raw_flight_data,
                        region_selection: requests,
                    })),
                })),
            };
            let response = datanode
                .handle(request)
                .await
                .context(error::RequestRegionSnafu)?;
            return Ok(response.affected_rows);
        }

        let mut handles = Vec::with_capacity(mask_per_datanode.len());
        for (peer, masks) in mask_per_datanode {
            let node_manager = self.node_manager.clone();
            let schema = schema_data.clone();
            let payload = raw_flight_data.clone();

            let handle: common_runtime::JoinHandle<error::Result<api::region::RegionResponse>> =
                common_runtime::spawn_global(async move {
                    let request = RegionRequest {
                        header: Some(RegionRequestHeader {
                            tracing_context: TracingContext::from_current_span().to_w3c(),
                            ..Default::default()
                        }),
                        body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                            body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                                schema,
                                payload,
                                region_selection: masks,
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

        let region_responses = futures::future::try_join_all(handles)
            .await
            .context(error::JoinTaskSnafu)?;
        datanode_handle_timer.observe_duration();
        let mut rows_inserted: usize = 0;
        for res in region_responses {
            rows_inserted += res?.affected_rows;
        }
        Ok(rows_inserted)
    }
}
