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
    RegionSelection,
};
use common_base::AffectedRows;
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_grpc::FlightData;
use prost::Message;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error;
use crate::insert::Inserter;

impl Inserter {
    /// Handle bulk insert requests.
    pub async fn handle_bulk_inserts(
        &self,
        table_id: TableId,
        decoder: &mut FlightDecoder,
        data: FlightData,
    ) -> error::Result<AffectedRows> {
        // Build region server requests
        let message = decoder
            .try_decode(data)
            .context(error::DecodeFlightDataSnafu)?;
        let FlightMessage::Recordbatch(rb) = message else {
            return Ok(0);
        };

        let data = FlightEncoder::default()
            .encode(FlightMessage::Schema(decoder.schema().unwrap().clone()));
        // todo(hl): find a way to embed raw FlightData messages in greptimedb proto files so we don't have to encode here.
        let schema_data = data.encode_to_vec();
        let flight_data = data.encode_to_vec();
        let record_batch = rb.df_record_batch();

        let partition_rule = self
            .partition_manager
            .find_table_partition_rule(table_id)
            .await
            .context(error::InvalidPartitionSnafu)?;

        let mut handles = Vec::new();
        // find partitions for each row in the record batch
        let region_masks = partition_rule.split_record_batch(record_batch).unwrap();

        let mut mask_per_datanode = HashMap::with_capacity(region_masks.len());
        for (region_number, mask) in region_masks {
            let region_id = RegionId::new(table_id, region_number);
            let datanode = self
                .partition_manager
                .find_region_leader(region_id)
                .await
                .unwrap();
            let selection = RegionSelection {
                region_id: region_id.as_u64(),
                selection: mask.values().inner().as_slice().to_vec(),
            };
            mask_per_datanode
                .entry(datanode)
                .or_insert_with(Vec::new)
                .push(selection);
        }

        for (peer, masks) in mask_per_datanode {
            let node_manager = self.node_manager.clone();
            let schema = schema_data.clone();
            let payload = flight_data.clone();

            let handle: common_runtime::JoinHandle<error::Result<api::region::RegionResponse>> =
                common_runtime::spawn_global(async move {
                    let request = RegionRequest {
                        header: Default::default(),
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
        let mut rows_inserted: usize = 0;
        for res in region_responses {
            rows_inserted += res?.affected_rows;
        }
        Ok(rows_inserted)
    }
}
