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

use ahash::{HashMap, HashMapExt};
use api::v1::ArrowIpc;
use api::v1::flow::{DirtyWindowRequest, DirtyWindowRequests};
use api::v1::region::{
    BulkInsertRequest, RegionRequest, RegionRequestHeader, bulk_insert_request, region_request,
};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use common_base::AffectedRows;
use common_grpc::FlightData;
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_telemetry::error;
use common_telemetry::tracing_context::TracingContext;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;
use table::TableRef;
use table::metadata::TableInfoRef;

use crate::insert::Inserter;
use crate::{error, metrics};

impl Inserter {
    /// Handle bulk insert request.
    pub async fn handle_bulk_insert(
        &self,
        table: TableRef,
        decoder: &mut FlightDecoder,
        data: FlightData,
    ) -> error::Result<AffectedRows> {
        let table_info = table.table_info();
        let table_id = table_info.table_id();
        let db_name = table_info.get_db_string();
        let decode_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["decode_request"])
            .start_timer();
        let body_size = data.data_body.len();
        // Build region server requests
        let message = decoder
            .try_decode(&data)
            .context(error::DecodeFlightDataSnafu)?
            .context(error::NotSupportedSnafu {
                feat: "bulk insert RecordBatch with dictionary arrays",
            })?;
        let FlightMessage::RecordBatch(record_batch) = message else {
            return Ok(0);
        };
        decode_timer.observe_duration();

        if record_batch.num_rows() == 0 {
            return Ok(0);
        }

        // TODO(yingwen): Fill record batch impure default values.
        // notify flownode to update dirty timestamps if flow is configured.
        self.maybe_update_flow_dirty_window(table_info.clone(), record_batch.clone());

        metrics::BULK_REQUEST_MESSAGE_SIZE.observe(body_size as f64);
        metrics::BULK_REQUEST_ROWS
            .with_label_values(&["raw"])
            .observe(record_batch.num_rows() as f64);

        // safety: when reach here schema must be present.
        let schema_bytes = decoder.schema_bytes().unwrap();
        let partition_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["partition"])
            .start_timer();
        let partition_rule = self
            .partition_manager
            .find_table_partition_rule(&table_info)
            .await
            .context(error::InvalidPartitionSnafu)?;

        // find partitions for each row in the record batch
        let region_masks = partition_rule
            .split_record_batch(&record_batch)
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
            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                    region_id: region_id.as_u64(),
                    body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                        schema: schema_bytes,
                        data_header: data.data_header,
                        payload: data.data_body,
                    })),
                })),
            };

            let _datanode_handle_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                .with_label_values(&["datanode_handle"])
                .start_timer();
            let datanode = self.node_manager.datanode(&datanode).await;
            let result = datanode
                .handle(request)
                .await
                .context(error::RequestRegionSnafu)
                .map(|r| r.affected_rows);
            if let Ok(rows) = result {
                crate::metrics::DIST_INGEST_ROW_COUNT
                    .with_label_values(&[db_name.as_str()])
                    .inc_by(rows as u64);
            }
            return result;
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

        // raw daya header and payload bytes.
        let mut raw_data_bytes = None;
        for (peer, masks) in mask_per_datanode {
            for (region_id, mask) in masks {
                if mask.select_none() {
                    continue;
                }
                let rb = record_batch.clone();
                let schema_bytes = schema_bytes.clone();
                let node_manager = self.node_manager.clone();
                let peer = peer.clone();
                let raw_header_and_data = if mask.select_all() {
                    Some(
                        raw_data_bytes
                            .get_or_insert_with(|| {
                                (data.data_header.clone(), data.data_body.clone())
                            })
                            .clone(),
                    )
                } else {
                    None
                };
                let handle: common_runtime::JoinHandle<error::Result<api::region::RegionResponse>> =
                    common_runtime::spawn_global(async move {
                        let (header, payload) = if mask.select_all() {
                            // SAFETY: raw data must be present, we can avoid re-encoding.
                            raw_header_and_data.unwrap()
                        } else {
                            let filter_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                                .with_label_values(&["filter"])
                                .start_timer();
                            let batch = arrow::compute::filter_record_batch(&rb, mask.array())
                                .context(error::ComputeArrowSnafu)?;
                            filter_timer.observe_duration();
                            metrics::BULK_REQUEST_ROWS
                                .with_label_values(&["rows_per_region"])
                                .observe(batch.num_rows() as f64);

                            let encode_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
                                .with_label_values(&["encode"])
                                .start_timer();
                            let mut iter = FlightEncoder::default()
                                .encode(FlightMessage::RecordBatch(batch))
                                .into_iter();
                            let Some(flight_data) = iter.next() else {
                                // Safety: `iter` on a type of `Vec1`, which is guaranteed to have
                                // at least one element.
                                unreachable!()
                            };
                            ensure!(
                                iter.next().is_none(),
                                error::NotSupportedSnafu {
                                    feat: "bulk insert RecordBatch with dictionary arrays",
                                }
                            );
                            encode_timer.observe_duration();
                            (flight_data.data_header, flight_data.data_body)
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
                                region_id: region_id.as_u64(),
                                body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                                    schema: schema_bytes,
                                    data_header: header,
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
        crate::metrics::DIST_INGEST_ROW_COUNT
            .with_label_values(&[db_name.as_str()])
            .inc_by(rows_inserted as u64);
        Ok(rows_inserted)
    }

    fn maybe_update_flow_dirty_window(&self, table_info: TableInfoRef, record_batch: RecordBatch) {
        let table_id = table_info.table_id();
        let table_flownode_set_cache = self.table_flownode_set_cache.clone();
        let node_manager = self.node_manager.clone();
        common_runtime::spawn_global(async move {
            let result = table_flownode_set_cache
                .get(table_id)
                .await
                .context(error::RequestInsertsSnafu);
            let flownodes = match result {
                Ok(flownodes) => flownodes.unwrap_or_default(),
                Err(e) => {
                    error!(e; "Failed to get flownodes for table id: {}", table_id);
                    return;
                }
            };

            let peers: HashSet<_> = flownodes.values().cloned().collect();
            if peers.is_empty() {
                return;
            }

            let Ok(timestamps) = extract_timestamps(
                &record_batch,
                &table_info
                    .meta
                    .schema
                    .timestamp_column()
                    .as_ref()
                    .unwrap()
                    .name,
            )
            .inspect_err(|e| {
                error!(e; "Failed to extract timestamps from record batch");
            }) else {
                return;
            };

            for peer in peers {
                let node_manager = node_manager.clone();
                let timestamps = timestamps.clone();
                common_runtime::spawn_global(async move {
                    if let Err(e) = node_manager
                        .flownode(&peer)
                        .await
                        .handle_mark_window_dirty(DirtyWindowRequests {
                            requests: vec![DirtyWindowRequest {
                                table_id,
                                timestamps,
                            }],
                        })
                        .await
                        .context(error::RequestInsertsSnafu)
                    {
                        error!(e; "Failed to mark timestamps as dirty, table: {}", table_id);
                    }
                });
            }
        });
    }
}

/// Calculate the timestamp range of record batch. Return `None` if record batch is empty.
fn extract_timestamps(rb: &RecordBatch, timestamp_index_name: &str) -> error::Result<Vec<i64>> {
    let ts_col = rb
        .column_by_name(timestamp_index_name)
        .context(error::ColumnNotFoundSnafu {
            msg: timestamp_index_name,
        })?;
    if rb.num_rows() == 0 {
        return Ok(vec![]);
    }
    let (primitive, _) =
        datatypes::timestamp::timestamp_array_to_primitive(ts_col).with_context(|| {
            error::InvalidTimeIndexTypeSnafu {
                ty: ts_col.data_type().clone(),
            }
        })?;
    Ok(primitive.iter().flatten().collect())
}
