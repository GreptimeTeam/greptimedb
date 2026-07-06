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
use api::v1::flow::{DirtyWindowRequest, DirtyWindowRequests};
use api::v1::region::{
    BulkInsertRequest, RegionRequest, RegionRequestHeader, bulk_insert_request, region_request,
};
use api::v1::{ArrowIpc, PartitionExprVersion};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common_base::AffectedRows;
use common_grpc::FlightData;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
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
        raw_flight_data: FlightData,
        record_batch: RecordBatch,
        schema_bytes: Bytes,
    ) -> error::Result<AffectedRows> {
        let table_info = table.table_info();
        let table_id = table_info.table_id();
        let db_name = table_info.get_db_string();

        if record_batch.num_rows() == 0 {
            return Ok(0);
        }

        let body_size = raw_flight_data.data_body.len();

        // Precompute the flow dirty-window notification before any source write
        // so cache/timestamp errors fail before commit.
        let dirty_task =
            FlowDirtyWindowTask::new(&self.table_flownode_set_cache, &table_info, &record_batch)
                .await?;

        metrics::BULK_REQUEST_MESSAGE_SIZE.observe(body_size as f64);
        metrics::BULK_REQUEST_ROWS
            .with_label_values(&["raw"])
            .observe(record_batch.num_rows() as f64);

        let partition_timer = metrics::HANDLE_BULK_INSERT_ELAPSED
            .with_label_values(&["partition"])
            .start_timer();
        let (partition_rule, partition_versions) = self
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
            let partition_expr_version = partition_versions
                .get(&region_number)
                .copied()
                .unwrap_or_default();
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
                    partition_expr_version: partition_expr_version
                        .map(|value| PartitionExprVersion { value }),
                    aligned_schema_version: None,
                    body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                        schema: schema_bytes.clone(),
                        data_header: raw_flight_data.data_header,
                        payload: raw_flight_data.data_body,
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
                dirty_task.detach(self.node_manager.clone());
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

        for (peer, masks) in mask_per_datanode {
            for (region_id, mask) in masks {
                if mask.select_none() {
                    continue;
                }
                let partition_expr_version = partition_versions
                    .get(&region_id.region_number())
                    .copied()
                    .unwrap_or_default();
                let rb = record_batch.clone();
                let schema_bytes = schema_bytes.clone();
                let node_manager = self.node_manager.clone();
                let peer = peer.clone();
                let raw_header_and_data = if mask.select_all() {
                    Some((
                        raw_flight_data.data_header.clone(),
                        raw_flight_data.data_body.clone(),
                    ))
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
                                partition_expr_version: partition_expr_version
                                    .map(|value| PartitionExprVersion { value }),
                                aligned_schema_version: None,
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

        let region_responses = futures::future::join_all(handles).await;
        wait_all_datanode_timer.observe_duration();
        let mut rows_inserted: usize = 0;
        let mut any_success = false;
        let mut first_error = None;
        for handle_result in region_responses {
            match handle_result {
                Ok(Ok(ref resp)) => {
                    rows_inserted += resp.affected_rows;
                    any_success = true;
                }
                Ok(Err(e)) => {
                    first_error = first_error.or(Some(e));
                }
                Err(join_err) => {
                    // Collect join errors instead of early-returning,
                    // so we still check any_success and dispatch dirty below.
                    first_error = first_error.or(Some(
                        Err::<(), common_runtime::JoinError>(join_err)
                            .context(error::JoinTaskSnafu)
                            .unwrap_err(),
                    ));
                }
            }
        }

        if any_success {
            dirty_task.detach(self.node_manager.clone());
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        crate::metrics::DIST_INGEST_ROW_COUNT
            .with_label_values(&[db_name.as_str()])
            .inc_by(rows_inserted as u64);
        Ok(rows_inserted)
    }
}

/// Precompute the flow dirty-window notification before any source write.
/// `new()` extracts timestamps and resolves flownode peers so cache/timestamp
/// failures happen before commit. `detach()` does fire-and-forget best-effort
/// dispatch only after the relevant success boundary.
struct FlowDirtyWindowTask {
    table_id: u32,
    timestamps: Vec<i64>,
    peers: Vec<Peer>,
}

impl FlowDirtyWindowTask {
    async fn new(
        cache: &TableFlownodeSetCacheRef,
        table_info: &TableInfoRef,
        record_batch: &RecordBatch,
    ) -> error::Result<Self> {
        let table_id = table_info.table_id();
        let flownodes = cache
            .get(table_id)
            .await
            .context(error::RequestInsertsSnafu)?
            .unwrap_or_default();
        let peers: Vec<Peer> = flownodes
            .values()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let timestamps = if peers.is_empty() {
            vec![]
        } else {
            extract_timestamps(
                record_batch,
                table_info
                    .meta
                    .schema
                    .timestamp_column()
                    .as_ref()
                    .unwrap()
                    .name
                    .as_str(),
            )?
        };

        Ok(Self {
            table_id,
            timestamps,
            peers,
        })
    }

    fn detach(self, node_manager: NodeManagerRef) {
        if self.peers.is_empty() || self.timestamps.is_empty() {
            return;
        }
        for peer in self.peers {
            let timestamps = self.timestamps.clone();
            let node_manager = node_manager.clone();
            let table_id = self.table_id;
            common_runtime::spawn_global(async move {
                let result = node_manager
                    .flownode(&peer)
                    .await
                    .handle_mark_window_dirty(DirtyWindowRequests {
                        requests: vec![DirtyWindowRequest {
                            table_id,
                            timestamps,
                        }],
                    })
                    .await;
                if let Err(e) = result {
                    error!(e; "Failed to mark timestamps as dirty, table: {}", table_id);
                }
            });
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::flow::FlowResponse;
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::cache::new_table_flownode_set_cache;
    use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
    use common_meta::instruction::{CacheIdent, CreateFlow};
    use common_meta::node_manager::{DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef};
    use common_meta::peer::Peer;
    use common_meta::test_util::MockDatanodeManager;
    use moka::future::Cache;
    use tokio::sync::mpsc;

    use super::*;
    use crate::tests::prepare_mocked_backend;

    /// Combined mock for DatanodeManager + FlownodeManager (same as in insert.rs tests).
    struct CombinedNodeManager<D: DatanodeManager, F: FlownodeManager> {
        datanode_mgr: D,
        flownode_mgr: F,
    }

    #[async_trait::async_trait]
    impl<D: DatanodeManager + Send + Sync, F: FlownodeManager + Send + Sync> DatanodeManager
        for CombinedNodeManager<D, F>
    {
        async fn datanode(&self, peer: &Peer) -> DatanodeRef {
            self.datanode_mgr.datanode(peer).await
        }
    }

    #[async_trait::async_trait]
    impl<D: DatanodeManager + Send + Sync, F: FlownodeManager + Send + Sync> FlownodeManager
        for CombinedNodeManager<D, F>
    {
        async fn flownode(&self, peer: &Peer) -> FlownodeRef {
            self.flownode_mgr.flownode(peer).await
        }
    }

    /// A `MockFlownodeHandler` that records flownode calls through a channel.
    #[derive(Clone)]
    struct RecordingFlownodeHandler {
        tx: mpsc::UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl common_meta::test_util::MockFlownodeHandler for RecordingFlownodeHandler {
        async fn handle_inserts(
            &self,
            _peer: &Peer,
            _requests: api::v1::region::InsertRequests,
        ) -> common_meta::error::Result<FlowResponse> {
            let _ = self.tx.send(());
            Ok(FlowResponse::default())
        }

        async fn handle_mark_window_dirty(
            &self,
            _peer: &Peer,
            _req: api::v1::flow::DirtyWindowRequests,
        ) -> common_meta::error::Result<FlowResponse> {
            let _ = self.tx.send(());
            Ok(FlowResponse::default())
        }
    }

    /// Populates the flownode set cache so that `table_id` resolves to `peers`.
    async fn populate_flownode_cache(
        cache: &TableFlownodeSetCacheRef,
        table_id: u32,
        peers: Vec<Peer>,
    ) {
        let ident = vec![CacheIdent::CreateFlow(CreateFlow {
            flow_id: 1,
            source_table_ids: vec![table_id],
            partition_to_peer_mapping: peers
                .into_iter()
                .enumerate()
                .map(|(i, p)| (i as u32, p))
                .collect(),
        })];
        cache.invalidate(&ident).await.unwrap();
    }

    /// Create a `RecordBatch` with a single timestamp column and a value column.
    fn make_record_batch(ts_millis: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, true),
        ]));
        let ts_array = Arc::new(TimestampMillisecondArray::from(
            ts_millis.iter().map(|&ts| Some(ts)).collect::<Vec<_>>(),
        ));
        let val_array = Arc::new(Int64Array::from(vec![42i64; ts_millis.len()]));
        RecordBatch::try_new(schema, vec![ts_array, val_array]).unwrap()
    }

    /// Create a `TableInfoRef` with a timestamp column named "ts".
    fn make_table_info_ref(table_id: u32) -> TableInfoRef {
        let col_schemas = vec![
            datatypes::schema::ColumnSchema::new(
                "ts",
                datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            datatypes::schema::ColumnSchema::new(
                "val",
                datatypes::data_type::ConcreteDataType::int64_datatype(),
                true,
            ),
        ];
        let schema = Arc::new(
            datatypes::schema::SchemaBuilder::try_from(col_schemas)
                .unwrap()
                .build()
                .unwrap(),
        );
        let meta = table::metadata::TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![])
            .value_indices(vec![1])
            .engine("mito")
            .next_column_id(2)
            .options(Default::default())
            .created_on(Default::default())
            .build()
            .unwrap();
        Arc::new(
            table::metadata::TableInfoBuilder::default()
                .table_id(table_id)
                .table_version(0)
                .name("test_table")
                .schema_name(DEFAULT_SCHEMA_NAME)
                .catalog_name(DEFAULT_CATALOG_NAME)
                .desc(None)
                .table_type(table::metadata::TableType::Base)
                .meta(meta)
                .build()
                .unwrap(),
        )
    }

    // --- Flow dirty-window ordering tests ---

    /// Verifies that `FlowDirtyWindowTask::detach` sends dirty-window requests
    /// to the flownode via the node manager.
    #[tokio::test]
    async fn test_flow_dirty_window_task_detach_sends_dirty() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let kv_backend = prepare_mocked_backend().await;
        let cache = Arc::new(new_table_flownode_set_cache(
            String::new(),
            Cache::new(100),
            kv_backend,
        ));
        // Populate cache so table 1 has flownode peer (id=1)
        populate_flownode_cache(&cache, 1, vec![Peer::empty(1)]).await;

        let flownode_mgr =
            common_meta::test_util::MockFlownodeManager::new(RecordingFlownodeHandler {
                tx: tx.clone(),
            });
        let datanode_mgr = MockDatanodeManager::new(NaiveDatanodeHandler);
        let node_manager: NodeManagerRef = Arc::new(CombinedNodeManager {
            datanode_mgr,
            flownode_mgr,
        });

        let record_batch = make_record_batch(&[1000, 2000, 3000]);
        let table_info = make_table_info_ref(1);

        // Precompute the task — should extract timestamps and find flownode peers
        let task = FlowDirtyWindowTask::new(&cache, &table_info, &record_batch)
            .await
            .unwrap();

        assert!(!task.timestamps.is_empty(), "should extract timestamps");
        assert!(!task.peers.is_empty(), "should find flownode peers");

        // Detach should spawn flownode calls
        task.detach(node_manager);

        // Wait for the spawned task to actually send
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("flownode handle_mark_window_dirty should be called after detach");
    }

    /// Verifies that `FlowDirtyWindowTask` with no flownode peers is a no-op on detach.
    #[tokio::test]
    async fn test_flow_dirty_window_task_no_peers_noop() {
        let kv_backend = prepare_mocked_backend().await;
        let cache = Arc::new(new_table_flownode_set_cache(
            String::new(),
            Cache::new(100),
            kv_backend,
        ));
        // Don't populate cache — no flownode peers

        let record_batch = make_record_batch(&[1000, 2000]);
        let table_info = make_table_info_ref(1);

        let task = FlowDirtyWindowTask::new(&cache, &table_info, &record_batch)
            .await
            .unwrap();

        // No peers found — timestamps should be empty
        assert!(task.timestamps.is_empty());
        assert!(task.peers.is_empty());

        // Detach should be a no-op (no flownode calls spawned)
        // We use MockDatanodeManager (flownode() is unimplemented!) but
        // detach returns early when peers is empty, so it's safe.
        let datanode_mgr = MockDatanodeManager::new(NaiveDatanodeHandler);
        task.detach(Arc::new(datanode_mgr));
    }

    /// Verifies that `extract_timestamps` correctly extracts all timestamp
    /// values from a record batch.
    #[test]
    fn test_extract_timestamps() {
        let rb = make_record_batch(&[1000, 2000, 3000, 1000]);
        let timestamps = extract_timestamps(&rb, "ts").unwrap();
        assert_eq!(timestamps, vec![1000, 2000, 3000, 1000]);
    }

    /// Verifies that `extract_timestamps` returns an empty vec for an empty batch.
    #[test]
    fn test_extract_timestamps_empty() {
        let rb = make_record_batch(&[]);
        let timestamps = extract_timestamps(&rb, "ts").unwrap();
        assert!(timestamps.is_empty());
    }

    // ── Note on bulk insert ordering guarantees ──
    //
    // Full `handle_bulk_insert` ordering tests (blocking datanode, asserting
    // no dirty before release, multi-region partial failure) require heavy
    // integration setup: TableRef, FlightData, partition manager with real
    // region routes, etc. The structural guarantees are enforced by the code:
    //
    // - Single-region: dirty_task.detach() is only called inside
    //   `if let Ok(rows) = result { ... }`, after datanode write completes.
    // - Multi-region: dirty_task.detach() is only called after
    //   `join_all(handles).await` completes and `any_success` is computed.
    //   Join errors are collected (not early-returned) before dirty dispatch.
    //
    // The isolated `FlowDirtyWindowTask` tests above verify that:
    // - `new()` extracts timestamps and resolves flownode peers (precompute)
    // - `detach()` spawns flownode dirty-window calls (best-effort dispatch)
}
