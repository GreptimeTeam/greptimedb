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

use api::helper::to_pb_time_unit;
use api::v1::region::region_request::Body as RegionRequestBody;
use api::v1::region::{
    BuildIndexRequest, CancelCompactionRequest, CompactRequest, CompactionTimeRange, FlushRequest,
    PendingTails, RegionRequestHeader, TruncateRequest, Unflushed, build_index_request,
    truncate_request,
};
use catalog::CatalogManagerRef;
use common_catalog::build_db_string;
use common_catalog::consts::METRIC_ENGINE;
use common_meta::node_manager::{AffectedRows, NodeManagerRef};
use common_meta::peer::Peer;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, error, info, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit as TimestampUnit;
use futures_util::future;
use partition::cache::PhysicalPartitionInfo;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::prelude::*;
use store_api::metric_engine_consts;
use store_api::storage::RegionId;
use table::requests::{BuildIndexTableRequest, CompactTableRequest, FlushTableRequest};
use table::table_name::TableName;

use crate::error::{
    CatalogSnafu, FindRegionLeaderSnafu, FindTablePartitionRuleSnafu, JoinTaskSnafu,
    NotSupportedSnafu, RequestRegionSnafu, Result, TableNotFoundSnafu,
    UnsupportedRegionRequestSnafu,
};
use crate::region_req_factory::RegionRequestFactory;

/// Region requester which processes flush, compact requests etc.
pub struct Requester {
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
}

pub type RequesterRef = Arc<Requester>;

impl Requester {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            node_manager,
        }
    }

    /// Handle the request to flush table.
    pub async fn handle_table_flush(
        &self,
        request: FlushTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let partitions = &self
            .get_table_partition_info(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )
            .await?
            .partitions;

        let requests = partitions
            .iter()
            .map(|partition| {
                RegionRequestBody::Flush(FlushRequest {
                    region_id: partition.id.into(),
                })
            })
            .collect();

        info!("Handle table manual flush request: {:?}", request);

        self.do_request(
            requests,
            Some(build_db_string(&request.catalog_name, &request.schema_name)),
            &ctx,
        )
        .await
    }

    /// Handle the request to build index for table.
    pub async fn handle_table_build_index(
        &self,
        request: BuildIndexTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        if matches!(
            request.options,
            Some(build_index_request::Options::SeriesIndex(_))
        ) {
            let table = self
                .catalog_manager
                .table(
                    &request.catalog_name,
                    &request.schema_name,
                    &request.table_name,
                    None,
                )
                .await
                .context(CatalogSnafu)?;
            let table = table.with_context(|| TableNotFoundSnafu {
                table_name: common_catalog::format_full_table_name(
                    &request.catalog_name,
                    &request.schema_name,
                    &request.table_name,
                ),
            })?;
            let info = table.table_info();
            ensure_build_series_index_supported(&info.meta.engine, info.is_physical_table())?;
        }
        let partitions = &self
            .get_table_partition_info(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )
            .await?
            .partitions;

        let requests = partitions
            .iter()
            .map(|partition| {
                RegionRequestBody::BuildIndex(BuildIndexRequest {
                    region_id: partition.id.into(),
                    options: request.options,
                })
            })
            .collect();

        info!(
            "Handle table manual build index for table {}",
            request.table_name
        );
        debug!("Request details: {:?}", request);

        self.do_request(
            requests,
            Some(build_db_string(&request.catalog_name, &request.schema_name)),
            &ctx,
        )
        .await
    }

    /// Handle the request to compact table.
    pub async fn handle_table_compaction(
        &self,
        request: CompactTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let partitions = &self
            .get_table_partition_info(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )
            .await?
            .partitions;

        let time_range = request
            .time_range
            .map(to_pb_compaction_time_range)
            .transpose()?;
        let requests = partitions
            .iter()
            .map(|partition| {
                RegionRequestBody::Compact(CompactRequest {
                    region_id: partition.id.into(),
                    parallelism: request.parallelism,
                    options: Some(request.compact_options),
                    time_range,
                })
            })
            .collect();

        info!("Handle table manual compaction request: {:?}", request);

        self.do_request(
            requests,
            Some(build_db_string(&request.catalog_name, &request.schema_name)),
            &ctx,
        )
        .await
    }

    /// Handle the request to flush the region.
    pub async fn handle_region_flush(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let request = RegionRequestBody::Flush(FlushRequest {
            region_id: region_id.into(),
        });

        info!("Handle region manual flush request: {region_id}");
        self.do_request(vec![request], None, &ctx).await
    }

    /// Handle the request to compact the region.
    pub async fn handle_region_compaction(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let request = RegionRequestBody::Compact(CompactRequest {
            region_id: region_id.into(),
            parallelism: 1,
            options: None, // todo(hl): maybe also support parameters in region compaction.
            time_range: None,
        });

        info!("Handle region manual compaction request: {region_id}");
        self.do_request(vec![request], None, &ctx).await
    }

    /// Cancels one identified compaction task through the region leader.
    pub async fn handle_cancel_region_compaction(
        &self,
        region_id: RegionId,
        task_id: String,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        ensure!(
            region_id.region_group() != metric_engine_consts::METRIC_METADATA_REGION_GROUP,
            NotSupportedSnafu {
                feat: "cancelling compaction for an internal metric metadata region",
            }
        );

        let request = RegionRequestBody::CancelCompaction(CancelCompactionRequest {
            region_id: region_id.into(),
            task_id,
        });
        self.do_request(vec![request], None, &ctx).await
    }

    /// Discard all unflushed data from the region.
    pub async fn handle_discard_unflushed_data(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let request = RegionRequestBody::Truncate(TruncateRequest {
            region_id: region_id.into(),
            kind: Some(truncate_request::Kind::Unflushed(Unflushed {})),
        });

        info!("Handle region discard unflushed data request: {region_id}");
        self.do_request(vec![request], None, &ctx).await
    }

    /// Discard all unflushed data from all regions of the table.
    pub async fn handle_discard_unflushed_data_by_table(
        &self,
        table_name: TableName,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let requests = self
            .table_truncate_requests(&table_name, truncate_request::Kind::Unflushed(Unflushed {}))
            .await?;

        info!("Handle table discard unflushed data request: {table_name}");
        self.do_request(
            requests
                .into_iter()
                .map(RegionRequestBody::Truncate)
                .collect(),
            Some(build_db_string(
                &table_name.catalog_name,
                &table_name.schema_name,
            )),
            &ctx,
        )
        .await
    }

    /// Discards the region's pending WAL tails that datanode `node_id` wrote.
    pub(crate) async fn handle_discard_pending_tails(
        &self,
        region_id: RegionId,
        node_id: u64,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let request = RegionRequestBody::Truncate(TruncateRequest {
            region_id: region_id.into(),
            kind: Some(truncate_request::Kind::PendingTails(PendingTails {
                node_id,
            })),
        });

        info!(
            user = ctx.current_user().username(),
            "Handle region discard pending tails request: {region_id}, node: {node_id}"
        );
        let discarded = self.do_request(vec![request], None, &ctx).await?;
        if discarded == 0 {
            info!("No pending tails from node {node_id} in region {region_id}");
        }
        Ok(discarded)
    }

    /// Discards the pending WAL tails that datanode `node_id` wrote in every region of the
    /// table. Each failed region is logged with its error, and the first error is returned
    /// after a warning with the number of tails the regions that succeeded discarded.
    pub(crate) async fn handle_discard_pending_tails_by_table(
        &self,
        table_name: TableName,
        node_id: u64,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let requests = self
            .table_truncate_requests(
                &table_name,
                truncate_request::Kind::PendingTails(PendingTails { node_id }),
            )
            .await?;
        let region_ids = requests
            .iter()
            .map(|request| RegionId::from_u64(request.region_id))
            .collect::<Vec<_>>();

        info!(
            user = ctx.current_user().username(),
            "Handle table discard pending tails request: {table_name}, node: {node_id}"
        );
        let results = self
            .do_requests(
                requests
                    .into_iter()
                    .map(RegionRequestBody::Truncate)
                    .collect(),
                Some(build_db_string(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                )),
                &ctx,
            )
            .await?;
        let discarded = results.iter().flatten().sum::<AffectedRows>();
        let mut first_error = None;
        for (region_id, result) in region_ids.into_iter().zip(results) {
            if let Err(e) = result {
                error!(e; "Failed to discard the pending tails from node {node_id} in region {region_id} of table {table_name}");
                first_error.get_or_insert(e);
            }
        }
        if let Some(e) = first_error {
            warn!(
                "Discarded {discarded} pending tails from node {node_id} in the regions of table {table_name} that succeeded"
            );
            return Err(e);
        }
        if discarded == 0 {
            info!("No pending tails from node {node_id} in table {table_name}");
        }
        Ok(discarded)
    }

    /// Returns a truncate request of `kind` for every region of the table. A Metric Engine
    /// logical table is rejected as not supported.
    async fn table_truncate_requests(
        &self,
        table_name: &TableName,
        kind: truncate_request::Kind,
    ) -> Result<Vec<TruncateRequest>> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                None,
            )
            .await
            .context(CatalogSnafu)?;
        let table = table.with_context(|| TableNotFoundSnafu {
            table_name: table_name.to_string(),
        })?;
        let table_info = table.table_info();
        ensure_discard_supported(
            &table_info.meta.engine,
            table_info.is_physical_table(),
            &kind,
        )?;

        let partitions = &self
            .partition_manager
            .find_physical_partition_info(table_info.ident.table_id)
            .await
            .with_context(|_| FindTablePartitionRuleSnafu {
                table_name: table_name.to_string(),
            })?
            .partitions;
        Ok(partitions
            .iter()
            .map(|partition| TruncateRequest {
                region_id: partition.id.into(),
                kind: Some(kind.clone()),
            })
            .collect())
    }
}

fn to_pb_compaction_time_range(range: TimestampRange) -> Result<CompactionTimeRange> {
    let (Some(start), Some(end)) = (*range.start(), *range.end()) else {
        return crate::error::InvalidTimestampRangeSnafu {
            start: format!("{:?}", range.start()),
            end: format!("{:?}", range.end()),
        }
        .fail();
    };
    let original_start = start;
    let original_end = end;
    let start = start.convert_to(TimestampUnit::Second).with_context(|| {
        crate::error::InvalidTimestampRangeSnafu {
            start: format!("{original_start:?}"),
            end: format!("{original_end:?}"),
        }
    })?;
    let end = end
        .convert_to_ceil(TimestampUnit::Second)
        .with_context(|| crate::error::InvalidTimestampRangeSnafu {
            start: format!("{original_start:?}"),
            end: format!("{original_end:?}"),
        })?;
    ensure!(
        start < end,
        crate::error::InvalidTimestampRangeSnafu {
            start: format!("{original_start:?}"),
            end: format!("{original_end:?}"),
        }
    );

    Ok(CompactionTimeRange {
        start: start.value(),
        end: end.value(),
        time_unit: to_pb_time_unit(TimestampUnit::Second) as i32,
    })
}

impl Requester {
    async fn do_request(
        &self,
        requests: Vec<RegionRequestBody>,
        db_string: Option<String>,
        ctx: &QueryContextRef,
    ) -> Result<AffectedRows> {
        self.do_requests(requests, db_string, ctx)
            .await?
            .into_iter()
            .sum()
    }

    /// Sends `requests` to their region leaders and returns the result of each.
    async fn do_requests(
        &self,
        requests: Vec<RegionRequestBody>,
        db_string: Option<String>,
        ctx: &QueryContextRef,
    ) -> Result<Vec<Result<AffectedRows>>> {
        let request_factory = RegionRequestFactory::new(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            dbname: db_string.unwrap_or_else(|| ctx.get_db_string()),
            ..Default::default()
        });

        let tasks = requests.into_iter().map(|req_body| {
            let request = request_factory.build_request(req_body.clone());
            let partition_manager = self.partition_manager.clone();
            let node_manager = self.node_manager.clone();
            common_runtime::spawn_global(async move {
                let peer =
                    Self::find_region_leader_by_request(partition_manager, &req_body).await?;
                node_manager
                    .datanode(&peer)
                    .await
                    .handle(request)
                    .await
                    .context(RequestRegionSnafu)
            })
        });
        let results = future::try_join_all(tasks).await.context(JoinTaskSnafu)?;

        Ok(results
            .into_iter()
            .map(|resp| resp.map(|r| r.affected_rows))
            .collect())
    }

    async fn find_region_leader_by_request(
        partition_manager: PartitionRuleManagerRef,
        req: &RegionRequestBody,
    ) -> Result<Peer> {
        let region_id = match req {
            RegionRequestBody::Flush(req) => req.region_id,
            RegionRequestBody::Compact(req) => req.region_id,
            RegionRequestBody::CancelCompaction(req) => req.region_id,
            RegionRequestBody::BuildIndex(req) => req.region_id,
            RegionRequestBody::Truncate(req) => req.region_id,
            _ => {
                error!("Unsupported region request: {:?}", req);
                return UnsupportedRegionRequestSnafu {}.fail();
            }
        };

        partition_manager
            .find_region_leader(region_id.into())
            .await
            .context(FindRegionLeaderSnafu)
    }

    async fn get_table_partition_info(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Arc<PhysicalPartitionInfo>> {
        let table = self
            .catalog_manager
            .table(catalog, schema, table_name, None)
            .await
            .context(CatalogSnafu)?;

        let table = table.with_context(|| TableNotFoundSnafu {
            table_name: common_catalog::format_full_table_name(catalog, schema, table_name),
        })?;
        let table_info = table.table_info();

        self.partition_manager
            .find_physical_partition_info(table_info.ident.table_id)
            .await
            .with_context(|_| FindTablePartitionRuleSnafu {
                table_name: common_catalog::format_full_table_name(catalog, schema, table_name),
            })
    }
}

fn ensure_build_series_index_supported(engine: &str, is_physical_table: bool) -> Result<()> {
    ensure!(
        engine == METRIC_ENGINE && is_physical_table,
        NotSupportedSnafu {
            feat: "building series indexes requires a physical metric table",
        }
    );
    Ok(())
}

fn ensure_discard_supported(
    engine: &str,
    is_physical_table: bool,
    kind: &truncate_request::Kind,
) -> Result<()> {
    let feat = match kind {
        truncate_request::Kind::Unflushed(_) => {
            "discarding unflushed data from a Metric Engine logical table"
        }
        truncate_request::Kind::PendingTails(_) => {
            "discarding pending WAL tails from a Metric Engine logical table"
        }
        truncate_request::Kind::All(_) | truncate_request::Kind::TimeRanges(_) => {
            "truncating a Metric Engine logical table"
        }
    };
    ensure!(
        engine != METRIC_ENGINE || is_physical_table,
        NotSupportedSnafu { feat }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use api::region::RegionResponse;
    use api::v1::TimeUnit;
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_meta::ddl::test_util::datanode_handler::DatanodeWatcher;
    use common_meta::test_util::MockDatanodeManager;
    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use session::context::QueryContext;

    use super::*;
    use crate::test_util;

    #[tokio::test]
    async fn test_cancel_region_compaction_routes_to_region_leader() {
        let backend = test_util::prepare_mocked_backend().await;
        let partition_manager = test_util::create_partition_rule_manager(backend).await;
        let (sender, mut requests) = tokio::sync::mpsc::channel(4);
        let watcher = DatanodeWatcher::new(sender).with_handler(|_, request| {
            let Some(RegionRequestBody::CancelCompaction(request)) = request.body else {
                panic!("expected cancellation through Handle");
            };
            Ok(RegionResponse::new(usize::from(
                request.task_id == "live-cycle",
            )))
        });
        let requester = Requester::new(
            catalog::memory::MemoryCatalogManager::new(),
            partition_manager,
            Arc::new(MockDatanodeManager::new(watcher)),
        );
        let ctx = Arc::new(QueryContext::with("greptime", "cancel_test"));
        for (region_number, task_id, affected_rows) in
            [(1, "live-cycle", 1), (3, "absent-cycle", 0)]
        {
            let region_id = RegionId::new(1, region_number);
            assert_eq!(
                requester
                    .handle_cancel_region_compaction(region_id, task_id.into(), ctx.clone())
                    .await
                    .unwrap(),
                affected_rows
            );
            let (peer, request) = requests.try_recv().unwrap();
            assert_eq!(peer.id, u64::from(region_number));
            assert_eq!(request.header.unwrap().dbname, ctx.get_db_string());
            assert_eq!(
                request.body,
                Some(RegionRequestBody::CancelCompaction(
                    CancelCompactionRequest {
                        region_id: region_id.as_u64(),
                        task_id: task_id.into(),
                    }
                ))
            );
        }
        // Metadata regions are rejected before routing, even without a table route.
        for table_id in [1, u32::MAX] {
            let region_id = RegionId::with_group_and_seq(
                table_id,
                metric_engine_consts::METRIC_METADATA_REGION_GROUP,
                1,
            );
            let error = requester
                .handle_cancel_region_compaction(region_id, "live-cycle".into(), ctx.clone())
                .await
                .unwrap_err();
            assert_eq!(error.status_code(), StatusCode::Unsupported);
            assert!(matches!(error, crate::error::Error::NotSupported { .. }));
            assert!(
                error
                    .to_string()
                    .contains("internal metric metadata region")
            );
        }
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_discard_pending_tails_routes_by_node_and_sums_regions() {
        use catalog::RegisterTableRequest;
        use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
        use table::dist_table::DummyDataSource;

        let backend = test_util::prepare_mocked_backend().await;
        let partition_manager = test_util::create_partition_rule_manager(backend).await;
        let (sender, mut requests) = tokio::sync::mpsc::channel(8);
        // Region `n` discards `n` tails, except that region 3 fails for node 7.
        let watcher = DatanodeWatcher::new(sender).with_handler(|peer, request| {
            let Some(RegionRequestBody::Truncate(TruncateRequest {
                kind: Some(truncate_request::Kind::PendingTails(PendingTails { node_id })),
                ..
            })) = request.body
            else {
                panic!("expected a pending tails discard");
            };
            if peer.id == 3 && node_id == 7 {
                return common_meta::error::UnexpectedSnafu {
                    err_msg: "mock error",
                }
                .fail();
            }
            Ok(RegionResponse::new(peer.id as usize))
        });
        let catalog = catalog::memory::MemoryCatalogManager::with_default_setup();
        let table = Arc::new(table::Table::new(
            Arc::new(test_util::new_test_table_info(1, "table_1", [].into_iter())),
            table::metadata::FilterPushDownType::Unsupported,
            Arc::new(DummyDataSource),
        ));
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "table_1".to_string(),
                table_id: 1,
                table,
            })
            .unwrap();
        let requester = Requester::new(
            catalog,
            partition_manager,
            Arc::new(MockDatanodeManager::new(watcher)),
        );
        let ctx = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));
        let expected = |region_number, node_id| {
            Some(RegionRequestBody::Truncate(TruncateRequest {
                region_id: RegionId::new(1, region_number).as_u64(),
                kind: Some(truncate_request::Kind::PendingTails(PendingTails {
                    node_id,
                })),
            }))
        };

        let discarded = requester
            .handle_discard_pending_tails(RegionId::new(1, 1), 7, ctx.clone())
            .await
            .unwrap();
        assert_eq!(1, discarded);
        let (peer, request) = requests.try_recv().unwrap();
        assert_eq!(1, peer.id);
        assert_eq!(expected(1, 7), request.body);
        assert!(requests.try_recv().is_err());

        let table_name = TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "table_1");
        let mut sent_to_regions = |node_id| {
            let mut sent = Vec::new();
            while let Ok((peer, request)) = requests.try_recv() {
                assert_eq!(expected(peer.id as u32, node_id), request.body);
                sent.push(peer.id);
            }
            sent.sort();
            sent
        };
        let error = requester
            .handle_discard_pending_tails_by_table(table_name.clone(), 7, ctx.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, crate::error::Error::RequestRegion { .. }));
        assert_eq!(vec![1, 2, 3], sent_to_regions(7));

        let discarded = requester
            .handle_discard_pending_tails_by_table(table_name, 8, ctx)
            .await
            .unwrap();
        assert_eq!(1 + 2 + 3, discarded);
        assert_eq!(vec![1, 2, 3], sent_to_regions(8));
    }

    #[test]
    fn test_build_series_index_requires_physical_metric_table() {
        ensure_build_series_index_supported(METRIC_ENGINE, true).unwrap();
        assert!(ensure_build_series_index_supported(METRIC_ENGINE, false).is_err());
        assert!(ensure_build_series_index_supported("mito", false).is_err());
        assert!(ensure_build_series_index_supported("mito", true).is_err());
    }

    #[test]
    fn test_to_pb_compaction_time_range_normalizes_mixed_units() {
        let range = TimestampRange::new(
            Timestamp::new_millisecond(1_500),
            Timestamp::new_microsecond(2_500_000),
        )
        .unwrap();

        let pb_range = to_pb_compaction_time_range(range).unwrap();
        assert_eq!(1, pb_range.start);
        assert_eq!(3, pb_range.end);
        assert_eq!(TimeUnit::Second as i32, pb_range.time_unit);
    }

    #[test]
    fn test_ensure_discard_supported() {
        let kind = truncate_request::Kind::PendingTails(PendingTails { node_id: 7 });
        let error = ensure_discard_supported(METRIC_ENGINE, false, &kind).unwrap_err();
        assert!(matches!(error, crate::error::Error::NotSupported { .. }));
        assert!(error.to_string().contains("pending WAL tails"), "{error}");
        let kind = truncate_request::Kind::Unflushed(Unflushed {});
        let error = ensure_discard_supported(METRIC_ENGINE, false, &kind).unwrap_err();
        assert!(error.to_string().contains("unflushed data"), "{error}");

        ensure_discard_supported(METRIC_ENGINE, true, &kind).unwrap();
        ensure_discard_supported("mito", false, &kind).unwrap();
    }

    #[test]
    fn test_to_pb_compaction_time_range() {
        let range = TimestampRange::new(
            Timestamp::new_microsecond(1_000),
            Timestamp::new_microsecond(2_000),
        )
        .unwrap();

        let pb_range = to_pb_compaction_time_range(range).unwrap();
        assert_eq!(0, pb_range.start);
        assert_eq!(1, pb_range.end);
        assert_eq!(TimeUnit::Second as i32, pb_range.time_unit);
    }
}
