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

use std::time::{Duration, Instant};

use api::v1::meta::{HeartbeatRequest, Role};
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, Value};
use client::inserter::{Context as InserterContext, Inserter};
use client::DEFAULT_CATALOG_NAME;
use common_catalog::consts::DEFAULT_PRIVATE_SCHEMA_NAME;
use common_macro::ToRow;
use common_meta::datanode::RegionStat;
use common_meta::DatanodeId;
use common_telemetry::warn;
use common_time::util::current_time_millis;
use dashmap::DashMap;
use store_api::region_engine::RegionRole;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

/// The handler to persist stats.
pub struct PersistStatsHandler {
    inserter: Box<dyn Inserter>,
    last_persisted_time: DashMap<DatanodeId, Instant>,
    persist_interval: Duration,
}

/// The name of the table to persist region stats.
const META_REGION_STATS_TABLE_NAME: &str = "region_statistics";
/// The default context to persist region stats.
const DEFAULT_CONTEXT: InserterContext = InserterContext {
    catalog: DEFAULT_CATALOG_NAME,
    schema: DEFAULT_PRIVATE_SCHEMA_NAME,
};

#[derive(ToRow)]
struct PersistRegionStat<'a> {
    table_id: u32,
    region_id: u64,
    region_number: u32,
    manifest_size: u64,
    datanode_id: u64,
    #[col(datatype = "string")]
    engine: &'a str,
    num_rows: u64,
    sst_num: u64,
    sst_size: u64,
    write_bytes_per_secs: u64,
    #[col(
        name = "greptime_timestamp",
        semantic = "Timestamp",
        datatype = "TimestampMillisecond"
    )]
    timestamp_millis: i64,
}

impl PersistStatsHandler {
    /// Creates a new [`PersistRegionStatsHandler`].
    ///
    /// # Panics
    ///
    /// Panics if `ttl` is zero.
    pub fn new(inserter: Box<dyn Inserter>, mut persist_interval: Duration) -> Self {
        if persist_interval < Duration::from_secs(60) {
            warn!("persist_interval is less than 60 seconds, set to 60 seconds");
            persist_interval = Duration::from_secs(60);
        }
        assert!(
            persist_interval.as_millis() != 0,
            "persist_interval must be greater than zero"
        );

        Self {
            inserter,
            last_persisted_time: DashMap::new(),
            persist_interval,
        }
    }

    fn should_persist(&self, datanode_id: DatanodeId) -> bool {
        let Some(last_persisted_time) = self.last_persisted_time.get(&datanode_id) else {
            return true;
        };

        last_persisted_time.elapsed() >= self.persist_interval
    }

    async fn persist(&self, datanode_id: DatanodeId, region_stats: &[RegionStat]) {
        let timestamp = current_time_millis();
        let persist_interval_millis = self.persist_interval.as_millis() as i64;
        // Safety: `persist_interval_millis` is guaranteed to be greater than zero.
        let aligned_ts = timestamp / persist_interval_millis * persist_interval_millis;
        let rows = region_stats
            .iter()
            .flat_map(|s| {
                if matches!(s.role, RegionRole::Leader) {
                    Some({
                        PersistRegionStat {
                            table_id: s.id.table_id(),
                            region_id: s.id.as_u64(),
                            region_number: s.id.region_number(),
                            manifest_size: s.manifest_size,
                            datanode_id,
                            engine: s.engine.as_str(),
                            num_rows: s.num_rows,
                            sst_num: s.sst_num,
                            sst_size: s.sst_size,
                            write_bytes_per_secs: s.write_bytes_per_sec,
                            timestamp_millis: aligned_ts,
                        }
                        .to_row()
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if rows.is_empty() {
            return;
        }

        if let Err(err) = self
            .inserter
            .insert_rows(
                &DEFAULT_CONTEXT,
                RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: META_REGION_STATS_TABLE_NAME.to_string(),
                        rows: Some(Rows {
                            schema: PersistRegionStat::schema(),
                            rows,
                        }),
                    }],
                },
            )
            .await
        {
            warn!(
                "Failed to persist region stats, datanode_id: {}, error: {:?}",
                datanode_id, err
            );
            return;
        }

        self.last_persisted_time.insert(datanode_id, Instant::now());
    }
}

#[async_trait::async_trait]
impl HeartbeatHandler for PersistStatsHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        _: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(current_stat) = acc.stat.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        if !self.should_persist(current_stat.id) {
            return Ok(HandleControl::Continue);
        }

        self.persist(current_stat.id, &current_stat.region_stats)
            .await;

        Ok(HandleControl::Continue)
    }
}
