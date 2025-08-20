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
use store_api::storage::RegionId;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

/// The handler to persist stats.
pub struct PersistStatsHandler {
    inserter: Box<dyn Inserter>,
    last_persisted_region_stats: DashMap<RegionId, PersistedRegionStat>,
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

#[derive(Debug, Clone, Copy)]
struct PersistedRegionStat {
    region_id: RegionId,
    write_bytess: u64,
}

impl From<&RegionStat> for PersistedRegionStat {
    fn from(stat: &RegionStat) -> Self {
        Self {
            region_id: stat.id,
            write_bytess: stat.write_bytes,
        }
    }
}

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
    write_bytes_delta: u64,
    #[col(
        name = "greptime_timestamp",
        semantic = "Timestamp",
        datatype = "TimestampMillisecond"
    )]
    timestamp_millis: i64,
}

fn persist_region_stats(
    region_stat: &RegionStat,
    datanode_id: DatanodeId,
    aligned_ts: i64,
    persisted_region_stat: Option<PersistedRegionStat>,
) -> PersistRegionStat {
    let write_bytes_delta = persisted_region_stat
        .and_then(|persisted_region_stat| {
            region_stat
                .write_bytes
                .checked_sub(persisted_region_stat.write_bytess)
        })
        .unwrap_or_default();

    PersistRegionStat {
        table_id: region_stat.id.table_id(),
        region_id: region_stat.id.as_u64(),
        region_number: region_stat.id.region_number(),
        manifest_size: region_stat.manifest_size,
        datanode_id,
        engine: region_stat.engine.as_str(),
        num_rows: region_stat.num_rows,
        sst_num: region_stat.sst_num,
        sst_size: region_stat.sst_size,
        write_bytes_delta,
        timestamp_millis: aligned_ts,
    }
}

impl PersistStatsHandler {
    /// Creates a new [`PersistStatsHandler`].
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
            last_persisted_region_stats: DashMap::new(),
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
        let (rows, incoming_region_stats): (Vec<_>, Vec<_>) = region_stats
            .iter()
            .flat_map(|region_stat| {
                if matches!(region_stat.role, RegionRole::Leader) {
                    let persisted_region_stat = self
                        .last_persisted_region_stats
                        .get(&region_stat.id)
                        .map(|s| *s);
                    Some((
                        persist_region_stats(
                            region_stat,
                            datanode_id,
                            aligned_ts,
                            persisted_region_stat,
                        )
                        .to_row(),
                        PersistedRegionStat::from(region_stat),
                    ))
                } else {
                    None
                }
            })
            .unzip();
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
        for s in incoming_region_stats {
            self.last_persisted_region_stats.insert(s.region_id, s);
        }
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
