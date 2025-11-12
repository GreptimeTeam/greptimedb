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
use client::DEFAULT_CATALOG_NAME;
use client::inserter::{Context as InserterContext, Inserter};
use common_catalog::consts::DEFAULT_PRIVATE_SCHEMA_NAME;
use common_macro::{Schema, ToRow};
use common_meta::DatanodeId;
use common_meta::datanode::RegionStat;
use common_telemetry::warn;
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
const META_REGION_STATS_HISTORY_TABLE_NAME: &str = "region_statistics_history";
/// The default context to persist region stats.
const DEFAULT_CONTEXT: InserterContext = InserterContext {
    catalog: DEFAULT_CATALOG_NAME,
    schema: DEFAULT_PRIVATE_SCHEMA_NAME,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedRegionStat {
    region_id: RegionId,
    written_bytes: u64,
}

impl From<&RegionStat> for PersistedRegionStat {
    fn from(stat: &RegionStat) -> Self {
        Self {
            region_id: stat.id,
            written_bytes: stat.written_bytes,
        }
    }
}

#[derive(ToRow, Schema)]
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
        // This col name is for the information schema table, so we don't touch it
        name = "greptime_timestamp",
        semantic = "Timestamp",
        datatype = "TimestampMillisecond"
    )]
    timestamp_millis: i64,
}

/// Compute the region stat to persist.
fn compute_persist_region_stat(
    region_stat: &RegionStat,
    datanode_id: DatanodeId,
    timestamp_millis: i64,
    persisted_region_stat: Option<PersistedRegionStat>,
) -> PersistRegionStat<'_> {
    let write_bytes_delta = persisted_region_stat
        .and_then(|persisted_region_stat| {
            region_stat
                .written_bytes
                .checked_sub(persisted_region_stat.written_bytes)
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
        timestamp_millis,
    }
}

fn to_persisted_if_leader(
    region_stat: &RegionStat,
    last_persisted_region_stats: &DashMap<RegionId, PersistedRegionStat>,
    datanode_id: DatanodeId,
    timestamp_millis: i64,
) -> Option<(Row, PersistedRegionStat)> {
    if matches!(region_stat.role, RegionRole::Leader) {
        let persisted_region_stat = last_persisted_region_stats.get(&region_stat.id).map(|s| *s);
        Some((
            compute_persist_region_stat(
                region_stat,
                datanode_id,
                timestamp_millis,
                persisted_region_stat,
            )
            .to_row(),
            PersistedRegionStat::from(region_stat),
        ))
    } else {
        None
    }
}

/// Align the timestamp to the nearest interval.
///
/// # Panics
/// Panics if `interval` as milliseconds is zero.
fn align_ts(ts: i64, interval: Duration) -> i64 {
    assert!(
        interval.as_millis() != 0,
        "interval must be greater than zero"
    );
    ts / interval.as_millis() as i64 * interval.as_millis() as i64
}

impl PersistStatsHandler {
    /// Creates a new [`PersistStatsHandler`].
    pub fn new(inserter: Box<dyn Inserter>, mut persist_interval: Duration) -> Self {
        if persist_interval < Duration::from_mins(10) {
            warn!("persist_interval is less than 10 minutes, set to 10 minutes");
            persist_interval = Duration::from_mins(10);
        }

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

    async fn persist(
        &self,
        timestamp_millis: i64,
        datanode_id: DatanodeId,
        region_stats: &[RegionStat],
    ) {
        // Safety: persist_interval is greater than zero.
        let aligned_ts = align_ts(timestamp_millis, self.persist_interval);
        let (rows, incoming_region_stats): (Vec<_>, Vec<_>) = region_stats
            .iter()
            .flat_map(|region_stat| {
                to_persisted_if_leader(
                    region_stat,
                    &self.last_persisted_region_stats,
                    datanode_id,
                    aligned_ts,
                )
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
                        table_name: META_REGION_STATS_HISTORY_TABLE_NAME.to_string(),
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

        self.persist(
            current_stat.timestamp_millis,
            current_stat.id,
            &current_stat.region_stats,
        )
        .await;

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use client::inserter::{Context as InserterContext, InsertOptions};
    use common_meta::datanode::{RegionManifestInfo, RegionStat, Stat};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use super::*;
    use crate::handler::test_utils::TestEnv;

    fn create_test_region_stat(
        table_id: u32,
        region_number: u32,
        written_bytes: u64,
        engine: &str,
    ) -> RegionStat {
        let region_id = RegionId::new(table_id, region_number);
        RegionStat {
            id: region_id,
            rcus: 100,
            wcus: 200,
            approximate_bytes: 1024,
            engine: engine.to_string(),
            role: RegionRole::Leader,
            num_rows: 1000,
            memtable_size: 512,
            manifest_size: 256,
            sst_size: 2048,
            sst_num: 5,
            index_size: 128,
            region_manifest: RegionManifestInfo::Mito {
                manifest_version: 1,
                flushed_entry_id: 100,
                file_removed_cnt: 0,
            },
            written_bytes,
            data_topic_latest_entry_id: 200,
            metadata_topic_latest_entry_id: 200,
        }
    }

    #[test]
    fn test_compute_persist_region_stat_with_no_persisted_stat() {
        let region_stat = create_test_region_stat(1, 1, 1000, "mito");
        let datanode_id = 123;
        let timestamp_millis = 1640995200000; // 2022-01-01 00:00:00 UTC
        let result = compute_persist_region_stat(&region_stat, datanode_id, timestamp_millis, None);
        assert_eq!(result.table_id, 1);
        assert_eq!(result.region_id, region_stat.id.as_u64());
        assert_eq!(result.region_number, 1);
        assert_eq!(result.manifest_size, 256);
        assert_eq!(result.datanode_id, datanode_id);
        assert_eq!(result.engine, "mito");
        assert_eq!(result.num_rows, 1000);
        assert_eq!(result.sst_num, 5);
        assert_eq!(result.sst_size, 2048);
        assert_eq!(result.write_bytes_delta, 0); // No previous stat, so delta is 0
        assert_eq!(result.timestamp_millis, timestamp_millis);
    }

    #[test]
    fn test_compute_persist_region_stat_with_persisted_stat_increase() {
        let region_stat = create_test_region_stat(2, 3, 1500, "mito");
        let datanode_id = 456;
        let timestamp_millis = 1640995260000; // 2022-01-01 00:01:00 UTC
        let persisted_stat = PersistedRegionStat {
            region_id: region_stat.id,
            written_bytes: 1000, // Previous write bytes
        };
        let result = compute_persist_region_stat(
            &region_stat,
            datanode_id,
            timestamp_millis,
            Some(persisted_stat),
        );
        assert_eq!(result.table_id, 2);
        assert_eq!(result.region_id, region_stat.id.as_u64());
        assert_eq!(result.region_number, 3);
        assert_eq!(result.manifest_size, 256);
        assert_eq!(result.datanode_id, datanode_id);
        assert_eq!(result.engine, "mito");
        assert_eq!(result.num_rows, 1000);
        assert_eq!(result.sst_num, 5);
        assert_eq!(result.sst_size, 2048);
        assert_eq!(result.write_bytes_delta, 500); // 1500 - 1000 = 500
        assert_eq!(result.timestamp_millis, timestamp_millis);
    }

    #[test]
    fn test_compute_persist_region_stat_with_persisted_stat_decrease() {
        let region_stat = create_test_region_stat(3, 5, 800, "mito");
        let datanode_id = 789;
        let timestamp_millis = 1640995320000; // 2022-01-01 00:02:00 UTC
        let persisted_stat = PersistedRegionStat {
            region_id: region_stat.id,
            written_bytes: 1200, // Previous write bytes (higher than current)
        };
        let result = compute_persist_region_stat(
            &region_stat,
            datanode_id,
            timestamp_millis,
            Some(persisted_stat),
        );
        assert_eq!(result.table_id, 3);
        assert_eq!(result.region_id, region_stat.id.as_u64());
        assert_eq!(result.region_number, 5);
        assert_eq!(result.manifest_size, 256);
        assert_eq!(result.datanode_id, datanode_id);
        assert_eq!(result.engine, "mito");
        assert_eq!(result.num_rows, 1000);
        assert_eq!(result.sst_num, 5);
        assert_eq!(result.sst_size, 2048);
        assert_eq!(result.write_bytes_delta, 0); // 800 - 1200 would be negative, so 0 due to checked_sub
        assert_eq!(result.timestamp_millis, timestamp_millis);
    }

    #[test]
    fn test_compute_persist_region_stat_with_persisted_stat_equal() {
        let region_stat = create_test_region_stat(4, 7, 2000, "mito");
        let datanode_id = 101;
        let timestamp_millis = 1640995380000; // 2022-01-01 00:03:00 UTC
        let persisted_stat = PersistedRegionStat {
            region_id: region_stat.id,
            written_bytes: 2000, // Same as current write bytes
        };
        let result = compute_persist_region_stat(
            &region_stat,
            datanode_id,
            timestamp_millis,
            Some(persisted_stat),
        );
        assert_eq!(result.table_id, 4);
        assert_eq!(result.region_id, region_stat.id.as_u64());
        assert_eq!(result.region_number, 7);
        assert_eq!(result.manifest_size, 256);
        assert_eq!(result.datanode_id, datanode_id);
        assert_eq!(result.engine, "mito");
        assert_eq!(result.num_rows, 1000);
        assert_eq!(result.sst_num, 5);
        assert_eq!(result.sst_size, 2048);
        assert_eq!(result.write_bytes_delta, 0); // 2000 - 2000 = 0
        assert_eq!(result.timestamp_millis, timestamp_millis);
    }

    #[test]
    fn test_compute_persist_region_stat_with_overflow_protection() {
        let region_stat = create_test_region_stat(8, 15, 500, "mito");
        let datanode_id = 505;
        let timestamp_millis = 1640995620000; // 2022-01-01 00:07:00 UTC
        let persisted_stat = PersistedRegionStat {
            region_id: region_stat.id,
            written_bytes: 1000, // Higher than current, would cause underflow
        };
        let result = compute_persist_region_stat(
            &region_stat,
            datanode_id,
            timestamp_millis,
            Some(persisted_stat),
        );
        assert_eq!(result.table_id, 8);
        assert_eq!(result.region_id, region_stat.id.as_u64());
        assert_eq!(result.region_number, 15);
        assert_eq!(result.manifest_size, 256);
        assert_eq!(result.datanode_id, datanode_id);
        assert_eq!(result.engine, "mito");
        assert_eq!(result.num_rows, 1000);
        assert_eq!(result.sst_num, 5);
        assert_eq!(result.sst_size, 2048);
        assert_eq!(result.write_bytes_delta, 0); // checked_sub returns None, so default to 0
        assert_eq!(result.timestamp_millis, timestamp_millis);
    }

    struct MockInserter {
        requests: Arc<Mutex<Vec<api::v1::RowInsertRequest>>>,
    }

    #[async_trait::async_trait]
    impl Inserter for MockInserter {
        async fn insert_rows(
            &self,
            _context: &InserterContext<'_>,
            requests: api::v1::RowInsertRequests,
        ) -> client::error::Result<()> {
            self.requests.lock().unwrap().extend(requests.inserts);

            Ok(())
        }

        fn set_options(&mut self, _options: &InsertOptions) {}
    }

    #[tokio::test]
    async fn test_not_persist_region_stats() {
        let env = TestEnv::new();
        let mut ctx = env.ctx();

        let requests = Arc::new(Mutex::new(vec![]));
        let inserter = MockInserter {
            requests: requests.clone(),
        };
        let handler = PersistStatsHandler::new(Box::new(inserter), Duration::from_secs(10));
        let mut acc = HeartbeatAccumulator {
            stat: Some(Stat {
                id: 1,
                timestamp_millis: 1640995200000,
                region_stats: vec![create_test_region_stat(1, 1, 1000, "mito")],
                ..Default::default()
            }),
            ..Default::default()
        };
        handler.last_persisted_time.insert(1, Instant::now());
        // Do not persist
        handler
            .handle(&HeartbeatRequest::default(), &mut ctx, &mut acc)
            .await
            .unwrap();
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_persist_region_stats() {
        let env = TestEnv::new();
        let mut ctx = env.ctx();
        let requests = Arc::new(Mutex::new(vec![]));
        let inserter = MockInserter {
            requests: requests.clone(),
        };

        let handler = PersistStatsHandler::new(Box::new(inserter), Duration::from_secs(10));

        let region_stat = create_test_region_stat(1, 1, 1000, "mito");
        let timestamp_millis = 1640995200000;
        let datanode_id = 1;
        let region_id = RegionId::new(1, 1);
        let mut acc = HeartbeatAccumulator {
            stat: Some(Stat {
                id: datanode_id,
                timestamp_millis,
                region_stats: vec![region_stat.clone()],
                ..Default::default()
            }),
            ..Default::default()
        };

        handler.last_persisted_region_stats.insert(
            region_id,
            PersistedRegionStat {
                region_id,
                written_bytes: 500,
            },
        );
        let (expected_row, expected_persisted_region_stat) = to_persisted_if_leader(
            &region_stat,
            &handler.last_persisted_region_stats,
            datanode_id,
            timestamp_millis,
        )
        .unwrap();
        let before_insert_time = Instant::now();
        // Persist
        handler
            .handle(&HeartbeatRequest::default(), &mut ctx, &mut acc)
            .await
            .unwrap();
        let request = {
            let mut requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            requests.pop().unwrap()
        };
        assert_eq!(
            request.table_name,
            META_REGION_STATS_HISTORY_TABLE_NAME.to_string()
        );
        assert_eq!(request.rows.unwrap().rows, vec![expected_row]);

        // Check last persisted time
        assert!(
            handler
                .last_persisted_time
                .get(&datanode_id)
                .unwrap()
                .gt(&before_insert_time)
        );

        // Check last persisted region stats
        assert_eq!(
            handler
                .last_persisted_region_stats
                .get(&region_id)
                .unwrap()
                .value(),
            &expected_persisted_region_stat
        );
    }
}
