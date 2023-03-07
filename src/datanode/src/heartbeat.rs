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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, NodeStat, Peer, RegionStat, TableName};
use catalog::{table_regions_map, CatalogManagerRef, FullTableName};
use common_telemetry::{error, info, warn};
use common_wrcu::{StatKey, Statistics, WrcuStat};
use meta_client::client::{HeartbeatSender, MetaClient};
use snafu::ResultExt;

use crate::error::{MetaClientInitSnafu, Result};

pub struct HeartbeatTask {
    node_id: u64,
    server_addr: String,
    server_hostname: Option<String>,
    running: Arc<AtomicBool>,
    meta_client: Arc<MetaClient>,
    catalog_manager: CatalogManagerRef,
    wrcu_stat: WrcuStat,
    interval: u64,
}

impl Drop for HeartbeatTask {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

impl HeartbeatTask {
    /// Create a new heartbeat task instance.
    pub fn new(
        node_id: u64,
        server_addr: String,
        server_hostname: Option<String>,
        meta_client: Arc<MetaClient>,
        catalog_manager: CatalogManagerRef,
        wrcu_stat: WrcuStat,
    ) -> Self {
        Self {
            node_id,
            server_addr,
            server_hostname,
            running: Arc::new(AtomicBool::new(false)),
            meta_client,
            catalog_manager,
            wrcu_stat,
            interval: 5_000, // default interval is set to 5 secs
        }
    }

    pub async fn create_streams(
        meta_client: &MetaClient,
        running: Arc<AtomicBool>,
    ) -> Result<HeartbeatSender> {
        let (tx, mut rx) = meta_client.heartbeat().await.context(MetaClientInitSnafu)?;
        common_runtime::spawn_bg(async move {
            while let Some(res) = match rx.message().await {
                Ok(m) => m,
                Err(e) => {
                    error!(e; "Error while reading heartbeat response");
                    None
                }
            } {
                Self::handle_response(res).await;
                if !running.load(Ordering::Acquire) {
                    info!("Heartbeat task shutdown");
                }
            }
            info!("Heartbeat handling loop exit.")
        });
        Ok(tx)
    }

    async fn handle_response(resp: HeartbeatResponse) {
        info!("heartbeat response: {:?}", resp);
    }

    /// Start heartbeat task, spawn background task.
    pub async fn start(&self) -> Result<()> {
        let running = self.running.clone();
        if running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Heartbeat task started multiple times");
            return Ok(());
        }
        let interval = self.interval;
        let node_id = self.node_id;
        let addr = resolve_addr(&self.server_addr, &self.server_hostname);
        let meta_client = self.meta_client.clone();

        let catalog_manager_clone = self.catalog_manager.clone();
        let mut tx = Self::create_streams(&meta_client, running.clone()).await?;

        let wrcu_stat = self.wrcu_stat.clone();

        common_runtime::spawn_bg(async move {
            while running.load(Ordering::Acquire) {
                let table_regions_map = table_regions_map(&catalog_manager_clone).await;

                let total_regions = match table_regions_map.as_ref() {
                    Ok(map) => map.iter().map(|(_, regions)| regions.len()).sum::<usize>() as i64,
                    Err(e) => {
                        error!("failed to get region number, err: {e:?}");
                        -1
                    }
                };

                let Statistics {
                    wcus,
                    rcus,
                    region_wcu_map,
                    region_rcu_map,
                } = wrcu_stat.statistics_and_clear();

                let region_stats = match table_regions_map.as_ref() {
                    Ok(map) => to_region_stats(region_wcu_map, region_rcu_map, map),
                    Err(e) => {
                        error!("failed to get region stats, err: {e:?}");
                        vec![]
                    }
                };

                let req = HeartbeatRequest {
                    peer: Some(Peer {
                        id: node_id,
                        addr: addr.clone(),
                    }),
                    node_stat: Some(NodeStat {
                        region_num: total_regions,
                        wcus: wcus as i64,
                        rcus: rcus as i64,
                        ..Default::default()
                    }),
                    region_stats,
                    ..Default::default()
                };

                if let Err(e) = tx.send(req).await {
                    error!("Failed to send heartbeat to metasrv, error: {:?}", e);
                    match Self::create_streams(&meta_client, running.clone()).await {
                        Ok(new_tx) => {
                            info!("Reconnected to metasrv");
                            tx = new_tx;
                        }
                        Err(e) => {
                            error!(e;"Failed to reconnect to metasrv!");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(interval)).await;
            }
        });

        Ok(())
    }
}

/// Resolves hostname:port address for meta registration
fn resolve_addr(bind_addr: &str, hostname_addr: &Option<String>) -> String {
    match hostname_addr {
        Some(hostname_addr) => {
            // it has port configured
            if hostname_addr.contains(':') {
                hostname_addr.clone()
            } else {
                // otherwise, resolve port from bind_addr
                // should be safe to unwrap here because bind_addr is already validated
                let port = bind_addr.split(':').nth(1).unwrap();
                format!("{hostname_addr}:{port}")
            }
        }
        None => bind_addr.to_owned(),
    }
}

/// Combined region status according to the parameters
fn to_region_stats(
    region_wcu_map: HashMap<StatKey, u64>,
    region_rcu_map: HashMap<StatKey, u64>,
    table_regions_map: &HashMap<FullTableName, Vec<u32>>,
) -> Vec<RegionStat> {
    let mut region_stats = vec![];
    for (
        FullTableName {
            catalog_name,
            schema_name,
            table_name,
        },
        regions,
    ) in table_regions_map
    {
        for region_id in regions {
            let full_table_name = Some(TableName {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            });

            let wcus = region_wcu_map
                .get(&StatKey {
                    catalog: catalog_name.clone(),
                    schema: schema_name.clone(),
                    table: table_name.clone(),
                    region_number: *region_id,
                })
                .map(|x| *x as i64)
                .unwrap_or_default();

            let rcus = region_rcu_map
                .get(&StatKey {
                    catalog: catalog_name.clone(),
                    schema: schema_name.clone(),
                    table: table_name.clone(),
                    region_number: *region_id,
                })
                .map(|x| *x as i64)
                .unwrap_or_default();

            let region_stat = RegionStat {
                region_id: *region_id as u64,
                table_name: full_table_name,
                wcus,
                rcus,
                ..Default::default()
            };

            region_stats.push(region_stat);
        }
    }
    region_stats
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use catalog::FullTableName;
    use common_wrcu::StatKey;

    use super::to_region_stats;

    #[test]
    fn test_to_region_stat() {
        let mut region_wcu_map = HashMap::new();
        region_wcu_map.insert(
            StatKey {
                catalog: "c1".to_string(),
                schema: "s1".to_string(),
                table: "t1".to_string(),
                region_number: 1,
            },
            10,
        );

        let mut region_rcu_map = HashMap::with_capacity(3);
        region_rcu_map.insert(
            StatKey {
                catalog: "c1".to_string(),
                schema: "s1".to_string(),
                table: "t2".to_string(),
                region_number: 2,
            },
            20,
        );

        let mut table_regions_map = HashMap::new();
        table_regions_map.insert(
            FullTableName {
                catalog_name: "c1".to_string(),
                schema_name: "s1".to_string(),
                table_name: "t1".to_string(),
            },
            vec![1],
        );
        table_regions_map.insert(
            FullTableName {
                catalog_name: "c1".to_string(),
                schema_name: "s1".to_string(),
                table_name: "t2".to_string(),
            },
            vec![2],
        );

        let region_stats = to_region_stats(region_wcu_map, region_rcu_map, &table_regions_map);

        assert_eq!(2, region_stats.len());

        assert_eq!(10, region_stats[0].wcus);
        assert_eq!(0, region_stats[0].rcus);

        assert_eq!(0, region_stats[1].wcus);
        assert_eq!(20, region_stats[1].rcus);
    }

    #[test]
    fn test_resolve_addr() {
        assert_eq!(
            "tomcat:3001",
            super::resolve_addr("127.0.0.1:3001", &Some("tomcat".to_owned()))
        );

        assert_eq!(
            "tomcat:3002",
            super::resolve_addr("127.0.0.1:3001", &Some("tomcat:3002".to_owned()))
        );

        assert_eq!(
            "127.0.0.1:3001",
            super::resolve_addr("127.0.0.1:3001", &None)
        );
    }
}
