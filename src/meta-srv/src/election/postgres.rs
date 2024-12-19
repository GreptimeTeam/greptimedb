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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{self, Duration};

use common_meta::distributed_time_constants::{META_KEEP_ALIVE_INTERVAL_SECS, META_LEASE_SECS};
use common_meta::kv_backend::postgres::{CAS, POINT_GET, PREFIX_SCAN, PUT_IF_NOT_EXISTS};
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{timeout, MissedTickBehavior};
use tokio_postgres::Client;

use crate::election::{
    Election, LeaderChangeMessage, LeaderKey, CANDIDATES_ROOT, CANDIDATE_LEASE_SECS, ELECTION_KEY,
    KEEP_ALIVE_INTERVAL_SECS,
};
use crate::error::{
    DeserializeFromJsonSnafu, NoLeaderSnafu, PostgresExecutionSnafu, Result, SerializeToJsonSnafu,
    UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

const CAMPAIGN: &str = "SELECT pg_try_advisory_lock(1)";

#[derive(Debug, Serialize, Deserialize)]
struct ValueWithLease {
    value: String,
    expire_time: f64,
}

#[derive(Debug, Clone)]
struct PgLeaderKey {
    name: Vec<u8>,
    key: Vec<u8>,
    rev: i64,
    lease: i64,
}

impl LeaderKey for PgLeaderKey {
    fn name(&self) -> &[u8] {
        &self.name
    }

    fn key(&self) -> &[u8] {
        &self.key
    }

    fn rev(&self) -> i64 {
        self.rev
    }

    fn lease(&self) -> i64 {
        self.lease
    }
}

pub struct PgElection {
    leader_value: String,
    client: Client,
    is_leader: AtomicBool,
    infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
}

impl PgElection {
    pub async fn with_pg_client(
        leader_value: String,
        client: Client,
        store_key_prefix: String,
    ) -> Result<ElectionRef> {
        let (tx, mut rx) = broadcast::channel(100);
        let leader_ident = leader_value.clone();
        let _handle = common_runtime::spawn_global(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg {
                        LeaderChangeMessage::Elected(key) => {
                            info!(
                                "[{leader_ident}] is elected as leader: {:?}, lease: {}",
                                key.name(),
                                key.lease()
                            );
                        }
                        LeaderChangeMessage::StepDown(key) => {
                            warn!(
                                "[{leader_ident}] is stepping down: {:?}, lease: {}",
                                key.name(),
                                key.lease()
                            );
                        }
                    },
                    Err(RecvError::Lagged(_)) => {
                        warn!("Log printing is too slow or leader changed too fast!");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        Ok(Arc::new(Self {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
        }))
    }

    fn election_key(&self) -> String {
        format!("{}{}", self.store_key_prefix, ELECTION_KEY)
    }

    fn candidate_root(&self) -> String {
        format!("{}{}", self.store_key_prefix, CANDIDATES_ROOT)
    }

    fn candidate_key(&self) -> String {
        format!("{}{}", self.candidate_root(), self.leader_value)
    }
}

#[async_trait::async_trait]
impl Election for PgElection {
    type Leader = LeaderValue;

    fn subscribe_leader_change(&self) -> broadcast::Receiver<LeaderChangeMessage> {
        self.leader_watcher.subscribe()
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    fn in_infancy(&self) -> bool {
        self.infancy
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        let key = self.candidate_key().into_bytes();
        let node_info =
            serde_json::to_string(node_info).with_context(|_| SerializeToJsonSnafu {
                input: format!("{node_info:?}"),
            })?;
        let value_with_lease = ValueWithLease {
            value: node_info,
            expire_time: time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
                + CANDIDATE_LEASE_SECS as f64,
        };
        self.put_value_with_lease(&key, &value_with_lease).await?;

        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(KEEP_ALIVE_INTERVAL_SECS));

        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            // TODO: Add rev and lease information
            rev: 0,
            lease: 0,
        };
        loop {
            let _ = keep_alive_interval.tick().await;
            match self.keep_alive(&key, leader_key.clone(), false).await {
                Ok(_) => {}
                Err(e) => {
                    warn!(e; "Candidate lease expired, key: {}", self.candidate_key());
                    break;
                }
            }
        }

        Ok(())
    }

    async fn all_candidates(&self) -> Result<Vec<MetasrvNodeInfo>> {
        let key_prefix = self.candidate_root().into_bytes();
        let mut candidates = self.get_value_with_lease_by_prefix(&key_prefix).await?;
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        // Remove expired candidates
        candidates.retain(|c| c.expire_time > now);

        let mut valid_candidates = Vec::with_capacity(candidates.len());
        for c in candidates {
            let node_info: MetasrvNodeInfo =
                serde_json::from_str(&c.value).with_context(|_| DeserializeFromJsonSnafu {
                    input: format!("{:?}", c.value),
                })?;
            valid_candidates.push(node_info);
        }
        Ok(valid_candidates)
    }

    async fn campaign(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let res = self
            .client
            .query(CAMPAIGN, &[])
            .await
            .context(PostgresExecutionSnafu)?;
        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            // TODO: Add rev and lease information
            rev: 0,
            lease: 0,
        };

        if let Some(row) = res.first() {
            let is_locked: bool = row.get(0);
            // If the lock is acquired, then the current node is the leader
            if is_locked {
                let leader_value_with_lease = ValueWithLease {
                    value: self.leader_value.clone(),
                    expire_time: time::SystemTime::now()
                        .duration_since(time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64()
                        + META_LEASE_SECS as f64,
                };
                self.put_value_with_lease(&key, &leader_value_with_lease)
                    .await?;

                let keep_lease_duration = Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS);
                let mut keep_alive_interval = tokio::time::interval(keep_lease_duration);
                keep_alive_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    match timeout(
                        keep_lease_duration,
                        self.keep_alive(&key, leader_key.clone(), true),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            let _ = keep_alive_interval.tick().await;
                        }
                        Ok(Err(err)) => {
                            error!(err; "Failed to keep alive");
                            break;
                        }
                        Err(_) => {
                            error!("Refresh lease timeout");
                            break;
                        }
                    }
                }

                // Step down
                if self
                    .is_leader
                    .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    if let Err(e) = self
                        .leader_watcher
                        .send(LeaderChangeMessage::StepDown(Arc::new(leader_key)))
                    {
                        error!(e; "Failed to send leader change message");
                    }
                }
            } else {
                // Not the leader, we check if the leader is still alive
                let check_interval = Duration::from_secs(META_LEASE_SECS);
                let mut check_interval = tokio::time::interval(check_interval);
                loop {
                    let _ = check_interval.tick().await;
                    let leader_value_with_lease = self.get_value_with_lease(&key).await?;
                    let now = time::SystemTime::now()
                        .duration_since(time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64();
                    // If the leader is expired, we re-initiate the campaign
                    if leader_value_with_lease.expire_time <= now {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn leader(&self) -> Result<Self::Leader> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(self.leader_value.as_bytes().into())
        } else {
            let key = self.election_key().into_bytes();
            let value_with_lease = self.get_value_with_lease(&key).await?;
            if value_with_lease.expire_time
                > time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()
            {
                Ok(value_with_lease.value.into())
            } else {
                return NoLeaderSnafu {}.fail();
            }
        }
    }

    async fn resign(&self) -> Result<()> {
        todo!()
    }
}

impl PgElection {
    async fn get_value_with_lease(&self, key: &Vec<u8>) -> Result<ValueWithLease> {
        let prev = self
            .client
            .query(POINT_GET, &[&key])
            .await
            .context(PostgresExecutionSnafu)?;

        if let Some(row) = prev.first() {
            let value: String = row.get(0);
            let value_with_lease: ValueWithLease =
                serde_json::from_str(&value).with_context(|_| DeserializeFromJsonSnafu {
                    input: format!("{value:?}"),
                })?;
            Ok(value_with_lease)
        } else {
            UnexpectedSnafu {
                violated: format!(
                    "Failed to get value from key: {:?}",
                    String::from_utf8_lossy(key)
                ),
            }
            .fail()
        }
    }

    async fn get_value_with_lease_by_prefix(
        &self,
        key_prefix: &Vec<u8>,
    ) -> Result<Vec<ValueWithLease>> {
        let prev = self
            .client
            .query(PREFIX_SCAN, &[key_prefix])
            .await
            .context(PostgresExecutionSnafu)?;

        let mut res = Vec::new();
        for row in prev {
            let value: String = row.get(0);
            let value_with_lease: ValueWithLease =
                serde_json::from_str(&value).with_context(|_| DeserializeFromJsonSnafu {
                    input: format!("{value:?}"),
                })?;
            res.push(value_with_lease);
        }

        Ok(res)
    }

    async fn update_value_with_lease(
        &self,
        key: &Vec<u8>,
        prev: &ValueWithLease,
        updated: &ValueWithLease,
    ) -> Result<()> {
        let prev = serde_json::to_string(prev)
            .with_context(|_| SerializeToJsonSnafu {
                input: format!("{prev:?}"),
            })?
            .into_bytes();

        let updated = serde_json::to_string(updated)
            .with_context(|_| SerializeToJsonSnafu {
                input: format!("{updated:?}"),
            })?
            .into_bytes();

        let res = self
            .client
            .query(CAS, &[key, &prev, &updated])
            .await
            .context(PostgresExecutionSnafu)?;

        // CAS operation will return the updated value if the operation is successful
        match res.is_empty() {
            false => Ok(()),
            true => UnexpectedSnafu {
                violated: format!(
                    "Failed to update value from key: {:?}",
                    String::from_utf8_lossy(key)
                ),
            }
            .fail(),
        }
    }

    // Returns `true` if the insertion is successful
    async fn put_value_with_lease(&self, key: &Vec<u8>, value: &ValueWithLease) -> Result<()> {
        let value = serde_json::to_string(value)
            .with_context(|_| SerializeToJsonSnafu {
                input: format!("{value:?}"),
            })?
            .into_bytes();

        let res = self
            .client
            .query(PUT_IF_NOT_EXISTS, &[key, &value])
            .await
            .context(PostgresExecutionSnafu)?;

        ensure!(
            res.is_empty(),
            UnexpectedSnafu {
                violated: format!(
                    "Failed to insert value from key: {:?}",
                    String::from_utf8_lossy(key)
                ),
            }
        );

        Ok(())
    }

    async fn keep_alive(
        &self,
        key: &Vec<u8>,
        leader: PgLeaderKey,
        keep_leader: bool,
    ) -> Result<()> {
        // Check if the current lease has expired
        let prev = self.get_value_with_lease(key).await?;
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        ensure!(
            prev.expire_time > now,
            UnexpectedSnafu {
                violated: format!(
                    "Failed to renew lease. Lease expired, key: {:?}",
                    String::from_utf8_lossy(key)
                ),
            }
        );

        // Renew the lease
        let updated = ValueWithLease {
            value: prev.value.clone(),
            expire_time: now + CANDIDATE_LEASE_SECS as f64,
        };
        self.update_value_with_lease(key, &prev, &updated).await?;

        if keep_leader
            && self
                .is_leader
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.infancy.store(true, Ordering::Relaxed);

            if let Err(e) = self
                .leader_watcher
                .send(LeaderChangeMessage::Elected(Arc::new(leader)))
            {
                error!(e; "Failed to send leader change message");
            }
        }
        Ok(())
    }
}
