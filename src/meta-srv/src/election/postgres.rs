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
use common_meta::kv_backend::postgres::{
    CAS, POINT_DELETE, POINT_GET, PREFIX_SCAN, PUT_IF_NOT_EXISTS,
};
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_postgres::Client;

use crate::election::{
    Election, LeaderChangeMessage, LeaderKey, CANDIDATES_ROOT, CANDIDATE_LEASE_SECS, ELECTION_KEY,
    KEEP_ALIVE_INTERVAL_SECS,
};
use crate::error::{
    DeserializeFromJsonSnafu, NoLeaderSnafu, PostgresExecutionSnafu, Result, SendLeaderChangeSnafu,
    SerializeToJsonSnafu, UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

// TODO: make key id and idle session timeout configurable.
const CAMPAIGN: &str = "SELECT pg_try_advisory_lock(1)";
const UNLOCK: &str = "SELECT pg_advisory_unlock(1)";
const SET_IDLE_SESSION_TIMEOUT: &str = "SET idle_in_transaction_session_timeout = $1";
// Currently the session timeout is longer than the leader lease time, so the leader lease may expire while the session is still alive.
// Either the leader reconnects and step down or the session expires and the lock is released.
const IDLE_SESSION_TIMEOUT: &str = "10s";

/// Value with a expire time. The expire time is in seconds since UNIX epoch.
#[derive(Debug, Serialize, Deserialize, Default)]
struct ValueWithLease {
    value: String,
    expire_time: f64,
}

/// Leader key for PostgreSql.
#[derive(Debug, Clone, Default)]
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

/// PostgreSql implementation of Election.
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
        // Set idle session timeout to IDLE_SESSION_TIMEOUT to avoid dead advisory lock.
        client
            .execute(SET_IDLE_SESSION_TIMEOUT, &[&IDLE_SESSION_TIMEOUT])
            .await
            .context(PostgresExecutionSnafu)?;

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
        let res = self.put_value_with_lease(&key, &value_with_lease).await?;
        // May registered before, check if the lease expired. If so, delete and re-register.
        if !res {
            let prev = self.get_value_with_lease(&key).await?.unwrap_or_default();
            if prev.expire_time
                <= time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()
            {
                self.delete_value(&key).await?;
                self.put_value_with_lease(&key, &value_with_lease).await?;
            }
        }

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(KEEP_ALIVE_INTERVAL_SECS));
        loop {
            let _ = keep_alive_interval.tick().await;

            let prev = self.get_value_with_lease(&key).await?.unwrap_or_default();
            let now = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();

            ensure!(
                prev.expire_time > now,
                UnexpectedSnafu {
                    violated: format!(
                        "Candidate lease expired, key: {:?}",
                        String::from_utf8_lossy(&key)
                    ),
                }
            );

            let updated = ValueWithLease {
                value: prev.value.clone(),
                expire_time: now + CANDIDATE_LEASE_SECS as f64,
            };
            self.update_value_with_lease(&key, &prev, &updated).await?;
        }
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

    /// Campaign loop, every metasrv node will:
    /// - Try to acquire the advisory lock.
    ///   - If successful, the current node is the leader. Leader should check the lease:
    ///     - If newly elected, put the leader key with the lease and notify the leader watcher.
    ///     - If the lease is not expired, keep the lock and renew the lease.
    ///     - If the lease expired, delete the key, unlock, step down, try to initiate a new campaign.
    ///   - If not successful, the current node is a follower. Follower should check the lease:
    ///     - If the lease expired, delete the key and return, try to initiate a new campaign.
    /// Caution: The leader may still hold the advisory lock while the lease is expired. The leader should step down in this case.
    async fn campaign(&self) -> Result<()> {
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS));
        loop {
            let _ = keep_alive_interval.tick().await;
            let res = self
                .client
                .query(CAMPAIGN, &[])
                .await
                .context(PostgresExecutionSnafu)?;
            if let Some(row) = res.first() {
                match row.try_get(0) {
                    Ok(true) => self.leader_action().await?,
                    Ok(false) => self.follower_action().await?,
                    Err(_) => {
                        return UnexpectedSnafu {
                            violated: "Failed to acquire the advisory lock".to_string(),
                        }
                        .fail();
                    }
                }
            } else {
                return UnexpectedSnafu {
                    violated: "Failed to acquire the advisory lock".to_string(),
                }
                .fail();
            }
        }
    }

    async fn leader(&self) -> Result<Self::Leader> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(self.leader_value.as_bytes().into())
        } else {
            let key = self.election_key().into_bytes();
            let value_with_lease = self
                .get_value_with_lease(&key)
                .await?
                .ok_or_else(|| NoLeaderSnafu {}.build())?;
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
    async fn get_value_with_lease(&self, key: &Vec<u8>) -> Result<Option<ValueWithLease>> {
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
            Ok(Some(value_with_lease))
        } else {
            Ok(None)
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

    /// Returns `true` if the insertion is successful
    async fn put_value_with_lease(&self, key: &Vec<u8>, value: &ValueWithLease) -> Result<bool> {
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

        Ok(res.is_empty())
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &Vec<u8>) -> Result<bool> {
        let res = self
            .client
            .query(POINT_DELETE, &[key])
            .await
            .context(PostgresExecutionSnafu)?;

        Ok(res.len() == 1)
    }

    /// Step down the leader. The leader should delete the key and notify the leader watcher.
    /// Do not check if the deletion is successful, since the key may be deleted by other followers.
    /// Caution: Should only step down while holding the advisory lock.
    async fn step_down(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            ..Default::default()
        };
        if self
            .is_leader
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.delete_value(&key).await?;
            self.client
                .query(UNLOCK, &[])
                .await
                .context(PostgresExecutionSnafu)?;
            self.leader_watcher
                .send(LeaderChangeMessage::StepDown(Arc::new(leader_key)))
                .context(SendLeaderChangeSnafu)?;
        }
        Ok(())
    }

    /// Elected as leader. The leader should put the key and notify the leader watcher.
    /// Caution: Should only elected while holding the advisory lock.
    async fn elected(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            ..Default::default()
        };
        self.delete_value(&key).await?;
        self.put_value_with_lease(
            &key,
            &ValueWithLease {
                value: self.leader_value.clone(),
                expire_time: time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()
                    + META_LEASE_SECS as f64,
            },
        )
        .await?;
        self.leader_watcher
            .send(LeaderChangeMessage::Elected(Arc::new(leader_key)))
            .context(SendLeaderChangeSnafu)?;
        Ok(())
    }

    /// Leader failed to acquire the advisory lock, just step down and tell the leader watcher.
    async fn step_down_without_lock(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            ..Default::default()
        };
        if self
            .is_leader
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.leader_watcher
                .send(LeaderChangeMessage::StepDown(Arc::new(leader_key)))
                .context(SendLeaderChangeSnafu)?;
        }
        Ok(())
    }

    async fn leader_action(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let new_leader_value_with_lease = ValueWithLease {
            value: self.leader_value.clone(),
            expire_time: now + META_LEASE_SECS as f64,
        };
        if self.is_leader() {
            // Old leader, renew the lease
            match self.get_value_with_lease(&key).await? {
                Some(prev) => {
                    if prev.value != self.leader_value || prev.expire_time <= now {
                        self.step_down().await?;
                    }
                    self.update_value_with_lease(&key, &prev, &new_leader_value_with_lease)
                        .await?;
                }
                None => {
                    warn!("Leader lease not found, but still hold the lock. Now stepping down.");
                    self.step_down().await?;
                }
            }
        } else {
            // Newly elected
            self.elected().await?;
        }
        Ok(())
    }

    async fn follower_action(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        // Previously held the advisory lock, but failed to acquire it. Step down.
        if self.is_leader() {
            self.step_down_without_lock().await?;
        }
        let prev = self.get_value_with_lease(&key).await?.ok_or_else(|| {
            UnexpectedSnafu {
                violated: "Advisory lock held by others but leader key not found",
            }
            .build()
        })?;
        if prev.expire_time
            <= time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
        {
            warn!("Leader lease expired, now re-init campaign.");
            return Err(NoLeaderSnafu {}.build());
        }
        Ok(())
    }
}
