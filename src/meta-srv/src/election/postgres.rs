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
use std::time::Duration;

use common_meta::distributed_time_constants::{META_KEEP_ALIVE_INTERVAL_SECS, META_LEASE_SECS};
use common_telemetry::{error, warn};
use common_time::Timestamp;
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use crate::election::{
    listen_leader_change, Election, LeaderChangeMessage, LeaderKey, CANDIDATES_ROOT, ELECTION_KEY,
};
use crate::error::{
    DeserializeFromJsonSnafu, NoLeaderSnafu, PostgresExecutionSnafu, Result, SerializeToJsonSnafu,
    UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

// TODO(CookiePie): The lock id should be configurable.
const CAMPAIGN: &str = "SELECT pg_try_advisory_lock({})";
const STEP_DOWN: &str = "SELECT pg_advisory_unlock({})";
// Currently the session timeout is longer than the leader lease time, so the leader lease may expire while the session is still alive.
// Either the leader reconnects and step down or the session expires and the lock is released.
const SET_IDLE_SESSION_TIMEOUT: &str = "SET idle_in_transaction_session_timeout = '10s';";

// Separator between value and expire time.
const LEASE_SEP: &str = r#"||__metadata_lease_sep||"#;

// SQL to put a value with expire time. Parameters: key, value, LEASE_SEP, expire_time
const PUT_IF_NOT_EXISTS_WITH_EXPIRE_TIME: &str = r#"
WITH prev AS (
    SELECT k, v FROM greptime_metakv WHERE k = $1
), insert AS (
    INSERT INTO greptime_metakv
    VALUES($1, convert_to($2 || $3 || TO_CHAR(CURRENT_TIMESTAMP + INTERVAL '1 second' * $4, 'YYYY-MM-DD HH24:MI:SS.MS'), 'UTF8'))
    ON CONFLICT (k) DO NOTHING
)

SELECT k, v FROM prev;
"#;

// SQL to update a value with expire time. Parameters: key, prev_value_with_lease, updated_value, LEASE_SEP, expire_time
const CAS_WITH_EXPIRE_TIME: &str = r#"
UPDATE greptime_metakv
SET k=$1,
v=convert_to($3 || $4 || TO_CHAR(CURRENT_TIMESTAMP + INTERVAL '1 second' * $5, 'YYYY-MM-DD HH24:MI:SS.MS'), 'UTF8')
WHERE 
    k=$1 AND v=$2
"#;

const GET_WITH_CURRENT_TIMESTAMP: &str = r#"SELECT v, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') FROM greptime_metakv WHERE k = $1"#;

const PREFIX_GET_WITH_CURRENT_TIMESTAMP: &str = r#"SELECT v, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') FROM greptime_metakv WHERE k LIKE $1"#;

const POINT_DELETE: &str = "DELETE FROM greptime_metakv WHERE k = $1 RETURNING k,v;";

fn campaign_sql(lock_id: u64) -> String {
    CAMPAIGN.replace("{}", &lock_id.to_string())
}

fn step_down_sql(lock_id: u64) -> String {
    STEP_DOWN.replace("{}", &lock_id.to_string())
}

/// Parse the value and expire time from the given string. The value should be in the format "value || LEASE_SEP || expire_time".
fn parse_value_and_expire_time(value: &str) -> Result<(String, Timestamp)> {
    let (value, expire_time) = value
        .split(LEASE_SEP)
        .collect_tuple()
        .context(UnexpectedSnafu {
            violated: format!(
                "Invalid value {}, expect node info || {} || expire time",
                value, LEASE_SEP
            ),
        })?;
    // Given expire_time is in the format 'YYYY-MM-DD HH24:MI:SS.MS'
    let expire_time = match Timestamp::from_str(expire_time, None) {
        Ok(ts) => ts,
        Err(_) => UnexpectedSnafu {
            violated: format!("Invalid timestamp: {}", expire_time),
        }
        .fail()?,
    };
    Ok((value.to_string(), expire_time))
}

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

    fn revision(&self) -> i64 {
        self.rev
    }

    fn lease_id(&self) -> i64 {
        self.lease
    }
}

/// PostgreSql implementation of Election.
pub struct PgElection {
    leader_value: String,
    client: Client,
    is_leader: AtomicBool,
    leader_infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
    candidate_lease_ttl_secs: u64,
    lock_id: u64,
}

impl PgElection {
    pub async fn with_pg_client(
        leader_value: String,
        client: Client,
        store_key_prefix: String,
        candidate_lease_ttl_secs: u64,
    ) -> Result<ElectionRef> {
        // Set idle session timeout to IDLE_SESSION_TIMEOUT to avoid dead advisory lock.
        client
            .execute(SET_IDLE_SESSION_TIMEOUT, &[])
            .await
            .context(PostgresExecutionSnafu)?;

        let tx = listen_leader_change(leader_value.clone());
        Ok(Arc::new(Self {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(false),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
            // TODO(CookiePie): The lock id should be configurable.
            lock_id: 28319,
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

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    fn in_leader_infancy(&self) -> bool {
        self.leader_infancy
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// TODO(CookiePie): Split the candidate registration and keep alive logic into separate methods, so that upper layers can call them separately.
    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        let key = self.candidate_key();
        let node_info =
            serde_json::to_string(node_info).with_context(|_| SerializeToJsonSnafu {
                input: format!("{node_info:?}"),
            })?;
        let res = self
            .put_value_with_lease(&key, &node_info, self.candidate_lease_ttl_secs)
            .await?;
        // May registered before, just update the lease.
        if !res {
            self.delete_value(&key).await?;
            self.put_value_with_lease(&key, &node_info, self.candidate_lease_ttl_secs)
                .await?;
        }

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(self.candidate_lease_ttl_secs / 2));
        loop {
            let _ = keep_alive_interval.tick().await;

            let (_, prev_expire_time, current_time, origin) = self
                .get_value_with_lease(&key, true)
                .await?
                .unwrap_or_default();

            ensure!(
                prev_expire_time > current_time,
                UnexpectedSnafu {
                    violated: format!(
                        "Candidate lease expired, key: {:?}",
                        String::from_utf8_lossy(&key.into_bytes())
                    ),
                }
            );

            // Safety: origin is Some since we are using `get_value_with_lease` with `true`.
            let origin = origin.unwrap();
            self.update_value_with_lease(&key, &origin, &node_info)
                .await?;
        }
    }

    async fn all_candidates(&self) -> Result<Vec<MetasrvNodeInfo>> {
        let key_prefix = self.candidate_root();
        let (mut candidates, current) = self.get_value_with_lease_by_prefix(&key_prefix).await?;
        // Remove expired candidates
        candidates.retain(|c| c.1 > current);
        let mut valid_candidates = Vec::with_capacity(candidates.len());
        for (c, _) in candidates {
            let node_info: MetasrvNodeInfo =
                serde_json::from_str(&c).with_context(|_| DeserializeFromJsonSnafu {
                    input: format!("{:?}", c),
                })?;
            valid_candidates.push(node_info);
        }
        Ok(valid_candidates)
    }

    /// Attempts to acquire leadership by executing a campaign. This function continuously checks
    /// if the current instance can become the leader by acquiring an advisory lock in the PostgreSQL database.
    ///
    /// The function operates in a loop, where it:
    ///
    /// 1. Waits for a predefined interval before attempting to acquire the lock again.
    /// 2. Executes the `CAMPAIGN` SQL query to try to acquire the advisory lock.
    /// 3. Checks the result of the query:
    ///    - If the lock is successfully acquired (result is true), it calls the `leader_action` method
    ///      to perform actions as the leader.
    ///    - If the lock is not acquired (result is false), it calls the `follower_action` method
    ///      to perform actions as a follower.
    async fn campaign(&self) -> Result<()> {
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS));
        keep_alive_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let res = self
                .client
                .query(&campaign_sql(self.lock_id), &[])
                .await
                .context(PostgresExecutionSnafu)?;
            if let Some(row) = res.first() {
                match row.try_get(0) {
                    Ok(true) => self.leader_action().await?,
                    Ok(false) => self.follower_action().await?,
                    Err(_) => {
                        return UnexpectedSnafu {
                            violated: "Failed to get the result of acquiring advisory lock"
                                .to_string(),
                        }
                        .fail();
                    }
                }
            } else {
                return UnexpectedSnafu {
                    violated: "Failed to get the result of acquiring advisory lock".to_string(),
                }
                .fail();
            }
            let _ = keep_alive_interval.tick().await;
        }
    }

    async fn leader(&self) -> Result<Self::Leader> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(self.leader_value.as_bytes().into())
        } else {
            let key = self.election_key();
            if let Some((leader, expire_time, current, _)) =
                self.get_value_with_lease(&key, false).await?
            {
                ensure!(expire_time > current, NoLeaderSnafu);
                Ok(leader.as_bytes().into())
            } else {
                NoLeaderSnafu.fail()
            }
        }
    }

    async fn resign(&self) -> Result<()> {
        todo!()
    }

    fn subscribe_leader_change(&self) -> broadcast::Receiver<LeaderChangeMessage> {
        self.leader_watcher.subscribe()
    }
}

impl PgElection {
    /// Returns value, expire time and current time. If `with_origin` is true, the origin string is also returned.
    async fn get_value_with_lease(
        &self,
        key: &str,
        with_origin: bool,
    ) -> Result<Option<(String, Timestamp, Timestamp, Option<String>)>> {
        let key = key.as_bytes().to_vec();
        let res = self
            .client
            .query(GET_WITH_CURRENT_TIMESTAMP, &[&key as &(dyn ToSql + Sync)])
            .await
            .context(PostgresExecutionSnafu)?;

        if res.is_empty() {
            Ok(None)
        } else {
            // Safety: Checked if res is empty above.
            let current_time_str = res[0].try_get(1).unwrap_or_default();
            let current_time = match Timestamp::from_str(current_time_str, None) {
                Ok(ts) => ts,
                Err(_) => UnexpectedSnafu {
                    violated: format!("Invalid timestamp: {}", current_time_str),
                }
                .fail()?,
            };
            // Safety: Checked if res is empty above.
            let value_and_expire_time =
                String::from_utf8_lossy(res[0].try_get(0).unwrap_or_default());
            let (value, expire_time) = parse_value_and_expire_time(&value_and_expire_time)?;

            if with_origin {
                Ok(Some((
                    value,
                    expire_time,
                    current_time,
                    Some(value_and_expire_time.to_string()),
                )))
            } else {
                Ok(Some((value, expire_time, current_time, None)))
            }
        }
    }

    /// Returns all values and expire time with the given key prefix. Also returns the current time.
    async fn get_value_with_lease_by_prefix(
        &self,
        key_prefix: &str,
    ) -> Result<(Vec<(String, Timestamp)>, Timestamp)> {
        let key_prefix = format!("{}%", key_prefix).as_bytes().to_vec();
        let res = self
            .client
            .query(
                PREFIX_GET_WITH_CURRENT_TIMESTAMP,
                &[(&key_prefix as &(dyn ToSql + Sync))],
            )
            .await
            .context(PostgresExecutionSnafu)?;

        let mut values_with_leases = vec![];
        let mut current = Timestamp::default();
        for row in res {
            let current_time_str = row.try_get(1).unwrap_or_default();
            current = match Timestamp::from_str(current_time_str, None) {
                Ok(ts) => ts,
                Err(_) => UnexpectedSnafu {
                    violated: format!("Invalid timestamp: {}", current_time_str),
                }
                .fail()?,
            };

            let value_and_expire_time = String::from_utf8_lossy(row.try_get(0).unwrap_or_default());
            let (value, expire_time) = parse_value_and_expire_time(&value_and_expire_time)?;

            values_with_leases.push((value, expire_time));
        }
        Ok((values_with_leases, current))
    }

    async fn update_value_with_lease(&self, key: &str, prev: &str, updated: &str) -> Result<()> {
        let key = key.as_bytes().to_vec();
        let prev = prev.as_bytes().to_vec();
        let res = self
            .client
            .execute(
                CAS_WITH_EXPIRE_TIME,
                &[
                    &key as &(dyn ToSql + Sync),
                    &prev as &(dyn ToSql + Sync),
                    &updated,
                    &LEASE_SEP,
                    &(self.candidate_lease_ttl_secs as f64),
                ],
            )
            .await
            .context(PostgresExecutionSnafu)?;

        ensure!(
            res == 1,
            UnexpectedSnafu {
                violated: format!("Failed to update key: {}", String::from_utf8_lossy(&key)),
            }
        );

        Ok(())
    }

    /// Returns `true` if the insertion is successful
    async fn put_value_with_lease(
        &self,
        key: &str,
        value: &str,
        lease_ttl_secs: u64,
    ) -> Result<bool> {
        let key = key.as_bytes().to_vec();
        let lease_ttl_secs = lease_ttl_secs as f64;
        let params: Vec<&(dyn ToSql + Sync)> = vec![
            &key as &(dyn ToSql + Sync),
            &value as &(dyn ToSql + Sync),
            &LEASE_SEP,
            &lease_ttl_secs,
        ];
        let res = self
            .client
            .query(PUT_IF_NOT_EXISTS_WITH_EXPIRE_TIME, &params)
            .await
            .context(PostgresExecutionSnafu)?;
        Ok(res.is_empty())
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &str) -> Result<bool> {
        let key = key.as_bytes().to_vec();
        let res = self
            .client
            .query(POINT_DELETE, &[&key as &(dyn ToSql + Sync)])
            .await
            .context(PostgresExecutionSnafu)?;

        Ok(res.len() == 1)
    }

    /// Handles the actions of a leader in the election process.
    ///
    /// This function performs the following checks and actions:
    ///
    /// - **Case 1**: If the current instance believes it is the leader from the previous term,
    ///   it attempts to renew the lease. It checks if the lease is still valid and either renews it
    ///   or steps down if it has expired.
    ///
    ///   - **Case 1.1**: If the instance is still the leader and the lease is valid, it renews the lease
    ///     by updating the value associated with the election key.
    ///   - **Case 1.2**: If the instance is still the leader but the lease has expired, it logs a warning
    ///     and steps down, initiating a new campaign for leadership.
    ///   - **Case 1.3**: If the instance is not the leader (which is a rare scenario), it logs a warning
    ///     indicating that it still holds the lock and steps down to re-initiate the campaign. This may
    ///     happen if the leader has failed to renew the lease and the session has expired, and recovery
    ///     after a period of time during which other leaders have been elected and stepped down.
    ///   - **Case 1.4**: If no lease information is found, it also steps down and re-initiates the campaign.
    ///
    /// - **Case 2**: If the current instance is not leader previously, it calls the
    ///   `elected` method as a newly elected leader.
    async fn leader_action(&self) -> Result<()> {
        let key = self.election_key();
        // Case 1
        if self.is_leader() {
            match self.get_value_with_lease(&key, true).await? {
                Some((prev_leader, expire_time, current, prev)) => {
                    match (prev_leader == self.leader_value, expire_time > current) {
                        // Case 1.1
                        (true, true) => {
                            // Safety: prev is Some since we are using `get_value_with_lease` with `true`.
                            let prev = prev.unwrap();
                            self.update_value_with_lease(&key, &prev, &self.leader_value)
                                .await?;
                        }
                        // Case 1.2
                        (true, false) => {
                            warn!("Leader lease expired, now stepping down.");
                            self.step_down().await?;
                        }
                        // Case 1.3
                        (false, _) => {
                            warn!("Leader lease not found, but still hold the lock. Now stepping down.");
                            self.step_down().await?;
                        }
                    }
                }
                // Case 1.4
                None => {
                    warn!("Leader lease not found, but still hold the lock. Now stepping down.");
                    self.step_down().await?;
                }
            }
        // Case 2
        } else {
            self.elected().await?;
        }
        Ok(())
    }

    /// Handles the actions of a follower in the election process.
    ///
    /// This function performs the following checks and actions:
    ///
    /// - **Case 1**: If the current instance believes it is the leader from the previous term,
    ///   it steps down without deleting the key.
    /// - **Case 2**: If the current instance is not the leader but the lease has expired, it raises an error
    ///   to re-initiate the campaign. If the leader failed to renew the lease, its session will expire and the lock
    ///   will be released.
    /// - **Case 3**: If all checks pass, the function returns without performing any actions.
    async fn follower_action(&self) -> Result<()> {
        let key = self.election_key();
        // Case 1
        if self.is_leader() {
            self.step_down_without_lock().await?;
        }
        let (_, expire_time, current, _) = self
            .get_value_with_lease(&key, false)
            .await?
            .context(NoLeaderSnafu)?;
        // Case 2
        ensure!(expire_time > current, NoLeaderSnafu);
        // Case 3
        Ok(())
    }

    /// Step down the leader. The leader should delete the key and notify the leader watcher.
    ///
    /// __DO NOT__ check if the deletion is successful, since the key may be deleted by others elected.
    ///
    /// ## Caution:
    /// Should only step down while holding the advisory lock.
    async fn step_down(&self) -> Result<()> {
        let key = self.election_key();
        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        if self
            .is_leader
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.delete_value(&key).await?;
            self.client
                .query(&step_down_sql(self.lock_id), &[])
                .await
                .context(PostgresExecutionSnafu)?;
            if let Err(e) = self
                .leader_watcher
                .send(LeaderChangeMessage::StepDown(Arc::new(leader_key)))
            {
                error!(e; "Failed to send leader change message");
            }
        }
        Ok(())
    }

    /// Still consider itself as the leader locally but failed to acquire the lock. Step down without deleting the key.
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
            if let Err(e) = self
                .leader_watcher
                .send(LeaderChangeMessage::StepDown(Arc::new(leader_key)))
            {
                error!(e; "Failed to send leader change message");
            }
        }
        Ok(())
    }

    /// Elected as leader. The leader should put the key and notify the leader watcher.
    /// Caution: Should only elected while holding the advisory lock.
    async fn elected(&self) -> Result<()> {
        let key = self.election_key();
        let leader_key = PgLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        self.delete_value(&key).await?;
        self.put_value_with_lease(&key, &self.leader_value, META_LEASE_SECS)
            .await?;

        if self
            .is_leader
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.leader_infancy.store(true, Ordering::Relaxed);

            if let Err(e) = self
                .leader_watcher
                .send(LeaderChangeMessage::Elected(Arc::new(leader_key)))
            {
                error!(e; "Failed to send leader change message");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tokio_postgres::{Client, NoTls};

    use super::*;
    use crate::error::PostgresExecutionSnafu;

    async fn create_postgres_client() -> Result<Client> {
        let endpoint = env::var("GT_POSTGRES_ENDPOINTS").unwrap_or_default();
        if endpoint.is_empty() {
            return UnexpectedSnafu {
                violated: "Postgres endpoint is empty".to_string(),
            }
            .fail();
        }
        let (client, connection) = tokio_postgres::connect(&endpoint, NoTls)
            .await
            .context(PostgresExecutionSnafu)?;
        tokio::spawn(async move {
            connection.await.context(PostgresExecutionSnafu).unwrap();
        });
        Ok(client)
    }

    #[tokio::test]
    async fn test_postgres_crud() {
        let client = create_postgres_client().await.unwrap();

        let key = "test_key".to_string();
        let value = "test_value".to_string();

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value: "test_leader".to_string(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid::Uuid::new_v4().to_string(),
            candidate_lease_ttl_secs: 10,
            lock_id: 28319,
        };

        let res = pg_election
            .put_value_with_lease(&key, &value, 10)
            .await
            .unwrap();
        assert!(res);

        let (value, _, _, prev) = pg_election
            .get_value_with_lease(&key, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, value);

        let prev = prev.unwrap();
        pg_election
            .update_value_with_lease(&key, &prev, &value)
            .await
            .unwrap();

        let res = pg_election.delete_value(&key).await.unwrap();
        assert!(res);

        let res = pg_election.get_value_with_lease(&key, false).await.unwrap();
        assert!(res.is_none());

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let value = format!("test_value_{}", i);
            pg_election
                .put_value_with_lease(&key, &value, 10)
                .await
                .unwrap();
        }

        let key_prefix = "test_key".to_string();
        let (res, _) = pg_election
            .get_value_with_lease_by_prefix(&key_prefix)
            .await
            .unwrap();
        assert_eq!(res.len(), 10);

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let res = pg_election.delete_value(&key).await.unwrap();
            assert!(res);
        }

        let (res, current) = pg_election
            .get_value_with_lease_by_prefix(&key_prefix)
            .await
            .unwrap();
        assert!(res.is_empty());
        assert!(current == Timestamp::default());
    }

    async fn candidate(
        leader_value: String,
        candidate_lease_ttl_secs: u64,
        store_key_prefix: String,
    ) {
        let client = create_postgres_client().await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
            lock_id: 28319,
        };

        let node_info = MetasrvNodeInfo {
            addr: "test_addr".to_string(),
            version: "test_version".to_string(),
            git_commit: "test_git_commit".to_string(),
            start_time_ms: 0,
        };
        pg_election.register_candidate(&node_info).await.unwrap();
    }

    #[tokio::test]
    async fn test_candidate_registration() {
        let leader_value_prefix = "test_leader".to_string();
        let candidate_lease_ttl_secs = 5;
        let store_key_prefix = uuid::Uuid::new_v4().to_string();
        let mut handles = vec![];
        for i in 0..10 {
            let leader_value = format!("{}{}", leader_value_prefix, i);
            let handle = tokio::spawn(candidate(
                leader_value,
                candidate_lease_ttl_secs,
                store_key_prefix.clone(),
            ));
            handles.push(handle);
        }
        // Wait for candidates to registrate themselves and renew their leases at least once.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let client = create_postgres_client().await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let leader_value = "test_leader".to_string();
        let pg_election = PgElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: store_key_prefix.clone(),
            candidate_lease_ttl_secs,
            lock_id: 28319,
        };

        let candidates = pg_election.all_candidates().await.unwrap();
        assert_eq!(candidates.len(), 10);

        for handle in handles {
            handle.abort();
        }

        // Wait for the candidate leases to expire.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let candidates = pg_election.all_candidates().await.unwrap();
        assert!(candidates.is_empty());

        // Garbage collection
        for i in 0..10 {
            let key = format!(
                "{}{}{}{}",
                store_key_prefix, CANDIDATES_ROOT, leader_value_prefix, i
            );
            let res = pg_election.delete_value(&key).await.unwrap();
            assert!(res);
        }
    }

    #[tokio::test]
    async fn test_elected_and_step_down() {
        let leader_value = "test_leader".to_string();
        let candidate_lease_ttl_secs = 5;
        let client = create_postgres_client().await.unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_pg_election = PgElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid::Uuid::new_v4().to_string(),
            candidate_lease_ttl_secs,
            lock_id: 28320,
        };

        leader_pg_election.elected().await.unwrap();
        let (leader, expire_time, current, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(expire_time > current);
        assert!(leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        leader_pg_election.step_down_without_lock().await.unwrap();
        let (leader, _, _, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(!leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        leader_pg_election.elected().await.unwrap();
        let (leader, expire_time, current, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(expire_time > current);
        assert!(leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        leader_pg_election.step_down().await.unwrap();
        let res = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap();
        assert!(res.is_none());
        assert!(!leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }
    }

    #[tokio::test]
    async fn test_leader_action() {
        let leader_value = "test_leader".to_string();
        let store_key_prefix = uuid::Uuid::new_v4().to_string();
        let candidate_lease_ttl_secs = 5;
        let client = create_postgres_client().await.unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_pg_election = PgElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
            lock_id: 28321,
        };

        // Step 1: No leader exists, campaign and elected.
        let res = leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let (leader, expire_time, current, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(expire_time > current);
        assert!(leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        // Step 2: As a leader, renew the lease.
        let res = leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let (leader, new_expire_time, current, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(new_expire_time > current && new_expire_time > expire_time);
        assert!(leader_pg_election.is_leader());

        // Step 3: Something wrong, the leader lease expired.
        tokio::time::sleep(Duration::from_secs(META_LEASE_SECS)).await;

        let res = leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let res = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap();
        assert!(res.is_none());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        // Step 4: Re-campaign and elected.
        let res = leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let (leader, expire_time, current, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(expire_time > current);
        assert!(leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        // Step 5: Something wrong, the leader key is deleted by other followers.
        leader_pg_election
            .delete_value(&leader_pg_election.election_key())
            .await
            .unwrap();
        leader_pg_election.leader_action().await.unwrap();
        let res = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap();
        assert!(res.is_none());
        assert!(!leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        // Step 6: Re-campaign and elected.
        let res = leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let (leader, expire_time, current, _) = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(leader == leader_value);
        assert!(expire_time > current);
        assert!(leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        // Step 7: Something wrong, the leader key changed by others.
        let res = leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election
            .delete_value(&leader_pg_election.election_key())
            .await
            .unwrap();
        leader_pg_election
            .put_value_with_lease(&leader_pg_election.election_key(), "test", 10)
            .await
            .unwrap();
        leader_pg_election.leader_action().await.unwrap();
        let res = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key(), false)
            .await
            .unwrap();
        assert!(res.is_none());
        assert!(!leader_pg_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        // Clean up
        leader_pg_election
            .client
            .query(&step_down_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_follower_action() {
        let candidate_lease_ttl_secs = 5;
        let store_key_prefix = uuid::Uuid::new_v4().to_string();

        let follower_client = create_postgres_client().await.unwrap();
        let (tx, mut rx) = broadcast::channel(100);
        let follower_pg_election = PgElection {
            leader_value: "test_follower".to_string(),
            client: follower_client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: store_key_prefix.clone(),
            candidate_lease_ttl_secs,
            lock_id: 28322,
        };

        let leader_client = create_postgres_client().await.unwrap();
        let (tx, _) = broadcast::channel(100);
        let leader_pg_election = PgElection {
            leader_value: "test_leader".to_string(),
            client: leader_client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
            lock_id: 28322,
        };

        leader_pg_election
            .client
            .query(&campaign_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
        leader_pg_election.elected().await.unwrap();

        // Step 1: As a follower, the leader exists and the lease is not expired.
        follower_pg_election.follower_action().await.unwrap();

        // Step 2: As a follower, the leader exists but the lease expired.
        tokio::time::sleep(Duration::from_secs(META_LEASE_SECS)).await;
        assert!(follower_pg_election.follower_action().await.is_err());

        // Step 3: As a follower, the leader does not exist.
        leader_pg_election
            .delete_value(&leader_pg_election.election_key())
            .await
            .unwrap();
        assert!(follower_pg_election.follower_action().await.is_err());

        // Step 4: Follower thinks it's the leader but failed to acquire the lock.
        follower_pg_election
            .is_leader
            .store(true, Ordering::Relaxed);
        assert!(follower_pg_election.follower_action().await.is_err());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), "test_follower");
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    follower_pg_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        // Clean up
        leader_pg_election
            .client
            .query(&step_down_sql(leader_pg_election.lock_id), &[])
            .await
            .unwrap();
    }
}
