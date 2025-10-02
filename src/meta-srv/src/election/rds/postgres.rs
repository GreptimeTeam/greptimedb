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

use common_meta::key::{CANDIDATES_ROOT, ELECTION_KEY};
use common_telemetry::{error, warn};
use common_time::Timestamp;
use deadpool_postgres::{Manager, Pool};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::{broadcast, RwLock};
use tokio::time::MissedTickBehavior;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use crate::election::rds::{parse_value_and_expire_time, Lease, RdsLeaderKey, LEASE_SEP};
use crate::election::{
    listen_leader_change, send_leader_change_and_set_flags, Election, LeaderChangeMessage,
};
use crate::error::{
    DeserializeFromJsonSnafu, GetPostgresClientSnafu, NoLeaderSnafu, PostgresExecutionSnafu,
    Result, SerializeToJsonSnafu, SqlExecutionTimeoutSnafu, UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

struct ElectionSqlFactory<'a> {
    lock_id: u64,
    table_name: &'a str,
}

struct ElectionSqlSet {
    campaign: String,
    step_down: String,
    // SQL to put a value with expire time.
    //
    // Parameters for the query:
    // `$1`: key,
    // `$2`: value,
    // `$3`: lease time in seconds
    //
    // Returns:
    // If the key already exists, return the previous value.
    put_value_with_lease: String,
    // SQL to update a value with expire time.
    //
    // Parameters for the query:
    // `$1`: key,
    // `$2`: previous value,
    // `$3`: updated value,
    // `$4`: lease time in seconds
    update_value_with_lease: String,
    // SQL to get a value with expire time.
    //
    // Parameters:
    // `$1`: key
    get_value_with_lease: String,
    // SQL to get all values with expire time with the given key prefix.
    //
    // Parameters:
    // `$1`: key prefix like 'prefix%'
    //
    // Returns:
    // column 0: value,
    // column 1: current timestamp
    get_value_with_lease_by_prefix: String,
    // SQL to delete a value.
    //
    // Parameters:
    // `$1`: key
    //
    // Returns:
    // column 0: key deleted,
    // column 1: value deleted
    delete_value: String,
}

impl<'a> ElectionSqlFactory<'a> {
    fn new(lock_id: u64, table_name: &'a str) -> Self {
        Self {
            lock_id,
            table_name,
        }
    }

    fn build(self) -> ElectionSqlSet {
        ElectionSqlSet {
            campaign: self.campaign_sql(),
            step_down: self.step_down_sql(),
            put_value_with_lease: self.put_value_with_lease_sql(),
            update_value_with_lease: self.update_value_with_lease_sql(),
            get_value_with_lease: self.get_value_with_lease_sql(),
            get_value_with_lease_by_prefix: self.get_value_with_lease_by_prefix_sql(),
            delete_value: self.delete_value_sql(),
        }
    }

    fn campaign_sql(&self) -> String {
        format!("SELECT pg_try_advisory_lock({})", self.lock_id)
    }

    fn step_down_sql(&self) -> String {
        format!("SELECT pg_advisory_unlock({})", self.lock_id)
    }

    fn put_value_with_lease_sql(&self) -> String {
        format!(
            r#"WITH prev AS (
                SELECT k, v FROM "{}" WHERE k = $1
            ), insert AS (
                INSERT INTO "{}"
                VALUES($1, convert_to($2 || '{}' || TO_CHAR(CURRENT_TIMESTAMP + INTERVAL '1 second' * $3, 'YYYY-MM-DD HH24:MI:SS.MS'), 'UTF8'))
                ON CONFLICT (k) DO NOTHING
            )
            SELECT k, v FROM prev;
            "#,
            self.table_name, self.table_name, LEASE_SEP
        )
    }

    fn update_value_with_lease_sql(&self) -> String {
        format!(
            r#"UPDATE "{}"
               SET v = convert_to($3 || '{}' || TO_CHAR(CURRENT_TIMESTAMP + INTERVAL '1 second' * $4, 'YYYY-MM-DD HH24:MI:SS.MS'), 'UTF8')
               WHERE k = $1 AND v = $2"#,
            self.table_name, LEASE_SEP
        )
    }

    fn get_value_with_lease_sql(&self) -> String {
        format!(
            r#"SELECT v, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') FROM "{}" WHERE k = $1"#,
            self.table_name
        )
    }

    fn get_value_with_lease_by_prefix_sql(&self) -> String {
        format!(
            r#"SELECT v, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') FROM "{}" WHERE k LIKE $1"#,
            self.table_name
        )
    }

    fn delete_value_sql(&self) -> String {
        format!(
            "DELETE FROM \"{}\" WHERE k = $1 RETURNING k,v;",
            self.table_name
        )
    }
}

/// PgClient for election.
pub struct ElectionPgClient {
    current: Option<deadpool::managed::Object<Manager>>,
    pool: Pool,
    /// The client-side timeout for statement execution.
    ///
    /// This timeout is enforced by the client application and is independent of any server-side timeouts.
    /// If a statement takes longer than this duration to execute, the client will abort the operation.
    execution_timeout: Duration,

    /// The idle session timeout.
    ///
    /// This timeout is configured per client session and is enforced by the PostgreSQL server.
    /// If a session remains idle for longer than this duration, the server will terminate it.
    idle_session_timeout: Duration,

    /// The statement timeout.
    ///
    /// This timeout is configured per client session and is enforced by the PostgreSQL server.
    /// If a statement takes longer than this duration to execute, the server will abort it.
    statement_timeout: Duration,
}

impl ElectionPgClient {
    pub fn new(
        pool: Pool,
        execution_timeout: Duration,
        idle_session_timeout: Duration,
        statement_timeout: Duration,
    ) -> Result<ElectionPgClient> {
        Ok(ElectionPgClient {
            current: None,
            pool,
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        })
    }

    fn set_idle_session_timeout_sql(&self) -> String {
        format!(
            "SET idle_session_timeout = '{}s';",
            self.idle_session_timeout.as_secs()
        )
    }

    fn set_statement_timeout_sql(&self) -> String {
        format!(
            "SET statement_timeout = '{}s';",
            self.statement_timeout.as_secs()
        )
    }

    async fn reset_client(&mut self) -> Result<()> {
        self.current = None;
        self.maybe_init_client().await
    }

    async fn maybe_init_client(&mut self) -> Result<()> {
        if self.current.is_none() {
            let client = self.pool.get().await.context(GetPostgresClientSnafu)?;

            self.current = Some(client);
            // Set idle session timeout and statement timeout.
            let idle_session_timeout_sql = self.set_idle_session_timeout_sql();
            self.execute(&idle_session_timeout_sql, &[]).await?;
            let statement_timeout_sql = self.set_statement_timeout_sql();
            self.execute(&statement_timeout_sql, &[]).await?;
        }

        Ok(())
    }

    /// Returns the result of the query.
    ///
    /// # Panics
    /// if `current` is `None`.
    async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        let result = tokio::time::timeout(
            self.execution_timeout,
            self.current.as_ref().unwrap().execute(sql, params),
        )
        .await
        .map_err(|_| {
            SqlExecutionTimeoutSnafu {
                sql: sql.to_string(),
                duration: self.execution_timeout,
            }
            .build()
        })?;

        result.context(PostgresExecutionSnafu { sql })
    }

    /// Returns the result of the query.
    ///
    /// # Panics
    /// if `current` is `None`.
    async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        let result = tokio::time::timeout(
            self.execution_timeout,
            self.current.as_ref().unwrap().query(sql, params),
        )
        .await
        .map_err(|_| {
            SqlExecutionTimeoutSnafu {
                sql: sql.to_string(),
                duration: self.execution_timeout,
            }
            .build()
        })?;

        result.context(PostgresExecutionSnafu { sql })
    }
}

/// PostgreSql implementation of Election.
pub struct PgElection {
    leader_value: String,
    pg_client: RwLock<ElectionPgClient>,
    is_leader: AtomicBool,
    leader_infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
    candidate_lease_ttl: Duration,
    meta_lease_ttl: Duration,
    sql_set: ElectionSqlSet,
}

impl PgElection {
    async fn maybe_init_client(&self) -> Result<()> {
        if self.pg_client.read().await.current.is_none() {
            self.pg_client.write().await.maybe_init_client().await?;
        }

        Ok(())
    }

    pub async fn with_pg_client(
        leader_value: String,
        pg_client: ElectionPgClient,
        store_key_prefix: String,
        candidate_lease_ttl: Duration,
        meta_lease_ttl: Duration,
        table_name: &str,
        lock_id: u64,
    ) -> Result<ElectionRef> {
        let sql_factory = ElectionSqlFactory::new(lock_id, table_name);

        let tx = listen_leader_change(leader_value.clone());
        Ok(Arc::new(Self {
            leader_value,
            pg_client: RwLock::new(pg_client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(false),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: sql_factory.build(),
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
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        let key = self.candidate_key();
        let node_info =
            serde_json::to_string(node_info).with_context(|_| SerializeToJsonSnafu {
                input: format!("{node_info:?}"),
            })?;
        let res = self
            .put_value_with_lease(&key, &node_info, self.candidate_lease_ttl)
            .await?;
        // May registered before, just update the lease.
        if !res {
            self.delete_value(&key).await?;
            self.put_value_with_lease(&key, &node_info, self.candidate_lease_ttl)
                .await?;
        }

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval = tokio::time::interval(self.candidate_lease_ttl / 2);
        loop {
            let _ = keep_alive_interval.tick().await;

            let lease = self
                .get_value_with_lease(&key)
                .await?
                .context(UnexpectedSnafu {
                    violated: format!("Failed to get lease for key: {:?}", key),
                })?;

            ensure!(
                lease.expire_time > lease.current,
                UnexpectedSnafu {
                    violated: format!(
                        "Candidate lease expired at {:?} (current time {:?}), key: {:?}",
                        lease.expire_time, lease.current, key
                    ),
                }
            );

            // Safety: origin is Some since we are using `get_value_with_lease` with `true`.
            self.update_value_with_lease(&key, &lease.origin, &node_info, self.candidate_lease_ttl)
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
        let mut keep_alive_interval = tokio::time::interval(self.meta_lease_ttl / 2);
        keep_alive_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        self.maybe_init_client().await?;
        loop {
            let res = self
                .pg_client
                .read()
                .await
                .query(&self.sql_set.campaign, &[])
                .await?;
            let row = res.first().context(UnexpectedSnafu {
                violated: "Failed to get the result of acquiring advisory lock",
            })?;
            let is_leader = row.try_get(0).map_err(|_| {
                UnexpectedSnafu {
                    violated: "Failed to get the result of get lock",
                }
                .build()
            })?;
            if is_leader {
                self.leader_action().await?;
            } else {
                self.follower_action().await?;
            }
            let _ = keep_alive_interval.tick().await;
        }
    }

    async fn reset_campaign(&self) {
        if let Err(err) = self.pg_client.write().await.reset_client().await {
            error!(err; "Failed to reset client");
        }
    }

    async fn leader(&self) -> Result<Self::Leader> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(self.leader_value.as_bytes().into())
        } else {
            let key = self.election_key();
            if let Some(lease) = self.get_value_with_lease(&key).await? {
                ensure!(lease.expire_time > lease.current, NoLeaderSnafu);
                Ok(lease.leader_value.as_bytes().into())
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
    async fn get_value_with_lease(&self, key: &str) -> Result<Option<Lease>> {
        let key = key.as_bytes();
        self.maybe_init_client().await?;
        let res = self
            .pg_client
            .read()
            .await
            .query(&self.sql_set.get_value_with_lease, &[&key])
            .await?;

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

            Ok(Some(Lease {
                leader_value: value,
                expire_time,
                current: current_time,
                origin: value_and_expire_time.to_string(),
            }))
        }
    }

    /// Returns all values and expire time with the given key prefix. Also returns the current time.
    async fn get_value_with_lease_by_prefix(
        &self,
        key_prefix: &str,
    ) -> Result<(Vec<(String, Timestamp)>, Timestamp)> {
        let key_prefix = format!("{}%", key_prefix).as_bytes().to_vec();
        self.maybe_init_client().await?;
        let res = self
            .pg_client
            .read()
            .await
            .query(&self.sql_set.get_value_with_lease_by_prefix, &[&key_prefix])
            .await?;

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

    async fn update_value_with_lease(
        &self,
        key: &str,
        prev: &str,
        updated: &str,
        lease_ttl: Duration,
    ) -> Result<()> {
        let key = key.as_bytes();
        let prev = prev.as_bytes();
        self.maybe_init_client().await?;
        let lease_ttl_secs = lease_ttl.as_secs() as f64;
        let res = self
            .pg_client
            .read()
            .await
            .execute(
                &self.sql_set.update_value_with_lease,
                &[&key, &prev, &updated, &lease_ttl_secs],
            )
            .await?;

        ensure!(
            res == 1,
            UnexpectedSnafu {
                violated: format!("Failed to update key: {}", String::from_utf8_lossy(key)),
            }
        );

        Ok(())
    }

    /// Returns `true` if the insertion is successful
    async fn put_value_with_lease(
        &self,
        key: &str,
        value: &str,
        lease_ttl: Duration,
    ) -> Result<bool> {
        let key = key.as_bytes();
        let lease_ttl_secs = lease_ttl.as_secs() as f64;
        let params: Vec<&(dyn ToSql + Sync)> = vec![&key, &value, &lease_ttl_secs];
        self.maybe_init_client().await?;
        let res = self
            .pg_client
            .read()
            .await
            .query(&self.sql_set.put_value_with_lease, &params)
            .await?;
        Ok(res.is_empty())
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &str) -> Result<bool> {
        let key = key.as_bytes();
        self.maybe_init_client().await?;
        let res = self
            .pg_client
            .read()
            .await
            .query(&self.sql_set.delete_value, &[&key])
            .await?;

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
            match self.get_value_with_lease(&key).await? {
                Some(lease) => {
                    match (
                        lease.leader_value == self.leader_value,
                        lease.expire_time > lease.current,
                    ) {
                        // Case 1.1
                        (true, true) => {
                            // Safety: prev is Some since we are using `get_value_with_lease` with `true`.
                            self.update_value_with_lease(
                                &key,
                                &lease.origin,
                                &self.leader_value,
                                self.meta_lease_ttl,
                            )
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
        let lease = self
            .get_value_with_lease(&key)
            .await?
            .context(NoLeaderSnafu)?;
        // Case 2
        ensure!(lease.expire_time > lease.current, NoLeaderSnafu);
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
        let leader_key = RdsLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        self.delete_value(&key).await?;
        self.maybe_init_client().await?;
        self.pg_client
            .read()
            .await
            .query(&self.sql_set.step_down, &[])
            .await?;
        send_leader_change_and_set_flags(
            &self.is_leader,
            &self.leader_infancy,
            &self.leader_watcher,
            LeaderChangeMessage::StepDown(Arc::new(leader_key)),
        );
        Ok(())
    }

    /// Still consider itself as the leader locally but failed to acquire the lock. Step down without deleting the key.
    async fn step_down_without_lock(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let leader_key = RdsLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            ..Default::default()
        };
        if self
            .is_leader
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
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
        let leader_key = RdsLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        self.delete_value(&key).await?;
        self.put_value_with_lease(&key, &self.leader_value, self.meta_lease_ttl)
            .await?;

        if self
            .is_leader
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.leader_infancy.store(true, Ordering::Release);

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
    use std::assert_matches::assert_matches;
    use std::env;

    use common_meta::maybe_skip_postgres_integration_test;

    use super::*;
    use crate::bootstrap::create_postgres_pool;
    use crate::error;

    async fn create_postgres_client(
        table_name: Option<&str>,
        execution_timeout: Duration,
        idle_session_timeout: Duration,
        statement_timeout: Duration,
    ) -> Result<ElectionPgClient> {
        let endpoint = env::var("GT_POSTGRES_ENDPOINTS").unwrap_or_default();
        if endpoint.is_empty() {
            return UnexpectedSnafu {
                violated: "Postgres endpoint is empty".to_string(),
            }
            .fail();
        }
        let pool = create_postgres_pool(&[endpoint], None).await.unwrap();
        let mut pg_client = ElectionPgClient::new(
            pool,
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .unwrap();
        pg_client.maybe_init_client().await?;
        if let Some(table_name) = table_name {
            let create_table_sql = format!(
                "CREATE TABLE IF NOT EXISTS \"{}\"(k bytea PRIMARY KEY, v bytea);",
                table_name
            );
            pg_client.execute(&create_table_sql, &[]).await?;
        }
        Ok(pg_client)
    }

    async fn drop_table(pg_election: &PgElection, table_name: &str) {
        let sql = format!("DROP TABLE IF EXISTS \"{}\";", table_name);
        pg_election
            .pg_client
            .read()
            .await
            .execute(&sql, &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_postgres_crud() {
        maybe_skip_postgres_integration_test!();
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_postgres_crud_greptime_metakv";
        let candidate_lease_ttl = Duration::from_secs(10);
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_postgres_client(
            Some(table_name),
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value: "test_leader".to_string(),
            pg_client: RwLock::new(client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28319, table_name).build(),
        };

        let res = pg_election
            .put_value_with_lease(&key, &value, candidate_lease_ttl)
            .await
            .unwrap();
        assert!(res);

        let lease = pg_election
            .get_value_with_lease(&key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.leader_value, value);

        pg_election
            .update_value_with_lease(&key, &lease.origin, &value, pg_election.meta_lease_ttl)
            .await
            .unwrap();

        let res = pg_election.delete_value(&key).await.unwrap();
        assert!(res);

        let res = pg_election.get_value_with_lease(&key).await.unwrap();
        assert!(res.is_none());

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let value = format!("test_value_{}", i);
            pg_election
                .put_value_with_lease(&key, &value, candidate_lease_ttl)
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

        drop_table(&pg_election, table_name).await;
    }

    async fn candidate(
        leader_value: String,
        candidate_lease_ttl: Duration,
        store_key_prefix: String,
        table_name: String,
    ) {
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_postgres_client(
            None,
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value,
            pg_client: RwLock::new(client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28319, &table_name).build(),
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
        maybe_skip_postgres_integration_test!();
        let leader_value_prefix = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_candidate_registration_greptime_metakv";
        let mut handles = vec![];
        let candidate_lease_ttl = Duration::from_secs(5);
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_postgres_client(
            Some(table_name),
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();

        for i in 0..10 {
            let leader_value = format!("{}{}", leader_value_prefix, i);
            let handle = tokio::spawn(candidate(
                leader_value,
                candidate_lease_ttl,
                uuid.clone(),
                table_name.to_string(),
            ));
            handles.push(handle);
        }
        // Wait for candidates to register themselves and renew their leases at least once.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let (tx, _) = broadcast::channel(100);
        let leader_value = "test_leader".to_string();
        let pg_election = PgElection {
            leader_value,
            pg_client: RwLock::new(client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid.clone(),
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28319, table_name).build(),
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
            let key = format!("{}{}{}{}", uuid, CANDIDATES_ROOT, leader_value_prefix, i);
            let res = pg_election.delete_value(&key).await.unwrap();
            assert!(res);
        }

        drop_table(&pg_election, table_name).await;
    }

    #[tokio::test]
    async fn test_elected_and_step_down() {
        maybe_skip_postgres_integration_test!();
        let leader_value = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_elected_and_step_down_greptime_metakv";
        let candidate_lease_ttl = Duration::from_secs(5);
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_postgres_client(
            Some(table_name),
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_pg_election = PgElection {
            leader_value: leader_value.clone(),
            pg_client: RwLock::new(client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28320, table_name).build(),
        };

        leader_pg_election.elected().await.unwrap();
        let lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.leader_value == leader_value);
        assert!(lease.expire_time > lease.current);
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
        let lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.leader_value == leader_value);
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
        let lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.leader_value == leader_value);
        assert!(lease.expire_time > lease.current);
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
            .get_value_with_lease(&leader_pg_election.election_key())
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

        drop_table(&leader_pg_election, table_name).await;
    }

    #[tokio::test]
    async fn test_leader_action() {
        maybe_skip_postgres_integration_test!();
        let leader_value = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_leader_action_greptime_metakv";
        let candidate_lease_ttl = Duration::from_secs(5);
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_postgres_client(
            Some(table_name),
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_pg_election = PgElection {
            leader_value: leader_value.clone(),
            pg_client: RwLock::new(client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28321, table_name).build(),
        };

        // Step 1: No leader exists, campaign and elected.
        let res = leader_pg_election
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.leader_value == leader_value);
        assert!(lease.expire_time > lease.current);
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
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let new_lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(new_lease.leader_value == leader_value);
        assert!(
            new_lease.expire_time > new_lease.current && new_lease.expire_time > lease.expire_time
        );
        assert!(leader_pg_election.is_leader());

        // Step 3: Something wrong, the leader lease expired.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let res = leader_pg_election
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let res = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
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
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.leader_value == leader_value);
        assert!(lease.expire_time > lease.current);
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
            .get_value_with_lease(&leader_pg_election.election_key())
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
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election.leader_action().await.unwrap();
        let lease = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.leader_value == leader_value);
        assert!(lease.expire_time > lease.current);
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
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        let res: bool = res[0].get(0);
        assert!(res);
        leader_pg_election
            .delete_value(&leader_pg_election.election_key())
            .await
            .unwrap();
        leader_pg_election
            .put_value_with_lease(
                &leader_pg_election.election_key(),
                "test",
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        leader_pg_election.leader_action().await.unwrap();
        let res = leader_pg_election
            .get_value_with_lease(&leader_pg_election.election_key())
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
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.step_down, &[])
            .await
            .unwrap();

        drop_table(&leader_pg_election, table_name).await;
    }

    #[tokio::test]
    async fn test_follower_action() {
        maybe_skip_postgres_integration_test!();
        common_telemetry::init_default_ut_logging();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_follower_action_greptime_metakv";

        let candidate_lease_ttl = Duration::from_secs(5);
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let follower_client = create_postgres_client(
            Some(table_name),
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();
        let (tx, mut rx) = broadcast::channel(100);
        let follower_pg_election = PgElection {
            leader_value: "test_follower".to_string(),
            pg_client: RwLock::new(follower_client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid.clone(),
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28322, table_name).build(),
        };

        let leader_client = create_postgres_client(
            Some(table_name),
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();
        let (tx, _) = broadcast::channel(100);
        let leader_pg_election = PgElection {
            leader_value: "test_leader".to_string(),
            pg_client: RwLock::new(leader_client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(28322, table_name).build(),
        };

        leader_pg_election
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.campaign, &[])
            .await
            .unwrap();
        leader_pg_election.elected().await.unwrap();

        // Step 1: As a follower, the leader exists and the lease is not expired.
        follower_pg_election.follower_action().await.unwrap();

        // Step 2: As a follower, the leader exists but the lease expired.
        tokio::time::sleep(Duration::from_secs(2)).await;
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
            .pg_client
            .read()
            .await
            .query(&leader_pg_election.sql_set.step_down, &[])
            .await
            .unwrap();

        drop_table(&follower_pg_election, table_name).await;
    }

    #[tokio::test]
    async fn test_idle_session_timeout() {
        maybe_skip_postgres_integration_test!();
        common_telemetry::init_default_ut_logging();
        let execution_timeout = Duration::from_secs(10);
        let statement_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(1);
        let mut client = create_postgres_client(
            None,
            execution_timeout,
            idle_session_timeout,
            statement_timeout,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        // Wait for the idle session timeout.
        let err = client.query("SELECT 1", &[]).await.unwrap_err();
        assert_matches!(err, error::Error::PostgresExecution { .. });
        let error::Error::PostgresExecution { error, .. } = err else {
            panic!("Expected PostgresExecution error");
        };
        assert!(error.is_closed());
        // Reset the client and try again.
        client.reset_client().await.unwrap();
        let _ = client.query("SELECT 1", &[]).await.unwrap();
    }
}
