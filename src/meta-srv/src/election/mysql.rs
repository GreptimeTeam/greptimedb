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
use sqlx::mysql::{MySqlArguments, MySqlRow};
use sqlx::query::Query;
use sqlx::{MySql, MySqlConnection, MySqlTransaction, Row};
use tokio::sync::{broadcast, Mutex, MutexGuard};
use tokio::time::{Interval, MissedTickBehavior};

use crate::election::{
    listen_leader_change, Election, LeaderChangeMessage, LeaderKey, CANDIDATES_ROOT, ELECTION_KEY,
};
use crate::error::{
    DeserializeFromJsonSnafu, MySqlExecutionSnafu, NoLeaderSnafu, Result, SerializeToJsonSnafu,
    UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

// Separator between value and expire time.
const LEASE_SEP: &str = r#"||__metadata_lease_sep||"#;

struct ElectionSqlFactory<'a> {
    table_name: &'a str,
}

struct ElectionSqlSet {
    campaign: String,
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
    // `$1`: updated value,
    // `$2`: lease time in seconds
    // `$3`: key,
    // `$4`: previous value,
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
    // `?`: key
    //
    // Returns:
    // Rows affected
    delete_value: String,
}

impl<'a> ElectionSqlFactory<'a> {
    fn new(table_name: &'a str) -> Self {
        Self { table_name }
    }

    fn build(self) -> ElectionSqlSet {
        ElectionSqlSet {
            campaign: self.campaign_sql(),
            put_value_with_lease: self.put_value_with_lease_sql(),
            update_value_with_lease: self.update_value_with_lease_sql(),
            get_value_with_lease: self.get_value_with_lease_sql(),
            get_value_with_lease_by_prefix: self.get_value_with_lease_by_prefix_sql(),
            delete_value: self.delete_value_sql(),
        }
    }

    // Currently the session timeout is longer than the leader lease time.
    // So the leader will renew the lease twice before the session timeout if everything goes well.
    fn set_idle_session_timeout_sql(&self) -> String {
        format!("SET SESSION wait_timeout = {};", META_LEASE_SECS + 1)
    }

    fn set_lock_wait_timeout_sql(&self) -> &str {
        "SET SESSION innodb_lock_wait_timeout = 1;"
    }

    fn create_table_sql(&self) -> String {
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                k VARBINARY(3072) PRIMARY KEY,
                v BLOB
            );
            "#,
            self.table_name
        )
    }

    fn insert_once(&self) -> String {
        format!(
            "INSERT IGNORE INTO {} (k, v) VALUES ('__place_holder_for_lock', '');",
            self.table_name
        )
    }

    fn campaign_sql(&self) -> String {
        format!("SELECT * FROM {} FOR UPDATE;", self.table_name)
    }

    fn put_value_with_lease_sql(&self) -> String {
        format!(
            r#"
            INSERT INTO {} (k, v) VALUES (
                ?,
                CONCAT(
                    ?,
                    '{}',
                    DATE_FORMAT(DATE_ADD(NOW(4), INTERVAL ? SECOND), '%Y-%m-%d %T.%f')
                )
            )
            ON DUPLICATE KEY UPDATE v = VALUES(v);
            "#,
            self.table_name, LEASE_SEP
        )
    }

    fn update_value_with_lease_sql(&self) -> String {
        format!(
            r#"UPDATE {}
               SET v = CONCAT(?, '{}', DATE_FORMAT(DATE_ADD(NOW(4), INTERVAL ? SECOND), '%Y-%m-%d %T.%f'))
               WHERE k = ? AND v = ?"#,
            self.table_name, LEASE_SEP
        )
    }

    fn get_value_with_lease_sql(&self) -> String {
        format!(
            r#"SELECT v, DATE_FORMAT(NOW(4), '%Y-%m-%d %T.%f') FROM {} WHERE k = ?"#,
            self.table_name
        )
    }

    fn get_value_with_lease_by_prefix_sql(&self) -> String {
        format!(
            r#"SELECT v, DATE_FORMAT(NOW(4), '%Y-%m-%d %T.%f') FROM {} WHERE k LIKE ?"#,
            self.table_name
        )
    }

    fn delete_value_sql(&self) -> String {
        format!("DELETE FROM {} WHERE k = ?;", self.table_name)
    }
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
struct MySqlLeaderKey {
    name: Vec<u8>,
    key: Vec<u8>,
    rev: i64,
    lease: i64,
}

impl LeaderKey for MySqlLeaderKey {
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

enum Executor<'a> {
    Default(MutexGuard<'a, MySqlConnection>),
    Txn(MySqlTransaction<'a>),
}

impl Executor<'_> {
    async fn query(&mut self, query: Query<'_, MySql, MySqlArguments>) -> Result<Vec<MySqlRow>> {
        match self {
            Executor::Default(client) => {
                let res = query
                    .fetch_all(&mut **client)
                    .await
                    .context(MySqlExecutionSnafu)?;
                Ok(res)
            }
            Executor::Txn(txn) => {
                let res = query
                    .fetch_all(&mut **txn)
                    .await
                    .context(MySqlExecutionSnafu)?;
                Ok(res)
            }
        }
    }

    async fn execute(&mut self, query: Query<'_, MySql, MySqlArguments>) -> Result<u64> {
        match self {
            Executor::Default(client) => {
                let res = query
                    .execute(&mut **client)
                    .await
                    .context(MySqlExecutionSnafu)?;
                Ok(res.rows_affected())
            }
            Executor::Txn(txn) => {
                let res = query
                    .execute(&mut **txn)
                    .await
                    .context(MySqlExecutionSnafu)?;
                Ok(res.rows_affected())
            }
        }
    }

    async fn commit(self) -> Result<()> {
        match self {
            Executor::Txn(txn) => {
                txn.commit().await.context(MySqlExecutionSnafu)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// MySQL implementation of Election.
pub struct MySqlElection {
    leader_value: String,
    client: Mutex<MySqlConnection>,
    is_leader: AtomicBool,
    leader_infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
    candidate_lease_ttl_secs: u64,
    sql_set: ElectionSqlSet,
}

impl MySqlElection {
    pub async fn with_mysql_client(
        leader_value: String,
        mut client: sqlx::MySqlConnection,
        store_key_prefix: String,
        candidate_lease_ttl_secs: u64,
        table_name: &str,
    ) -> Result<ElectionRef> {
        let sql_factory = ElectionSqlFactory::new(table_name);
        sqlx::query(&sql_factory.create_table_sql())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu)?;
        // Set idle session timeout to IDLE_SESSION_TIMEOUT to avoid dead lock.
        sqlx::query(&sql_factory.set_idle_session_timeout_sql())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu)?;
        // Set lock wait timeout to LOCK_WAIT_TIMEOUT to avoid waiting too long.
        sqlx::query(sql_factory.set_lock_wait_timeout_sql())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu)?;
        // Insert at least one row for `SELECT * FOR UPDATE` to work.
        sqlx::query(&sql_factory.insert_once())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu)?;
        let tx = listen_leader_change(leader_value.clone());
        Ok(Arc::new(Self {
            leader_value,
            client: Mutex::new(client),
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(false),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
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
impl Election for MySqlElection {
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

        let client = self.client.lock().await;
        let mut executor = Executor::Default(client);
        let res = self
            .put_value_with_lease(
                &key,
                &node_info,
                self.candidate_lease_ttl_secs,
                &mut executor,
            )
            .await?;
        // May registered before, just update the lease.
        if !res {
            self.delete_value(&key, &mut executor).await?;
            self.put_value_with_lease(
                &key,
                &node_info,
                self.candidate_lease_ttl_secs,
                &mut executor,
            )
            .await?;
        }
        std::mem::drop(executor);

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(self.candidate_lease_ttl_secs / 2));
        loop {
            let _ = keep_alive_interval.tick().await;
            let client = self.client.lock().await;
            let mut executor = Executor::Default(client);
            let (_, prev_expire_time, current_time, origin) = self
                .get_value_with_lease(&key, true, &mut executor)
                .await?
                .unwrap_or_default();

            ensure!(
                prev_expire_time > current_time,
                UnexpectedSnafu {
                    violated: format!(
                        "Candidate lease expired at {:?} (current time: {:?}), key: {:?}",
                        prev_expire_time,
                        current_time,
                        String::from_utf8_lossy(&key.into_bytes())
                    ),
                }
            );

            // Safety: origin is Some since we are using `get_value_with_lease` with `true`.
            let origin = origin.unwrap();
            self.update_value_with_lease(&key, &origin, &node_info, &mut executor)
                .await?;
            std::mem::drop(executor);
        }
    }

    async fn all_candidates(&self) -> Result<Vec<MetasrvNodeInfo>> {
        let key_prefix = self.candidate_root();
        let client = self.client.lock().await;
        let mut executor = Executor::Default(client);
        let (mut candidates, current) = self
            .get_value_with_lease_by_prefix(&key_prefix, &mut executor)
            .await?;
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

    async fn campaign(&self) -> Result<()> {
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS));
        keep_alive_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let _ = self.do_campaign(&mut keep_alive_interval).await;
        }
    }

    async fn leader(&self) -> Result<Self::Leader> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(self.leader_value.as_bytes().into())
        } else {
            let key = self.election_key();

            let client = self.client.lock().await;
            let mut executor = Executor::Default(client);
            if let Some((leader, expire_time, current, _)) = self
                .get_value_with_lease(&key, false, &mut executor)
                .await?
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

impl MySqlElection {
    /// Returns value, expire time and current time. If `with_origin` is true, the origin string is also returned.
    async fn get_value_with_lease(
        &self,
        key: &str,
        with_origin: bool,
        executor: &mut Executor<'_>,
    ) -> Result<Option<(String, Timestamp, Timestamp, Option<String>)>> {
        let key = key.as_bytes();
        let query = sqlx::query(&self.sql_set.get_value_with_lease).bind(key);
        let res = executor.query(query).await?;

        if res.is_empty() {
            return Ok(None);
        }
        // Safety: Checked if res is empty above.
        let current_time_str = String::from_utf8_lossy(res[0].try_get(1).unwrap());
        let current_time = match Timestamp::from_str(&current_time_str, None) {
            Ok(ts) => ts,
            Err(_) => UnexpectedSnafu {
                violated: format!("Invalid timestamp: {}", current_time_str),
            }
            .fail()?,
        };
        // Safety: Checked if res is empty above.
        let value_and_expire_time = String::from_utf8_lossy(res[0].try_get(0).unwrap_or_default());
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

    /// Returns all values and expire time with the given key prefix. Also returns the current time.
    async fn get_value_with_lease_by_prefix(
        &self,
        key_prefix: &str,
        executor: &mut Executor<'_>,
    ) -> Result<(Vec<(String, Timestamp)>, Timestamp)> {
        let key_prefix = format!("{}%", key_prefix).as_bytes().to_vec();
        let query = sqlx::query(&self.sql_set.get_value_with_lease_by_prefix).bind(key_prefix);
        let res = executor.query(query).await?;

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
        executor: &mut Executor<'_>,
    ) -> Result<()> {
        let key = key.as_bytes();
        let prev = prev.as_bytes();
        let updated = updated.as_bytes();

        let query = sqlx::query(&self.sql_set.update_value_with_lease)
            .bind(updated)
            .bind(self.candidate_lease_ttl_secs as f64)
            .bind(key)
            .bind(prev);
        let res = executor.execute(query).await?;

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
        lease_ttl_secs: u64,
        executor: &mut Executor<'_>,
    ) -> Result<bool> {
        let key = key.as_bytes();
        let lease_ttl_secs = lease_ttl_secs as f64;
        let query = sqlx::query(&self.sql_set.put_value_with_lease)
            .bind(key)
            .bind(value)
            .bind(lease_ttl_secs);
        let res = executor.query(query).await?;
        Ok(res.is_empty())
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &str, executor: &mut Executor<'_>) -> Result<bool> {
        let key = key.as_bytes();
        let query = sqlx::query(&self.sql_set.delete_value).bind(key);
        let res = executor.execute(query).await?;

        Ok(res == 1)
    }

    /// Attempts to acquire leadership by executing a campaign. This function continuously checks
    /// if the current instance can become the leader by acquiring an advisory lock in the PostgreSQL database.
    ///
    /// The function operates in a loop, where it:
    ///
    /// 1. Waits for a predefined interval before attempting to acquire the lock again.
    /// 2. Executes the `SELECT FOR UPDATE` SQL query to try to acquire the lock.
    /// 3. Checks the result of the query:
    ///    - If the lock is successfully acquired (result is true), it calls the `leader_action` method.
    ///      Different from PgElection, it does not mean you are the leader. So it performs a check before
    ///      renewing the lease.
    ///    - If the lock is not acquired (result is false), it calls the `follower_action` method
    ///      to perform a check.
    async fn do_campaign(&self, interval: &mut Interval) -> Result<()> {
        // Need to restrict the scope of the client to avoid ambiguous overloads.
        use sqlx::Acquire;

        loop {
            let mut client = self.client.lock().await;
            let txn = client.begin().await.context(MySqlExecutionSnafu)?;
            let mut executor = Executor::Txn(txn);
            let query = sqlx::query(&self.sql_set.campaign);
            match executor.query(query).await {
                // Caution: leader action does not mean you are the leader.
                // It just means you get the lock, and now you can perform a check.
                Ok(_) => self.leader_action(executor).await?,
                // If the lock is not acquired, another instance is in the check.
                Err(_) => {
                    executor.commit().await?;
                    let executor = Executor::Default(client);
                    self.follower_action(executor).await?;
                }
            }
            interval.tick().await;
        }
    }

    /// Handles the actions with the acquired lock in the election process.
    /// Note: Leader action does not mean you are the leader. It just means you get the lock, and now you can perform a unique check.
    ///
    /// This function performs the following checks and actions:
    ///
    /// - **Case 1**: If the instance is still the leader and the lease is valid, it renews the lease
    ///   by updating the value associated with the election key.
    /// - **Case 2**: If the instance is still the leader but the lease has expired, it logs a warning
    ///   and steps down, initiating a new campaign for leadership.
    /// - **Case 3**: If the instance is not the leader and the lease has expired, it is elected as the leader.
    /// - **Case 4**: If the instance is the leader and the lease is valid but the leader has changed,
    ///   it logs a warning and steps down.
    /// - **Case 5**: If the instance is not the leader and the lease is valid, it does nothing.
    /// - **Case 6**: If the instance is the leader and no lease information is found, it logs a warning and steps down.
    /// - **Case 7**: If the instance is not the leader and no lease information is found, it is elected as the leader.
    async fn leader_action(&self, mut executor: Executor<'_>) -> Result<()> {
        let key = self.election_key();
        match self.get_value_with_lease(&key, true, &mut executor).await? {
            Some((prev_leader, expire_time, current, prev)) => {
                match (prev_leader == self.leader_value, expire_time > current) {
                    // Case 1: Believe itself as the leader and the lease is still valid.
                    (true, true) => {
                        // Safety: prev is Some since we are using `get_value_with_lease` with `true`.
                        let prev = prev.unwrap();
                        self.update_value_with_lease(
                            &key,
                            &prev,
                            &self.leader_value,
                            &mut executor,
                        )
                        .await?;
                        executor.commit().await?;
                    }
                    (_, false) => {
                        if self.is_leader() {
                            // Case 2: Believe itself as the leader but the lease has expired.
                            warn!(
                                "Leader lease expired at {:?} (current time: {:?}), stepping down",
                                expire_time, current
                            );
                            self.step_down(executor).await?;
                        } else {
                            // Case 3: Do not believe itself as the leader and the lease has expired.
                            self.elected(&mut executor).await?;
                            executor.commit().await?;
                        }
                    }
                    (false, true) => {
                        if self.is_leader() {
                            // Case 4: Believe itself as the leader but the leader has changed.
                            warn!(
                                "Leader changed from {:?} to {:?}, stepping down",
                                prev_leader, self.leader_value
                            );
                            // Do not delete the key, since another leader is elected.
                            self.step_down_without_lock().await?;
                        }
                        // Case 5: Do not believe itself as the leader and the lease is still valid.
                        executor.commit().await?;
                    }
                }
            }
            None => {
                if self.is_leader() {
                    // Case 6: Believe itself as the leader but no lease information found.
                    warn!("No lease information found, stepping down, key: {:?}", key);
                    self.step_down(executor).await?;
                } else {
                    // Case 7: Do not believe itself as the leader and no lease information found.
                    self.elected(&mut executor).await?;
                    executor.commit().await?;
                }
            }
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
    async fn follower_action(&self, mut executor: Executor<'_>) -> Result<()> {
        let key = self.election_key();
        // Case 1
        if self.is_leader() {
            self.step_down_without_lock().await?;
        }
        let (_, expire_time, current, _) = self
            .get_value_with_lease(&key, false, &mut executor)
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
    /// Should only step down while holding the lock.
    async fn step_down(&self, mut executor: Executor<'_>) -> Result<()> {
        let key = self.election_key();
        let leader_key = MySqlLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        if self
            .is_leader
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.delete_value(&key, &mut executor).await?;
            executor.commit().await?;
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
        let leader_key = MySqlLeaderKey {
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
    /// Caution: Should only elected while holding the lock.
    async fn elected(&self, executor: &mut Executor<'_>) -> Result<()> {
        let key = self.election_key();
        let leader_key = MySqlLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        self.delete_value(&key, executor).await?;
        self.put_value_with_lease(&key, &self.leader_value, META_LEASE_SECS, executor)
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
