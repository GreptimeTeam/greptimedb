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
use tokio::time::MissedTickBehavior;

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

/// Lease information.
/// TODO(CookiePie): PgElection can also use this struct. Refactor it to a common module.
#[derive(Default, Clone)]
struct Lease {
    leader_value: String,
    expire_time: Timestamp,
    current: Timestamp,
    // origin is the origin value of the lease, used for CAS.
    origin: String,
}

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
            CREATE TABLE IF NOT EXISTS `{}` (
                k VARBINARY(3072) PRIMARY KEY,
                v BLOB
            );
            "#,
            self.table_name
        )
    }

    fn insert_once(&self) -> String {
        format!(
            "INSERT IGNORE INTO `{}` (k, v) VALUES ('__place_holder_for_lock', '');",
            self.table_name
        )
    }

    fn check_version(&self) -> &str {
        "SELECT @@version;"
    }

    fn campaign_sql(&self) -> String {
        format!("SELECT * FROM `{}` FOR UPDATE;", self.table_name)
    }

    fn put_value_with_lease_sql(&self) -> String {
        format!(
            r#"
            INSERT INTO `{}` (k, v) VALUES (
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
            r#"UPDATE `{}`
               SET v = CONCAT(?, '{}', DATE_FORMAT(DATE_ADD(NOW(4), INTERVAL ? SECOND), '%Y-%m-%d %T.%f'))
               WHERE k = ? AND v = ?"#,
            self.table_name, LEASE_SEP
        )
    }

    fn get_value_with_lease_sql(&self) -> String {
        format!(
            r#"SELECT v, DATE_FORMAT(NOW(4), '%Y-%m-%d %T.%f') FROM `{}` WHERE k = ?"#,
            self.table_name
        )
    }

    fn get_value_with_lease_by_prefix_sql(&self) -> String {
        format!(
            r#"SELECT v, DATE_FORMAT(NOW(4), '%Y-%m-%d %T.%f') FROM `{}` WHERE k LIKE ?"#,
            self.table_name
        )
    }

    fn delete_value_sql(&self) -> String {
        format!("DELETE FROM {} WHERE k = ?;", self.table_name)
    }
}

/// Parse the value and expire time from the given string. The value should be in the format "value || LEASE_SEP || expire_time".
fn parse_value_and_expire_time(value: &str) -> Result<(String, Timestamp)> {
    let (value, expire_time) =
        value
            .split(LEASE_SEP)
            .collect_tuple()
            .with_context(|| UnexpectedSnafu {
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
    async fn query(
        &mut self,
        query: Query<'_, MySql, MySqlArguments>,
        sql: &str,
    ) -> Result<Vec<MySqlRow>> {
        match self {
            Executor::Default(client) => {
                let res = query
                    .fetch_all(&mut **client)
                    .await
                    .context(MySqlExecutionSnafu { sql })?;
                Ok(res)
            }
            Executor::Txn(txn) => {
                let res = query
                    .fetch_all(&mut **txn)
                    .await
                    .context(MySqlExecutionSnafu { sql })?;
                Ok(res)
            }
        }
    }

    async fn execute(&mut self, query: Query<'_, MySql, MySqlArguments>, sql: &str) -> Result<u64> {
        match self {
            Executor::Default(client) => {
                let res = query
                    .execute(&mut **client)
                    .await
                    .context(MySqlExecutionSnafu { sql })?;
                Ok(res.rows_affected())
            }
            Executor::Txn(txn) => {
                let res = query
                    .execute(&mut **txn)
                    .await
                    .context(MySqlExecutionSnafu { sql })?;
                Ok(res.rows_affected())
            }
        }
    }

    async fn commit(self) -> Result<()> {
        match self {
            Executor::Txn(txn) => {
                txn.commit()
                    .await
                    .context(MySqlExecutionSnafu { sql: "COMMIT" })?;
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
            .context(MySqlExecutionSnafu {
                sql: &sql_factory.create_table_sql(),
            })?;
        // Set idle session timeout to IDLE_SESSION_TIMEOUT to avoid dead lock.
        sqlx::query(&sql_factory.set_idle_session_timeout_sql())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu {
                sql: &sql_factory.set_idle_session_timeout_sql(),
            })?;
        // Set lock wait timeout to LOCK_WAIT_TIMEOUT to avoid waiting too long.
        sqlx::query(sql_factory.set_lock_wait_timeout_sql())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu {
                sql: sql_factory.set_lock_wait_timeout_sql(),
            })?;
        // Insert at least one row for `SELECT * FOR UPDATE` to work.
        sqlx::query(&sql_factory.insert_once())
            .execute(&mut client)
            .await
            .context(MySqlExecutionSnafu {
                sql: &sql_factory.insert_once(),
            })?;
        // Check MySQL version
        Self::check_version(&mut client, sql_factory.check_version()).await?;
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

    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        let key = self.candidate_key();
        let node_info =
            serde_json::to_string(node_info).with_context(|_| SerializeToJsonSnafu {
                input: format!("{node_info:?}"),
            })?;

        {
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
                warn!("Candidate already registered, update the lease");
                self.delete_value(&key, &mut executor).await?;
                self.put_value_with_lease(
                    &key,
                    &node_info,
                    self.candidate_lease_ttl_secs,
                    &mut executor,
                )
                .await?;
            }
        }

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(self.candidate_lease_ttl_secs / 2));
        loop {
            let _ = keep_alive_interval.tick().await;
            let client = self.client.lock().await;
            let mut executor = Executor::Default(client);
            let lease = self
                .get_value_with_lease(&key, &mut executor)
                .await?
                .unwrap_or_default();

            ensure!(
                lease.expire_time > lease.current,
                UnexpectedSnafu {
                    violated: format!(
                        "Candidate lease expired at {:?} (current time: {:?}), key: {:?}",
                        lease.expire_time,
                        lease.current,
                        String::from_utf8_lossy(&key.into_bytes())
                    ),
                }
            );

            self.update_value_with_lease(&key, &lease.origin, &node_info, &mut executor)
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
            let _ = self.do_campaign().await;
            keep_alive_interval.tick().await;
        }
    }

    async fn leader(&self) -> Result<Self::Leader> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(self.leader_value.as_bytes().into())
        } else {
            let key = self.election_key();

            let client = self.client.lock().await;
            let mut executor = Executor::Default(client);
            if let Some(lease) = self.get_value_with_lease(&key, &mut executor).await? {
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

impl MySqlElection {
    /// Returns value, expire time and current time.
    async fn get_value_with_lease(
        &self,
        key: &str,
        executor: &mut Executor<'_>,
    ) -> Result<Option<Lease>> {
        let key = key.as_bytes();
        let query = sqlx::query(&self.sql_set.get_value_with_lease).bind(key);
        let res = executor
            .query(query, &self.sql_set.get_value_with_lease)
            .await?;

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

        Ok(Some(Lease {
            leader_value: value,
            expire_time,
            current: current_time,
            origin: value_and_expire_time.to_string(),
        }))
    }

    /// Returns all values and expire time with the given key prefix. Also returns the current time.
    async fn get_value_with_lease_by_prefix(
        &self,
        key_prefix: &str,
        executor: &mut Executor<'_>,
    ) -> Result<(Vec<(String, Timestamp)>, Timestamp)> {
        let key_prefix = format!("{}%", key_prefix).as_bytes().to_vec();
        let query = sqlx::query(&self.sql_set.get_value_with_lease_by_prefix).bind(key_prefix);
        let res = executor
            .query(query, &self.sql_set.get_value_with_lease_by_prefix)
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
        let res = executor
            .execute(query, &self.sql_set.update_value_with_lease)
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
        lease_ttl_secs: u64,
        executor: &mut Executor<'_>,
    ) -> Result<bool> {
        let key = key.as_bytes();
        let lease_ttl_secs = lease_ttl_secs as f64;
        let query = sqlx::query(&self.sql_set.put_value_with_lease)
            .bind(key)
            .bind(value)
            .bind(lease_ttl_secs);
        let res = executor
            .execute(query, &self.sql_set.put_value_with_lease)
            .await?;
        Ok(res == 1)
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &str, executor: &mut Executor<'_>) -> Result<bool> {
        let key = key.as_bytes();
        let query = sqlx::query(&self.sql_set.delete_value).bind(key);
        let res = executor.execute(query, &self.sql_set.delete_value).await?;

        Ok(res == 1)
    }

    /// Attempts to acquire leadership by executing a campaign. This function continuously checks
    /// if the current lease is still valid.
    async fn do_campaign(&self) -> Result<()> {
        // Need to restrict the scope of the client to avoid ambiguous overloads.
        use sqlx::Acquire;

        let client = self.client.lock().await;
        let executor = Executor::Default(client);
        let mut lease = Lease::default();
        match (
            self.lease_check(executor, &mut lease).await,
            self.is_leader(),
            self.leader_value == lease.leader_value,
        ) {
            // If the leader lease is valid and I'm the leader, renew the lease.
            (Ok(_), true, true) => {
                let mut client = self.client.lock().await;
                let txn = client
                    .begin()
                    .await
                    .context(MySqlExecutionSnafu { sql: "BEGIN" })?;
                let mut executor = Executor::Txn(txn);
                let query = sqlx::query(&self.sql_set.campaign);
                executor.query(query, &self.sql_set.campaign).await?;
                self.renew_lease(executor, lease).await?;
            }
            // If the leader lease expires and I'm the leader, notify the leader watcher and step down.
            // Another instance should be elected as the leader in this case.
            (Err(_), true, _) | (Ok(_), true, false) => {
                warn!("Leader lease expired, step down...");
                self.step_down_without_lock().await?;
            }
            // If the leader lease expires and I'm not the leader, elect myself.
            (Err(_), false, _) => {
                warn!("Leader lease expired, elected.");
                let mut client = self.client.lock().await;
                let txn = client
                    .begin()
                    .await
                    .context(MySqlExecutionSnafu { sql: "BEGIN" })?;
                let mut executor = Executor::Txn(txn);
                let query = sqlx::query(&self.sql_set.campaign);
                executor.query(query, &self.sql_set.campaign).await?;
                self.elected(&mut executor).await?;
                executor.commit().await?;
            }
            // If the leader lease is valid and I'm the leader, but I don't think I'm the leader.
            // Just re-elect myself.
            (Ok(_), false, true) => {
                warn!("I should be the leader, but I don't think so. Something went wrong.");
                let mut client = self.client.lock().await;
                let txn = client
                    .begin()
                    .await
                    .context(MySqlExecutionSnafu { sql: "BEGIN" })?;
                let mut executor = Executor::Txn(txn);
                let query = sqlx::query(&self.sql_set.campaign);
                executor.query(query, &self.sql_set.campaign).await?;
                self.elected(&mut executor).await?;
                executor.commit().await?;
            }
            // If the leader lease is valid and I'm not the leader, do nothing.
            (Ok(_), false, false) => {}
        }
        Ok(())
    }

    /// Renew the lease
    async fn renew_lease(&self, mut executor: Executor<'_>, lease: Lease) -> Result<()> {
        let key = self.election_key();
        self.update_value_with_lease(&key, &lease.origin, &self.leader_value, &mut executor)
            .await?;
        executor.commit().await?;
        Ok(())
    }

    /// Performs a lease check during the election process.
    ///
    /// This function performs the following checks and actions:
    ///
    /// - **Case 1**: If the current instance is not the leader but the lease has expired, it raises an error
    ///   to re-initiate the campaign. If the leader failed to renew the lease, its session will expire and the lock
    ///   will be released.
    /// - **Case 2**: If all checks pass, the function returns without performing any actions.
    async fn lease_check(&self, mut executor: Executor<'_>, lease: &mut Lease) -> Result<()> {
        let key = self.election_key();
        let check_lease = self
            .get_value_with_lease(&key, &mut executor)
            .await?
            .context(NoLeaderSnafu)?;
        *lease = check_lease;
        // Case 1: Lease expired
        ensure!(lease.expire_time > lease.current, NoLeaderSnafu);
        // Case 2: Everything is fine
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

    /// Check if the MySQL version is supported.
    async fn check_version(client: &mut MySqlConnection, sql: &str) -> Result<()> {
        let query = sqlx::query(sql);
        match query.fetch_one(client).await {
            Ok(row) => {
                let version: String = row.try_get(0).unwrap();
                if !version.starts_with("8.0") || !version.starts_with("5.7") {
                    warn!(
                        "Unsupported MySQL version: {}, expected: [5.7, 8.0]",
                        version
                    );
                }
            }
            Err(e) => {
                warn!(e; "Failed to check MySQL version through sql: {}", sql);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use common_telemetry::init_default_ut_logging;
    use sqlx::Connection;

    use super::*;
    use crate::error::MySqlExecutionSnafu;

    async fn create_mysql_client(table_name: Option<&str>) -> Result<Mutex<MySqlConnection>> {
        init_default_ut_logging();
        let endpoint = env::var("GT_MYSQL_ENDPOINTS").unwrap_or_default();
        if endpoint.is_empty() {
            return UnexpectedSnafu {
                violated: "MySQL endpoint is empty".to_string(),
            }
            .fail();
        }
        let mut client = MySqlConnection::connect(&endpoint).await.unwrap();
        if let Some(table_name) = table_name {
            let create_table_sql = format!(
                "CREATE TABLE IF NOT EXISTS {}(k VARCHAR(255) PRIMARY KEY, v BLOB);",
                table_name
            );
            sqlx::query(&create_table_sql)
                .execute(&mut client)
                .await
                .context(MySqlExecutionSnafu {
                    sql: create_table_sql,
                })?;
        }
        Ok(Mutex::new(client))
    }

    async fn drop_table(client: &Mutex<MySqlConnection>, table_name: &str) {
        let mut client = client.lock().await;
        let sql = format!("DROP TABLE IF EXISTS {};", table_name);
        sqlx::query(&sql)
            .execute(&mut *client)
            .await
            .context(MySqlExecutionSnafu { sql })
            .unwrap();
    }

    #[tokio::test]
    async fn test_mysql_crud() {
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_mysql_crud_greptime_metakv";
        let client = create_mysql_client(Some(table_name)).await.unwrap();

        let mut a = client.lock().await;
        let txn = a.begin().await.unwrap();
        let mut executor = Executor::Txn(txn);
        let raw_query = format!("SELECT * FROM {} FOR UPDATE;", table_name);
        let query = sqlx::query(&raw_query);
        let _ = executor.query(query, &raw_query).await.unwrap();
        std::mem::drop(executor);
        std::mem::drop(a);

        let (tx, _) = broadcast::channel(100);
        let mysql_election = MySqlElection {
            leader_value: "test_leader".to_string(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl_secs: 10,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };
        let client = mysql_election.client.lock().await;
        let mut executor = Executor::Default(client);
        let res = mysql_election
            .put_value_with_lease(&key, &value, 10, &mut executor)
            .await
            .unwrap();
        assert!(res);

        let lease = mysql_election
            .get_value_with_lease(&key, &mut executor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.leader_value, value);

        mysql_election
            .update_value_with_lease(&key, &lease.origin, &value, &mut executor)
            .await
            .unwrap();

        let res = mysql_election
            .delete_value(&key, &mut executor)
            .await
            .unwrap();
        assert!(res);

        let res = mysql_election
            .get_value_with_lease(&key, &mut executor)
            .await
            .unwrap();
        assert!(res.is_none());

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let value = format!("test_value_{}", i);
            mysql_election
                .put_value_with_lease(&key, &value, 10, &mut executor)
                .await
                .unwrap();
        }

        let key_prefix = "test_key".to_string();
        let (res, _) = mysql_election
            .get_value_with_lease_by_prefix(&key_prefix, &mut executor)
            .await
            .unwrap();
        assert_eq!(res.len(), 10);

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let res = mysql_election
                .delete_value(&key, &mut executor)
                .await
                .unwrap();
            assert!(res);
        }

        let (res, current) = mysql_election
            .get_value_with_lease_by_prefix(&key_prefix, &mut executor)
            .await
            .unwrap();
        assert!(res.is_empty());
        assert!(current == Timestamp::default());

        // Should drop manually.
        std::mem::drop(executor);
        drop_table(&mysql_election.client, table_name).await;
    }

    async fn candidate(
        leader_value: String,
        candidate_lease_ttl_secs: u64,
        store_key_prefix: String,
        table_name: String,
    ) {
        let client = create_mysql_client(None).await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let mysql_election = MySqlElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
            sql_set: ElectionSqlFactory::new(&table_name).build(),
        };

        let node_info = MetasrvNodeInfo {
            addr: "test_addr".to_string(),
            version: "test_version".to_string(),
            git_commit: "test_git_commit".to_string(),
            start_time_ms: 0,
        };
        mysql_election.register_candidate(&node_info).await.unwrap();
    }

    #[tokio::test]
    async fn test_candidate_registration() {
        let leader_value_prefix = "test_leader".to_string();
        let candidate_lease_ttl_secs = 5;
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_candidate_registration_greptime_metakv";
        let mut handles = vec![];
        let client = create_mysql_client(Some(table_name)).await.unwrap();

        for i in 0..10 {
            let leader_value = format!("{}{}", leader_value_prefix, i);
            let handle = tokio::spawn(candidate(
                leader_value,
                candidate_lease_ttl_secs,
                uuid.clone(),
                table_name.to_string(),
            ));
            handles.push(handle);
        }
        // Wait for candidates to registrate themselves and renew their leases at least once.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let (tx, _) = broadcast::channel(100);
        let leader_value = "test_leader".to_string();
        let mysql_election = MySqlElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid.clone(),
            candidate_lease_ttl_secs,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        let candidates = mysql_election.all_candidates().await.unwrap();
        assert_eq!(candidates.len(), 10);

        for handle in handles {
            handle.abort();
        }

        // Wait for the candidate leases to expire.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let candidates = mysql_election.all_candidates().await.unwrap();
        assert!(candidates.is_empty());

        // Garbage collection
        let client = mysql_election.client.lock().await;
        let mut executor = Executor::Default(client);
        for i in 0..10 {
            let key = format!("{}{}{}{}", uuid, CANDIDATES_ROOT, leader_value_prefix, i);
            let res = mysql_election
                .delete_value(&key, &mut executor)
                .await
                .unwrap();
            assert!(res);
        }

        // Should drop manually.
        std::mem::drop(executor);
        drop_table(&mysql_election.client, table_name).await;
    }

    async fn elected(election: &MySqlElection, table_name: &str) {
        let mut client = election.client.lock().await;
        let txn = client.begin().await.unwrap();
        let mut executor = Executor::Txn(txn);
        let raw_query = format!("SELECT * FROM {} FOR UPDATE;", table_name);
        let query = sqlx::query(&raw_query);
        let _ = executor.query(query, &raw_query).await.unwrap();
        election.elected(&mut executor).await.unwrap();
        executor.commit().await.unwrap();
    }

    async fn get_lease(election: &MySqlElection) -> Option<Lease> {
        let client = election.client.lock().await;
        let mut executor = Executor::Default(client);
        election
            .get_value_with_lease(&election.election_key(), &mut executor)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_elected_and_step_down() {
        let leader_value = "test_leader".to_string();
        let candidate_lease_ttl_secs = 5;
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_elected_and_step_down_greptime_metakv";
        let client = create_mysql_client(Some(table_name)).await.unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl_secs,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        elected(&leader_mysql_election, table_name).await;
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        leader_mysql_election
            .step_down_without_lock()
            .await
            .unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(!leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        elected(&leader_mysql_election, table_name).await;
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        drop_table(&leader_mysql_election.client, table_name).await;
    }

    #[tokio::test]
    async fn test_campaign() {
        let leader_value = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_leader_action_greptime_metakv";
        let candidate_lease_ttl_secs = 5;
        let client = create_mysql_client(Some(table_name)).await.unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl_secs,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        // Step 1: No leader exists, campaign and elected.
        leader_mysql_election.do_campaign().await.unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        // Step 2: As a leader, renew the lease.
        leader_mysql_election.do_campaign().await.unwrap();
        let new_lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        // The lease should be renewed.
        assert!(new_lease.expire_time > lease.expire_time);
        assert!(new_lease.expire_time > new_lease.current);
        assert!(leader_mysql_election.is_leader());

        // Step 3: Something wrong, the leader lease expired.
        tokio::time::sleep(Duration::from_secs(META_LEASE_SECS + 1)).await;
        leader_mysql_election.do_campaign().await.unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time <= lease.current);
        assert!(!leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        // Step 4: Re-elect itself.
        leader_mysql_election.do_campaign().await.unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        // Step 5: Something wrong, the leader key is deleted by other followers.
        {
            let client = leader_mysql_election.client.lock().await;
            let mut executor = Executor::Default(client);
            leader_mysql_election
                .delete_value(&leader_mysql_election.election_key(), &mut executor)
                .await
                .unwrap();
        }
        leader_mysql_election.do_campaign().await.unwrap();
        let res = get_lease(&leader_mysql_election).await;
        assert!(res.is_none());
        assert!(!leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        // Step 6: Re-elect itself.
        leader_mysql_election.do_campaign().await.unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        // Step 7: Something wrong, the leader key changed by others.
        let another_leader_key = "another_leader";
        {
            let client = leader_mysql_election.client.lock().await;
            let mut executor = Executor::Default(client);
            leader_mysql_election
                .delete_value(&leader_mysql_election.election_key(), &mut executor)
                .await
                .unwrap();
            leader_mysql_election
                .put_value_with_lease(
                    &leader_mysql_election.election_key(),
                    another_leader_key,
                    10,
                    &mut executor,
                )
                .await
                .unwrap();
        }
        leader_mysql_election.do_campaign().await.unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        // Different from pg, mysql will not delete the key, just step down.
        assert_eq!(lease.leader_value, another_leader_key);
        assert!(lease.expire_time > lease.current);
        assert!(!leader_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::StepDown(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), leader_value);
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    leader_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::StepDown"),
        }

        drop_table(&leader_mysql_election.client, table_name).await;
    }

    #[tokio::test]
    async fn test_follower_action() {
        common_telemetry::init_default_ut_logging();
        let candidate_lease_ttl_secs = 5;
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_follower_action_greptime_metakv";

        let follower_client = create_mysql_client(Some(table_name)).await.unwrap();
        let (tx, mut rx) = broadcast::channel(100);
        let follower_mysql_election = MySqlElection {
            leader_value: "test_follower".to_string(),
            client: follower_client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid.clone(),
            candidate_lease_ttl_secs,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        let leader_client = create_mysql_client(Some(table_name)).await.unwrap();
        let (tx, _) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: "test_leader".to_string(),
            client: leader_client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl_secs,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        leader_mysql_election.do_campaign().await.unwrap();

        // Step 1: As a follower, the leader exists and the lease is not expired. Do nothing.
        follower_mysql_election.do_campaign().await.unwrap();

        // Step 2: As a follower, the leader exists but the lease expired. Re-elect itself.
        tokio::time::sleep(Duration::from_secs(META_LEASE_SECS + 1)).await;
        follower_mysql_election.do_campaign().await.unwrap();
        assert!(follower_mysql_election.is_leader());

        match rx.recv().await {
            Ok(LeaderChangeMessage::Elected(key)) => {
                assert_eq!(String::from_utf8_lossy(key.name()), "test_follower");
                assert_eq!(
                    String::from_utf8_lossy(key.key()),
                    follower_mysql_election.election_key()
                );
                assert_eq!(key.lease_id(), i64::default());
                assert_eq!(key.revision(), i64::default());
            }
            _ => panic!("Expected LeaderChangeMessage::Elected"),
        }

        drop_table(&follower_mysql_election.client, table_name).await;
    }
}
