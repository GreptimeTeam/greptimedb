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
use common_telemetry::{error, info, warn};
use common_time::Timestamp;
use snafu::{ensure, OptionExt, ResultExt};
use sqlx::mysql::{MySqlArguments, MySqlRow};
use sqlx::pool::PoolConnection;
use sqlx::query::Query;
use sqlx::{MySql, MySqlPool, MySqlTransaction, Row};
use tokio::sync::{broadcast, Mutex, MutexGuard};
use tokio::time::MissedTickBehavior;

use crate::election::rds::{parse_value_and_expire_time, Lease, RdsLeaderKey, LEASE_SEP};
use crate::election::{
    listen_leader_change, send_leader_change_and_set_flags, Election, LeaderChangeMessage,
};
use crate::error::{
    AcquireMySqlClientSnafu, DecodeSqlValueSnafu, DeserializeFromJsonSnafu,
    LeaderLeaseChangedSnafu, LeaderLeaseExpiredSnafu, MySqlExecutionSnafu, NoLeaderSnafu, Result,
    SerializeToJsonSnafu, SqlExecutionTimeoutSnafu, UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

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

    /// Use `SELECT FOR UPDATE` to lock for compatibility with other MySQL-compatible databases
    /// instead of directly using `GET_LOCK`.
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
        format!("DELETE FROM `{}` WHERE k = ?;", self.table_name)
    }
}

enum Executor<'a> {
    Default(MutexGuard<'a, ElectionMysqlClient>),
    Txn(TransactionWithExecutionTimeout<'a>),
}

impl Executor<'_> {
    async fn query(
        &mut self,
        query: Query<'_, MySql, MySqlArguments>,
        sql: &str,
    ) -> Result<Vec<MySqlRow>> {
        match self {
            Executor::Default(client) => client.query(query, sql).await,
            Executor::Txn(txn) => txn.query(query, sql).await,
        }
    }

    async fn execute(&mut self, query: Query<'_, MySql, MySqlArguments>, sql: &str) -> Result<u64> {
        match self {
            Executor::Default(client) => client.execute(query, sql).await,
            Executor::Txn(txn) => txn.execute(query, sql).await,
        }
    }

    async fn commit(self) -> Result<()> {
        match self {
            Executor::Txn(txn) => txn.commit().await,
            _ => Ok(()),
        }
    }
}

/// MySQL client for election.
pub struct ElectionMysqlClient {
    current: Option<PoolConnection<MySql>>,
    pool: MySqlPool,

    /// The client-side timeout for statement execution.
    ///
    /// This timeout is enforced by the client application and is independent of any server-side timeouts.
    /// If a statement takes longer than this duration to execute, the client will abort the operation.
    execution_timeout: Duration,

    /// The maximum execution time for the statement.
    ///
    /// This timeout is enforced by the server and is independent of any client-side timeouts.
    /// If a statement takes longer than this duration to execute, the server will abort the operation.
    max_execution_time: Duration,

    /// The lock wait timeout for the session.
    ///
    /// This timeout determines how long the server waits for a lock to be acquired before timing out.
    /// If a lock cannot be acquired within this duration, the server will abort the operation.
    innode_lock_wait_timeout: Duration,

    /// The wait timeout for the session.
    ///
    /// This timeout determines how long the server waits for activity on a noninteractive connection
    /// before closing it. If a connection is idle for longer than this duration, the server will
    /// terminate it.
    wait_timeout: Duration,

    /// The table name for election.
    table_for_election: String,
}

impl ElectionMysqlClient {
    pub fn new(
        pool: MySqlPool,
        execution_timeout: Duration,
        max_execution_time: Duration,
        innode_lock_wait_timeout: Duration,
        wait_timeout: Duration,
        table_for_election: &str,
    ) -> Self {
        Self {
            current: None,
            pool,
            execution_timeout,
            max_execution_time,
            innode_lock_wait_timeout,
            wait_timeout,
            table_for_election: table_for_election.to_string(),
        }
    }

    fn create_table_sql(&self) -> String {
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS `{}` (
                k VARBINARY(3072) PRIMARY KEY,
                v BLOB
            );"#,
            self.table_for_election
        )
    }

    fn insert_once_sql(&self) -> String {
        format!(
            "INSERT IGNORE INTO `{}` (k, v) VALUES ('__place_holder_for_lock', '');",
            self.table_for_election
        )
    }

    fn check_version_sql(&self) -> String {
        "SELECT @@version;".to_string()
    }

    async fn reset_client(&mut self) -> Result<()> {
        self.current = None;
        self.maybe_init_client().await
    }

    async fn ensure_table_exists(&mut self) -> Result<()> {
        let create_table_sql = self.create_table_sql();
        let query = sqlx::query(&create_table_sql);
        self.execute(query, &create_table_sql).await?;
        // Insert at least one row for `SELECT * FOR UPDATE` to work.
        let insert_once_sql = self.insert_once_sql();
        let query = sqlx::query(&insert_once_sql);
        self.execute(query, &insert_once_sql).await?;
        Ok(())
    }

    async fn maybe_init_client(&mut self) -> Result<()> {
        if self.current.is_none() {
            let client = self.pool.acquire().await.context(AcquireMySqlClientSnafu)?;

            self.current = Some(client);
            let (query, sql) = if !self.wait_timeout.is_zero() {
                let sql = "SET SESSION wait_timeout = ?, innodb_lock_wait_timeout = ?, max_execution_time = ?;";
                (
                    sqlx::query(sql)
                        .bind(self.wait_timeout.as_secs())
                        .bind(self.innode_lock_wait_timeout.as_secs())
                        .bind(self.max_execution_time.as_millis() as u64),
                    sql,
                )
            } else {
                let sql = "SET SESSION innodb_lock_wait_timeout = ?, max_execution_time = ?;";
                (
                    sqlx::query(sql)
                        .bind(self.innode_lock_wait_timeout.as_secs())
                        .bind(self.max_execution_time.as_millis() as u64),
                    sql,
                )
            };
            self.set_session_isolation_level().await?;
            self.execute(query, sql).await?;
            self.check_version(&self.check_version_sql()).await?;
        }
        Ok(())
    }

    /// Set session isolation level to serializable.
    ///
    /// # Panics
    /// if `current` is `None`.
    async fn set_session_isolation_level(&mut self) -> Result<()> {
        let sql = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        let query = sqlx::query(sql);
        self.execute(query, sql).await?;
        Ok(())
    }

    /// Check if the MySQL version is supported.
    ///
    /// # Panics
    /// if `current` is `None`.
    async fn check_version(&mut self, sql: &str) -> Result<()> {
        // Check if the MySQL version is supported.
        let query = sqlx::query(sql);
        // Safety: `maybe_init_client` ensures `current` is not `None`.
        let client = self.current.as_mut().unwrap();
        let row = tokio::time::timeout(self.execution_timeout, query.fetch_one(&mut **client))
            .await
            .map_err(|_| {
                SqlExecutionTimeoutSnafu {
                    sql,
                    duration: self.execution_timeout,
                }
                .build()
            })?;
        match row {
            Ok(row) => {
                let version: String = row.try_get(0).context(DecodeSqlValueSnafu {})?;
                if !version.starts_with("8.0") && !version.starts_with("5.7") {
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

    /// Returns the result of the query.
    ///
    /// # Panics
    /// if `current` is `None`.
    async fn execute(&mut self, query: Query<'_, MySql, MySqlArguments>, sql: &str) -> Result<u64> {
        // Safety: `maybe_init_client` ensures `current` is not `None`.
        let client = self.current.as_mut().unwrap();
        let future = query.execute(&mut **client);
        let rows_affected = tokio::time::timeout(self.execution_timeout, future)
            .await
            .map_err(|_| {
                SqlExecutionTimeoutSnafu {
                    sql,
                    duration: self.execution_timeout,
                }
                .build()
            })?
            .context(MySqlExecutionSnafu { sql })?
            .rows_affected();
        Ok(rows_affected)
    }

    /// Returns the result of the query.
    ///
    /// # Panics
    /// if `current` is `None`.
    async fn query(
        &mut self,
        query: Query<'_, MySql, MySqlArguments>,
        sql: &str,
    ) -> Result<Vec<MySqlRow>> {
        // Safety: `maybe_init_client` ensures `current` is not `None`.
        let client = self.current.as_mut().unwrap();
        let future = query.fetch_all(&mut **client);
        tokio::time::timeout(self.execution_timeout, future)
            .await
            .map_err(|_| {
                SqlExecutionTimeoutSnafu {
                    sql,
                    duration: self.execution_timeout,
                }
                .build()
            })?
            .context(MySqlExecutionSnafu { sql })
    }

    async fn transaction(&mut self) -> Result<TransactionWithExecutionTimeout<'_>> {
        use sqlx::Acquire;
        let client = self.current.as_mut().unwrap();
        let transaction = client
            .begin()
            .await
            .context(MySqlExecutionSnafu { sql: "BEGIN" })?;

        Ok(TransactionWithExecutionTimeout {
            transaction,
            execution_timeout: self.execution_timeout,
        })
    }
}

struct TransactionWithExecutionTimeout<'a> {
    transaction: MySqlTransaction<'a>,
    execution_timeout: Duration,
}

impl TransactionWithExecutionTimeout<'_> {
    async fn query(
        &mut self,
        query: Query<'_, MySql, MySqlArguments>,
        sql: &str,
    ) -> Result<Vec<MySqlRow>> {
        let res = tokio::time::timeout(
            self.execution_timeout,
            query.fetch_all(&mut *self.transaction),
        )
        .await
        .map_err(|_| {
            SqlExecutionTimeoutSnafu {
                sql,
                duration: self.execution_timeout,
            }
            .build()
        })?
        .context(MySqlExecutionSnafu { sql })?;
        Ok(res)
    }

    async fn execute(&mut self, query: Query<'_, MySql, MySqlArguments>, sql: &str) -> Result<u64> {
        let res = tokio::time::timeout(
            self.execution_timeout,
            query.execute(&mut *self.transaction),
        )
        .await
        .map_err(|_| {
            SqlExecutionTimeoutSnafu {
                sql,
                duration: self.execution_timeout,
            }
            .build()
        })?
        .context(MySqlExecutionSnafu { sql })?;
        Ok(res.rows_affected())
    }

    async fn commit(self) -> Result<()> {
        tokio::time::timeout(self.execution_timeout, self.transaction.commit())
            .await
            .map_err(|_| {
                SqlExecutionTimeoutSnafu {
                    sql: "COMMIT",
                    duration: self.execution_timeout,
                }
                .build()
            })?
            .context(MySqlExecutionSnafu { sql: "COMMIT" })?;
        Ok(())
    }
}

/// MySQL implementation of Election.
pub struct MySqlElection {
    leader_value: String,
    client: Mutex<ElectionMysqlClient>,
    is_leader: AtomicBool,
    leader_infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
    candidate_lease_ttl: Duration,
    meta_lease_ttl: Duration,
    sql_set: ElectionSqlSet,
}

impl MySqlElection {
    pub async fn with_mysql_client(
        leader_value: String,
        mut client: ElectionMysqlClient,
        store_key_prefix: String,
        candidate_lease_ttl: Duration,
        meta_lease_ttl: Duration,
        table_name: &str,
    ) -> Result<ElectionRef> {
        let sql_factory = ElectionSqlFactory::new(table_name);
        client.maybe_init_client().await?;
        client.ensure_table_exists().await?;
        let tx = listen_leader_change(leader_value.clone());
        Ok(Arc::new(Self {
            leader_value,
            client: Mutex::new(client),
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

    async fn maybe_init_client(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.maybe_init_client().await?;
        Ok(())
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
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        self.maybe_init_client().await?;
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
                    self.candidate_lease_ttl.as_secs(),
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
                    self.candidate_lease_ttl.as_secs(),
                    &mut executor,
                )
                .await?;
            }
        }

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval = tokio::time::interval(self.candidate_lease_ttl / 2);
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

            self.update_value_with_lease(
                &key,
                &lease.origin,
                &node_info,
                self.candidate_lease_ttl.as_secs(),
                &mut executor,
            )
            .await?;
            std::mem::drop(executor);
        }
    }

    async fn all_candidates(&self) -> Result<Vec<MetasrvNodeInfo>> {
        self.maybe_init_client().await?;
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
        self.maybe_init_client().await?;
        let mut keep_alive_interval = tokio::time::interval(self.meta_lease_ttl / 2);
        keep_alive_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            self.do_campaign().await?;
            keep_alive_interval.tick().await;
        }
    }

    async fn reset_campaign(&self) {
        info!("Resetting campaign");
        if self.is_leader.load(Ordering::Relaxed) {
            if let Err(err) = self.step_down_without_lock().await {
                error!(err; "Failed to step down without lock");
            }
            info!("Step down without lock successfully, due to reset campaign");
        }
        if let Err(err) = self.client.lock().await.reset_client().await {
            error!(err; "Failed to reset client");
        }
    }

    async fn leader(&self) -> Result<Self::Leader> {
        self.maybe_init_client().await?;
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
        lease_ttl: u64,
        executor: &mut Executor<'_>,
    ) -> Result<()> {
        let key = key.as_bytes();
        let prev = prev.as_bytes();
        let updated = updated.as_bytes();

        let query = sqlx::query(&self.sql_set.update_value_with_lease)
            .bind(updated)
            .bind(lease_ttl as f64)
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
        let lease = {
            let client = self.client.lock().await;
            let mut executor = Executor::Default(client);
            self.get_value_with_lease(&self.election_key(), &mut executor)
                .await?
        };

        let is_leader = self.is_leader();
        // If current leader value is the same as the leader value in the remote lease,
        // it means the current leader is still valid.
        let is_current_leader = lease
            .as_ref()
            .map(|lease| lease.leader_value == self.leader_value)
            .unwrap_or(false);
        match (self.lease_check(&lease), is_leader, is_current_leader) {
            // If the leader lease is valid and I'm the leader, renew the lease.
            (Ok(_), true, true) => {
                let mut client = self.client.lock().await;
                let txn = client.transaction().await?;
                let mut executor = Executor::Txn(txn);
                let query = sqlx::query(&self.sql_set.campaign);
                executor.query(query, &self.sql_set.campaign).await?;
                // Safety: Checked if lease is not None above.
                self.renew_lease(executor, lease.unwrap()).await?;
            }
            // If the leader lease expires and I'm the leader, notify the leader watcher and step down.
            // Another instance should be elected as the leader in this case.
            (Err(err), true, _) => {
                warn!(err; "Leader lease expired, step down...");
                self.step_down_without_lock().await?;
            }
            (Ok(_), true, false) => {
                warn!("Leader lease expired, step down...");
                self.step_down_without_lock().await?;
            }
            // If the leader lease expires and I'm not the leader, elect myself.
            (Err(err), false, _) => {
                warn!(err; "Leader lease expired, elect myself.");
                let mut client = self.client.lock().await;
                let txn = client.transaction().await?;
                let mut executor = Executor::Txn(txn);
                let query = sqlx::query(&self.sql_set.campaign);
                executor.query(query, &self.sql_set.campaign).await?;
                self.elected(executor, lease).await?;
            }
            // If the leader lease is valid and I'm the leader, but I don't think I'm the leader.
            // Just re-elect myself.
            (Ok(_), false, true) => {
                warn!("I should be the leader, but I don't think so. Something went wrong.");
                let mut client = self.client.lock().await;
                let txn = client.transaction().await?;
                let mut executor = Executor::Txn(txn);
                let query = sqlx::query(&self.sql_set.campaign);
                executor.query(query, &self.sql_set.campaign).await?;
                // Safety: Checked if lease is not None above.
                self.renew_lease(executor, lease.unwrap()).await?;
            }
            // If the leader lease is valid and I'm not the leader, do nothing.
            (Ok(_), false, false) => {}
        }
        Ok(())
    }

    /// Renew the lease
    async fn renew_lease(&self, mut executor: Executor<'_>, lease: Lease) -> Result<()> {
        let key = self.election_key();
        self.update_value_with_lease(
            &key,
            &lease.origin,
            &self.leader_value,
            self.meta_lease_ttl.as_secs(),
            &mut executor,
        )
        .await?;
        executor.commit().await?;

        if !self.is_leader() {
            let key = self.election_key();
            let leader_key = RdsLeaderKey {
                name: self.leader_value.clone().into_bytes(),
                key: key.clone().into_bytes(),
                ..Default::default()
            };
            send_leader_change_and_set_flags(
                &self.is_leader,
                &self.leader_infancy,
                &self.leader_watcher,
                LeaderChangeMessage::Elected(Arc::new(leader_key)),
            );
        }

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
    fn lease_check(&self, lease: &Option<Lease>) -> Result<Lease> {
        let lease = lease.as_ref().context(NoLeaderSnafu)?;
        // Case 1: Lease expired
        ensure!(lease.expire_time > lease.current, LeaderLeaseExpiredSnafu);
        // Case 2: Everything is fine
        Ok(lease.clone())
    }

    /// Still consider itself as the leader locally but failed to acquire the lock. Step down without deleting the key.
    async fn step_down_without_lock(&self) -> Result<()> {
        let key = self.election_key().into_bytes();
        let leader_key = RdsLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone(),
            ..Default::default()
        };
        send_leader_change_and_set_flags(
            &self.is_leader,
            &self.leader_infancy,
            &self.leader_watcher,
            LeaderChangeMessage::StepDown(Arc::new(leader_key)),
        );
        Ok(())
    }

    /// Elected as leader. The leader should put the key and notify the leader watcher.
    /// Caution: Should only elected while holding the lock.
    async fn elected(
        &self,
        mut executor: Executor<'_>,
        expected_lease: Option<Lease>,
    ) -> Result<()> {
        let key = self.election_key();
        let leader_key = RdsLeaderKey {
            name: self.leader_value.clone().into_bytes(),
            key: key.clone().into_bytes(),
            ..Default::default()
        };
        let remote_lease = self.get_value_with_lease(&key, &mut executor).await?;
        ensure!(
            expected_lease.map(|lease| lease.origin) == remote_lease.map(|lease| lease.origin),
            LeaderLeaseChangedSnafu
        );
        self.delete_value(&key, &mut executor).await?;
        self.put_value_with_lease(
            &key,
            &self.leader_value,
            self.meta_lease_ttl.as_secs(),
            &mut executor,
        )
        .await?;
        executor.commit().await?;

        send_leader_change_and_set_flags(
            &self.is_leader,
            &self.leader_infancy,
            &self.leader_watcher,
            LeaderChangeMessage::Elected(Arc::new(leader_key)),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::env;

    use common_meta::maybe_skip_mysql_integration_test;
    use common_telemetry::init_default_ut_logging;

    use super::*;
    use crate::bootstrap::create_mysql_pool;
    use crate::error;

    async fn create_mysql_client(
        table_name: Option<&str>,
        execution_timeout: Duration,
        wait_timeout: Duration,
    ) -> Result<Mutex<ElectionMysqlClient>> {
        init_default_ut_logging();
        let endpoint = env::var("GT_MYSQL_ENDPOINTS").unwrap_or_default();
        if endpoint.is_empty() {
            return UnexpectedSnafu {
                violated: "MySQL endpoint is empty".to_string(),
            }
            .fail();
        }
        let pool = create_mysql_pool(&[endpoint]).await.unwrap();
        let mut client = ElectionMysqlClient::new(
            pool,
            execution_timeout,
            execution_timeout,
            Duration::from_secs(1),
            wait_timeout,
            table_name.unwrap_or("default_greptime_metakv-election"),
        );
        client.maybe_init_client().await?;
        if table_name.is_some() {
            client.ensure_table_exists().await?;
        }
        Ok(Mutex::new(client))
    }

    async fn drop_table(client: &Mutex<ElectionMysqlClient>, table_name: &str) {
        let mut client = client.lock().await;
        let sql = format!("DROP TABLE IF EXISTS `{}`;", table_name);
        client.execute(sqlx::query(&sql), &sql).await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_crud() {
        maybe_skip_mysql_integration_test!();
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_mysql_crud_greptime-metakv";
        let candidate_lease_ttl = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);

        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
            .await
            .unwrap();

        {
            let mut a = client.lock().await;
            let txn = a.transaction().await.unwrap();
            let mut executor = Executor::Txn(txn);
            let raw_query = format!("SELECT * FROM `{}` FOR UPDATE;", table_name);
            let query = sqlx::query(&raw_query);
            let _ = executor.query(query, &raw_query).await.unwrap();
        }

        let (tx, _) = broadcast::channel(100);
        let mysql_election = MySqlElection {
            leader_value: "test_leader".to_string(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
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
            .update_value_with_lease(&key, &lease.origin, &value, 10, &mut executor)
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
        candidate_lease_ttl: Duration,
        store_key_prefix: String,
        table_name: String,
    ) {
        let meta_lease_ttl = Duration::from_secs(2);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let client =
            create_mysql_client(Some(&table_name), execution_timeout, idle_session_timeout)
                .await
                .unwrap();

        let (tx, _) = broadcast::channel(100);
        let mysql_election = MySqlElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl,
            meta_lease_ttl,
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
        maybe_skip_mysql_integration_test!();
        let leader_value_prefix = "test_leader".to_string();
        let candidate_lease_ttl = Duration::from_secs(2);
        let execution_timeout = Duration::from_secs(10);
        let meta_lease_ttl = Duration::from_secs(2);
        let idle_session_timeout = Duration::from_secs(0);
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_candidate_registration_greptime-metakv";
        let mut handles = vec![];
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
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
        tokio::time::sleep(candidate_lease_ttl / 2 + Duration::from_secs(1)).await;

        let (tx, _) = broadcast::channel(100);
        let leader_value = "test_leader".to_string();
        let mysql_election = MySqlElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid.clone(),
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        let candidates = mysql_election.all_candidates().await.unwrap();
        assert_eq!(candidates.len(), 10);

        for handle in handles {
            handle.abort();
        }

        // Wait for the candidate leases to expire.
        tokio::time::sleep(candidate_lease_ttl + Duration::from_secs(1)).await;
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

    async fn elected(
        election: &MySqlElection,
        table_name: &str,
        expected_lease: Option<Lease>,
    ) -> Result<()> {
        let mut client = election.client.lock().await;
        let txn = client.transaction().await.unwrap();
        let mut executor = Executor::Txn(txn);
        let raw_query = format!("SELECT * FROM `{}` FOR UPDATE;", table_name);
        let query = sqlx::query(&raw_query);
        let _ = executor.query(query, &raw_query).await.unwrap();
        election.elected(executor, expected_lease).await
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
    async fn test_elected_with_incorrect_lease_fails() {
        maybe_skip_mysql_integration_test!();
        let leader_value = "test_leader".to_string();
        let candidate_lease_ttl = Duration::from_secs(5);
        let meta_lease_ttl = Duration::from_secs(2);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_elected_failed_greptime-metakv";
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        let incorrect_lease = Lease::default();
        let err = elected(&leader_mysql_election, table_name, Some(incorrect_lease))
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::LeaderLeaseChanged { .. });
        let lease = get_lease(&leader_mysql_election).await;
        assert!(lease.is_none());
        drop_table(&leader_mysql_election.client, table_name).await;
    }

    #[tokio::test]
    async fn test_reelection_with_idle_session_timeout() {
        maybe_skip_mysql_integration_test!();
        let leader_value = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_reelection_greptime-metakv";
        let candidate_lease_ttl = Duration::from_secs(5);
        let meta_lease_ttl = Duration::from_secs(5);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(2);
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        elected(&leader_mysql_election, table_name, None)
            .await
            .unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());
        // Wait for mysql server close the inactive connection.
        tokio::time::sleep(Duration::from_millis(2100)).await;
        // Should be failed.
        leader_mysql_election
            .client
            .lock()
            .await
            .query(sqlx::query("SELECT 1"), "SELECT 1")
            .await
            .unwrap_err();
        // Reset the client.
        leader_mysql_election
            .client
            .lock()
            .await
            .reset_client()
            .await
            .unwrap();

        // Should able to re-elected.
        elected(&leader_mysql_election, table_name, Some(lease.clone()))
            .await
            .unwrap();
        let lease = get_lease(&leader_mysql_election).await.unwrap();
        assert_eq!(lease.leader_value, leader_value);
        assert!(lease.expire_time > lease.current);
        assert!(leader_mysql_election.is_leader());
        drop_table(&leader_mysql_election.client, table_name).await;
    }

    #[tokio::test]
    async fn test_elected_and_step_down() {
        maybe_skip_mysql_integration_test!();
        let leader_value = "test_leader".to_string();
        let candidate_lease_ttl = Duration::from_secs(5);
        let meta_lease_ttl = Duration::from_secs(2);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_elected_and_step_down_greptime-metakv";
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
            .await
            .unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        elected(&leader_mysql_election, table_name, None)
            .await
            .unwrap();
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

        elected(&leader_mysql_election, table_name, Some(lease.clone()))
            .await
            .unwrap();
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
        maybe_skip_mysql_integration_test!();
        let leader_value = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_leader_action_greptime-metakv";
        let candidate_lease_ttl = Duration::from_secs(5);
        let meta_lease_ttl = Duration::from_secs(2);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
            .await
            .unwrap();

        let (tx, mut rx) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: leader_value.clone(),
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
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
        tokio::time::sleep(meta_lease_ttl + Duration::from_secs(1)).await;
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
    async fn test_reset_campaign() {
        maybe_skip_mysql_integration_test!();
        common_telemetry::init_default_ut_logging();
        let leader_value = "test_leader".to_string();
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_reset_campaign_greptime-metakv";
        let candidate_lease_ttl = Duration::from_secs(5);
        let meta_lease_ttl = Duration::from_secs(2);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let client = create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };
        leader_mysql_election
            .is_leader
            .store(true, Ordering::Relaxed);
        leader_mysql_election.reset_campaign().await;
        assert!(!leader_mysql_election.is_leader());
        drop_table(&leader_mysql_election.client, table_name).await;
    }

    #[tokio::test]
    async fn test_follower_action() {
        maybe_skip_mysql_integration_test!();
        common_telemetry::init_default_ut_logging();
        let candidate_lease_ttl = Duration::from_secs(5);
        let meta_lease_ttl = Duration::from_secs(1);
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(0);
        let uuid = uuid::Uuid::new_v4().to_string();
        let table_name = "test_follower_action_greptime-metakv";

        let follower_client =
            create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
                .await
                .unwrap();
        let (tx, mut rx) = broadcast::channel(100);
        let follower_mysql_election = MySqlElection {
            leader_value: "test_follower".to_string(),
            client: follower_client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid.clone(),
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        let leader_client =
            create_mysql_client(Some(table_name), execution_timeout, idle_session_timeout)
                .await
                .unwrap();
        let (tx, _) = broadcast::channel(100);
        let leader_mysql_election = MySqlElection {
            leader_value: "test_leader".to_string(),
            client: leader_client,
            is_leader: AtomicBool::new(false),
            leader_infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: uuid,
            candidate_lease_ttl,
            meta_lease_ttl,
            sql_set: ElectionSqlFactory::new(table_name).build(),
        };

        leader_mysql_election.do_campaign().await.unwrap();

        // Step 1: As a follower, the leader exists and the lease is not expired. Do nothing.
        follower_mysql_election.do_campaign().await.unwrap();

        // Step 2: As a follower, the leader exists but the lease expired. Re-elect itself.
        tokio::time::sleep(meta_lease_ttl + Duration::from_secs(1)).await;
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

    #[tokio::test]
    async fn test_wait_timeout() {
        maybe_skip_mysql_integration_test!();
        common_telemetry::init_default_ut_logging();
        let execution_timeout = Duration::from_secs(10);
        let idle_session_timeout = Duration::from_secs(1);

        let client = create_mysql_client(None, execution_timeout, idle_session_timeout)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        // Wait for the idle session timeout.
        let err = client
            .lock()
            .await
            .query(sqlx::query("SELECT 1"), "SELECT 1")
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::MySqlExecution { .. });
        // Reset the client and try again.
        client.lock().await.reset_client().await.unwrap();
        let _ = client
            .lock()
            .await
            .query(sqlx::query("SELECT 1"), "SELECT 1")
            .await
            .unwrap();
    }
}
