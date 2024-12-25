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

use common_time::Timestamp;
use itertools::Itertools;
use snafu::{ensure, ResultExt};
use tokio::sync::broadcast;
use tokio_postgres::Client;

use crate::election::{Election, LeaderChangeMessage, CANDIDATES_ROOT, ELECTION_KEY};
use crate::error::{
    DeserializeFromJsonSnafu, PostgresExecutionSnafu, Result, SerializeToJsonSnafu, UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

// Separator between value and expire time.
const LEASE_SEP: &str = r#"||__metadata_lease_sep||"#;

// SQL to put a value with expire time. Parameters: key, value, lease_prefix, expire_time
const PUT_IF_NOT_EXISTS_WITH_EXPIRE_TIME: &str = r#"
WITH prev AS (
    SELECT k, v FROM greptime_metakv WHERE k = $1
), insert AS (
    INSERT INTO greptime_metakv
    VALUES($1, $2 || $3 || TO_CHAR(CURRENT_TIMESTAMP + INTERVAL '1 second' * $4, 'YYYY-MM-DD HH24:MI:SS.MS'))
    ON CONFLICT (k) DO NOTHING
)

SELECT k, v FROM prev;
"#;

// SQL to update a value with expire time. Parameters: key, prev_value_with_lease, updated_value, lease_prefix, expire_time
const CAS_WITH_EXPIRE_TIME: &str = r#"
UPDATE greptime_metakv
SET k=$1,
v=$3 || $4 || TO_CHAR(CURRENT_TIMESTAMP + INTERVAL '1 second' * $5, 'YYYY-MM-DD HH24:MI:SS.MS')
WHERE 
    k=$1 AND v=$2
"#;

const GET_WITH_CURRENT_TIMESTAMP: &str = r#"SELECT v, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') FROM greptime_metakv WHERE k = $1"#;

const PREFIX_GET_WITH_CURRENT_TIMESTAMP: &str = r#"SELECT v, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') FROM greptime_metakv WHERE k LIKE $1"#;

const POINT_DELETE: &str = "DELETE FROM greptime_metakv WHERE k = $1 RETURNING k,v;";

/// Parse the value and expire time from the given string. The value should be in the format "value || __metadata_lease_prefix || expire_time".
fn parse_value_and_expire_time(value: &str) -> Result<(String, Timestamp)> {
    if let Some((value, expire_time)) = value.split(LEASE_SEP).collect_tuple() {
        // Given expire_time is in the format 'YYYY-MM-DD HH24:MI:SS.MS'
        let expire_time = match Timestamp::from_str(expire_time, None) {
            Ok(ts) => ts,
            Err(_) => UnexpectedSnafu {
                violated: format!("Invalid timestamp: {}", expire_time),
            }
            .fail()?,
        };
        Ok((value.to_string(), expire_time))
    } else {
        UnexpectedSnafu {
            violated: format!(
                "Invalid value {}, expect node info || {} || expire time",
                value, LEASE_SEP
            ),
        }
        .fail()
    }
}

/// PostgreSql implementation of Election.
/// TODO(CookiePie): Currently only support candidate registration. Add election logic.
pub struct PgElection {
    leader_value: String,
    client: Client,
    is_leader: AtomicBool,
    infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
    candidate_lease_ttl_secs: u64,
}

impl PgElection {
    pub async fn with_pg_client(
        leader_value: String,
        client: Client,
        store_key_prefix: String,
        candidate_lease_ttl_secs: u64,
    ) -> Result<ElectionRef> {
        let (tx, _) = broadcast::channel(100);
        Ok(Arc::new(Self {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix,
            candidate_lease_ttl_secs,
        }))
    }

    fn _election_key(&self) -> String {
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

    /// TODO(CookiePie): Split the candidate registration and keep alive logic into separate methods, so that upper layers can call them separately.
    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        let key = self.candidate_key();
        let node_info =
            serde_json::to_string(node_info).with_context(|_| SerializeToJsonSnafu {
                input: format!("{node_info:?}"),
            })?;
        let res = self.put_value_with_lease(&key, &node_info).await?;
        // May registered before, just update the lease.
        if !res {
            self.delete_value(&key).await?;
            self.put_value_with_lease(&key, &node_info).await?;
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

    async fn campaign(&self) -> Result<()> {
        todo!()
    }

    async fn leader(&self) -> Result<Self::Leader> {
        todo!()
    }

    async fn resign(&self) -> Result<()> {
        todo!()
    }
}

impl PgElection {
    /// Returns value, expire time and current time. If `with_origin` is true, the origin string is also returned.
    async fn get_value_with_lease(
        &self,
        key: &String,
        with_origin: bool,
    ) -> Result<Option<(String, Timestamp, Timestamp, Option<String>)>> {
        let res = self
            .client
            .query(GET_WITH_CURRENT_TIMESTAMP, &[&key])
            .await
            .context(PostgresExecutionSnafu)?;

        if res.is_empty() {
            Ok(None)
        } else {
            let current_time_str = res[0].get(1);
            let current_time = match Timestamp::from_str(current_time_str, None) {
                Ok(ts) => ts,
                Err(_) => UnexpectedSnafu {
                    violated: format!("Invalid timestamp: {}", current_time_str),
                }
                .fail()?,
            };

            let value_and_expire_time = res[0].get(0);
            let (value, expire_time) = parse_value_and_expire_time(value_and_expire_time)?;

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
        let key_prefix = format!("{}%", key_prefix);
        let res = self
            .client
            .query(PREFIX_GET_WITH_CURRENT_TIMESTAMP, &[&key_prefix])
            .await
            .context(PostgresExecutionSnafu)?;

        let mut values_with_leases = vec![];
        let mut current = Timestamp::default();
        for row in res {
            let current_time_str = row.get(1);
            current = match Timestamp::from_str(current_time_str, None) {
                Ok(ts) => ts,
                Err(_) => UnexpectedSnafu {
                    violated: format!("Invalid timestamp: {}", current_time_str),
                }
                .fail()?,
            };

            let value_and_expire_time = row.get(0);
            let (value, expire_time) = parse_value_and_expire_time(value_and_expire_time)?;

            values_with_leases.push((value, expire_time));
        }
        Ok((values_with_leases, current))
    }

    async fn update_value_with_lease(&self, key: &str, prev: &str, updated: &str) -> Result<()> {
        let res = self
            .client
            .execute(
                CAS_WITH_EXPIRE_TIME,
                &[
                    &key,
                    &prev,
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
                violated: format!("Failed to update key: {}", key),
            }
        );

        Ok(())
    }

    /// Returns `true` if the insertion is successful
    async fn put_value_with_lease(&self, key: &str, value: &str) -> Result<bool> {
        let res = self
            .client
            .query(
                PUT_IF_NOT_EXISTS_WITH_EXPIRE_TIME,
                &[
                    &key,
                    &value,
                    &LEASE_SEP,
                    &(self.candidate_lease_ttl_secs as f64),
                ],
            )
            .await
            .context(PostgresExecutionSnafu)?;
        Ok(res.is_empty())
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &String) -> Result<bool> {
        let res = self
            .client
            .query(POINT_DELETE, &[&key])
            .await
            .context(PostgresExecutionSnafu)?;

        Ok(res.len() == 1)
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
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: "test_prefix".to_string(),
            candidate_lease_ttl_secs: 10,
        };

        let res = pg_election
            .put_value_with_lease(&key, &value)
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
                .put_value_with_lease(&key, &value)
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

    async fn candidate(leader_value: String, candidate_lease_ttl_secs: u64) {
        let client = create_postgres_client().await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: "test_prefix".to_string(),
            candidate_lease_ttl_secs,
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
        let mut handles = vec![];
        for i in 0..10 {
            let leader_value = format!("{}{}", leader_value_prefix, i);
            let handle = tokio::spawn(candidate(leader_value, candidate_lease_ttl_secs));
            handles.push(handle);
        }
        // Wait for candidates to registrate themselves and renew their leases at least once.
        tokio::time::sleep(Duration::from_secs(6)).await;

        let client = create_postgres_client().await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let leader_value = "test_leader".to_string();
        let pg_election = PgElection {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: "test_prefix".to_string(),
            candidate_lease_ttl_secs,
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
                "test_prefix", CANDIDATES_ROOT, leader_value_prefix, i
            );
            let res = pg_election.delete_value(&key).await.unwrap();
            assert!(res);
        }
    }
}
