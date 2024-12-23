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

use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::{CompareAndPutRequest, PutRequest, RangeRequest};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::sync::broadcast;

use crate::election::{
    Election, LeaderChangeMessage, CANDIDATES_ROOT, CANDIDATE_LEASE_SECS, ELECTION_KEY,
};
use crate::error::{
    DeserializeFromJsonSnafu, KvBackendSnafu, Result, SerializeToJsonSnafu, UnexpectedSnafu,
};
use crate::metasrv::{ElectionRef, LeaderValue, MetasrvNodeInfo};

/// Value with a expire time. The expire time is in seconds since UNIX epoch.
#[derive(Debug, Serialize, Deserialize, Default)]
struct ValueWithLease {
    value: String,
    expire_time: f64,
}

/// PostgreSql implementation of Election.
/// TODO(CookiePie): Currently only support candidate registration. Add election logic.
pub struct PgElection {
    leader_value: String,
    kv_backend: KvBackendRef,
    is_leader: AtomicBool,
    infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
    candidate_lease_ttl_secs: u64,
}

impl PgElection {
    pub async fn with_pg_client(
        leader_value: String,
        kv_backend: KvBackendRef,
        store_key_prefix: String,
        candidate_lease_ttl_secs: u64,
    ) -> Result<ElectionRef> {
        let (tx, _) = broadcast::channel(100);
        Ok(Arc::new(Self {
            leader_value,
            kv_backend,
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

    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()> {
        let key = self.candidate_key();
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
                + self.candidate_lease_ttl_secs as f64,
        };
        let res = self.put_value_with_lease(&key, &value_with_lease).await?;
        // May registered before, just update the lease.
        if !res {
            self.delete_value(&key).await?;
            self.put_value_with_lease(&key, &value_with_lease).await?;
        }

        // Check if the current lease has expired and renew the lease.
        let mut keep_alive_interval =
            tokio::time::interval(Duration::from_secs(self.candidate_lease_ttl_secs / 2));
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
                        String::from_utf8_lossy(&key.into_bytes())
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
        let key_prefix = self.candidate_root();
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
    async fn get_value_with_lease(&self, key: &String) -> Result<Option<ValueWithLease>> {
        let prev = self
            .kv_backend
            .get(key.as_bytes())
            .await
            .context(KvBackendSnafu)?;

        if let Some(kv) = prev {
            let value: String = String::from_utf8_lossy(kv.value()).to_string();
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
        key_prefix: &str,
    ) -> Result<Vec<ValueWithLease>> {
        let range_request = RangeRequest::new().with_prefix(key_prefix);
        let res = self
            .kv_backend
            .range(range_request)
            .await
            .context(KvBackendSnafu)?;

        let mut value_with_leases = Vec::with_capacity(res.kvs.len());
        for kv in res.kvs {
            let value: String = String::from_utf8_lossy(kv.value()).to_string();
            let value_with_lease: ValueWithLease =
                serde_json::from_str(&value).with_context(|_| DeserializeFromJsonSnafu {
                    input: format!("{value:?}"),
                })?;
            value_with_leases.push(value_with_lease);
        }

        Ok(value_with_leases)
    }

    async fn update_value_with_lease(
        &self,
        key: &str,
        prev: &ValueWithLease,
        updated: &ValueWithLease,
    ) -> Result<()> {
        let prev = serde_json::to_string(prev).with_context(|_| SerializeToJsonSnafu {
            input: format!("{prev:?}"),
        })?;
        let updated = serde_json::to_string(updated).with_context(|_| SerializeToJsonSnafu {
            input: format!("{updated:?}"),
        })?;

        let cas_request = CompareAndPutRequest::new()
            .with_key(key)
            .with_expect(updated)
            .with_value(prev);
        let res = self
            .kv_backend
            .compare_and_put(cas_request)
            .await
            .context(KvBackendSnafu)?;

        match res.success {
            true => Ok(()),
            false => UnexpectedSnafu {
                violated: format!(
                    "CAS operation failed, key: {:?}",
                    String::from_utf8_lossy(key.as_bytes())
                ),
            }
            .fail(),
        }
    }

    /// Returns `true` if the insertion is successful
    async fn put_value_with_lease(&self, key: &str, value: &ValueWithLease) -> Result<bool> {
        let value = serde_json::to_string(value).with_context(|_| SerializeToJsonSnafu {
            input: format!("{value:?}"),
        })?;

        let put_request = PutRequest::new().with_key(key).with_value(value);
        let res = self
            .kv_backend
            .put(put_request)
            .await
            .context(KvBackendSnafu)?;

        Ok(res.prev_kv.is_none())
    }

    /// Returns `true` if the deletion is successful.
    /// Caution: Should only delete the key if the lease is expired.
    async fn delete_value(&self, key: &String) -> Result<bool> {
        let res = self
            .kv_backend
            .delete(key.as_bytes(), false)
            .await
            .context(KvBackendSnafu)?;

        Ok(res.is_none())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use common_meta::kv_backend::postgres::PgStore;
    use tokio_postgres::{Client, NoTls};

    use super::*;
    use crate::error::PostgresExecutionSnafu;

    async fn create_postgres_client(endpoint: &str) -> Result<Client> {
        if endpoint.is_empty() {
            return UnexpectedSnafu {
                violated: "Postgres endpoint is empty".to_string(),
            }
            .fail();
        }
        let (client, connection) = tokio_postgres::connect(endpoint, NoTls)
            .await
            .context(PostgresExecutionSnafu)?;
        tokio::spawn(async move {
            connection.await.context(PostgresExecutionSnafu).unwrap();
        });
        Ok(client)
    }

    async fn create_pg_kvbackend() -> Result<KvBackendRef> {
        let endpoint = env::var("GT_POSTGRES_ENDPOINTS").unwrap_or_default();
        let client = create_postgres_client(&endpoint).await?;
        let kv_backend = PgStore::with_pg_client(client)
            .await
            .context(KvBackendSnafu)?;
        Ok(kv_backend)
    }

    #[tokio::test]
    async fn test_postgres_crud() {
        let kv_backend = create_pg_kvbackend().await.unwrap();

        let key = "test_key".to_string();
        let value = ValueWithLease {
            value: "test_value".to_string(),
            expire_time: 0.0,
        };

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value: "test_leader".to_string(),
            kv_backend,
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

        let res = pg_election
            .get_value_with_lease(&key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res.value, value.value);

        let res = pg_election.delete_value(&key).await.unwrap();
        assert!(res);

        let res = pg_election.get_value_with_lease(&key).await.unwrap();
        assert!(res.is_none());

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let value = ValueWithLease {
                value: format!("test_value_{}", i),
                expire_time: 0.0,
            };
            pg_election
                .put_value_with_lease(&key, &value)
                .await
                .unwrap();
        }

        let key_prefix = "test_key".to_string();
        let res = pg_election
            .get_value_with_lease_by_prefix(&key_prefix)
            .await
            .unwrap();
        assert_eq!(res.len(), 10);

        for i in 0..10 {
            let key = format!("test_key_{}", i);
            let res = pg_election.delete_value(&key).await.unwrap();
            assert!(res);
        }

        let res = pg_election
            .get_value_with_lease_by_prefix(&key_prefix)
            .await
            .unwrap();
        assert!(res.is_empty());
    }

    async fn candidate(leader_value: String) {
        let kv_backend = create_pg_kvbackend().await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let pg_election = PgElection {
            leader_value,
            kv_backend,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: "test_prefix".to_string(),
            candidate_lease_ttl_secs: 10,
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
        let mut handles = vec![];
        for i in 0..10 {
            let leader_value = format!("{}{}", leader_value_prefix, i);
            let handle = tokio::spawn(candidate(leader_value.clone()));
            handles.push(handle);
        }
        // Wait for candidates to registrate themselves.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let kv_backend = create_pg_kvbackend().await.unwrap();

        let (tx, _) = broadcast::channel(100);
        let leader_value = "test_leader".to_string();
        let pg_election = PgElection {
            leader_value,
            kv_backend,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(true),
            leader_watcher: tx,
            store_key_prefix: "test_prefix".to_string(),
            candidate_lease_ttl_secs: 5,
        };

        let candidates = pg_election.all_candidates().await.unwrap();
        assert_eq!(candidates.len(), 10);

        for handle in handles {
            handle.abort();
        }

        // Wait for the candidate leases to expire.
        tokio::time::sleep(Duration::from_secs(10)).await;
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
