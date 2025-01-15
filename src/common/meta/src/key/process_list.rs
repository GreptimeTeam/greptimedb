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

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use common_telemetry::{debug, info, warn};
use common_time::util::current_time_millis;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error;
use crate::key::{MetadataKey, MetadataValue, PROCESS_LIST_PATTERN, PROCESS_LIST_PREFIX};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::PaginationStream;
use crate::rpc::store::{DeleteRangeRequest, PutRequest, RangeRequest};
use crate::sequence::{SequenceBuilder, SequenceRef};

/// Key for running queries tracked in metasrv.
/// Layout: `__process/{frontend server addr}-{query id}`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessKey {
    //todo(hl): maybe we don't have to own a string
    pub frontend_ip: String,
    pub id: u64,
}

impl Display for ProcessKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}-{}",
            PROCESS_LIST_PREFIX, self.frontend_ip, self.id
        )
    }
}

impl MetadataKey<'_, ProcessKey> for ProcessKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> error::Result<ProcessKey> {
        let key_str = std::str::from_utf8(bytes).map_err(|_e| {
            error::InvalidProcessKeySnafu {
                key: String::from_utf8_lossy(bytes).into_owned(),
            }
            .build()
        })?;
        let captures = PROCESS_LIST_PATTERN.captures(key_str).with_context(|| {
            error::InvalidProcessKeySnafu {
                key: String::from_utf8_lossy(bytes).into_owned(),
            }
        })?;
        let id = u64::from_str(&captures[2]).map_err(|_| {
            error::InvalidProcessKeySnafu {
                key: String::from_utf8_lossy(bytes).into_owned(),
            }
            .build()
        })?;
        Ok(Self {
            frontend_ip: captures[1].to_string(),
            id,
        })
    }
}

/// Detail value of process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessValue {
    /// The running query sql.
    pub query: String,
    /// Query start timestamp in milliseconds.
    pub start_timestamp_ms: i64,
}

/// Sequence key for process list entries.
pub const PROCESS_ID_SEQ: &str = "process_id_seq";

/// Running process instance.
pub struct Process {
    key: ProcessKey,
    value: ProcessValue,
}

impl Process {
    /// Returns server address of running process.
    pub fn server_addr(&self) -> &str {
        &self.key.frontend_ip
    }

    /// Returns id of query.
    pub fn query_id(&self) -> u64 {
        self.key.id
    }

    /// Returns query string details.
    pub fn query_string(&self) -> &str {
        &self.value.query
    }

    /// Calculates the elapsed time of query. Returns None of system clock jumps backwards.
    pub fn query_elapsed(&self) -> Option<Duration> {
        let now = current_time_millis();
        if now < self.value.start_timestamp_ms {
            None
        } else {
            Some(Duration::from_millis(
                (now - self.value.start_timestamp_ms) as u64,
            ))
        }
    }
}

pub struct ProcessManager {
    server_addr: String,
    sequencer: SequenceRef,
    kv_client: KvBackendRef,
}

impl ProcessManager {
    /// Create a [ProcessManager] instance with server address and kv client.
    pub fn new(server_addr: String, kv_client: KvBackendRef) -> Self {
        let sequencer = Arc::new(
            SequenceBuilder::new(PROCESS_ID_SEQ, kv_client.clone())
                .initial(0)
                .step(100)
                .build(),
        );
        Self {
            server_addr,
            sequencer,
            kv_client,
        }
    }

    /// Registers a submitted query.
    pub async fn register_query(&self, query: String) -> error::Result<u64> {
        let process_id = self.sequencer.next().await?;
        let key = ProcessKey {
            frontend_ip: self.server_addr.clone(),
            id: process_id,
        }
        .to_bytes();
        let current_time = current_time_millis();
        let value = ProcessValue {
            query,
            start_timestamp_ms: current_time,
        }
        .try_as_raw_value()?;

        self.kv_client
            .put(PutRequest {
                key,
                value,
                prev_kv: false,
            })
            .await?;
        Ok(process_id)
    }

    /// De-register a query from process list.
    pub async fn deregister_query(&self, id: u64) -> error::Result<()> {
        let key = ProcessKey {
            frontend_ip: self.server_addr.clone(),
            id,
        }
        .to_bytes();
        let prev_kv = self.kv_client.delete(&key, true).await?;

        if let Some(_kv) = prev_kv {
            debug!("Successfully deregistered process {}", id);
        } else {
            warn!("Cannot find process to deregister process: {}", id);
        }
        Ok(())
    }

    /// De-register all queries running on current frontend.
    pub async fn deregister_all_queries(&self) -> error::Result<()> {
        let prefix = format!("{}/{}-", PROCESS_LIST_PREFIX, self.server_addr);
        let delete_range_request = DeleteRangeRequest::new().with_prefix(prefix.as_bytes());
        self.kv_client.delete_range(delete_range_request).await?;
        info!("All queries on {} has been deregistered", self.server_addr);
        Ok(())
    }

    /// List all running processes in cluster.
    pub fn list_all_processes(&self) -> error::Result<PaginationStream<Process>> {
        let prefix = format!("{}/{}-", PROCESS_LIST_PREFIX, self.server_addr);
        let req = RangeRequest::new().with_prefix(prefix.as_bytes());
        let stream = PaginationStream::new(self.kv_client.clone(), req, 100, |kv| {
            let key = ProcessKey::from_bytes(&kv.key)?;
            let value = ProcessValue::try_from_raw_value(&kv.value)?;
            Ok(Process { key, value })
        });
        Ok(stream)
    }

    #[cfg(test)]
    async fn dump(&self) -> error::Result<Vec<Process>> {
        use futures_util::TryStreamExt;
        self.list_all_processes()?.into_stream().try_collect().await
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::key::MetadataKey;

    fn check_serialization(input: ProcessKey) {
        let serialized = input.to_bytes();
        let key = ProcessKey::from_bytes(&serialized).unwrap();
        assert_eq!(&input.frontend_ip, &key.frontend_ip);
        assert_eq!(input.id, key.id);
    }

    #[test]
    fn test_capture_process_list() {
        check_serialization(ProcessKey {
            frontend_ip: "192.168.0.1:4001".to_string(),
            id: 1,
        });
        check_serialization(ProcessKey {
            frontend_ip: "192.168.0.1:4002".to_string(),
            id: 0,
        });
        check_serialization(ProcessKey {
            frontend_ip: "255.255.255.255:80".to_string(),
            id: 0,
        });
    }

    #[test]
    fn test_process_key_display() {
        let key = ProcessKey {
            frontend_ip: "127.0.0.1:3000".to_string(),
            id: 42,
        };
        assert_eq!(key.to_string(), "__process/127.0.0.1:3000-42");
    }

    #[test]
    fn test_process_key_from_bytes_valid() {
        let bytes = b"__process/10.0.0.1:8080-123";
        let key = ProcessKey::from_bytes(bytes).unwrap();
        assert_eq!(key.frontend_ip, "10.0.0.1:8080");
        assert_eq!(key.id, 123);
    }

    #[test]
    fn test_process_key_from_bytes_invalid_format() {
        let bytes = b"invalid_format";
        let result = ProcessKey::from_bytes(bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_key_from_bytes_invalid_id() {
        let bytes = b"__process/10.0.0.1:8080-abc";
        let result = ProcessKey::from_bytes(bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_value_serialization() {
        let value = ProcessValue {
            query: "SELECT * FROM test".to_string(),
            start_timestamp_ms: 1690000000000,
        };

        // Test serialization roundtrip
        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized: ProcessValue = serde_json::from_str(&serialized).unwrap();

        assert_eq!(value.query, deserialized.query);
        assert_eq!(value.start_timestamp_ms, deserialized.start_timestamp_ms);
    }

    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_register_query() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());
        let process_id = process_manager
            .register_query("SELECT * FROM table".to_string())
            .await
            .unwrap();

        let running_processes = process_manager.dump().await.unwrap();
        assert_eq!(running_processes.len(), 1);
        assert_eq!(running_processes[0].key.frontend_ip, "127.0.0.1:8000");
        assert_eq!(running_processes[0].key.id, process_id);
        assert_eq!(running_processes[0].value.query, "SELECT * FROM table");

        process_manager.deregister_query(process_id).await.unwrap();
        assert_eq!(process_manager.dump().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_register_multiple_queries() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());

        // Register multiple queries
        let id1 = process_manager
            .register_query("SELECT 1".to_string())
            .await
            .unwrap();
        let id2 = process_manager
            .register_query("SELECT 2".to_string())
            .await
            .unwrap();
        let id3 = process_manager
            .register_query("SELECT 3".to_string())
            .await
            .unwrap();

        // Verify all are registered
        assert_eq!(process_manager.dump().await.unwrap().len(), 3);

        // Deregister middle one
        process_manager.deregister_query(id2).await.unwrap();
        let processes = process_manager.dump().await.unwrap();
        assert_eq!(processes.len(), 2);
        assert!(processes.iter().any(|p| p.key.id == id1));
        assert!(processes.iter().any(|p| p.key.id == id3));
    }

    #[tokio::test]
    async fn test_deregister_nonexistent_query() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());

        // Try to deregister non-existent ID
        let result = process_manager.deregister_query(999).await;
        assert!(result.is_ok()); // Should succeed with warning
    }

    #[tokio::test]
    async fn test_process_timestamps() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        process_manager
            .register_query("SELECT NOW()".to_string())
            .await
            .unwrap();

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let running_processes = process_manager.dump().await.unwrap();
        let process = &running_processes[0];

        assert!(process.value.start_timestamp_ms >= before);
        assert!(process.value.start_timestamp_ms <= after);
    }

    #[tokio::test]
    async fn test_multiple_frontends() {
        let kv_client = Arc::new(MemoryKvBackend::new());

        // Create two process managers with different frontend addresses
        let pm1 = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());
        let pm2 = ProcessManager::new("127.0.0.1:8001".to_string(), kv_client.clone());

        let id1 = pm1.register_query("SELECT 1".to_string()).await.unwrap();
        let id2 = pm2.register_query("SELECT 2".to_string()).await.unwrap();

        // Verify both processes are registered with correct frontend IPs
        let pm1_processes = pm1.dump().await.unwrap();
        assert_eq!(pm1_processes.len(), 1);

        let p1 = pm1_processes.iter().find(|p| p.key.id == id1).unwrap();
        assert_eq!(p1.key.frontend_ip, "127.0.0.1:8000");

        let p2_processes = pm2.dump().await.unwrap();
        assert_eq!(p2_processes.len(), 1);
        let p2 = p2_processes.iter().find(|p| p.key.id == id2).unwrap();
        assert_eq!(p2.key.frontend_ip, "127.0.0.1:8001");

        // deregister all queries on instance 1
        pm1.deregister_all_queries().await.unwrap();

        let processes: Vec<_> = pm1.dump().await.unwrap();
        assert_eq!(processes.len(), 0);

        assert_eq!(pm2.dump().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_list_empty_processes() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());

        assert!(process_manager.dump().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_deregister_all_queries() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());

        // Register multiple queries
        process_manager
            .register_query("SELECT 1".to_string())
            .await
            .unwrap();
        process_manager
            .register_query("SELECT 2".to_string())
            .await
            .unwrap();
        process_manager
            .register_query("SELECT 3".to_string())
            .await
            .unwrap();

        // Verify they exist
        assert_eq!(process_manager.dump().await.unwrap().len(), 3);

        // Deregister all
        process_manager.deregister_all_queries().await.unwrap();

        // Verify none remain
        assert_eq!(process_manager.dump().await.unwrap().len(), 0);
    }
}
