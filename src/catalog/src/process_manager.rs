use std::sync::Arc;

use common_meta::error;
use common_meta::key::process_list::{Process, ProcessKey, ProcessValue, PROCESS_ID_SEQ};
use common_meta::key::{MetadataKey, MetadataValue, PROCESS_LIST_PREFIX};
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::PaginationStream;
use common_meta::rpc::store::{DeleteRangeRequest, PutRequest, RangeRequest};
use common_meta::sequence::{SequenceBuilder, SequenceRef};
use common_telemetry::{debug, info, warn};
use common_time::util::current_time_millis;

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
    pub async fn register_query(&self, database: String, query: String) -> error::Result<u64> {
        let process_id = self.sequencer.next().await?;
        let key = ProcessKey {
            frontend_ip: self.server_addr.clone(),
            id: process_id,
        }
        .to_bytes();
        let current_time = current_time_millis();
        let value = ProcessValue {
            database,
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
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use common_meta::kv_backend::memory::MemoryKvBackend;

    use crate::process_manager::ProcessManager;

    #[tokio::test]
    async fn test_register_query() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone());
        let process_id = process_manager
            .register_query("public".to_string(), "SELECT * FROM table".to_string())
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
            .register_query("public".to_string(), "SELECT 1".to_string())
            .await
            .unwrap();
        let id2 = process_manager
            .register_query("public".to_string(), "SELECT 2".to_string())
            .await
            .unwrap();
        let id3 = process_manager
            .register_query("public".to_string(), "SELECT 3".to_string())
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
            .register_query("public".to_string(), "SELECT NOW()".to_string())
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

        let id1 = pm1
            .register_query("public".to_string(), "SELECT 1".to_string())
            .await
            .unwrap();
        let id2 = pm2
            .register_query("public".to_string(), "SELECT 2".to_string())
            .await
            .unwrap();

        // Verify both processes are registered with correct frontend IPs
        let pm1_processes = pm1.dump().await.unwrap();
        assert_eq!(pm1_processes.len(), 1);

        let p1 = pm1_processes.iter().find(|p| p.key.id == id1).unwrap();
        assert_eq!(p1.key.frontend_ip, "127.0.0.1:8000");

        let p2_processes = pm2.dump().await.unwrap();
        assert_eq!(p2_processes.len(), 1);
        let p2 = p2_processes.iter().find(|p| p.key.id == id2).unwrap();
        assert_eq!(p2.key.frontend_ip, "127.0.0.1:8001");
        assert_eq!(p2.value.database, "public");
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
            .register_query("public".to_string(), "SELECT 1".to_string())
            .await
            .unwrap();
        process_manager
            .register_query("public".to_string(), "SELECT 2".to_string())
            .await
            .unwrap();
        process_manager
            .register_query("public".to_string(), "SELECT 3".to_string())
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
