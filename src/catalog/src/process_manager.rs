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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_meta::key::process_list::{Process, ProcessKey, ProcessValue, PROCESS_ID_SEQ};
use common_meta::key::{MetadataKey, PROCESS_LIST_PREFIX};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::{BatchDeleteRequest, BatchPutRequest, DeleteRangeRequest};
use common_meta::rpc::KeyValue;
use common_meta::sequence::{SequenceBuilder, SequenceRef};
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::{debug, info};
use common_time::util::current_time_millis;
use snafu::ResultExt;

use crate::error;

pub struct ProcessManager {
    server_addr: String,
    sequencer: SequenceRef,
    kv_client: KvBackendRef,
    _report_task: RepeatedTask<error::Error>,
    pending_queries: Arc<Mutex<HashMap<u64, ProcessWithState>>>,
}

impl ProcessManager {
    /// Create a [ProcessManager] instance with server address and kv client.
    pub fn new(server_addr: String, kv_client: KvBackendRef) -> error::Result<Self> {
        let sequencer = Arc::new(
            SequenceBuilder::new(PROCESS_ID_SEQ, kv_client.clone())
                .initial(0)
                .step(10)
                .build(),
        );

        let pending_queries = Arc::new(Mutex::new(HashMap::new()));
        let pending_queries_clone = pending_queries.clone();
        let report_task = RepeatedTask::new(
            Duration::from_secs(5),
            Box::new(ReportTask {
                queries: pending_queries_clone,
                kv_backend: kv_client.clone(),
            }),
        );
        report_task
            .start(common_runtime::global_runtime())
            .context(error::StartReportTaskSnafu)?;
        Ok(Self {
            server_addr,
            sequencer,
            kv_client,
            _report_task: report_task,
            pending_queries,
        })
    }

    /// Registers a submitted query.
    pub async fn register_query(&self, database: String, query: String) -> error::Result<u64> {
        let process_id = self
            .sequencer
            .next()
            .await
            .context(error::BumpSequenceSnafu)?;
        let key = ProcessKey {
            frontend_ip: self.server_addr.clone(),
            id: process_id,
        };
        let value = ProcessValue {
            database,
            query,
            start_timestamp_ms: current_time_millis(),
        };
        let process = Process { key, value };
        self.pending_queries.lock().unwrap().insert(
            process_id,
            ProcessWithState {
                process,
                state: ProcessState::Running,
            },
        );
        Ok(process_id)
    }

    /// De-register a query from process list.
    pub async fn deregister_query(&self, id: u64) -> error::Result<()> {
        if let Entry::Occupied(mut e) = self.pending_queries.lock().unwrap().entry(id) {
            let process = e.get_mut();
            if process.state == ProcessState::Running {
                e.remove();
            } else {
                debug_assert_eq!(process.state, ProcessState::Registered);
                process.state = ProcessState::Finished;
            }
        }
        Ok(())
    }

    /// De-register all queries running on current frontend.
    pub async fn deregister_all_queries(&self) -> error::Result<()> {
        let prefix = format!("{}/{}-", PROCESS_LIST_PREFIX, self.server_addr);
        let delete_range_request = DeleteRangeRequest::new().with_prefix(prefix.as_bytes());
        self.pending_queries.lock().unwrap().clear();
        self.kv_client
            .delete_range(delete_range_request)
            .await
            .context(error::ReportProcessSnafu)?;
        info!("All queries on {} has been deregistered", self.server_addr);
        Ok(())
    }

    /// List all running processes in cluster.
    pub fn list_all_processes(&self) -> error::Result<Vec<Process>> {
        let queries = self.pending_queries.lock().unwrap().clone();
        Ok(queries.into_values().map(|p| p.process).collect())
    }
}

#[derive(Debug, Clone)]
struct ProcessWithState {
    process: Process,
    state: ProcessState,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ProcessState {
    /// Running but not registered to meta
    Running,
    /// Running and registered to meta
    Registered,
    /// Finished
    Finished,
}

/// Periodical task to report running queries to kv backend.
struct ReportTask {
    queries: Arc<Mutex<HashMap<u64, ProcessWithState>>>,
    kv_backend: KvBackendRef,
}

#[async_trait::async_trait]
impl TaskFunction<error::Error> for ReportTask {
    async fn call(&mut self) -> error::Result<()> {
        let pending_queries = self.queries.lock().unwrap().clone();
        let mut queries_to_register = vec![];
        let mut queries_to_deregister = vec![];

        for (_, process) in pending_queries {
            match process.state {
                ProcessState::Running => {
                    queries_to_register.push(process);
                }
                ProcessState::Finished => {
                    queries_to_deregister.push(process);
                }
                _ => {}
            }
        }

        self.kv_backend
            .batch_put(BatchPutRequest {
                kvs: queries_to_register
                    .iter()
                    .map(|p| KeyValue::try_from(&p.process))
                    .collect::<Result<_, _>>()
                    .context(error::ReportProcessSnafu)?,
                prev_kv: false,
            })
            .await
            .context(error::ReportProcessSnafu)?;

        for registered in queries_to_register {
            // for running queries, it can either be still in running state or removed.
            if let Some(process) = self
                .queries
                .lock()
                .unwrap()
                .get_mut(&registered.process.key.id)
            {
                debug_assert_eq!(process.state, ProcessState::Running);
                process.state = ProcessState::Registered;
            } else {
                // Process can also be removed by `deregister_query`
                queries_to_deregister.push(registered);
            }
        }

        self.kv_backend
            .batch_delete(BatchDeleteRequest {
                keys: queries_to_deregister
                    .iter()
                    .map(|p| p.process.key.to_bytes())
                    .collect(),
                prev_kv: false,
            })
            .await
            .context(error::ReportProcessSnafu)?;
        for deregistered in queries_to_deregister {
            if let Some(process) = self
                .queries
                .lock()
                .unwrap()
                .remove(&deregistered.process.key.id)
            {
                debug!("Deregistered process {}", process.process.key);
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "ProcessManagerReportTask"
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
        let process_manager =
            ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();
        let process_id = process_manager
            .register_query("public".to_string(), "SELECT * FROM table".to_string())
            .await
            .unwrap();

        let running_processes = process_manager.list_all_processes().unwrap();
        assert_eq!(running_processes.len(), 1);
        assert_eq!(running_processes[0].key.frontend_ip, "127.0.0.1:8000");
        assert_eq!(running_processes[0].key.id, process_id);
        assert_eq!(running_processes[0].value.query, "SELECT * FROM table");

        process_manager.deregister_query(process_id).await.unwrap();
        assert_eq!(process_manager.list_all_processes().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_register_multiple_queries() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager =
            ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();

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
        assert_eq!(process_manager.list_all_processes().unwrap().len(), 3);

        // Deregister middle one
        process_manager.deregister_query(id2).await.unwrap();
        let processes = process_manager.list_all_processes().unwrap();
        assert_eq!(processes.len(), 2);
        assert!(processes.iter().any(|p| p.key.id == id1));
        assert!(processes.iter().any(|p| p.key.id == id3));
    }

    #[tokio::test]
    async fn test_deregister_nonexistent_query() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager =
            ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();

        // Try to deregister non-existent ID
        let result = process_manager.deregister_query(999).await;
        assert!(result.is_ok()); // Should succeed with warning
    }

    #[tokio::test]
    async fn test_process_timestamps() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager =
            ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();

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

        let running_processes = process_manager.list_all_processes().unwrap();
        let process = &running_processes[0];

        assert!(process.value.start_timestamp_ms >= before);
        assert!(process.value.start_timestamp_ms <= after);
    }

    #[tokio::test]
    async fn test_multiple_frontends() {
        let kv_client = Arc::new(MemoryKvBackend::new());

        // Create two process managers with different frontend addresses
        let pm1 = ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();
        let pm2 = ProcessManager::new("127.0.0.1:8001".to_string(), kv_client.clone()).unwrap();

        let id1 = pm1
            .register_query("public".to_string(), "SELECT 1".to_string())
            .await
            .unwrap();
        let id2 = pm2
            .register_query("public".to_string(), "SELECT 2".to_string())
            .await
            .unwrap();

        // Verify both processes are registered with correct frontend IPs
        let pm1_processes = pm1.list_all_processes().unwrap();
        assert_eq!(pm1_processes.len(), 1);

        let p1 = pm1_processes.iter().find(|p| p.key.id == id1).unwrap();
        assert_eq!(p1.key.frontend_ip, "127.0.0.1:8000");

        let p2_processes = pm2.list_all_processes().unwrap();
        assert_eq!(p2_processes.len(), 1);
        let p2 = p2_processes.iter().find(|p| p.key.id == id2).unwrap();
        assert_eq!(p2.key.frontend_ip, "127.0.0.1:8001");
        assert_eq!(p2.value.database, "public");
        // deregister all queries on instance 1
        pm1.deregister_all_queries().await.unwrap();

        let processes: Vec<_> = pm1.list_all_processes().unwrap();
        assert_eq!(processes.len(), 0);

        assert_eq!(pm2.list_all_processes().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_list_empty_processes() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager =
            ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();

        assert!(process_manager.list_all_processes().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_deregister_all_queries() {
        let kv_client = Arc::new(MemoryKvBackend::new());
        let process_manager =
            ProcessManager::new("127.0.0.1:8000".to_string(), kv_client.clone()).unwrap();

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
        assert_eq!(process_manager.list_all_processes().unwrap().len(), 3);

        // Deregister all
        process_manager.deregister_all_queries().await.unwrap();

        // Verify none remain
        assert_eq!(process_manager.list_all_processes().unwrap().len(), 0);
    }
}
