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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use api::v1::frontend::ProcessInfo;
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::{BatchDeleteRequest, BatchPutRequest, DeleteRangeRequest};
use common_meta::rpc::KeyValue;
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::{debug, info};
use common_time::util::current_time_millis;
use snafu::ResultExt;

use crate::error;

pub struct ProcessManager {
    server_addr: String,
    next_id: AtomicU64,
    catalogs: RwLock<HashMap<String, HashMap<u64, ProcessInfo>>>,
}

impl ProcessManager {
    /// Create a [ProcessManager] instance with server address and kv client.
    pub fn new(server_addr: String) -> error::Result<Self> {
        Ok(Self {
            server_addr,
            next_id: Default::default(),
            catalogs: Default::default(),
        })
    }

    /// Registers a submitted query.
    pub fn register_query(
        &self,
        catalog: String,
        schema: Vec<String>,
        query: String,
        client: String,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let process = ProcessInfo {
            id,
            catalog: catalog.clone(),
            schema,
            query,
            start_timestamp: current_time_millis(),
            client,
            frontend: self.server_addr.clone(),
        };
        self.catalogs
            .write()
            .unwrap()
            .entry(catalog)
            .or_default()
            .insert(id, process);
        id
    }

    /// De-register a query from process list.
    pub fn deregister_query(&self, catalog: String, id: u64) {
        if let Entry::Occupied(mut o) = self.catalogs.write().unwrap().entry(catalog) {
            let process = o.get_mut().remove(&id);
            debug!("Deregister process: {:?}", process);
            if o.get_mut().is_empty() {
                o.remove();
            }
        }
    }

    /// De-register all queries running on current frontend.
    pub fn deregister_all_queries(&self) {
        self.catalogs.write().unwrap().clear();
        info!("All queries on {} has been deregistered", self.server_addr);
    }

    /// List all running processes in cluster.
    pub fn list_all_processes(&self) -> Vec<ProcessInfo> {
        self.catalogs
            .read()
            .unwrap()
            .values()
            .flat_map(|v| v.values().cloned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::process_manager::ProcessManager;

    #[tokio::test]
    async fn test_register_query() {
        let process_manager = ProcessManager::new("127.0.0.1:8000".to_string()).unwrap();
        let process_id = process_manager.register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "".to_string(),
        );

        let running_processes = process_manager.list_all_processes();
        assert_eq!(running_processes.len(), 1);
        assert_eq!(&running_processes[0].frontend, "127.0.0.1:8000");
        assert_eq!(running_processes[0].id, process_id);
        assert_eq!(&running_processes[0].query, "SELECT * FROM table");

        process_manager.deregister_query("public".to_string(), process_id);
        assert_eq!(process_manager.list_all_processes().len(), 0);
    }
}
