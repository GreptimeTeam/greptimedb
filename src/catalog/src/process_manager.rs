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

use api::v1::frontend::{ListProcessRequest, ProcessInfo};
use common_frontend::selector::{FrontendSelector, MetaClientSelector};
use common_frontend::ProcessManager;
use common_telemetry::{debug, info};
use common_time::util::current_time_millis;

use crate::error;

pub struct MetaProcessManager {
    server_addr: String,
    next_id: AtomicU64,
    catalogs: RwLock<HashMap<String, HashMap<u64, ProcessInfo>>>,
    frontend_selector: Option<MetaClientSelector>,
}

impl MetaProcessManager {
    /// Create a [MetaProcessManager] instance with server address and kv client.
    pub fn new(server_addr: String) -> error::Result<Self> {
        Ok(Self {
            server_addr,
            next_id: Default::default(),
            catalogs: Default::default(),
            frontend_selector: None,
        })
    }
}

#[async_trait::async_trait]
impl ProcessManager for MetaProcessManager {
    /// Registers a submitted query.
    fn register_query(
        &self,
        catalog: String,
        schemas: Vec<String>,
        query: String,
        client: String,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let process = ProcessInfo {
            id,
            catalog: catalog.clone(),
            schemas,
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
    fn deregister_query(&self, catalog: String, id: u64) {
        if let Entry::Occupied(mut o) = self.catalogs.write().unwrap().entry(catalog) {
            let process = o.get_mut().remove(&id);
            debug!("Deregister process: {:?}", process);
            if o.get_mut().is_empty() {
                o.remove();
            }
        }
    }

    fn deregister_all_queries(&self) {
        self.catalogs.write().unwrap().clear();
        info!("All queries on {} has been deregistered", self.server_addr);
    }

    /// List local running processes in given catalog.
    fn local_processes(
        &self,
        catalog: Option<&str>,
    ) -> common_frontend::error::Result<Vec<ProcessInfo>> {
        let catalogs = self.catalogs.read().unwrap();
        let result = if let Some(catalog) = catalog {
            if let Some(catalogs) = catalogs.get(catalog) {
                catalogs.values().cloned().collect()
            } else {
                vec![]
            }
        } else {
            catalogs
                .values()
                .flat_map(|v| v.values().cloned())
                .collect()
        };
        Ok(result)
    }

    async fn list_all_processes(
        &self,
        catalog: Option<&str>,
    ) -> common_frontend::error::Result<Vec<ProcessInfo>> {
        let mut processes = vec![];
        if let Some(remote_frontend_selector) = self.frontend_selector.as_ref() {
            let frontends = remote_frontend_selector
                .select(|node| &node.peer.addr != &self.server_addr)
                .await?;
            for mut f in frontends {
                processes.extend(f.list_process(ListProcessRequest {}).await?.processes);
            }
        }
        processes.extend(self.local_processes(catalog)?);
        Ok(processes)
    }
}

#[cfg(test)]
mod tests {
    use common_frontend::ProcessManager;

    use crate::process_manager::MetaProcessManager;

    #[tokio::test]
    async fn test_register_query() {
        let process_manager = MetaProcessManager::new("127.0.0.1:8000".to_string()).unwrap();
        let process_id = process_manager.register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "".to_string(),
        );

        let running_processes = process_manager.local_processes(None).unwrap();
        assert_eq!(running_processes.len(), 1);
        assert_eq!(&running_processes[0].frontend, "127.0.0.1:8000");
        assert_eq!(running_processes[0].id, process_id);
        assert_eq!(&running_processes[0].query, "SELECT * FROM table");

        process_manager.deregister_query("public".to_string(), process_id);
        assert_eq!(process_manager.local_processes(None).unwrap().len(), 0);
    }
}
