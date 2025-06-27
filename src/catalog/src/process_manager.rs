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
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use api::v1::frontend::{KillProcessRequest, ListProcessRequest, ListProcessResponse, ProcessInfo};
use common_base::cancellation::CancellationHandle;
use common_frontend::selector::{FrontendSelector, MetaClientSelector};
use common_telemetry::{debug, info, warn};
use common_time::util::current_time_millis;
use meta_client::MetaClientRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::Error;
use crate::metrics::{PROCESS_KILL_COUNT, PROCESS_LIST_COUNT};

pub type ProcessId = u32;
pub type ProcessManagerRef = Arc<ProcessManager>;

/// Query process manager.
pub struct ProcessManager {
    /// Local frontend server address,
    server_addr: String,
    /// Next process id for local queries.
    next_id: AtomicU32,
    /// Running process per catalog.
    catalogs: RwLock<HashMap<String, HashMap<ProcessId, CancellableProcess>>>,
    /// Frontend selector to locate frontend nodes.
    frontend_selector: Option<MetaClientSelector>,
}

impl ProcessManager {
    /// Create a [ProcessManager] instance with server address and kv client.
    pub fn new(server_addr: String, meta_client: Option<MetaClientRef>) -> Self {
        let frontend_selector = meta_client.map(MetaClientSelector::new);
        Self {
            server_addr,
            next_id: Default::default(),
            catalogs: Default::default(),
            frontend_selector,
        }
    }
}

impl ProcessManager {
    /// Registers a submitted query. Use the provided id if present.
    #[must_use]
    pub fn register_query(
        self: &Arc<Self>,
        catalog: String,
        schemas: Vec<String>,
        query: String,
        client: String,
        query_id: Option<ProcessId>,
    ) -> Ticket {
        let id = query_id.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed));
        let process = ProcessInfo {
            id,
            catalog: catalog.clone(),
            schemas,
            query,
            start_timestamp: current_time_millis(),
            client,
            frontend: self.server_addr.clone(),
        };
        let cancellation_handle = Arc::new(CancellationHandle::default());
        let cancellable_process = CancellableProcess::new(cancellation_handle.clone(), process);

        self.catalogs
            .write()
            .unwrap()
            .entry(catalog.clone())
            .or_default()
            .insert(id, cancellable_process);

        Ticket {
            catalog,
            manager: self.clone(),
            id,
            cancellation_handle,
        }
    }

    /// Generates the next process id.
    pub fn next_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// De-register a query from process list.
    pub fn deregister_query(&self, catalog: String, id: ProcessId) {
        if let Entry::Occupied(mut o) = self.catalogs.write().unwrap().entry(catalog) {
            let process = o.get_mut().remove(&id);
            debug!("Deregister process: {:?}", process);
            if o.get().is_empty() {
                o.remove();
            }
        }
    }

    /// List local running processes in given catalog.
    pub fn local_processes(&self, catalog: Option<&str>) -> error::Result<Vec<ProcessInfo>> {
        let catalogs = self.catalogs.read().unwrap();
        let result = if let Some(catalog) = catalog {
            if let Some(catalogs) = catalogs.get(catalog) {
                catalogs.values().map(|p| p.process.clone()).collect()
            } else {
                vec![]
            }
        } else {
            catalogs
                .values()
                .flat_map(|v| v.values().map(|p| p.process.clone()))
                .collect()
        };
        Ok(result)
    }

    pub async fn list_all_processes(
        &self,
        catalog: Option<&str>,
    ) -> error::Result<Vec<ProcessInfo>> {
        let mut processes = vec![];
        if let Some(remote_frontend_selector) = self.frontend_selector.as_ref() {
            let frontends = remote_frontend_selector
                .select(|node| node.peer.addr != self.server_addr)
                .await
                .context(error::InvokeFrontendSnafu)?;
            for mut f in frontends {
                let result = f
                    .list_process(ListProcessRequest {
                        catalog: catalog.unwrap_or_default().to_string(),
                    })
                    .await
                    .context(error::InvokeFrontendSnafu);
                match result {
                    Ok(resp) => {
                        processes.extend(resp.processes);
                    }
                    Err(e) => {
                        warn!(e; "Skipping failing node")
                    }
                }
            }
        }
        processes.extend(self.local_processes(catalog)?);
        Ok(processes)
    }

    /// Kills query with provided catalog and id.
    pub async fn kill_process(
        &self,
        server_addr: String,
        catalog: String,
        id: ProcessId,
    ) -> error::Result<bool> {
        if server_addr == self.server_addr {
            self.kill_local_process(catalog, id).await
        } else {
            let mut nodes = self
                .frontend_selector
                .as_ref()
                .context(error::MetaClientMissingSnafu)?
                .select(|node| node.peer.addr == server_addr)
                .await
                .context(error::InvokeFrontendSnafu)?;
            ensure!(
                !nodes.is_empty(),
                error::FrontendNotFoundSnafu { addr: server_addr }
            );

            let request = KillProcessRequest {
                server_addr,
                catalog,
                process_id: id,
            };
            nodes[0]
                .kill_process(request)
                .await
                .context(error::InvokeFrontendSnafu)?;
            Ok(true)
        }
    }

    /// Kills local query with provided catalog and id.
    pub async fn kill_local_process(&self, catalog: String, id: ProcessId) -> error::Result<bool> {
        if let Some(catalogs) = self.catalogs.write().unwrap().get_mut(&catalog) {
            if let Some(process) = catalogs.remove(&id) {
                process.handle.cancel();
                info!(
                    "Killed process, catalog: {}, id: {:?}",
                    process.process.catalog, process.process.id
                );
                PROCESS_KILL_COUNT.with_label_values(&[&catalog]).inc();
                Ok(true)
            } else {
                debug!("Failed to kill process, id not found: {}", id);
                Ok(false)
            }
        } else {
            debug!("Failed to kill process, catalog not found: {}", catalog);
            Ok(false)
        }
    }
}

pub struct Ticket {
    pub(crate) catalog: String,
    pub(crate) manager: ProcessManagerRef,
    pub(crate) id: ProcessId,
    pub cancellation_handle: Arc<CancellationHandle>,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.manager
            .deregister_query(std::mem::take(&mut self.catalog), self.id);
    }
}

struct CancellableProcess {
    handle: Arc<CancellationHandle>,
    process: ProcessInfo,
}

impl Drop for CancellableProcess {
    fn drop(&mut self) {
        PROCESS_LIST_COUNT
            .with_label_values(&[&self.process.catalog])
            .dec();
    }
}

impl CancellableProcess {
    fn new(handle: Arc<CancellationHandle>, process: ProcessInfo) -> Self {
        PROCESS_LIST_COUNT
            .with_label_values(&[&process.catalog])
            .inc();
        Self { handle, process }
    }
}

impl Debug for CancellableProcess {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableProcess")
            .field("cancelled", &self.handle.is_cancelled())
            .field("process", &self.process)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::process_manager::ProcessManager;

    #[tokio::test]
    async fn test_register_query() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));
        let ticket = process_manager.clone().register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "".to_string(),
            None,
        );

        let running_processes = process_manager.local_processes(None).unwrap();
        assert_eq!(running_processes.len(), 1);
        assert_eq!(&running_processes[0].frontend, "127.0.0.1:8000");
        assert_eq!(running_processes[0].id, ticket.id);
        assert_eq!(&running_processes[0].query, "SELECT * FROM table");

        drop(ticket);
        assert_eq!(process_manager.local_processes(None).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_register_query_with_custom_id() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));
        let custom_id = 12345;

        let ticket = process_manager.clone().register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "client1".to_string(),
            Some(custom_id),
        );

        assert_eq!(ticket.id, custom_id);

        let running_processes = process_manager.local_processes(None).unwrap();
        assert_eq!(running_processes.len(), 1);
        assert_eq!(running_processes[0].id, custom_id);
        assert_eq!(&running_processes[0].client, "client1");
    }

    #[tokio::test]
    async fn test_multiple_queries_same_catalog() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        let ticket1 = process_manager.clone().register_query(
            "public".to_string(),
            vec!["schema1".to_string()],
            "SELECT * FROM table1".to_string(),
            "client1".to_string(),
            None,
        );

        let ticket2 = process_manager.clone().register_query(
            "public".to_string(),
            vec!["schema2".to_string()],
            "SELECT * FROM table2".to_string(),
            "client2".to_string(),
            None,
        );

        let running_processes = process_manager.local_processes(Some("public")).unwrap();
        assert_eq!(running_processes.len(), 2);

        // Verify both processes are present
        let ids: Vec<u32> = running_processes.iter().map(|p| p.id).collect();
        assert!(ids.contains(&ticket1.id));
        assert!(ids.contains(&ticket2.id));
    }

    #[tokio::test]
    async fn test_multiple_catalogs() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        let _ticket1 = process_manager.clone().register_query(
            "catalog1".to_string(),
            vec!["schema1".to_string()],
            "SELECT * FROM table1".to_string(),
            "client1".to_string(),
            None,
        );

        let _ticket2 = process_manager.clone().register_query(
            "catalog2".to_string(),
            vec!["schema2".to_string()],
            "SELECT * FROM table2".to_string(),
            "client2".to_string(),
            None,
        );

        // Test listing processes for specific catalog
        let catalog1_processes = process_manager.local_processes(Some("catalog1")).unwrap();
        assert_eq!(catalog1_processes.len(), 1);
        assert_eq!(&catalog1_processes[0].catalog, "catalog1");

        let catalog2_processes = process_manager.local_processes(Some("catalog2")).unwrap();
        assert_eq!(catalog2_processes.len(), 1);
        assert_eq!(&catalog2_processes[0].catalog, "catalog2");

        // Test listing all processes
        let all_processes = process_manager.local_processes(None).unwrap();
        assert_eq!(all_processes.len(), 2);
    }

    #[tokio::test]
    async fn test_deregister_query() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        let ticket = process_manager.clone().register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "client1".to_string(),
            None,
        );
        assert_eq!(process_manager.local_processes(None).unwrap().len(), 1);
        process_manager.deregister_query("public".to_string(), ticket.id);
        assert_eq!(process_manager.local_processes(None).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_cancellation_handle() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        let ticket = process_manager.clone().register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "client1".to_string(),
            None,
        );

        assert!(!ticket.cancellation_handle.is_cancelled());
        ticket.cancellation_handle.cancel();
        assert!(ticket.cancellation_handle.is_cancelled());
    }

    #[tokio::test]
    async fn test_kill_local_process() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        let ticket = process_manager.clone().register_query(
            "public".to_string(),
            vec!["test".to_string()],
            "SELECT * FROM table".to_string(),
            "client1".to_string(),
            None,
        );
        assert!(!ticket.cancellation_handle.is_cancelled());
        let killed = process_manager
            .kill_process(
                "127.0.0.1:8000".to_string(),
                "public".to_string(),
                ticket.id,
            )
            .await
            .unwrap();

        assert!(killed);
        assert_eq!(process_manager.local_processes(None).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_kill_nonexistent_process() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));
        let killed = process_manager
            .kill_process("127.0.0.1:8000".to_string(), "public".to_string(), 999)
            .await
            .unwrap();
        assert!(!killed);
    }

    #[tokio::test]
    async fn test_kill_process_nonexistent_catalog() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));
        let killed = process_manager
            .kill_process("127.0.0.1:8000".to_string(), "nonexistent".to_string(), 1)
            .await
            .unwrap();
        assert!(!killed);
    }

    #[tokio::test]
    async fn test_process_info_fields() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        let _ticket = process_manager.clone().register_query(
            "test_catalog".to_string(),
            vec!["schema1".to_string(), "schema2".to_string()],
            "SELECT COUNT(*) FROM users WHERE age > 18".to_string(),
            "test_client".to_string(),
            Some(42),
        );

        let processes = process_manager.local_processes(None).unwrap();
        assert_eq!(processes.len(), 1);

        let process = &processes[0];
        assert_eq!(process.id, 42);
        assert_eq!(&process.catalog, "test_catalog");
        assert_eq!(process.schemas, vec!["schema1", "schema2"]);
        assert_eq!(&process.query, "SELECT COUNT(*) FROM users WHERE age > 18");
        assert_eq!(&process.client, "test_client");
        assert_eq!(&process.frontend, "127.0.0.1:8000");
        assert!(process.start_timestamp > 0);
    }

    #[tokio::test]
    async fn test_ticket_drop_deregisters_process() {
        let process_manager = Arc::new(ProcessManager::new("127.0.0.1:8000".to_string(), None));

        {
            let _ticket = process_manager.clone().register_query(
                "public".to_string(),
                vec!["test".to_string()],
                "SELECT * FROM table".to_string(),
                "client1".to_string(),
                None,
            );

            // Process should be registered
            assert_eq!(process_manager.local_processes(None).unwrap().len(), 1);
        } // ticket goes out of scope here

        // Process should be automatically deregistered
        assert_eq!(process_manager.local_processes(None).unwrap().len(), 0);
    }
}
