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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use api::v1::frontend::{KillProcessRequest, ListProcessRequest, ProcessInfo};
use common_base::cancellation_handle::CancellationHandle;
use common_frontend::selector::{FrontendSelector, MetaClientSelector};
use common_telemetry::debug;
use common_time::util::current_time_millis;
use meta_client::MetaClientRef;
use snafu::{ensure, ResultExt};

use crate::error;

pub type ProcessManagerRef = Arc<ProcessManager>;

pub struct ProcessManager {
    server_addr: String,
    next_id: AtomicU64,
    catalogs: RwLock<HashMap<String, HashMap<u64, CancellableProcess>>>,
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
    pub fn register_query(
        self: &Arc<Self>,
        catalog: String,
        schemas: Vec<String>,
        query: String,
        client: String,
        id: Option<u64>,
    ) -> Ticket {
        let id = id.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed));
        let process = ProcessInfo {
            id,
            catalog: catalog.clone(),
            schemas,
            query,
            start_timestamp: current_time_millis(),
            client,
            frontend: self.server_addr.clone(),
        };
        let cancellation_handler = Arc::new(CancellationHandle::new());

        let cancellable_process = CancellableProcess {
            handle: cancellation_handler.clone(),
            process,
        };
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
            cancellation_handler,
        }
    }

    /// Generates the next process id.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
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
                processes.extend(
                    f.list_process(ListProcessRequest {
                        catalog: catalog.unwrap_or_default().to_string(),
                    })
                    .await
                    .context(error::InvokeFrontendSnafu)?
                    .processes,
                );
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
        id: u64,
    ) -> error::Result<bool> {
        if server_addr == self.server_addr {
            if let Some(catalogs) = self.catalogs.write().unwrap().get_mut(&catalog) {
                if let Some(process) = catalogs.remove(&id) {
                    debug!(
                        "Stopped process, catalog: {}, id: {:?}",
                        process.process.catalog, process.process.id
                    );
                    return Ok(true);
                }
            }
            Ok(false)
        } else {
            let mut nodes = self
                .frontend_selector
                .as_ref()
                .unwrap()
                .select(|node| node.peer.addr == server_addr)
                .await
                .context(error::InvokeFrontendSnafu)?;
            ensure!(
                nodes.is_empty(),
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
}

pub struct Ticket {
    pub(crate) catalog: String,
    pub(crate) manager: ProcessManagerRef,
    pub(crate) id: u64,
    pub cancellation_handler: Arc<CancellationHandle>,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.manager
            .deregister_query(std::mem::take(&mut self.catalog), self.id);
        self.cancellation_handler.cancel();
    }
}

struct CancellableProcess {
    handle: Arc<CancellationHandle>,
    process: ProcessInfo,
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
}
