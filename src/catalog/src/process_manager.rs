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
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use api::v1::frontend::ProcessInfo;
use common_telemetry::{debug, info};
use common_time::util::current_time_millis;
use snafu::OptionExt;

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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DisplayProcessId {
    pub server_addr: String,
    pub id: u64,
}

impl Display for DisplayProcessId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.server_addr, self.id)
    }
}

impl TryFrom<&str> for DisplayProcessId {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut split = value.split('/');
        let server_addr = split
            .next()
            .context(error::ParseProcessIdSnafu { s: value })?
            .to_string();
        let id = split
            .next()
            .context(error::ParseProcessIdSnafu { s: value })?;
        let id = u64::from_str(id)
            .ok()
            .context(error::ParseProcessIdSnafu { s: value })?;
        Ok(DisplayProcessId { server_addr, id })
    }
}

#[cfg(test)]
mod tests {
    use crate::process_manager::{DisplayProcessId, ProcessManager};

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

    #[test]
    fn test_display_process_id() {
        assert_eq!(
            DisplayProcessId::try_from("1.1.1.1:3000/123").unwrap(),
            DisplayProcessId {
                server_addr: "1.1.1.1:3000".to_string(),
                id: 123,
            }
        );
    }
}
