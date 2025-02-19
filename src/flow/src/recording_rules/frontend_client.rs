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

//! Frontend client to run flow as recording rule which is time-window-aware normal query triggered every tick set by user

use std::collections::BTreeMap;
use std::sync::Arc;

use client::{Client, Database, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role};
use common_meta::peer::Peer;
use common_meta::rpc::store::RangeRequest;
use common_query::Output;
use meta_client::client::MetaClient;
use snafu::{OptionExt, ResultExt};
use tokio::sync::Mutex;

use crate::error::{ExternalSnafu, UnexpectedSnafu};
use crate::Error;

/// A simple frontend client able to execute sql using grpc protocol
#[derive(Debug)]
pub enum FrontendClient {
    Distributed {
        meta_client: Arc<MetaClient>,
        /// list of frontend node and connection to it
        database_clients: Mutex<RoundRobinClients>,
    },
    Standalone {
        /// for the sake of simplicity still use grpc even in standalone mode
        /// notice the client here should all be lazy, so that can wait after frontend is booted then make conn
        /// TODO(discord9): not use grpc under standalone mode
        database_client: Database,
    },
}

#[derive(Debug, Default)]
pub struct RoundRobinClients {
    clients: BTreeMap<u64, (Peer, Database)>,
    next: usize,
}

impl RoundRobinClients {
    fn get_next_client(&mut self) -> Option<Database> {
        if self.clients.is_empty() {
            return None;
        }
        let idx = self.next % self.clients.len();
        self.next = (self.next + 1) % self.clients.len();
        Some(self.clients.iter().nth(idx).unwrap().1 .1.clone())
    }
}

impl FrontendClient {
    pub fn from_meta_client(meta_client: Arc<MetaClient>) -> Self {
        Self::Distributed {
            meta_client,
            database_clients: Default::default(),
        }
    }

    pub fn from_static_grpc_addr(addr: String) -> Self {
        let client = Client::with_urls(vec![addr]);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        Self::Standalone {
            database_client: database,
        }
    }
}

impl FrontendClient {
    async fn update_frontend_addr(&self) -> Result<(), Error> {
        // TODO(discord9): better error handling
        let Self::Distributed {
            meta_client,
            database_clients,
        } = self
        else {
            return Ok(());
        };
        let cluster_client = meta_client
            .cluster_client()
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let cluster_id = meta_client.id().0;
        let prefix = NodeInfoKey::key_prefix_with_role(cluster_id, Role::Frontend);
        let req = RangeRequest::new().with_prefix(prefix);
        let res = cluster_client
            .range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let mut clients = database_clients.lock().await;
        for kv in res.kvs {
            let key = NodeInfoKey::try_from(kv.key)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;

            let val = NodeInfo::try_from(kv.value)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;

            if key.role != Role::Frontend {
                return UnexpectedSnafu {
                    reason: format!(
                        "Unexpected role(should be frontend) in key: {:?}, val: {:?}",
                        key, val
                    ),
                }
                .fail();
            }
            if let Some((peer, database)) = clients.clients.get_mut(&key.node_id) {
                if val.peer != *peer {
                    // update only if not the same
                    *peer = val.peer.clone();
                    let client = Client::with_urls(vec![peer.addr.clone()]);
                    *database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
                }
            } else {
                let peer = val.peer;
                let client = Client::with_urls(vec![peer.addr.clone()]);
                let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
                clients
                    .clients
                    .insert(key.node_id, (peer.clone(), database.clone()));
            }
        }
        drop(clients);

        Ok(())
    }

    /// Get a database client, and possibly update it before returning.
    async fn get_database_client(&self) -> Result<Database, Error> {
        match self {
            Self::Standalone { database_client } => Ok(database_client.clone()),
            Self::Distributed {
                meta_client: _,
                database_clients,
            } => {
                self.update_frontend_addr().await?;
                database_clients
                    .lock()
                    .await
                    .get_next_client()
                    .context(UnexpectedSnafu {
                        reason: "Can't get any database client",
                    })
            }
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<Output, Error> {
        let db = self.get_database_client().await?;
        db.sql(sql)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}
