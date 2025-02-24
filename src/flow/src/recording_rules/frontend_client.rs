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

use std::sync::Arc;

use client::{Client, Database, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role};
use common_meta::peer::Peer;
use common_meta::rpc::store::RangeRequest;
use common_query::Output;
use meta_client::client::MetaClient;
use snafu::ResultExt;

use crate::error::{ExternalSnafu, UnexpectedSnafu};
use crate::Error;

/// A simple frontend client able to execute sql using grpc protocol
#[derive(Debug)]
pub enum FrontendClient {
    Distributed {
        meta_client: Arc<MetaClient>,
    },
    Standalone {
        /// for the sake of simplicity still use grpc even in standalone mode
        /// notice the client here should all be lazy, so that can wait after frontend is booted then make conn
        /// TODO(discord9): not use grpc under standalone mode
        database_client: DatabaseWithPeer,
    },
}

#[derive(Debug, Clone)]
pub struct DatabaseWithPeer {
    pub database: Database,
    pub peer: Peer,
}

impl DatabaseWithPeer {
    fn new(database: Database, peer: Peer) -> Self {
        Self { database, peer }
    }
}

impl FrontendClient {
    pub fn from_meta_client(meta_client: Arc<MetaClient>) -> Self {
        Self::Distributed { meta_client }
    }

    pub fn from_static_grpc_addr(addr: String) -> Self {
        let peer = Peer {
            id: 0,
            addr: addr.clone(),
        };

        let client = Client::with_urls(vec![addr]);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        Self::Standalone {
            database_client: DatabaseWithPeer::new(database, peer),
        }
    }
}

impl FrontendClient {
    async fn scan_for_frontend(&self) -> Result<Vec<(NodeInfoKey, NodeInfo)>, Error> {
        let Self::Distributed { meta_client, .. } = self else {
            return Ok(vec![]);
        };
        let cluster_client = meta_client
            .cluster_client()
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let cluster_id = meta_client.id().0;
        let prefix = NodeInfoKey::key_prefix_with_role(cluster_id, Role::Frontend);
        let req = RangeRequest::new().with_prefix(prefix);
        let resp = cluster_client
            .range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let mut res = Vec::with_capacity(resp.kvs.len());
        for kv in resp.kvs {
            let key = NodeInfoKey::try_from(kv.key)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;

            let val = NodeInfo::try_from(kv.value)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            res.push((key, val));
        }
        Ok(res)
    }

    /// Get the database with max `last_activity_ts`
    async fn get_last_active_frontend(&self) -> Result<DatabaseWithPeer, Error> {
        if let Self::Standalone { database_client } = self {
            return Ok(database_client.clone());
        }

        let frontends = self.scan_for_frontend().await?;
        let mut last_activity_ts = i64::MIN;
        let mut peer = None;
        for (_key, val) in frontends.iter() {
            if val.last_activity_ts > last_activity_ts {
                last_activity_ts = val.last_activity_ts;
                peer = Some(val.peer.clone());
            }
        }
        let Some(peer) = peer else {
            UnexpectedSnafu {
                reason: format!("No frontend available: {:?}", frontends),
            }
            .fail()?
        };
        let client = Client::with_urls(vec![peer.addr.clone()]);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        Ok(DatabaseWithPeer::new(database, peer))
    }

    /// Get a database client, and possibly update it before returning.
    pub async fn get_database_client(&self) -> Result<DatabaseWithPeer, Error> {
        match self {
            Self::Standalone { database_client } => Ok(database_client.clone()),
            Self::Distributed { meta_client: _ } => self.get_last_active_frontend().await,
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<Output, Error> {
        let db = self.get_database_client().await?;
        db.database
            .sql(sql)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}
