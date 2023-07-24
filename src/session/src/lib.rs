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

pub mod context;

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use common_catalog::build_db_string;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use context::QueryContextBuilder;

use crate::context::{Channel, ConnInfo, QueryContextRef, UserInfo};

/// Session for persistent connection such as MySQL, PostgreSQL etc.
#[derive(Debug)]
pub struct Session {
    catalog: ArcSwap<String>,
    schema: ArcSwap<String>,
    user_info: ArcSwap<UserInfo>,
    conn_info: ConnInfo,
}

pub type SessionRef = Arc<Session>;

impl Session {
    pub fn new(addr: Option<SocketAddr>, channel: Channel) -> Self {
        Session {
            catalog: ArcSwap::new(Arc::new(DEFAULT_CATALOG_NAME.into())),
            schema: ArcSwap::new(Arc::new(DEFAULT_SCHEMA_NAME.into())),
            user_info: ArcSwap::new(Arc::new(UserInfo::default())),
            conn_info: ConnInfo::new(addr, channel),
        }
    }

    #[inline]
    pub fn new_query_context(&self) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(self.catalog.load().to_string())
            .current_schema(self.schema.load().to_string())
            .sql_dialect(self.conn_info.channel.dialect())
            .build_to_arc()
    }

    #[inline]
    pub fn conn_info(&self) -> &ConnInfo {
        &self.conn_info
    }

    #[inline]
    pub fn mut_conn_info(&mut self) -> &mut ConnInfo {
        &mut self.conn_info
    }

    #[inline]
    pub fn user_info(&self) -> Arc<UserInfo> {
        self.user_info.load().clone()
    }

    #[inline]
    pub fn set_user_info(&self, user_info: UserInfo) {
        self.user_info.store(Arc::new(user_info));
    }

    #[inline]
    pub fn set_catalog(&self, catalog: String) {
        self.catalog.store(Arc::new(catalog));
    }

    #[inline]
    pub fn set_schema(&self, schema: String) {
        self.schema.store(Arc::new(schema));
    }

    pub fn get_db_string(&self) -> String {
        build_db_string(self.catalog.load().as_ref(), self.schema.load().as_ref())
    }
}
