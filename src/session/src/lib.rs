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
pub mod session_config;
pub mod table_name;

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use auth::UserInfoRef;
use common_catalog::build_db_string;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_time::timezone::get_timezone;
use common_time::Timezone;
use context::{ConfigurationVariables, QueryContextBuilder};

use crate::context::{Channel, ConnInfo, QueryContextRef};

/// Session for persistent connection such as MySQL, PostgreSQL etc.
#[derive(Debug)]
pub struct Session {
    catalog: RwLock<String>,
    mutable_inner: Arc<RwLock<MutableInner>>,
    conn_info: ConnInfo,
    configuration_variables: Arc<ConfigurationVariables>,
}

pub type SessionRef = Arc<Session>;

/// A container for mutable items in query context
#[derive(Debug)]
pub(crate) struct MutableInner {
    schema: String,
    user_info: UserInfoRef,
    timezone: Timezone,
}

impl Default for MutableInner {
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA_NAME.into(),
            user_info: auth::userinfo_by_name(None),
            timezone: get_timezone(None).clone(),
        }
    }
}

impl Session {
    pub fn new(
        addr: Option<SocketAddr>,
        channel: Channel,
        configuration_variables: ConfigurationVariables,
    ) -> Self {
        Session {
            catalog: RwLock::new(DEFAULT_CATALOG_NAME.into()),
            conn_info: ConnInfo::new(addr, channel),
            configuration_variables: Arc::new(configuration_variables),
            mutable_inner: Arc::new(RwLock::new(MutableInner::default())),
        }
    }

    pub fn new_query_context(&self) -> QueryContextRef {
        QueryContextBuilder::default()
            // catalog is not allowed for update in query context so we use
            // string here
            .current_catalog(self.catalog.read().unwrap().clone())
            .mutable_inner(self.mutable_inner.clone())
            .sql_dialect(self.conn_info.channel.dialect())
            .configuration_parameter(self.configuration_variables.clone())
            .channel(self.conn_info.channel)
            .build()
            .into()
    }

    pub fn conn_info(&self) -> &ConnInfo {
        &self.conn_info
    }

    pub fn mut_conn_info(&mut self) -> &mut ConnInfo {
        &mut self.conn_info
    }

    pub fn timezone(&self) -> Timezone {
        self.mutable_inner.read().unwrap().timezone.clone()
    }

    pub fn set_timezone(&self, tz: Timezone) {
        let mut inner = self.mutable_inner.write().unwrap();
        inner.timezone = tz;
    }

    pub fn user_info(&self) -> UserInfoRef {
        self.mutable_inner.read().unwrap().user_info.clone()
    }

    pub fn set_user_info(&self, user_info: UserInfoRef) {
        self.mutable_inner.write().unwrap().user_info = user_info;
    }

    pub fn set_catalog(&self, catalog: String) {
        *self.catalog.write().unwrap() = catalog;
    }

    pub fn catalog(&self) -> String {
        self.catalog.read().unwrap().clone()
    }

    pub fn schema(&self) -> String {
        self.mutable_inner.read().unwrap().schema.clone()
    }

    pub fn set_schema(&self, schema: String) {
        self.mutable_inner.write().unwrap().schema = schema;
    }

    pub fn get_db_string(&self) -> String {
        build_db_string(&self.catalog(), &self.schema())
    }
}
