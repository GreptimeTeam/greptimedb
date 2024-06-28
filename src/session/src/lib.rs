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
    catalog: Arc<RwLock<String>>,
    schema: Arc<RwLock<String>>,
    user_info: Arc<RwLock<UserInfoRef>>,
    conn_info: ConnInfo,
    timezone: Arc<RwLock<Timezone>>,
    configuration_variables: Arc<ConfigurationVariables>,
}

pub type SessionRef = Arc<Session>;

impl Session {
    pub fn new(
        addr: Option<SocketAddr>,
        channel: Channel,
        configuration_variables: ConfigurationVariables,
    ) -> Self {
        Session {
            catalog: Arc::new(RwLock::new(DEFAULT_CATALOG_NAME.into())),
            schema: Arc::new(RwLock::new(DEFAULT_SCHEMA_NAME.into())),
            user_info: Arc::new(RwLock::new(auth::userinfo_by_name(None))),
            conn_info: ConnInfo::new(addr, channel),
            timezone: Arc::new(RwLock::new(get_timezone(None).clone())),
            configuration_variables: Arc::new(configuration_variables),
        }
    }

    pub fn new_query_context(&self) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_user(self.user_info.clone())
            // catalog is not allowed for update in query context so we use
            // string here
            .current_catalog(self.catalog.read().unwrap().clone())
            .current_schema(self.schema.clone())
            .sql_dialect(self.conn_info.channel.dialect())
            .configuration_parameter(self.configuration_variables.clone())
            .timezone(self.timezone.clone())
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
        self.timezone.read().unwrap().clone()
    }

    pub fn set_timezone(&self, tz: Timezone) {
        *self.timezone.write().unwrap() = tz;
    }

    pub fn user_info(&self) -> UserInfoRef {
        self.user_info.read().unwrap().clone()
    }

    pub fn set_user_info(&self, user_info: UserInfoRef) {
        *self.user_info.write().unwrap() = user_info;
    }

    pub fn set_catalog(&self, catalog: String) {
        *self.catalog.write().unwrap() = catalog;
    }

    pub fn catalog(&self) -> String {
        self.catalog.read().unwrap().clone()
    }

    pub fn schema(&self) -> String {
        self.schema.read().unwrap().clone()
    }

    pub fn set_schema(&self, schema: String) {
        *self.schema.write().unwrap() = schema;
    }

    pub fn get_db_string(&self) -> String {
        build_db_string(&self.catalog(), &self.schema())
    }
}
