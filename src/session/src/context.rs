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

use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use common_catalog::build_db_string;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_telemetry::debug;
use common_time::TimeZone;
use sql::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

#[derive(Debug)]
pub struct QueryContext {
    current_catalog: ArcSwap<String>,
    current_schema: ArcSwap<String>,
    time_zone: ArcSwap<Option<TimeZone>>,
    sql_dialect: Box<dyn Dialect + Send + Sync>,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for QueryContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext{{catalog: {}, schema: {}}}",
            self.current_catalog(),
            self.current_schema()
        )
    }
}

impl QueryContext {
    pub fn arc() -> QueryContextRef {
        Arc::new(QueryContext::new())
    }

    pub fn new() -> Self {
        Self {
            current_catalog: ArcSwap::new(Arc::new(DEFAULT_CATALOG_NAME.to_string())),
            current_schema: ArcSwap::new(Arc::new(DEFAULT_SCHEMA_NAME.to_string())),
            time_zone: ArcSwap::new(Arc::new(None)),
            sql_dialect: Box::new(GenericDialect {}),
        }
    }

    pub fn with(catalog: &str, schema: &str) -> Self {
        Self::with_sql_dialect(catalog, schema, Box::new(GenericDialect {}))
    }

    pub fn with_sql_dialect(
        catalog: &str,
        schema: &str,
        sql_dialect: Box<dyn Dialect + Send + Sync>,
    ) -> Self {
        Self {
            current_catalog: ArcSwap::new(Arc::new(catalog.to_string())),
            current_schema: ArcSwap::new(Arc::new(schema.to_string())),
            time_zone: ArcSwap::new(Arc::new(None)),
            sql_dialect,
        }
    }

    #[inline]
    pub fn current_schema(&self) -> String {
        self.current_schema.load().as_ref().clone()
    }

    #[inline]
    pub fn current_catalog(&self) -> String {
        self.current_catalog.load().as_ref().clone()
    }

    #[inline]
    pub fn sql_dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &*self.sql_dialect
    }

    pub fn set_current_schema(&self, schema: &str) {
        let last = self.current_schema.swap(Arc::new(schema.to_string()));
        if schema != last.as_str() {
            debug!(
                "set new session default schema: {:?}, swap old: {:?}",
                schema, last
            )
        }
    }

    pub fn set_current_catalog(&self, catalog: &str) {
        let last = self.current_catalog.swap(Arc::new(catalog.to_string()));
        if catalog != last.as_str() {
            debug!(
                "set new session default catalog: {:?}, swap old: {:?}",
                catalog, last
            )
        }
    }

    pub fn get_db_string(&self) -> String {
        let catalog = self.current_catalog();
        let schema = self.current_schema();
        build_db_string(&catalog, &schema)
    }

    #[inline]
    pub fn time_zone(&self) -> Option<TimeZone> {
        self.time_zone.load().as_ref().clone()
    }

    #[inline]
    pub fn set_time_zone(&self, tz: Option<TimeZone>) {
        self.time_zone.swap(Arc::new(tz));
    }
}

pub const DEFAULT_USERNAME: &str = "greptime";

#[derive(Clone, Debug)]
pub struct UserInfo {
    username: String,
}

impl Default for UserInfo {
    fn default() -> Self {
        Self {
            username: DEFAULT_USERNAME.to_string(),
        }
    }
}

impl UserInfo {
    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
        }
    }
}

#[derive(Debug)]
pub struct ConnInfo {
    pub client_addr: Option<SocketAddr>,
    pub channel: Channel,
}

impl std::fmt::Display for ConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]",
            self.channel,
            self.client_addr
                .map(|addr| addr.to_string())
                .as_deref()
                .unwrap_or("unknown client addr")
        )
    }
}

impl ConnInfo {
    pub fn new(client_addr: Option<SocketAddr>, channel: Channel) -> Self {
        Self {
            client_addr,
            channel,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Channel {
    Mysql,
    Postgres,
    Opentsdb,
}

impl Channel {
    pub fn dialect(&self) -> Box<dyn Dialect + Send + Sync> {
        match self {
            Channel::Mysql => Box::new(MySqlDialect {}),
            Channel::Postgres => Box::new(PostgreSqlDialect {}),
            Channel::Opentsdb => Box::new(GenericDialect {}),
        }
    }
}

impl std::fmt::Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Channel::Mysql => write!(f, "mysql"),
            Channel::Postgres => write!(f, "postgres"),
            Channel::Opentsdb => write!(f, "opentsdb"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::context::{Channel, UserInfo};
    use crate::Session;

    #[test]
    fn test_session() {
        let session = Session::new(Some("127.0.0.1:9000".parse().unwrap()), Channel::Mysql);
        // test user_info
        assert_eq!(session.user_info().username(), "greptime");
        session.set_user_info(UserInfo::new("root"));
        assert_eq!(session.user_info().username(), "root");

        // test channel
        assert_eq!(session.conn_info().channel, Channel::Mysql);
        let client_addr = session.conn_info().client_addr.as_ref().unwrap();
        assert_eq!(client_addr.ip().to_string(), "127.0.0.1");
        assert_eq!(client_addr.port(), 9000);

        assert_eq!("mysql[127.0.0.1:9000]", session.conn_info().to_string());
    }

    #[test]
    fn test_context_db_string() {
        let context = QueryContext::new();

        context.set_current_catalog("a0b1c2d3");
        context.set_current_schema("test");

        assert_eq!("a0b1c2d3-test", context.get_db_string());

        context.set_current_catalog(DEFAULT_CATALOG_NAME);
        context.set_current_schema("test");

        assert_eq!("test", context.get_db_string());
    }
}
