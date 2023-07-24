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
use common_time::TimeZone;
use derive_builder::Builder;
use sql::dialect::{Dialect, GreptimeDbDialect, MySqlDialect, PostgreSqlDialect};

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

#[derive(Debug, Builder)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct QueryContext {
    current_catalog: String,
    current_schema: String,
    time_zone: ArcSwap<Option<TimeZone>>,
    sql_dialect: Box<dyn Dialect + Send + Sync>,
    trace_id: u64,
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
    #[cfg(any(test, feature = "testing"))]
    pub fn arc() -> QueryContextRef {
        QueryContextBuilder::default().build()
    }

    pub fn with(catalog: &str, schema: &str) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(schema.to_string())
            .build()
    }

    #[inline]
    pub fn current_schema(&self) -> String {
        self.current_schema.clone()
    }

    #[inline]
    pub fn current_catalog(&self) -> String {
        self.current_catalog.clone()
    }

    #[inline]
    pub fn sql_dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &*self.sql_dialect
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
        let _ = self.time_zone.swap(Arc::new(tz));
    }

    #[inline]
    pub fn trace_id(&self) -> u64 {
        self.trace_id
    }
}

impl QueryContextBuilder {
    pub fn build(self) -> QueryContextRef {
        Arc::new(QueryContext {
            current_catalog: self
                .current_catalog
                .unwrap_or(DEFAULT_CATALOG_NAME.to_string()),
            current_schema: self
                .current_schema
                .unwrap_or(DEFAULT_SCHEMA_NAME.to_string()),
            time_zone: self.time_zone.unwrap_or(ArcSwap::new(Arc::new(None))),
            sql_dialect: self.sql_dialect.unwrap_or(Box::new(GreptimeDbDialect {})),
            trace_id: self.trace_id.unwrap_or(common_telemetry::gen_trace_id()),
        })
    }

    pub fn try_trace_id(mut self, trace_id: Option<u64>) -> Self {
        self.trace_id = trace_id;
        self
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

impl Display for ConnInfo {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
}

impl Channel {
    pub fn dialect(&self) -> Box<dyn Dialect + Send + Sync> {
        match self {
            Channel::Mysql => Box::new(MySqlDialect {}),
            Channel::Postgres => Box::new(PostgreSqlDialect {}),
        }
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Channel::Mysql => write!(f, "mysql"),
            Channel::Postgres => write!(f, "postgres"),
        }
    }
}

#[cfg(test)]
mod test {
    use common_catalog::consts::DEFAULT_CATALOG_NAME;

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
        let context = QueryContext::with("a0b1c2d3", "test");
        assert_eq!("a0b1c2d3-test", context.get_db_string());

        let context = QueryContext::with(DEFAULT_CATALOG_NAME, "test");
        assert_eq!("test", context.get_db_string());
    }
}
