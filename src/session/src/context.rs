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

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

#[derive(Debug)]
pub struct QueryContext {
    current_catalog: ArcSwap<String>,
    current_schema: ArcSwap<String>,
    time_zone: ArcSwap<Option<TimeZone>>,
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
        }
    }

    pub fn with(catalog: &str, schema: &str) -> Self {
        Self {
            current_catalog: ArcSwap::new(Arc::new(catalog.to_string())),
            current_schema: ArcSwap::new(Arc::new(schema.to_string())),
            time_zone: ArcSwap::new(Arc::new(None)),
        }
    }

    pub fn current_schema(&self) -> String {
        self.current_schema.load().as_ref().clone()
    }

    pub fn current_catalog(&self) -> String {
        self.current_catalog.load().as_ref().clone()
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

    pub fn time_zone(&self) -> Option<TimeZone> {
        self.time_zone.load().as_ref().clone()
    }

    pub fn set_time_zone(&self, tz: TimeZone) {
        self.time_zone.swap(Arc::new(Some(tz)));
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

pub struct ConnInfo {
    pub client_host: SocketAddr,
    pub channel: Channel,
}

impl ConnInfo {
    pub fn new(client_host: SocketAddr, channel: Channel) -> Self {
        Self {
            client_host,
            channel,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Channel {
    Grpc,
    Http,
    Mysql,
    Postgres,
    Opentsdb,
    Influxdb,
    Prometheus,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::context::{Channel, UserInfo};
    use crate::Session;

    #[test]
    fn test_session() {
        let session = Session::new("127.0.0.1:9000".parse().unwrap(), Channel::Mysql);
        // test user_info
        assert_eq!(session.user_info().username(), "greptime");
        session.set_user_info(UserInfo::new("root"));
        assert_eq!(session.user_info().username(), "root");

        // test channel
        assert_eq!(session.conn_info().channel, Channel::Mysql);
        assert_eq!(
            session.conn_info().client_host.ip().to_string(),
            "127.0.0.1"
        );
        assert_eq!(session.conn_info().client_host.port(), 9000);
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
