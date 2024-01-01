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

use api::v1::region::RegionRequestHeader;
use arc_swap::ArcSwap;
use auth::UserInfoRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::{build_db_string, parse_catalog_and_schema_from_db_string};
use common_time::timezone::get_timezone;
use common_time::Timezone;
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
    current_user: ArcSwap<Option<UserInfoRef>>,
    timezone: Timezone,
    sql_dialect: Box<dyn Dialect + Send + Sync>,
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

impl From<&RegionRequestHeader> for QueryContext {
    fn from(value: &RegionRequestHeader) -> Self {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(&value.dbname);
        QueryContext {
            current_catalog: catalog.to_string(),
            current_schema: schema.to_string(),
            current_user: Default::default(),
            timezone: get_timezone(None),
            sql_dialect: Box::new(GreptimeDbDialect {}),
        }
    }
}

impl QueryContext {
    pub fn arc() -> QueryContextRef {
        QueryContextBuilder::default().build()
    }

    pub fn with(catalog: &str, schema: &str) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(schema.to_string())
            .build()
    }

    pub fn with_db_name(db_name: Option<&String>) -> QueryContextRef {
        let (catalog, schema) = db_name
            .map(|db| {
                let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
                (catalog.to_string(), schema.to_string())
            })
            .unwrap_or_else(|| {
                (
                    DEFAULT_CATALOG_NAME.to_string(),
                    DEFAULT_SCHEMA_NAME.to_string(),
                )
            });
        QueryContextBuilder::default()
            .current_catalog(catalog)
            .current_schema(schema)
            .build()
    }

    #[inline]
    pub fn current_schema(&self) -> &str {
        &self.current_schema
    }

    #[inline]
    pub fn current_catalog(&self) -> &str {
        &self.current_catalog
    }

    #[inline]
    pub fn sql_dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &*self.sql_dialect
    }

    pub fn get_db_string(&self) -> String {
        let catalog = self.current_catalog();
        let schema = self.current_schema();
        build_db_string(catalog, schema)
    }

    #[inline]
    pub fn timezone(&self) -> Timezone {
        self.timezone.clone()
    }

    #[inline]
    pub fn current_user(&self) -> Option<UserInfoRef> {
        self.current_user.load().as_ref().clone()
    }

    #[inline]
    pub fn set_current_user(&self, user: Option<UserInfoRef>) {
        let _ = self.current_user.swap(Arc::new(user));
    }
}

impl QueryContextBuilder {
    pub fn build(self) -> QueryContextRef {
        Arc::new(QueryContext {
            current_catalog: self
                .current_catalog
                .unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string()),
            current_schema: self
                .current_schema
                .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string()),
            current_user: self
                .current_user
                .unwrap_or_else(|| ArcSwap::new(Arc::new(None))),
            timezone: self.timezone.unwrap_or(get_timezone(None)),
            sql_dialect: self
                .sql_dialect
                .unwrap_or_else(|| Box::new(GreptimeDbDialect {})),
        })
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
    use crate::context::Channel;
    use crate::Session;

    #[test]
    fn test_session() {
        let session = Session::new(Some("127.0.0.1:9000".parse().unwrap()), Channel::Mysql);
        // test user_info
        assert_eq!(session.user_info().username(), "greptime");

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
