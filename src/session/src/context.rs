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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use api::v1::region::RegionRequestHeader;
use arc_swap::ArcSwap;
use auth::UserInfoRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::{build_db_string, parse_catalog_and_schema_from_db_string};
use common_time::timezone::parse_timezone;
use common_time::Timezone;
use derive_builder::Builder;
use sql::dialect::{Dialect, GenericDialect, GreptimeDbDialect, MySqlDialect, PostgreSqlDialect};

use crate::session_config::{PGByteaOutputValue, PGDateOrder, PGDateTimeStyle};
use crate::MutableInner;

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

#[derive(Debug, Builder, Clone)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct QueryContext {
    current_catalog: String,
    // we use Arc<RwLock>> for modifiable fields
    #[builder(default)]
    mutable_inner: Arc<RwLock<MutableInner>>,
    sql_dialect: Arc<dyn Dialect + Send + Sync>,
    #[builder(default)]
    extensions: HashMap<String, String>,
    // The configuration parameter are used to store the parameters that are set by the user
    #[builder(default)]
    configuration_parameter: Arc<ConfigurationVariables>,
    // Track which protocol the query comes from.
    #[builder(default)]
    channel: Channel,
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

impl QueryContextBuilder {
    pub fn current_schema(mut self, schema: String) -> Self {
        if self.mutable_inner.is_none() {
            self.mutable_inner = Some(Arc::new(RwLock::new(MutableInner::default())));
        }

        // safe for unwrap because previous none check
        self.mutable_inner.as_mut().unwrap().write().unwrap().schema = schema;
        self
    }

    pub fn timezone(mut self, timezone: Timezone) -> Self {
        if self.mutable_inner.is_none() {
            self.mutable_inner = Some(Arc::new(RwLock::new(MutableInner::default())));
        }

        self.mutable_inner
            .as_mut()
            .unwrap()
            .write()
            .unwrap()
            .timezone = timezone;
        self
    }
}

impl From<&RegionRequestHeader> for QueryContext {
    fn from(value: &RegionRequestHeader) -> Self {
        let mut builder = QueryContextBuilder::default();
        if let Some(ctx) = &value.query_context {
            builder = builder
                .current_catalog(ctx.current_catalog.clone())
                .current_schema(ctx.current_schema.clone())
                .timezone(parse_timezone(Some(&ctx.timezone)))
                .extensions(ctx.extensions.clone())
                .channel(ctx.channel.into());
        }
        builder.build()
    }
}

impl From<api::v1::QueryContext> for QueryContext {
    fn from(ctx: api::v1::QueryContext) -> Self {
        QueryContextBuilder::default()
            .current_catalog(ctx.current_catalog)
            .current_schema(ctx.current_schema)
            .timezone(parse_timezone(Some(&ctx.timezone)))
            .extensions(ctx.extensions)
            .channel(ctx.channel.into())
            .build()
    }
}

impl From<QueryContext> for api::v1::QueryContext {
    fn from(
        QueryContext {
            current_catalog,
            mutable_inner,
            extensions,
            channel,
            ..
        }: QueryContext,
    ) -> Self {
        let mutable_inner = mutable_inner.read().unwrap();
        api::v1::QueryContext {
            current_catalog,
            current_schema: mutable_inner.schema.clone(),
            timezone: mutable_inner.timezone.to_string(),
            extensions,
            channel: channel as u32,
        }
    }
}

impl QueryContext {
    pub fn arc() -> QueryContextRef {
        Arc::new(QueryContextBuilder::default().build())
    }

    pub fn with(catalog: &str, schema: &str) -> QueryContext {
        QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(schema.to_string())
            .build()
    }

    pub fn with_channel(catalog: &str, schema: &str, channel: Channel) -> QueryContext {
        QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(schema.to_string())
            .channel(channel)
            .build()
    }

    pub fn with_db_name(db_name: Option<&str>) -> QueryContext {
        let (catalog, schema) = db_name
            .map(|db| {
                let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
                (catalog, schema)
            })
            .unwrap_or_else(|| {
                (
                    DEFAULT_CATALOG_NAME.to_string(),
                    DEFAULT_SCHEMA_NAME.to_string(),
                )
            });
        QueryContextBuilder::default()
            .current_catalog(catalog)
            .current_schema(schema.to_string())
            .build()
    }

    pub fn current_schema(&self) -> String {
        self.mutable_inner.read().unwrap().schema.clone()
    }

    pub fn set_current_schema(&self, new_schema: &str) {
        self.mutable_inner.write().unwrap().schema = new_schema.to_string();
    }

    pub fn current_catalog(&self) -> &str {
        &self.current_catalog
    }

    pub fn set_current_catalog(&mut self, new_catalog: &str) {
        self.current_catalog = new_catalog.to_string();
    }

    pub fn sql_dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &*self.sql_dialect
    }

    pub fn get_db_string(&self) -> String {
        let catalog = self.current_catalog();
        let schema = self.current_schema();
        build_db_string(catalog, &schema)
    }

    pub fn timezone(&self) -> Timezone {
        self.mutable_inner.read().unwrap().timezone.clone()
    }

    pub fn set_timezone(&self, timezone: Timezone) {
        self.mutable_inner.write().unwrap().timezone = timezone;
    }

    pub fn current_user(&self) -> UserInfoRef {
        self.mutable_inner.read().unwrap().user_info.clone()
    }

    pub fn set_current_user(&self, user: UserInfoRef) {
        self.mutable_inner.write().unwrap().user_info = user;
    }

    pub fn set_extension<S1: Into<String>, S2: Into<String>>(&mut self, key: S1, value: S2) {
        self.extensions.insert(key.into(), value.into());
    }

    pub fn extension<S: AsRef<str>>(&self, key: S) -> Option<&str> {
        self.extensions.get(key.as_ref()).map(|v| v.as_str())
    }

    pub fn extensions(&self) -> HashMap<String, String> {
        self.extensions.clone()
    }

    /// Default to double quote and fallback to back quote
    pub fn quote_style(&self) -> char {
        if self.sql_dialect().is_delimited_identifier_start('"') {
            '"'
        } else if self.sql_dialect().is_delimited_identifier_start('\'') {
            '\''
        } else {
            '`'
        }
    }

    pub fn configuration_parameter(&self) -> &ConfigurationVariables {
        &self.configuration_parameter
    }

    pub fn channel(&self) -> Channel {
        self.channel
    }

    pub fn set_channel(&mut self, channel: Channel) {
        self.channel = channel;
    }
}

impl QueryContextBuilder {
    pub fn build(self) -> QueryContext {
        QueryContext {
            current_catalog: self
                .current_catalog
                .unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string()),
            mutable_inner: self.mutable_inner.unwrap_or_default(),
            sql_dialect: self
                .sql_dialect
                .unwrap_or_else(|| Arc::new(GreptimeDbDialect {})),
            extensions: self.extensions.unwrap_or_default(),
            configuration_parameter: self.configuration_parameter.unwrap_or_default(),
            channel: self.channel.unwrap_or_default(),
        }
    }

    pub fn set_extension(mut self, key: String, value: String) -> Self {
        self.extensions
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
        self
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

#[derive(Debug, PartialEq, Default, Clone, Copy)]
#[repr(u8)]
pub enum Channel {
    #[default]
    Unknown = 0,

    Mysql = 1,
    Postgres = 2,
    Http = 3,
    Prometheus = 4,
    Otlp = 5,
    Grpc = 6,
    Influx = 7,
    Opentsdb = 8,
}

impl From<u32> for Channel {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Mysql,
            2 => Self::Postgres,
            3 => Self::Http,
            4 => Self::Prometheus,
            5 => Self::Otlp,
            6 => Self::Grpc,
            7 => Self::Influx,
            8 => Self::Opentsdb,

            _ => Self::Unknown,
        }
    }
}

impl Channel {
    pub fn dialect(&self) -> Arc<dyn Dialect + Send + Sync> {
        match self {
            Channel::Mysql => Arc::new(MySqlDialect {}),
            Channel::Postgres => Arc::new(PostgreSqlDialect {}),
            _ => Arc::new(GenericDialect {}),
        }
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Channel::Mysql => write!(f, "mysql"),
            Channel::Postgres => write!(f, "postgres"),
            Channel::Http => write!(f, "http"),
            Channel::Prometheus => write!(f, "prometheus"),
            Channel::Otlp => write!(f, "otlp"),
            Channel::Grpc => write!(f, "grpc"),
            Channel::Influx => write!(f, "influx"),
            Channel::Opentsdb => write!(f, "opentsdb"),
            Channel::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Default, Debug)]
pub struct ConfigurationVariables {
    postgres_bytea_output: ArcSwap<PGByteaOutputValue>,
    pg_datestyle_format: ArcSwap<(PGDateTimeStyle, PGDateOrder)>,
}

impl Clone for ConfigurationVariables {
    fn clone(&self) -> Self {
        Self {
            postgres_bytea_output: ArcSwap::new(self.postgres_bytea_output.load().clone()),
            pg_datestyle_format: ArcSwap::new(self.pg_datestyle_format.load().clone()),
        }
    }
}

impl ConfigurationVariables {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_postgres_bytea_output(&self, value: PGByteaOutputValue) {
        let _ = self.postgres_bytea_output.swap(Arc::new(value));
    }

    pub fn postgres_bytea_output(&self) -> Arc<PGByteaOutputValue> {
        self.postgres_bytea_output.load().clone()
    }

    pub fn pg_datetime_style(&self) -> Arc<(PGDateTimeStyle, PGDateOrder)> {
        self.pg_datestyle_format.load().clone()
    }

    pub fn set_pg_datetime_style(&self, style: PGDateTimeStyle, order: PGDateOrder) {
        self.pg_datestyle_format.swap(Arc::new((style, order)));
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
        let session = Session::new(
            Some("127.0.0.1:9000".parse().unwrap()),
            Channel::Mysql,
            Default::default(),
        );
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
