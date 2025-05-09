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
use std::time::Duration;

use api::v1::region::RegionRequestHeader;
use api::v1::ExplainOptions;
use arc_swap::ArcSwap;
use auth::UserInfoRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::{build_db_string, parse_catalog_and_schema_from_db_string};
use common_recordbatch::cursor::RecordBatchStreamCursor;
use common_telemetry::warn;
use common_time::timezone::parse_timezone;
use common_time::Timezone;
use derive_builder::Builder;
use sql::dialect::{Dialect, GenericDialect, GreptimeDbDialect, MySqlDialect, PostgreSqlDialect};

use crate::session_config::{PGByteaOutputValue, PGDateOrder, PGDateTimeStyle};
use crate::{MutableInner, ReadPreference};

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

const CURSOR_COUNT_WARNING_LIMIT: usize = 10;

#[derive(Debug, Builder, Clone)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct QueryContext {
    current_catalog: String,
    /// mapping of RegionId to SequenceNumber, for snapshot read, meaning that the read should only
    /// container data that was committed before(and include) the given sequence number
    /// this field will only be filled if extensions contains a pair of "snapshot_read" and "true"
    snapshot_seqs: Arc<RwLock<HashMap<u64, u64>>>,
    /// Mappings of the RegionId to the minimal sequence of SST file to scan.
    sst_min_sequences: Arc<RwLock<HashMap<u64, u64>>>,
    // we use Arc<RwLock>> for modifiable fields
    #[builder(default)]
    mutable_session_data: Arc<RwLock<MutableInner>>,
    #[builder(default)]
    mutable_query_context_data: Arc<RwLock<QueryContextMutableFields>>,
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

/// This fields hold data that is only valid to current query context
#[derive(Debug, Builder, Clone, Default)]
pub struct QueryContextMutableFields {
    warning: Option<String>,
    // TODO: remove this when format is supported in datafusion
    explain_format: Option<String>,
    /// Explain options to control the verbose analyze output.
    explain_options: Option<ExplainOptions>,
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
        if self.mutable_session_data.is_none() {
            self.mutable_session_data = Some(Arc::new(RwLock::new(MutableInner::default())));
        }

        // safe for unwrap because previous none check
        self.mutable_session_data
            .as_mut()
            .unwrap()
            .write()
            .unwrap()
            .schema = schema;
        self
    }

    pub fn timezone(mut self, timezone: Timezone) -> Self {
        if self.mutable_session_data.is_none() {
            self.mutable_session_data = Some(Arc::new(RwLock::new(MutableInner::default())));
        }

        self.mutable_session_data
            .as_mut()
            .unwrap()
            .write()
            .unwrap()
            .timezone = timezone;
        self
    }

    pub fn explain_options(mut self, explain_options: Option<ExplainOptions>) -> Self {
        self.mutable_query_context_data
            .get_or_insert_default()
            .write()
            .unwrap()
            .explain_options = explain_options;
        self
    }

    pub fn read_preference(mut self, read_preference: ReadPreference) -> Self {
        self.mutable_session_data
            .get_or_insert_default()
            .write()
            .unwrap()
            .read_preference = read_preference;
        self
    }
}

impl From<&RegionRequestHeader> for QueryContext {
    fn from(value: &RegionRequestHeader) -> Self {
        if let Some(ctx) = &value.query_context {
            ctx.clone().into()
        } else {
            QueryContextBuilder::default().build()
        }
    }
}

impl From<api::v1::QueryContext> for QueryContext {
    fn from(ctx: api::v1::QueryContext) -> Self {
        let sequences = ctx.snapshot_seqs.as_ref();
        QueryContextBuilder::default()
            .current_catalog(ctx.current_catalog)
            .current_schema(ctx.current_schema)
            .timezone(parse_timezone(Some(&ctx.timezone)))
            .extensions(ctx.extensions)
            .channel(ctx.channel.into())
            .snapshot_seqs(Arc::new(RwLock::new(
                sequences
                    .map(|x| x.snapshot_seqs.clone())
                    .unwrap_or_default(),
            )))
            .sst_min_sequences(Arc::new(RwLock::new(
                sequences
                    .map(|x| x.sst_min_sequences.clone())
                    .unwrap_or_default(),
            )))
            .explain_options(ctx.explain)
            .build()
    }
}

impl From<QueryContext> for api::v1::QueryContext {
    fn from(
        QueryContext {
            current_catalog,
            mutable_session_data: mutable_inner,
            extensions,
            channel,
            snapshot_seqs,
            sst_min_sequences,
            mutable_query_context_data,
            ..
        }: QueryContext,
    ) -> Self {
        let explain = mutable_query_context_data.read().unwrap().explain_options;
        let mutable_inner = mutable_inner.read().unwrap();
        api::v1::QueryContext {
            current_catalog,
            current_schema: mutable_inner.schema.clone(),
            timezone: mutable_inner.timezone.to_string(),
            extensions,
            channel: channel as u32,
            snapshot_seqs: Some(api::v1::SnapshotSequences {
                snapshot_seqs: snapshot_seqs.read().unwrap().clone(),
                sst_min_sequences: sst_min_sequences.read().unwrap().clone(),
            }),
            explain,
        }
    }
}

impl From<&QueryContext> for api::v1::QueryContext {
    fn from(ctx: &QueryContext) -> Self {
        ctx.clone().into()
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
        self.mutable_session_data.read().unwrap().schema.clone()
    }

    pub fn set_current_schema(&self, new_schema: &str) {
        self.mutable_session_data.write().unwrap().schema = new_schema.to_string();
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
        self.mutable_session_data.read().unwrap().timezone.clone()
    }

    pub fn set_timezone(&self, timezone: Timezone) {
        self.mutable_session_data.write().unwrap().timezone = timezone;
    }

    pub fn read_preference(&self) -> ReadPreference {
        self.mutable_session_data.read().unwrap().read_preference
    }

    pub fn set_read_preference(&self, read_preference: ReadPreference) {
        self.mutable_session_data.write().unwrap().read_preference = read_preference;
    }

    pub fn current_user(&self) -> UserInfoRef {
        self.mutable_session_data.read().unwrap().user_info.clone()
    }

    pub fn set_current_user(&self, user: UserInfoRef) {
        self.mutable_session_data.write().unwrap().user_info = user;
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

    pub fn warning(&self) -> Option<String> {
        self.mutable_query_context_data
            .read()
            .unwrap()
            .warning
            .clone()
    }

    pub fn set_warning(&self, msg: String) {
        self.mutable_query_context_data.write().unwrap().warning = Some(msg);
    }

    pub fn explain_format(&self) -> Option<String> {
        self.mutable_query_context_data
            .read()
            .unwrap()
            .explain_format
            .clone()
    }

    pub fn set_explain_format(&self, format: String) {
        self.mutable_query_context_data
            .write()
            .unwrap()
            .explain_format = Some(format);
    }

    pub fn explain_verbose(&self) -> bool {
        self.mutable_query_context_data
            .read()
            .unwrap()
            .explain_options
            .map(|opts| opts.verbose)
            .unwrap_or(false)
    }

    pub fn set_explain_verbose(&self, verbose: bool) {
        self.mutable_query_context_data
            .write()
            .unwrap()
            .explain_options
            .get_or_insert_default()
            .verbose = verbose;
    }

    pub fn query_timeout(&self) -> Option<Duration> {
        self.mutable_session_data.read().unwrap().query_timeout
    }

    pub fn query_timeout_as_millis(&self) -> u128 {
        let timeout = self.mutable_session_data.read().unwrap().query_timeout;
        if let Some(t) = timeout {
            return t.as_millis();
        }
        0
    }

    pub fn set_query_timeout(&self, timeout: Duration) {
        self.mutable_session_data.write().unwrap().query_timeout = Some(timeout);
    }

    pub fn insert_cursor(&self, name: String, rb: RecordBatchStreamCursor) {
        let mut guard = self.mutable_session_data.write().unwrap();
        guard.cursors.insert(name, Arc::new(rb));

        let cursor_count = guard.cursors.len();
        if cursor_count > CURSOR_COUNT_WARNING_LIMIT {
            warn!("Current connection has {} open cursors", cursor_count);
        }
    }

    pub fn remove_cursor(&self, name: &str) {
        let mut guard = self.mutable_session_data.write().unwrap();
        guard.cursors.remove(name);
    }

    pub fn get_cursor(&self, name: &str) -> Option<Arc<RecordBatchStreamCursor>> {
        let guard = self.mutable_session_data.read().unwrap();
        let rb = guard.cursors.get(name);
        rb.cloned()
    }

    pub fn snapshots(&self) -> HashMap<u64, u64> {
        self.snapshot_seqs.read().unwrap().clone()
    }

    pub fn get_snapshot(&self, region_id: u64) -> Option<u64> {
        self.snapshot_seqs.read().unwrap().get(&region_id).cloned()
    }

    /// Returns `true` if the session can cast strings to numbers in MySQL style.
    pub fn auto_string_to_numeric(&self) -> bool {
        matches!(self.channel, Channel::Mysql)
    }

    /// Finds the minimal sequence of SST files to scan of a Region.
    pub fn sst_min_sequence(&self, region_id: u64) -> Option<u64> {
        self.sst_min_sequences
            .read()
            .unwrap()
            .get(&region_id)
            .copied()
    }
}

impl QueryContextBuilder {
    pub fn build(self) -> QueryContext {
        let channel = self.channel.unwrap_or_default();
        QueryContext {
            current_catalog: self
                .current_catalog
                .unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string()),
            snapshot_seqs: self.snapshot_seqs.unwrap_or_default(),
            sst_min_sequences: self.sst_min_sequences.unwrap_or_default(),
            mutable_session_data: self.mutable_session_data.unwrap_or_default(),
            mutable_query_context_data: self.mutable_query_context_data.unwrap_or_default(),
            sql_dialect: self
                .sql_dialect
                .unwrap_or_else(|| Arc::new(GreptimeDbDialect {})),
            extensions: self.extensions.unwrap_or_default(),
            configuration_parameter: self
                .configuration_parameter
                .unwrap_or_else(|| Arc::new(ConfigurationVariables::default())),
            channel,
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
    Loki = 9,
    Elasticsearch = 10,
    Jaeger = 11,
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
            9 => Self::Loki,
            10 => Self::Elasticsearch,
            11 => Self::Jaeger,
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
            Channel::Loki => write!(f, "loki"),
            Channel::Elasticsearch => write!(f, "elasticsearch"),
            Channel::Jaeger => write!(f, "jaeger"),
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
