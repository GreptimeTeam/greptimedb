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

mod auth_handler;
mod copy_in;
mod fixtures;
mod handler;
mod server;
mod types;
mod utils;

pub(crate) const METADATA_USER: &str = "user";
pub(crate) const METADATA_DATABASE: &str = "database";
/// key to store our parsed catalog
pub(crate) const METADATA_CATALOG: &str = "catalog";
/// key to store our parsed schema
pub(crate) const METADATA_SCHEMA: &str = "schema";

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ::auth::{UserInfoRef, UserProviderRef};
use derive_builder::Builder;
use pgwire::api::auth::{AuthDriver, ServerParameterProvider, StartupHandler};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers};
pub use server::PostgresServer;
use session::Session;
use session::context::Channel;

use self::auth_handler::{PgUserProviderSource, ZeroPidSecretKeyGenerator};
use self::handler::DefaultQueryParser;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::query_handler::{
    CopyInHandlerRef, InfluxdbLineProtocolHandlerRef, OpenTelemetryProtocolHandlerRef,
};

pub(crate) struct GreptimeDBStartupParameters {
    version: String,
}

impl GreptimeDBStartupParameters {
    fn new() -> GreptimeDBStartupParameters {
        GreptimeDBStartupParameters {
            version: format!(
                "16.3-{}-{}",
                common_version::product_name(),
                common_version::version()
            ),
        }
    }
}

impl ServerParameterProvider for GreptimeDBStartupParameters {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        Some(HashMap::from([
            ("server_version".to_owned(), self.version.clone()),
            ("server_encoding".to_owned(), "UTF8".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("DateStyle".to_owned(), "ISO YMD".to_owned()),
            ("integer_datetimes".to_owned(), "on".to_owned()),
        ]))
    }
}

pub struct PostgresServerHandlerInner {
    query_handler: ServerSqlQueryHandlerRef,
    auth_source: Option<Arc<PgUserProviderSource>>,
    copy_in_handler: CopyInHandlerRef,
    /// Executes influxdb line-protocol batches flushed by a table-less
    /// COPY FROM STDIN — the same handler the HTTP write endpoints use.
    /// Absent on servers not wired for it; such connections reject the
    /// influxdb format when a copy starts.
    influxdb_handler: Option<InfluxdbLineProtocolHandlerRef>,
    /// Executes the OTLP metrics payload flushed by a table-less
    /// `COPY FROM STDIN WITH (FORMAT otlp)` — the same handler the
    /// OTLP/HTTP endpoint uses.
    otlp_metrics_handler: Option<OpenTelemetryProtocolHandlerRef>,
    force_tls: bool,
    auth_driver: AuthDriver<Arc<GreptimeDBStartupParameters>, UserInfoRef>,
    /// SCRAM selected; failures get the uniform metric label.
    is_scram_auth: AtomicBool,

    batching_enabled: bool,
    session: Arc<Session>,
    query_parser: Arc<DefaultQueryParser>,

    /// State of an in-progress `COPY ... FROM STDIN`; set when the copy
    /// starts and taken when it finishes, fails or the connection drops.
    copy_in_state: tokio::sync::Mutex<Option<copy_in::CopyInState>>,
}

#[derive(Builder)]
pub(crate) struct MakePostgresServerHandler {
    query_handler: ServerSqlQueryHandlerRef,
    copy_in_handler: CopyInHandlerRef,
    #[builder(default)]
    influxdb_handler: Option<InfluxdbLineProtocolHandlerRef>,
    #[builder(default)]
    otlp_metrics_handler: Option<OpenTelemetryProtocolHandlerRef>,
    user_provider: Option<UserProviderRef>,
    #[builder(default = "Arc::new(GreptimeDBStartupParameters::new())")]
    param_provider: Arc<GreptimeDBStartupParameters>,
    force_tls: bool,
}

pub(crate) struct PostgresServerHandler(Arc<PostgresServerHandlerInner>);

impl PgWireServerHandlers for PostgresServerHandler {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.0.clone()
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        self.0.clone()
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        self.0.clone()
    }
}

impl MakePostgresServerHandler {
    fn make(
        &self,
        addr: Option<SocketAddr>,
        process_id: u32,
        batching_enabled: bool,
    ) -> PostgresServerHandler {
        let session = Arc::new(Session::new(
            addr,
            Channel::Postgres,
            Default::default(),
            process_id,
        ));
        let handler = PostgresServerHandlerInner {
            query_handler: self.query_handler.clone(),
            auth_source: self
                .user_provider
                .clone()
                .map(|p| Arc::new(PgUserProviderSource::new(p))),
            copy_in_handler: self.copy_in_handler.clone(),
            influxdb_handler: self.influxdb_handler.clone(),
            otlp_metrics_handler: self.otlp_metrics_handler.clone(),
            force_tls: self.force_tls,
            auth_driver: AuthDriver::new(self.param_provider.clone())
                .with_pid_secret_key_generator(Arc::new(ZeroPidSecretKeyGenerator)),
            is_scram_auth: AtomicBool::new(false),

            batching_enabled,
            session: session.clone(),
            query_parser: Arc::new(DefaultQueryParser::new(self.query_handler.clone(), session)),
            copy_in_state: tokio::sync::Mutex::new(None),
        };
        PostgresServerHandler(Arc::new(handler))
    }
}
