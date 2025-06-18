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
mod fixtures;
mod handler;
mod server;
mod types;

pub(crate) const METADATA_USER: &str = "user";
pub(crate) const METADATA_DATABASE: &str = "database";
/// key to store our parsed catalog
pub(crate) const METADATA_CATALOG: &str = "catalog";
/// key to store our parsed schema
pub(crate) const METADATA_SCHEMA: &str = "schema";

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use ::auth::UserProviderRef;
use catalog::process_manager::ProcessManagerRef;
use derive_builder::Builder;
use pgwire::api::auth::ServerParameterProvider;
use pgwire::api::auth::StartupHandler;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::ErrorHandler;
use pgwire::api::{ClientInfo, PgWireServerHandlers};
pub use server::PostgresServer;
use session::context::Channel;
use session::Session;

use self::auth_handler::PgLoginVerifier;
use self::handler::DefaultQueryParser;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

pub(crate) struct GreptimeDBStartupParameters {
    version: String,
}

impl GreptimeDBStartupParameters {
    fn new() -> GreptimeDBStartupParameters {
        GreptimeDBStartupParameters {
            version: format!("16.3-greptimedb-{}", env!("CARGO_PKG_VERSION")),
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
    process_manager: ProcessManagerRef,
    login_verifier: PgLoginVerifier,
    force_tls: bool,
    param_provider: Arc<GreptimeDBStartupParameters>,

    session: Arc<Session>,
    query_parser: Arc<DefaultQueryParser>,
}

#[derive(Builder)]
pub(crate) struct MakePostgresServerHandler {
    query_handler: ServerSqlQueryHandlerRef,
    process_manager: ProcessManagerRef,
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

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        self.0.clone()
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.0.clone()
    }
}

impl MakePostgresServerHandler {
    fn make(&self, addr: Option<SocketAddr>) -> PostgresServerHandler {
        let process_id = self.process_manager.next_id();
        let session = Arc::new(Session::new(
            addr,
            Channel::Postgres,
            Default::default(),
            process_id,
        ));
        let handler = PostgresServerHandlerInner {
            query_handler: self.query_handler.clone(),
            process_manager: self.process_manager.clone(),
            login_verifier: PgLoginVerifier::new(self.user_provider.clone()),
            force_tls: self.force_tls,
            param_provider: self.param_provider.clone(),

            session: session.clone(),
            query_parser: Arc::new(DefaultQueryParser::new(self.query_handler.clone(), session)),
        };
        PostgresServerHandler(Arc::new(handler))
    }
}
