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
use derive_builder::Builder;
use pgwire::api::auth::ServerParameterProvider;
use pgwire::api::ClientInfo;
pub use server::PostgresServer;
use session::context::Channel;
use session::Session;

use self::auth_handler::PgLoginVerifier;
use self::handler::DefaultQueryParser;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

pub(crate) struct GreptimeDBStartupParameters {
    version: &'static str,
}

impl GreptimeDBStartupParameters {
    fn new() -> GreptimeDBStartupParameters {
        GreptimeDBStartupParameters {
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

impl ServerParameterProvider for GreptimeDBStartupParameters {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        Some(HashMap::from([
            ("server_version".to_owned(), self.version.to_owned()),
            ("server_encoding".to_owned(), "UTF8".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("DateStyle".to_owned(), "ISO YMD".to_owned()),
            ("integer_datetimes".to_owned(), "on".to_owned()),
        ]))
    }
}

pub struct PostgresServerHandler {
    query_handler: ServerSqlQueryHandlerRef,
    login_verifier: PgLoginVerifier,
    force_tls: bool,
    param_provider: Arc<GreptimeDBStartupParameters>,

    session: Arc<Session>,
    query_parser: Arc<DefaultQueryParser>,
}

#[derive(Builder)]
pub(crate) struct MakePostgresServerHandler {
    query_handler: ServerSqlQueryHandlerRef,
    user_provider: Option<UserProviderRef>,
    #[builder(default = "Arc::new(GreptimeDBStartupParameters::new())")]
    param_provider: Arc<GreptimeDBStartupParameters>,
    force_tls: bool,
}

impl MakePostgresServerHandler {
    fn make(&self, addr: Option<SocketAddr>) -> PostgresServerHandler {
        let session = Arc::new(Session::new(addr, Channel::Postgres));
        PostgresServerHandler {
            query_handler: self.query_handler.clone(),
            login_verifier: PgLoginVerifier::new(self.user_provider.clone()),
            force_tls: self.force_tls,
            param_provider: self.param_provider.clone(),

            session: session.clone(),
            query_parser: Arc::new(DefaultQueryParser::new(self.query_handler.clone(), session)),
        }
    }
}
