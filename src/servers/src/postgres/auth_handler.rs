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

use std::fmt::Debug;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::auth::StartupHandler;
use pgwire::api::{auth, ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use session::context::{QueryContextRef, UserInfo};
use snafu::ResultExt;

use super::PostgresServerHandler;
use crate::auth::{Identity, Password, UserProviderRef};
use crate::error;
use crate::error::Result;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

pub(crate) struct PgLoginVerifier {
    user_provider: Option<UserProviderRef>,
}

impl PgLoginVerifier {
    pub(crate) fn new(user_provider: Option<UserProviderRef>) -> Self {
        Self { user_provider }
    }
}

#[allow(dead_code)]
struct LoginInfo {
    user: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    host: String,
}

impl LoginInfo {
    pub fn from_client_info<C>(client: &C) -> LoginInfo
    where
        C: ClientInfo,
    {
        LoginInfo {
            user: client.metadata().get(super::METADATA_USER).map(Into::into),
            catalog: client
                .metadata()
                .get(super::METADATA_CATALOG)
                .map(Into::into),
            schema: client
                .metadata()
                .get(super::METADATA_SCHEMA)
                .map(Into::into),
            host: client.socket_addr().ip().to_string(),
        }
    }
}

impl PgLoginVerifier {
    async fn verify_pwd(&self, password: &str, login: &LoginInfo) -> Result<bool> {
        if let Some(user_provider) = &self.user_provider {
            let user_name = match &login.user {
                Some(name) => name,
                None => return Ok(false),
            };

            // TODO(fys): pass user_info to context
            let _user_info = user_provider
                .authenticate(
                    Identity::UserId(user_name, None),
                    Password::PlainText(password),
                )
                .await
                .context(error::AuthSnafu)?;
        }
        Ok(true)
    }

    async fn authorize(&self, login: &LoginInfo) -> Result<bool> {
        // at this time, username in login info should be valid
        // TODO(shuiyisong): change to use actually user_info from session
        if let Some(user_provider) = &self.user_provider {
            let user_name = match &login.user {
                Some(name) => name,
                None => return Ok(false),
            };
            let catalog = match &login.catalog {
                Some(name) => name,
                None => return Ok(false),
            };
            let schema = match &login.schema {
                Some(name) => name,
                None => return Ok(false),
            };
            user_provider
                .authorize(catalog, schema, &UserInfo::new(user_name))
                .await?;
        }
        Ok(true)
    }
}

fn set_query_context_from_client_info<C>(client: &C, query_context: QueryContextRef)
where
    C: ClientInfo,
{
    if let Some(current_catalog) = client.metadata().get(super::METADATA_CATALOG) {
        query_context.set_current_catalog(current_catalog);
    }
    if let Some(current_schema) = client.metadata().get(super::METADATA_SCHEMA) {
        query_context.set_current_schema(current_schema);
    }
}

#[async_trait]
impl StartupHandler for PostgresServerHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                // check ssl requirement
                if !client.is_secure() && self.force_tls {
                    send_error(client, "FATAL", "28000", "No encryption".to_owned()).await?;
                    return Ok(());
                }

                auth::save_startup_parameters_to_metadata(client, startup);

                // check if db is valid
                match resolve_db_info(client, self.query_handler.clone())? {
                    DbResolution::Resolved(catalog, schema) => {
                        client
                            .metadata_mut()
                            .insert(super::METADATA_CATALOG.to_owned(), catalog);
                        client
                            .metadata_mut()
                            .insert(super::METADATA_SCHEMA.to_owned(), schema);
                    }
                    DbResolution::NotFound(msg) => {
                        send_error(client, "FATAL", "3D000", msg).await?;
                        return Ok(());
                    }
                }

                if self.login_verifier.user_provider.is_some() {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    set_query_context_from_client_info(client, self.query_ctx.clone());
                    auth::finish_authentication(client, self.param_provider.as_ref()).await;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                // the newer version of pgwire has a few variant password
                // message like cleartext/md5 password, saslresponse, etc. Here
                // we must manually coerce it into password
                let pwd = pwd.into_password()?;

                let login_info = LoginInfo::from_client_info(client);
                // do authenticate
                let authenticate_result = self
                    .login_verifier
                    .verify_pwd(pwd.password(), &login_info)
                    .await;
                if !matches!(authenticate_result, Ok(true)) {
                    return send_error(
                        client,
                        "FATAL",
                        "28P01",
                        "password authentication failed".to_owned(),
                    )
                    .await;
                }
                // do authorize
                let authorize_result = self.login_verifier.authorize(&login_info).await;
                if !matches!(authorize_result, Ok(true)) {
                    return send_error(
                        client,
                        "FATAL",
                        "28P01",
                        "password authorization failed".to_owned(),
                    )
                    .await;
                }
                set_query_context_from_client_info(client, self.query_ctx.clone());
                auth::finish_authentication(client, self.param_provider.as_ref()).await;
            }
            _ => {}
        }
        Ok(())
    }
}

async fn send_error<C>(client: &mut C, level: &str, code: &str, message: String) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let error = ErrorResponse::from(ErrorInfo::new(level.to_owned(), code.to_owned(), message));
    client
        .feed(PgWireBackendMessage::ErrorResponse(error))
        .await?;
    client.close().await?;
    Ok(())
}

enum DbResolution {
    Resolved(String, String),
    NotFound(String),
}

/// A function extracted to resolve lifetime and readability issues:
fn resolve_db_info<C>(
    client: &mut C,
    query_handler: ServerSqlQueryHandlerRef,
) -> PgWireResult<DbResolution>
where
    C: ClientInfo + Unpin + Send,
{
    let db_ref = client.metadata().get(super::METADATA_DATABASE);
    if let Some(db) = db_ref {
        let (catalog, schema) = crate::parse_catalog_and_schema_from_client_database_name(db);
        if query_handler
            .is_valid_schema(catalog, schema)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        {
            Ok(DbResolution::Resolved(
                catalog.to_owned(),
                schema.to_owned(),
            ))
        } else {
            Ok(DbResolution::NotFound(format!("Database not found: {db}")))
        }
    } else {
        Ok(DbResolution::NotFound("Database not specified".to_owned()))
    }
}
