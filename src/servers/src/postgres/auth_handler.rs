// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use futures::{Sink, SinkExt};
use pgwire::api::auth::{ServerParameterProvider, StartupHandler};
use pgwire::api::{auth, ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use snafu::ResultExt;

use crate::auth::{Identity, Password, UserProviderRef};
use crate::error;
use crate::error::Result;
use crate::query_handler::SqlQueryHandlerRef;

struct PgPwdVerifier {
    user_provider: Option<UserProviderRef>,
}

#[allow(dead_code)]
struct LoginInfo {
    user: Option<String>,
    database: Option<String>,
    host: String,
}

impl LoginInfo {
    pub fn from_client_info<C>(client: &C) -> LoginInfo
    where
        C: ClientInfo,
    {
        LoginInfo {
            user: client.metadata().get(super::METADATA_USER).map(Into::into),
            database: client
                .metadata()
                .get(super::METADATA_DATABASE)
                .map(Into::into),
            host: client.socket_addr().ip().to_string(),
        }
    }
}

impl PgPwdVerifier {
    async fn verify_pwd(&self, password: &str, login: LoginInfo) -> Result<bool> {
        if let Some(user_provider) = &self.user_provider {
            let user_name = match login.user {
                Some(name) => name,
                None => return Ok(false),
            };

            // TODO(fys): pass user_info to context
            let _user_info = user_provider
                .auth(
                    Identity::UserId(&user_name, None),
                    Password::PlainText(password),
                )
                .await
                .context(error::AuthSnafu)?;
        }
        Ok(true)
    }
}

struct GreptimeDBStartupParameters {
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
        let mut params = HashMap::with_capacity(4);
        params.insert("server_version".to_owned(), self.version.to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO YMD".to_owned());

        Some(params)
    }
}

pub struct PgAuthStartupHandler {
    verifier: PgPwdVerifier,
    param_provider: GreptimeDBStartupParameters,
    force_tls: bool,
    query_handler: SqlQueryHandlerRef,
}

impl PgAuthStartupHandler {
    pub fn new(
        user_provider: Option<UserProviderRef>,
        force_tls: bool,
        query_handler: SqlQueryHandlerRef,
    ) -> Self {
        PgAuthStartupHandler {
            verifier: PgPwdVerifier { user_provider },
            param_provider: GreptimeDBStartupParameters::new(),
            force_tls,
            query_handler,
        }
    }
}

#[async_trait]
impl StartupHandler for PgAuthStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &PgWireFrontendMessage,
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
                let db_ref = client.metadata().get(super::METADATA_DATABASE);
                if let Some(db) = db_ref {
                    if !self
                        .query_handler
                        .is_valid_schema(DEFAULT_CATALOG_NAME, db)
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                    {
                        send_error(
                            client,
                            "FATAL",
                            "3D000",
                            format!("Database not found: {}", db),
                        )
                        .await?;
                        return Ok(());
                    }
                } else {
                    send_error(
                        client,
                        "FATAL",
                        "3D000",
                        "Database not specified".to_owned(),
                    )
                    .await?;
                    return Ok(());
                }

                if self.verifier.user_provider.is_some() {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    auth::finish_authentication(client, &self.param_provider).await;
                }
            }
            PgWireFrontendMessage::Password(ref pwd) => {
                let login_info = LoginInfo::from_client_info(client);
                if let Ok(true) = self.verifier.verify_pwd(pwd.password(), login_info).await {
                    auth::finish_authentication(client, &self.param_provider).await
                } else {
                    send_error(
                        client,
                        "FATAL",
                        "28P01",
                        "Password authentication failed".to_owned(),
                    )
                    .await?;
                }
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
