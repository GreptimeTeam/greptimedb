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
use std::sync::Exclusive;

use ::auth::{userinfo_by_name, Identity, Password, UserInfoRef, UserProviderRef};
use async_trait::async_trait;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use futures::{Sink, SinkExt};
use pgwire::api::auth::StartupHandler;
use pgwire::api::{auth, ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use session::Session;
use snafu::IntoError;

use super::PostgresServerHandler;
use crate::error::{AuthSnafu, Result};
use crate::metrics::METRIC_AUTH_FAILURE;
use crate::postgres::types::PgErrorCode;
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
    async fn auth(&self, login: &LoginInfo, password: &str) -> Result<Option<UserInfoRef>> {
        let user_provider = match &self.user_provider {
            Some(provider) => provider,
            None => return Ok(None),
        };

        let user_name = match &login.user {
            Some(name) => name,
            None => return Ok(None),
        };
        let catalog = match &login.catalog {
            Some(name) => name,
            None => return Ok(None),
        };
        let schema = match &login.schema {
            Some(name) => name,
            None => return Ok(None),
        };

        match user_provider
            .auth(
                Identity::UserId(user_name, None),
                Password::PlainText(password.to_string().into()),
                catalog,
                schema,
            )
            .await
        {
            Err(e) => {
                METRIC_AUTH_FAILURE
                    .with_label_values(&[e.status_code().as_ref()])
                    .inc();
                Err(AuthSnafu.into_error(e))
            }
            Ok(user_info) => Ok(Some(user_info)),
        }
    }
}

fn set_client_info<C>(client: &C, session: &Session)
where
    C: ClientInfo,
{
    if let Some(current_catalog) = client.metadata().get(super::METADATA_CATALOG) {
        session.set_catalog(current_catalog.clone());
    }
    if let Some(current_schema) = client.metadata().get(super::METADATA_SCHEMA) {
        session.set_schema(current_schema.clone());
    }
    // set userinfo outside
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
                    send_error(
                        client,
                        PgErrorCode::Ec28000.to_err_info("No encryption".to_string()),
                    )
                    .await?;
                    return Ok(());
                }

                auth::save_startup_parameters_to_metadata(client, startup);

                // check if db is valid
                match resolve_db_info(Exclusive::new(client), self.query_handler.clone()).await? {
                    DbResolution::Resolved(catalog, schema) => {
                        let metadata = client.metadata_mut();
                        let _ = metadata.insert(super::METADATA_CATALOG.to_owned(), catalog);
                        let _ = metadata.insert(super::METADATA_SCHEMA.to_owned(), schema);
                    }
                    DbResolution::NotFound(msg) => {
                        send_error(client, PgErrorCode::Ec3D000.to_err_info(msg)).await?;
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
                    self.session.set_user_info(userinfo_by_name(
                        client.metadata().get(super::METADATA_USER).cloned(),
                    ));
                    set_client_info(client, &self.session);
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
                let auth_result = self.login_verifier.auth(&login_info, &pwd.password).await;

                if let Ok(Some(user_info)) = auth_result {
                    self.session.set_user_info(user_info);
                    set_client_info(client, &self.session);
                    auth::finish_authentication(client, self.param_provider.as_ref()).await;
                } else {
                    return send_error(
                        client,
                        PgErrorCode::Ec28P01
                            .to_err_info("password authentication failed".to_string()),
                    )
                    .await;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

async fn send_error<C>(client: &mut C, err_info: ErrorInfo) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let error = ErrorResponse::from(err_info);
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
async fn resolve_db_info<C>(
    client: Exclusive<&mut C>,
    query_handler: ServerSqlQueryHandlerRef,
) -> PgWireResult<DbResolution>
where
    C: ClientInfo + Unpin + Send,
{
    let db_ref = client.into_inner().metadata().get(super::METADATA_DATABASE);
    if let Some(db) = db_ref {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        if query_handler
            .is_valid_schema(&catalog, &schema)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        {
            Ok(DbResolution::Resolved(catalog, schema))
        } else {
            Ok(DbResolution::NotFound(format!("Database not found: {db}")))
        }
    } else {
        Ok(DbResolution::NotFound("Database not specified".to_owned()))
    }
}
