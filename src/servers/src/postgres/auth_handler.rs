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

use ::auth::{
    Identity, Password, PgAuthInfo, PgScramSha256Verifier, UserInfoRef, UserProviderRef,
    userinfo_by_name,
};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_time::Timezone;
use futures::{Sink, SinkExt};
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::sasl::SCRAM_SHA_256_METHOD;
use pgwire::api::{ClientInfo, PgWireConnectionState, auth};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::{Authentication, PasswordMessageFamily, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use session::Session;
use snafu::IntoError;
use tokio::sync::Mutex;

use crate::error::{AuthSnafu, Result};
use crate::metrics::METRIC_AUTH_FAILURE;
use crate::postgres::PostgresServerHandlerInner;
use crate::postgres::types::PgErrorCode;
use crate::postgres::utils::convert_err;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

pub(crate) struct PgLoginVerifier {
    user_provider: Option<UserProviderRef>,
    state: Mutex<PgAuthenticationState>,
}

impl PgLoginVerifier {
    pub(crate) fn new(user_provider: Option<UserProviderRef>) -> Self {
        Self {
            user_provider,
            state: Mutex::new(PgAuthenticationState::Initial),
        }
    }
}

enum PgAuthenticationState {
    Initial,
    Cleartext,
    SaslInitial {
        auth_info: PgAuthInfo,
    },
    SaslFinal {
        verifier: PgScramSha256Verifier,
        user_info: Option<UserInfoRef>,
        channel_binding: String,
        client_first_bare: String,
        server_first: String,
    },
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

    async fn postgres_auth_info(&self, login: &LoginInfo) -> Result<PgAuthInfo> {
        let user_provider = match &self.user_provider {
            Some(provider) => provider,
            None => return Ok(PgAuthInfo::Cleartext),
        };

        let user_name = match &login.user {
            Some(name) => name,
            None => return Ok(PgAuthInfo::Cleartext),
        };

        match user_provider
            .postgres_auth_info(Identity::UserId(user_name, None))
            .await
        {
            Err(e) => {
                METRIC_AUTH_FAILURE
                    .with_label_values(&[e.status_code().as_ref()])
                    .inc();
                Err(AuthSnafu.into_error(e))
            }
            Ok(auth_info) => Ok(auth_info),
        }
    }

    async fn authorize(&self, login: &LoginInfo, user_info: &UserInfoRef) -> Result<()> {
        let user_provider = match &self.user_provider {
            Some(provider) => provider,
            None => return Ok(()),
        };

        let catalog = match &login.catalog {
            Some(name) => name,
            None => return Ok(()),
        };
        let schema = match &login.schema {
            Some(name) => name,
            None => return Ok(()),
        };

        match user_provider.authorize(catalog, schema, user_info).await {
            Err(e) => {
                METRIC_AUTH_FAILURE
                    .with_label_values(&[e.status_code().as_ref()])
                    .inc();
                Err(AuthSnafu.into_error(e))
            }
            Ok(()) => Ok(()),
        }
    }
}

fn set_client_info<C>(client: &mut C, session: &Session)
where
    C: ClientInfo,
{
    if let Some(current_catalog) = client.metadata().get(super::METADATA_CATALOG) {
        session.set_catalog(current_catalog.clone());
    }
    if let Some(current_schema) = client.metadata().get(super::METADATA_SCHEMA) {
        session.set_schema(current_schema.clone());
    }

    // pass generated process id and secret key to client, this information will
    // be sent to postgres client for query cancellation.
    // use all 0 before we actually supported query cancellation
    client.set_pid_and_secret_key(0, SecretKey::I32(0));
    // set userinfo outside
}

#[async_trait]
impl StartupHandler for PostgresServerHandlerInner {
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

                // try to set TimeZone
                if let Some(tz) = client.metadata().get("TimeZone") {
                    match Timezone::from_tz_string(tz) {
                        Ok(tz) => self.session.set_timezone(tz),
                        Err(_) => {
                            send_error(
                                client,
                                PgErrorCode::Ec22023
                                    .to_err_info(format!("Invalid TimeZone: {}", tz)),
                            )
                            .await?;

                            return Ok(());
                        }
                    }
                }

                if self.login_verifier.user_provider.is_some() {
                    let login_info = LoginInfo::from_client_info(client);
                    let auth_info = match self.login_verifier.postgres_auth_info(&login_info).await
                    {
                        Ok(auth_info) => auth_info,
                        Err(_) => {
                            return send_password_authentication_failed(client).await;
                        }
                    };

                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    match auth_info {
                        PgAuthInfo::ScramSha256 { .. } => {
                            *self.login_verifier.state.lock().await =
                                PgAuthenticationState::SaslInitial { auth_info };
                            client
                                .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                                    vec![SCRAM_SHA_256_METHOD.to_string()],
                                )))
                                .await?;
                        }
                        PgAuthInfo::Cleartext => {
                            *self.login_verifier.state.lock().await =
                                PgAuthenticationState::Cleartext;
                            client
                                .send(PgWireBackendMessage::Authentication(
                                    Authentication::CleartextPassword,
                                ))
                                .await?;
                        }
                    }
                } else {
                    self.session.set_user_info(userinfo_by_name(
                        client.metadata().get(super::METADATA_USER).cloned(),
                    ));
                    set_client_info(client, &self.session);
                    auth::finish_authentication(client, self.param_provider.as_ref()).await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let login_info = LoginInfo::from_client_info(client);
                match self
                    .authenticate_password_message(client, &login_info, pwd)
                    .await?
                {
                    PgAuthenticationResult::Continue => {}
                    PgAuthenticationResult::Success(user_info) => {
                        self.session.set_user_info(user_info);
                        set_client_info(client, &self.session);
                        auth::finish_authentication(client, self.param_provider.as_ref()).await?;
                    }
                    PgAuthenticationResult::Failed => {
                        return send_password_authentication_failed(client).await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

enum PgAuthenticationResult {
    Continue,
    Success(UserInfoRef),
    Failed,
}

impl PostgresServerHandlerInner {
    async fn authenticate_password_message<C>(
        &self,
        client: &mut C,
        login_info: &LoginInfo,
        pwd: PasswordMessageFamily,
    ) -> PgWireResult<PgAuthenticationResult>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut state = self.login_verifier.state.lock().await;
        let current_state = std::mem::replace(&mut *state, PgAuthenticationState::Initial);

        match current_state {
            PgAuthenticationState::Cleartext => {
                let pwd = pwd.into_password()?;
                drop(state);
                match self.login_verifier.auth(login_info, &pwd.password).await {
                    Ok(Some(user_info)) => Ok(PgAuthenticationResult::Success(user_info)),
                    Ok(None) | Err(_) => Ok(PgAuthenticationResult::Failed),
                }
            }
            PgAuthenticationState::SaslInitial { auth_info } => {
                let sasl_initial = pwd.into_sasl_initial_response()?;
                if sasl_initial.auth_method != SCRAM_SHA_256_METHOD {
                    return Ok(PgAuthenticationResult::Failed);
                }
                let Some(data) = sasl_initial.data else {
                    return Ok(PgAuthenticationResult::Failed);
                };
                let Some(client_first) = ScramClientFirst::parse(&data) else {
                    return Ok(PgAuthenticationResult::Failed);
                };
                let PgAuthInfo::ScramSha256 {
                    verifier,
                    user_info,
                } = auth_info
                else {
                    return Ok(PgAuthenticationResult::Failed);
                };

                let server_nonce = BASE64.encode(rand::random::<[u8; 18]>());
                let nonce = format!("{}{}", client_first.nonce, server_nonce);
                let server_first = format!(
                    "r={},s={},i={}",
                    nonce,
                    BASE64.encode(verifier.salt()),
                    verifier.iterations()
                );
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::SASLContinue(server_first.clone().into()),
                    ))
                    .await?;
                *state = PgAuthenticationState::SaslFinal {
                    verifier,
                    user_info,
                    channel_binding: client_first.channel_binding,
                    client_first_bare: client_first.bare,
                    server_first,
                };
                Ok(PgAuthenticationResult::Continue)
            }
            PgAuthenticationState::SaslFinal {
                verifier,
                user_info,
                channel_binding,
                client_first_bare,
                server_first,
            } => {
                let sasl_response = pwd.into_sasl_response()?;
                let Some(client_final) = ScramClientFinal::parse(&sasl_response.data) else {
                    return Ok(PgAuthenticationResult::Failed);
                };
                if client_final.channel_binding != channel_binding {
                    return Ok(PgAuthenticationResult::Failed);
                }

                let auth_message = format!(
                    "{},{},{}",
                    client_first_bare, server_first, client_final.without_proof
                );
                let Ok(client_proof) = BASE64.decode(client_final.proof.as_bytes()) else {
                    return Ok(PgAuthenticationResult::Failed);
                };
                let Ok(Some(server_signature)) =
                    verifier.verify_client_proof(auth_message.as_bytes(), &client_proof)
                else {
                    return Ok(PgAuthenticationResult::Failed);
                };
                let Some(user_info) = user_info else {
                    return Ok(PgAuthenticationResult::Failed);
                };

                drop(state);
                if self
                    .login_verifier
                    .authorize(login_info, &user_info)
                    .await
                    .is_err()
                {
                    return Ok(PgAuthenticationResult::Failed);
                }

                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::SASLFinal(
                            format!("v={}", BASE64.encode(server_signature)).into(),
                        ),
                    ))
                    .await?;
                Ok(PgAuthenticationResult::Success(user_info))
            }
            PgAuthenticationState::Initial => Ok(PgAuthenticationResult::Failed),
        }
    }
}

struct ScramClientFirst {
    channel_binding: String,
    bare: String,
    nonce: String,
}

impl ScramClientFirst {
    fn parse(data: &[u8]) -> Option<Self> {
        let message = std::str::from_utf8(data).ok()?;
        let mut parts = message.splitn(3, ',');
        let cbind = parts.next()?;
        if !matches!(cbind, "n" | "y") {
            return None;
        }
        let authzid = parts.next()?;
        if !authzid.is_empty() {
            return None;
        }
        let channel_binding = BASE64.encode(format!("{cbind},,"));
        let bare = parts.next()?.to_string();
        let nonce = bare
            .split(',')
            .find_map(|chunk| chunk.strip_prefix("r="))?
            .to_string();
        Some(Self {
            channel_binding,
            bare,
            nonce,
        })
    }
}

struct ScramClientFinal {
    channel_binding: String,
    without_proof: String,
    proof: String,
}

impl ScramClientFinal {
    fn parse(data: &[u8]) -> Option<Self> {
        let message = std::str::from_utf8(data).ok()?;
        let proof_pos = message.rfind(",p=")?;
        let without_proof = message[..proof_pos].to_string();
        let proof = message[proof_pos + 3..].to_string();
        let channel_binding = without_proof
            .split(',')
            .find_map(|chunk| chunk.strip_prefix("c="))?;
        if !without_proof
            .split(',')
            .any(|chunk| chunk.strip_prefix("r=").is_some())
            || proof.is_empty()
        {
            return None;
        }

        Some(Self {
            channel_binding: channel_binding.to_string(),
            without_proof,
            proof,
        })
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

async fn send_password_authentication_failed<C>(client: &mut C) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    send_error(
        client,
        PgErrorCode::Ec28P01.to_err_info("password authentication failed".to_string()),
    )
    .await
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
            .map_err(convert_err)?
        {
            Ok(DbResolution::Resolved(catalog, schema))
        } else {
            Ok(DbResolution::NotFound(format!("Database not found: {db}")))
        }
    } else {
        Ok(DbResolution::NotFound("Database not specified".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scram_client_first_parse() {
        let message = b"n,,n=greptime,r=clientnonce";
        let client_first = ScramClientFirst::parse(message).unwrap();
        assert_eq!("biws", client_first.channel_binding);
        assert_eq!("n=greptime,r=clientnonce", client_first.bare);
        assert_eq!("clientnonce", client_first.nonce);

        let message = b"y,,n=greptime,r=clientnonce";
        let client_first = ScramClientFirst::parse(message).unwrap();
        assert_eq!("eSws", client_first.channel_binding);
        assert_eq!("n=greptime,r=clientnonce", client_first.bare);
        assert_eq!("clientnonce", client_first.nonce);

        assert!(ScramClientFirst::parse(b"p=tls-server-end-point,,n=greptime,r=nonce").is_none());
        assert!(ScramClientFirst::parse(b"n,a=authzid,n=greptime,r=nonce").is_none());
    }

    #[test]
    fn test_scram_client_final_parse() {
        let message = b"c=biws,r=clientnonceservernonce,p=dGVzdA==";
        let client_final = ScramClientFinal::parse(message).unwrap();
        assert_eq!("biws", client_final.channel_binding);
        assert_eq!(
            "c=biws,r=clientnonceservernonce",
            client_final.without_proof
        );
        assert_eq!("dGVzdA==", client_final.proof);

        assert!(ScramClientFinal::parse(b"r=nonce,p=dGVzdA==").is_none());
        assert!(ScramClientFinal::parse(b"c=biws,r=nonce").is_none());
    }
}
