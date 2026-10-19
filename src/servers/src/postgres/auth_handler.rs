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
use std::sync::{Arc, Mutex};

use ::auth::{
    BEARER_TOKEN_USER, Identity, Password, PgAuthInfo, PgScramSha256Verifier, UserInfoRef,
    UserProviderRef, userinfo_by_name,
};
use async_trait::async_trait;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_time::Timezone;
use futures::{Sink, SinkExt};
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::cleartext::CleartextPasswordAuth;
use pgwire::api::auth::driver::AuthOutcome;
use pgwire::api::auth::mechanism::{AuthMechanism, AuthMechanismFactory, AuthState};
use pgwire::api::auth::sasl::scram::{ScramAuth, ScramSecret};
use pgwire::api::auth::{AuthSource, LoginInfo as WireLoginInfo};
use pgwire::api::{ClientInfo, PidSecretKeyGenerator, auth};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::{PasswordMessageFamily, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use session::Session;
use snafu::IntoError;

use crate::error::{AuthSnafu, Result};
use crate::metrics::METRIC_AUTH_FAILURE;
use crate::postgres::PostgresServerHandlerInner;
use crate::postgres::types::PgErrorCode;
use crate::postgres::utils::convert_err;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

/// Query cancellation is not supported yet, so announce pid/secret key 0 as
/// before the `AuthDriver` adoption.
pub(crate) struct ZeroPidSecretKeyGenerator;

impl PidSecretKeyGenerator for ZeroPidSecretKeyGenerator {
    fn generate(&self, _client: &dyn ClientInfo) -> (i32, SecretKey) {
        (0, SecretKey::I32(0))
    }
}

struct LoginInfo {
    user: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
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
        }
    }
}

fn record_auth_failure(status_code: StatusCode) {
    METRIC_AUTH_FAILURE
        .with_label_values(&[status_code.as_ref()])
        .inc();
}

/// Verifies a cleartext password or bearer token; the provider verifies the
/// password itself, so pgwire's built-in cleartext mechanism can't be used.
async fn authenticate_password(
    user_provider: &UserProviderRef,
    login: &LoginInfo,
    password: &str,
) -> PgWireResult<UserInfoRef> {
    let (user, catalog, schema) = match (
        login.user.as_deref(),
        login.catalog.as_deref(),
        login.schema.as_deref(),
    ) {
        (Some(user), Some(catalog), Some(schema)) => (user, catalog, schema),
        // Unreachable: catalog/schema are resolved before mechanism
        // selection; fail closed without a metric.
        _ => return Err(PgWireError::UserNameRequired),
    };

    let result = if user == BEARER_TOKEN_USER {
        user_provider
            .auth_bearer_token(password, catalog, schema)
            .await
    } else {
        user_provider
            .auth(
                Identity::UserId(user, None),
                Password::PlainText(password.to_string().into()),
                catalog,
                schema,
            )
            .await
    };
    match result {
        Err(e) => {
            record_auth_failure(e.status_code());
            Err(PgWireError::ApiError(Box::new(AuthSnafu.into_error(e))))
        }
        Ok(user_info) => Ok(user_info),
    }
}

async fn postgres_auth_info(
    user_provider: &UserProviderRef,
    login: &LoginInfo,
) -> Result<PgAuthInfo> {
    let Some(user) = login.user.as_deref() else {
        return Ok(PgAuthInfo::Cleartext);
    };
    let Some(catalog) = login.catalog.as_deref() else {
        return Ok(PgAuthInfo::Cleartext);
    };

    match user_provider
        .postgres_auth_info(Identity::UserId(user, None), catalog)
        .await
    {
        Err(e) => {
            record_auth_failure(e.status_code());
            Err(AuthSnafu.into_error(e))
        }
        Ok(auth_info) => Ok(auth_info),
    }
}

async fn authorize(
    user_provider: &UserProviderRef,
    login: &LoginInfo,
    user_info: &UserInfoRef,
) -> PgWireResult<()> {
    let (catalog, schema) = match (login.catalog.as_deref(), login.schema.as_deref()) {
        (Some(catalog), Some(schema)) => (catalog, schema),
        _ => return Err(PgWireError::UserNameRequired),
    };

    match user_provider.authorize(catalog, schema, user_info).await {
        Err(e) => {
            // Records with the provider's status code; callers must not
            // record it again under a uniform label.
            record_auth_failure(e.status_code());
            Err(PgWireError::ApiError(Box::new(AuthSnafu.into_error(e))))
        }
        Ok(()) => Ok(()),
    }
}

fn to_scram_secret(verifier: &PgScramSha256Verifier) -> ScramSecret {
    ScramSecret::Verifier {
        salt: verifier.salt().to_vec(),
        iterations: verifier.iterations() as usize,
        stored_key: verifier.stored_key().to_vec(),
        server_key: verifier.server_key().to_vec(),
    }
}

/// Hands pgwire's SCRAM machinery the verifier and identity resolved at
/// connection start.
#[derive(Debug)]
struct FixedScramSecretSource {
    secret: Mutex<Option<(ScramSecret, Option<UserInfoRef>)>>,
}

impl FixedScramSecretSource {
    fn new(secret: ScramSecret, user_info: Option<UserInfoRef>) -> Self {
        Self {
            secret: Mutex::new(Some((secret, user_info))),
        }
    }
}

#[async_trait]
impl AuthSource<UserInfoRef> for FixedScramSecretSource {
    async fn get_password(
        &self,
        _login: &WireLoginInfo,
    ) -> PgWireResult<pgwire::api::auth::Password> {
        Err(PgWireError::ApiError(
            "no cleartext password available for a SCRAM verifier".into(),
        ))
    }

    async fn get_scram_secret(
        &self,
        _login: &WireLoginInfo,
    ) -> PgWireResult<(ScramSecret, Option<UserInfoRef>)> {
        self.secret
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PgWireError::InvalidSASLState)
    }
}

/// Backs pgwire's cleartext mechanism: the user provider verifies the
/// password (or bearer token) itself and resolves the identity, so the
/// built-in cleartext mechanism can be used directly.
struct GreptimePasswordSource {
    user_provider: UserProviderRef,
    login: LoginInfo,
}

impl Debug for GreptimePasswordSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GreptimePasswordSource")
            .field("user", &self.login.user)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource<UserInfoRef> for GreptimePasswordSource {
    async fn get_password(
        &self,
        _login: &WireLoginInfo,
    ) -> PgWireResult<pgwire::api::auth::Password> {
        Err(PgWireError::ApiError(
            "passwords are verified by the user provider, never served".into(),
        ))
    }

    async fn verify_password(
        &self,
        _login: &WireLoginInfo,
        presented_password: &[u8],
    ) -> PgWireResult<Option<UserInfoRef>> {
        let password = str::from_utf8(presented_password).map_err(|e| {
            PgWireError::ApiError(format!("password is not valid UTF-8: {e}").into())
        })?;
        // Metrics are recorded inside `authenticate_password`.
        let user_info = authenticate_password(&self.user_provider, &self.login, password).await?;
        Ok(Some(user_info))
    }
}

/// pgwire's SCRAM mechanism plus the GreptimeDB authorization check, run
/// after the client proof is verified so a rejection still precedes
/// `AuthenticationOk`.
struct PgScramMechanism {
    inner: Box<dyn AuthMechanism<UserInfoRef>>,
    user_provider: UserProviderRef,
    login: LoginInfo,
}

impl PgScramMechanism {
    /// The label is intentionally uniform (never `UserNotFound`), so the
    /// counter cannot distinguish a wrong password from an unknown user.
    fn record_scram_failure<T>(result: T) -> T {
        METRIC_AUTH_FAILURE
            .with_label_values(&[StatusCode::UserPasswordMismatch.as_ref()])
            .inc();
        result
    }
}

#[async_trait]
impl AuthMechanism<UserInfoRef> for PgScramMechanism {
    async fn begin(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> PgWireResult<AuthState<UserInfoRef>> {
        self.inner.begin(client).await
    }

    async fn next(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
        message: PasswordMessageFamily,
    ) -> PgWireResult<AuthState<UserInfoRef>> {
        match self.inner.next(client, message).await {
            // Proof verified: authorize before the handshake completes.
            Ok(AuthState::Ok {
                final_message,
                identity,
            }) => {
                let Some(user_info) = identity else {
                    // Unknown user, whose mock verifier can never be
                    // satisfied; fail closed with the uniform label.
                    return Ok(Self::record_scram_failure(AuthState::Failed(
                        PgWireError::InvalidPassword(self.login.user.clone().unwrap_or_default()),
                    )));
                };
                if let Err(e) = authorize(&self.user_provider, &self.login, &user_info).await {
                    // Already recorded by `authorize`; don't double-count.
                    return Ok(AuthState::Failed(e));
                }
                Ok(AuthState::Ok {
                    final_message,
                    identity: Some(user_info),
                })
            }
            Ok(state @ AuthState::InProgress(_)) => Ok(state),
            Ok(AuthState::Failed(e)) => Ok(Self::record_scram_failure(AuthState::Failed(e))),
            Err(e) => Err(Self::record_scram_failure(e)),
        }
    }
}

/// For connections without a configured user provider: reports success
/// immediately so the driver completes the startup protocol without
/// authentication.
struct NoAuthMechanism;

#[async_trait]
impl AuthMechanism<UserInfoRef> for NoAuthMechanism {
    async fn begin(
        &mut self,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> PgWireResult<AuthState<UserInfoRef>> {
        Ok(AuthState::Ok {
            final_message: None,
            identity: None,
        })
    }

    async fn next(
        &mut self,
        _client: &(dyn ClientInfo + Send + Sync),
        _message: PasswordMessageFamily,
    ) -> PgWireResult<AuthState<UserInfoRef>> {
        // `begin` completes immediately, so the driver never calls this.
        Err(PgWireError::NoAuthenticationInProgress)
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
}

#[async_trait]
impl StartupHandler for PostgresServerHandlerInner {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
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

                auth::protocol_negotiation(client, startup).await?;
                auth::save_startup_parameters_to_metadata(client, startup);

                // check if db is valid
                match resolve_db_info(client, self.query_handler.clone()).await? {
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

                let mechanism = match &self.user_provider {
                    None => None,
                    Some(user_provider) => {
                        let login = LoginInfo::from_client_info(client);
                        if login.user.as_deref() == Some(BEARER_TOKEN_USER) {
                            Some(Self::cleartext_mechanism(user_provider.clone(), login))
                        } else {
                            match postgres_auth_info(user_provider, &login).await {
                                Err(_) => {
                                    return send_password_authentication_failed(client).await;
                                }
                                Ok(PgAuthInfo::Cleartext) => {
                                    Some(Self::cleartext_mechanism(user_provider.clone(), login))
                                }
                                Ok(PgAuthInfo::ScramSha256 {
                                    verifier,
                                    user_info,
                                }) => Some(Self::scram_mechanism(
                                    user_provider.clone(),
                                    login,
                                    user_info,
                                    to_scram_secret(&verifier),
                                )),
                            }
                        }
                    }
                };

                match mechanism {
                    // No user provider configured: skip authentication with a
                    // mechanism that completes immediately.
                    None => {
                        self.session.set_user_info(userinfo_by_name(
                            client.metadata().get(super::METADATA_USER).cloned(),
                        ));
                        set_client_info(client, &self.session);
                        self.auth_driver
                            .begin_authentication(client, Box::new(NoAuthMechanism))
                            .await?;
                    }
                    Some(mechanism) => {
                        if self
                            .auth_driver
                            .begin_authentication(client, mechanism)
                            .await
                            .is_err()
                        {
                            return send_password_authentication_failed(client).await;
                        }
                    }
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                match self.auth_driver.advance_authentication(client, pwd).await {
                    // The handshake completed and the mechanism reported the
                    // verified identity; bind it to the session.
                    Ok(AuthOutcome::Completed(identity)) => {
                        if let Some(user_info) = identity {
                            self.session.set_user_info(user_info);
                        }
                        set_client_info(client, &self.session);
                    }
                    Ok(AuthOutcome::InProgress) => {}
                    Err(_) => return send_password_authentication_failed(client).await,
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl PostgresServerHandlerInner {
    fn cleartext_mechanism(
        user_provider: UserProviderRef,
        login: LoginInfo,
    ) -> Box<dyn AuthMechanism<UserInfoRef>> {
        CleartextPasswordAuth::new(Arc::new(GreptimePasswordSource {
            user_provider,
            login,
        }))
        .create()
    }

    fn scram_mechanism(
        user_provider: UserProviderRef,
        login: LoginInfo,
        user_info: Option<UserInfoRef>,
        secret: ScramSecret,
    ) -> Box<dyn AuthMechanism<UserInfoRef>> {
        let scram = ScramAuth::new(Arc::new(FixedScramSecretSource::new(secret, user_info)));
        Box::new(PgScramMechanism {
            inner: scram.create(),
            user_provider,
            login,
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
    client: &mut C,
    query_handler: ServerSqlQueryHandlerRef,
) -> PgWireResult<DbResolution>
where
    C: ClientInfo + Unpin + Send,
{
    let db_ref = client.metadata().get(super::METADATA_DATABASE);
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
