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
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
use pgwire::api::auth::mechanism::AuthMechanismFactory;
use pgwire::api::auth::sasl::scram::{ScramAuth, ScramSecret};
use pgwire::api::auth::{AuthOutcome, AuthSource, LoginInfo as WireLoginInfo};
use pgwire::api::{ClientInfo, PidSecretKeyGenerator, auth};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::SecretKey;
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

fn record_auth_failure(status_code: StatusCode) {
    METRIC_AUTH_FAILURE
        .with_label_values(&[status_code.as_ref()])
        .inc();
}

/// The [`AuthSource`] shared by pgwire's cleartext and SCRAM mechanisms.
/// Stateless by design: it only holds the user provider, everything about
/// the current login arrives with each call, and the verified
/// [`UserInfoRef`] travels back to the startup handler as the source
/// identity, reported by [`AuthOutcome::Authenticated`].
///
/// Read-only: it resolves credentials and checks authorization with the
/// provider, but holds no policy — authorization runs in the startup
/// handler after verification, before the startup completes.
pub(crate) struct PgUserProviderSource {
    user_provider: UserProviderRef,
}

impl Debug for PgUserProviderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgUserProviderSource")
            .finish_non_exhaustive()
    }
}

impl PgUserProviderSource {
    pub(crate) fn new(user_provider: UserProviderRef) -> Self {
        Self { user_provider }
    }

    /// (user, catalog, schema) of this login. Catalog/schema are parsed from
    /// the `database` startup parameter, which was validated at connection
    /// start.
    fn resolve(login: &WireLoginInfo<'_>) -> Option<(String, String, String)> {
        let user = login.user()?.to_string();
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(login.database()?);
        Some((user, catalog, schema))
    }

    /// Which mechanism to use for this login. Called once at connection
    /// start; the SCRAM machinery resolves the verifier itself at
    /// client-first time.
    async fn postgres_auth_info(&self, login: &WireLoginInfo<'_>) -> Result<PgAuthInfo> {
        let Some((user, catalog, _)) = Self::resolve(login) else {
            return Ok(PgAuthInfo::Cleartext);
        };

        match self
            .user_provider
            .postgres_auth_info(Identity::UserId(&user, None), &catalog)
            .await
        {
            Err(e) => {
                record_auth_failure(e.status_code());
                Err(AuthSnafu.into_error(e))
            }
            Ok(auth_info) => Ok(auth_info),
        }
    }

    /// Checks with the provider that the user may access this
    /// catalog/schema. Called by the startup handler after verification,
    /// while nothing terminal has been sent yet.
    async fn authorize(
        &self,
        login: &WireLoginInfo<'_>,
        user_info: &UserInfoRef,
    ) -> PgWireResult<()> {
        let Some((_, catalog, schema)) = Self::resolve(login) else {
            return Err(PgWireError::UserNameRequired);
        };

        match self
            .user_provider
            .authorize(&catalog, &schema, user_info)
            .await
        {
            Err(e) => {
                record_auth_failure(e.status_code());
                Err(PgWireError::ApiError(Box::new(AuthSnafu.into_error(e))))
            }
            Ok(()) => Ok(()),
        }
    }
}

#[async_trait]
impl AuthSource<UserInfoRef> for PgUserProviderSource {
    async fn get_password(
        &self,
        _login: &WireLoginInfo,
    ) -> PgWireResult<pgwire::api::auth::Password> {
        Err(PgWireError::ApiError(
            "no cleartext password available from the user provider".into(),
        ))
    }

    /// Cleartext and bearer-token verification; the provider verifies the
    /// password itself. Authorization is left to the startup handler.
    async fn verify_password(
        &self,
        login: &WireLoginInfo,
        presented_password: &[u8],
    ) -> PgWireResult<Option<UserInfoRef>> {
        let Some((user, catalog, _schema)) = Self::resolve(login) else {
            return Err(PgWireError::UserNameRequired);
        };
        let password = String::from_utf8_lossy(presented_password).into_owned();

        let result = if user == BEARER_TOKEN_USER {
            self.user_provider
                .authenticate_bearer_token(&password, &catalog)
                .await
        } else {
            self.user_provider
                .authenticate(
                    Identity::UserId(&user, None),
                    Password::PlainText(password.into()),
                )
                .await
        };
        match result {
            Err(e) => {
                record_auth_failure(e.status_code());
                Err(PgWireError::ApiError(Box::new(AuthSnafu.into_error(e))))
            }
            Ok(user_info) => Ok(Some(user_info)),
        }
    }

    async fn get_scram_secret(
        &self,
        login: &WireLoginInfo,
    ) -> PgWireResult<(ScramSecret, Option<UserInfoRef>)> {
        match self.postgres_auth_info(login).await {
            // For unknown users the provider hands out a mock verifier with
            // no identity, so their handshake fails at proof verification
            // exactly like a wrong password.
            Ok(PgAuthInfo::ScramSha256 {
                verifier,
                user_info,
            }) => Ok((to_scram_secret(&verifier), user_info)),
            // SCRAM was selected but the provider no longer backs it: fail
            // closed with a mock verifier.
            _ => {
                let user = login.user().unwrap_or_default();
                Ok((
                    to_scram_secret(&PgScramSha256Verifier::mock_for_unknown_user(
                        user.as_bytes(),
                    )),
                    None,
                ))
            }
        }
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

enum MechanismSelection {
    /// No user provider configured: skip authentication.
    NoAuth,
    Cleartext(Arc<PgUserProviderSource>),
    Scram(Arc<PgUserProviderSource>),
    /// Resolving the mechanism failed.
    Failed,
}

impl PostgresServerHandlerInner {
    async fn select_mechanism<C>(&self, client: &C) -> MechanismSelection
    where
        C: ClientInfo + Sync,
    {
        let Some(source) = self.auth_source.clone() else {
            return MechanismSelection::NoAuth;
        };
        let login = WireLoginInfo::from_client_info(client);
        if login.user() == Some(BEARER_TOKEN_USER) {
            return MechanismSelection::Cleartext(source);
        }
        match source.postgres_auth_info(&login).await {
            Err(_) => MechanismSelection::Failed,
            Ok(PgAuthInfo::Cleartext) => MechanismSelection::Cleartext(source),
            Ok(PgAuthInfo::ScramSha256 { .. }) => MechanismSelection::Scram(source),
        }
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

                match self.select_mechanism(client).await {
                    MechanismSelection::NoAuth => {
                        self.session.set_user_info(userinfo_by_name(
                            client.metadata().get(super::METADATA_USER).cloned(),
                        ));
                        set_client_info(client, &self.session);
                        self.auth_driver
                            .complete_authentication(client, None)
                            .await?;
                    }
                    MechanismSelection::Cleartext(source) => {
                        if self
                            .auth_driver
                            .begin_authentication(
                                client,
                                CleartextPasswordAuth::new(source).create(),
                            )
                            .await
                            .is_err()
                        {
                            return send_password_authentication_failed(client).await;
                        }
                    }
                    MechanismSelection::Scram(source) => {
                        self.is_scram_auth.store(true, Ordering::Relaxed);
                        if self
                            .auth_driver
                            .begin_authentication(client, ScramAuth::new(source).create())
                            .await
                            .is_err()
                        {
                            return send_password_authentication_failed(client).await;
                        }
                    }
                    MechanismSelection::Failed => {
                        return send_password_authentication_failed(client).await;
                    }
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                match self.auth_driver.advance_authentication(client, pwd).await {
                    Ok(AuthOutcome::InProgress) => {}
                    Ok(AuthOutcome::Authenticated {
                        identity,
                        final_message,
                    }) => {
                        self.is_scram_auth.store(false, Ordering::Relaxed);
                        let Some(user_info) = identity else {
                            return send_password_authentication_failed(client).await;
                        };

                        // Nothing terminal has been sent yet: authorize
                        // before completing, so a rejection reaches the
                        // client as an authentication failure. Failures are
                        // recorded by `authorize` itself.
                        let authorized = {
                            let login = WireLoginInfo::from_client_info(client);
                            match &self.auth_source {
                                Some(source) => source.authorize(&login, &user_info).await.is_ok(),
                                None => false,
                            }
                        };
                        if !authorized {
                            return send_password_authentication_failed(client).await;
                        }

                        self.session.set_user_info(user_info);
                        set_client_info(client, &self.session);
                        self.auth_driver
                            .complete_authentication(client, final_message)
                            .await?;
                    }
                    Err(_e) => {
                        // SCRAM failures carry a uniform label so the metric
                        // cannot distinguish wrong password, unknown and
                        // unauthorized users.
                        if self.is_scram_auth.swap(false, Ordering::Relaxed) {
                            record_auth_failure(StatusCode::UserPasswordMismatch);
                        }
                        return send_password_authentication_failed(client).await;
                    }
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
