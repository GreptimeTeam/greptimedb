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
use bytes::{Buf, BufMut, Bytes, BytesMut};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use futures::{Sink, SinkExt};
use pgwire::api::auth::StartupHandler;
use pgwire::api::{auth, ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::{Authentication, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion};
use snafu::IntoError;

use crate::error::{AuthSnafu, Result};
use crate::metrics::METRIC_AUTH_FAILURE;
use crate::postgres::types::PgErrorCode;
use crate::postgres::utils::convert_err;
use crate::postgres::PostgresServerHandlerInner;
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

fn do_encode_pg_secret_key_bytes(secret_key: i32, server_addr: &str, catalog: &str) -> Vec<u8> {
    let mut bytes = BytesMut::with_capacity(256);

    bytes.put_i32(secret_key);

    bytes.put_u8(server_addr.len() as u8);
    bytes.put_u8(catalog.len() as u8);

    bytes.put_slice(server_addr.as_bytes());
    bytes.put_slice(catalog.as_bytes());

    bytes.freeze().to_vec()
}

fn do_decode_pg_secret_key_bytes(mut buf: Bytes) -> Option<(i32, String, String)> {
    // this byte block should be at least 6-byte len
    if buf.remaining() > 6 {
        // get the i32 key
        let key = buf.get_i32();
        // get server addr len
        let server_addr_len = buf.get_u8() as usize;
        // get catalog len
        let catalog_len = buf.get_u8() as usize;

        if buf.remaining() >= server_addr_len + catalog_len {
            let server_addr = String::from_utf8_lossy(&buf.split_to(server_addr_len)).into();
            let catalog = String::from_utf8_lossy(&buf.split_to(catalog_len)).into();

            Some((key, server_addr, catalog))
        } else {
            None
        }
    } else {
        None
    }
}

impl PostgresServerHandlerInner {
    /// Generate a Postgres specific secret key
    ///
    /// The secret key has to carry enough information to call `kill_process` in
    /// a distributed setup. It has to carry:
    ///
    /// - A random i32 number
    /// - the local frontend server address: u8(byte length) + bytes
    /// - the catalog that client has authenticated: u8(byte length) + bytes
    ///
    /// The final byte content is:
    ///
    /// int32(secret_key) + u8(server_addr len) + u8(catalog len)
    /// + server_addr... + catalog...
    ///
    /// According to Postgres spec, the key should carry less than 256 bytes
    pub fn encode_secret_key_bytes(&self) -> Vec<u8> {
        do_encode_pg_secret_key_bytes(
            self.session.secret_key().unwrap_or(0),
            self.process_manager.server_addr(),
            &self.session.catalog(),
        )
    }

    /// Validate and decode secret key into (catalog, frontend) tuple
    pub fn decode_secret_key(&self, secret_key_bytes: Bytes) -> Option<(i32, String, String)> {
        do_decode_pg_secret_key_bytes(secret_key_bytes)
    }

    fn set_client_info<C>(&self, client: &mut C)
    where
        C: ClientInfo,
    {
        if let Some(current_catalog) = client.metadata().get(super::METADATA_CATALOG) {
            self.session.set_catalog(current_catalog.clone());
        }
        if let Some(current_schema) = client.metadata().get(super::METADATA_SCHEMA) {
            self.session.set_schema(current_schema.clone());
        }

        // pass generated process id and secret key to client, this information will
        // be sent to postgres client for query cancellation.
        if client.protocol_version() == ProtocolVersion::PROTOCOL3_0 {
            // 3.0 protocol is not supported for cancel, we give client all 0
            client.set_pid_and_secret_key(0, SecretKey::I32(0));
        } else {
            let secret_key_bytes = self.encode_secret_key_bytes();
            client.set_pid_and_secret_key(
                self.session.process_id() as i32,
                SecretKey::Bytes(Bytes::copy_from_slice(&secret_key_bytes)),
            );
        }

        // set userinfo outside
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

                // performance postgres protocol negotiation
                auth::protocol_negotiation(client, startup).await?;
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
                    self.set_client_info(client);
                    auth::finish_authentication(client, self.param_provider.as_ref()).await?;
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
                    self.set_client_info(client);
                    auth::finish_authentication(client, self.param_provider.as_ref()).await?;
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
mod test {
    use super::*;

    #[test]
    fn test_secret_key_roundtrip() {
        let tuple = (3244, "10.0.0.23", "greptime");
        let bytes = do_encode_pg_secret_key_bytes(tuple.0, tuple.1, tuple.2);
        let decoded = do_decode_pg_secret_key_bytes(Bytes::copy_from_slice(&bytes))
            .expect("failed to decode secret key");

        assert_eq!(tuple.0, decoded.0);
        assert_eq!(tuple.1, decoded.1);
        assert_eq!(tuple.2, decoded.2);
    }
}
