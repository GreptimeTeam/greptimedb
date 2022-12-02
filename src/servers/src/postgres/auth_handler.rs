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
use futures::{Sink, SinkExt};
use pgwire::api::auth::{ServerParameterProvider, StartupHandler};
use pgwire::api::{auth, ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::auth::postgres::{auth_pg, PgAuthPlugin};
use crate::auth::{Identity, UserProviderRef};
use crate::error::Result;

struct PgPwdVerifier {
    user_provider: Option<UserProviderRef>,
}

impl PgPwdVerifier {
    async fn verify_pwd(&self, pwd: &str, meta: HashMap<String, String>) -> Result<bool> {
        let user_name = match meta.get("user") {
            Some(name) => name,
            None => return Ok(false),
        };

        if let Some(user_provider) = &self.user_provider {
            match user_provider
                .get_user_info(&Identity::UserId(user_name.to_string(), None))
                .await?
            {
                Some(user_info) => {
                    let auth_method = match user_info.pg_auth_method(&PgAuthPlugin::PlainText) {
                        Some(auth_method) => auth_method,
                        None => return Ok(false),
                    };

                    return Ok(auth_pg(
                        PgAuthPlugin::PlainText,
                        pwd.as_bytes(),
                        &[],
                        auth_method,
                    ));
                }
                None => return Ok(false),
            }
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
    with_pwd: bool,
    force_tls: bool,
}

impl PgAuthStartupHandler {
    pub fn new(with_pwd: bool, user_provider: Option<UserProviderRef>, force_tls: bool) -> Self {
        PgAuthStartupHandler {
            verifier: PgPwdVerifier { user_provider },
            param_provider: GreptimeDBStartupParameters::new(),
            with_pwd,
            force_tls,
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
                if !client.is_secure() && self.force_tls {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28000".to_owned(),
                        "No encryption".to_owned(),
                    );
                    let error = ErrorResponse::from(error_info);

                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
                }
                auth::save_startup_parameters_to_metadata(client, startup);
                if self.with_pwd {
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
                let meta = client.metadata().clone();
                if let Ok(true) = self.verifier.verify_pwd(pwd.password(), meta).await {
                    auth::finish_authentication(client, &self.param_provider).await
                } else {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28P01".to_owned(),
                        "Password authentication failed".to_owned(),
                    );
                    let error = ErrorResponse::from(error_info);

                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}
