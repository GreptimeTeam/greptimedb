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

struct PgPwdVerifier;

impl PgPwdVerifier {
    async fn verify_pwd(&self, _pwd: &str, _meta: HashMap<String, String>) -> PgWireResult<bool> {
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
}

impl PgAuthStartupHandler {
    pub fn new(with_pwd: bool) -> Self {
        PgAuthStartupHandler {
            verifier: PgPwdVerifier,
            param_provider: GreptimeDBStartupParameters::new(),
            with_pwd,
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
