use axum::{
    http,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use common_telemetry::{error, info};

use crate::context::{AuthMethod, Channel, CtxBuilder};

pub async fn build_ctx<B>(mut req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
    let auth_option =
        req.headers()
            .get(http::header::AUTHORIZATION)
            .map_or(AuthMethod::None, |header| {
                if header.as_bytes().starts_with(b"TOKEN ") {
                    let bytes = &header.as_bytes()[b"TOKEN ".len()..];
                    return match String::from_utf8(bytes.to_vec()) {
                        Ok(token) => AuthMethod::Token(token),
                        Err(_) => {
                            info!(
                                "parse auth token from http header error: {:?}",
                                header.to_str()
                            );
                            AuthMethod::None
                        }
                    };
                }
                // unrecognized case
                AuthMethod::None
            });

    match CtxBuilder::new()
        .client_addr(
            req.headers()
                .get(http::header::HOST)
                .and_then(|h| h.to_str().ok())
                .map(|h| h.to_string()),
        )
        .set_channel(Some(Channel::HTTP))
        .set_auth_method(Some(auth_option))
        .build()
    {
        Ok(ctx) => {
            req.extensions_mut().insert(ctx);
            Ok(next.run(req).await)
        }
        Err(e) => {
            error!(e; "fail to create context");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
