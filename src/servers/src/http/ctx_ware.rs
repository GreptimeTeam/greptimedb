use axum::{
    http,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use common_telemetry::info;

use crate::context::{AuthMethod, Channel, CtxBuilder};

pub async fn build_ctx<B>(mut req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
    let auth_option = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .map(|header| {
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
            AuthMethod::None
        });

    if let Ok(ctx) = CtxBuilder::new()
        .client_addr(
            req.headers()
                .get(http::header::HOST)
                .and_then(|h| h.to_str().ok())
                .map(|h| h.to_string()),
        )
        .set_channel(Some(Channel::HTTP))
        .set_auth_method(auth_option)
        .build()
    {
        req.extensions_mut().insert(ctx);
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
