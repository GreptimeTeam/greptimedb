use axum::{
    http,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use common_telemetry::error;

use crate::context::{AuthMethod, Channel, CtxBuilder};

pub async fn build_ctx<B>(mut req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
    let auth_option = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .map(|header| {
            header
                .to_str()
                .map(|header_str| match header_str.split_once(' ') {
                    Some((name, content)) if name == "Bearer" || name == "TOKEN" => {
                        AuthMethod::Token(String::from(content))
                    }
                    _ => AuthMethod::None,
                })
                .unwrap_or(AuthMethod::None)
        })
        .or(Some(AuthMethod::None));

    match CtxBuilder::new()
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
