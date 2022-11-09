use axum::{
    http,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

use crate::context::CtxBuilder;

pub async fn build_ctx<B>(mut req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
    if let Ok(ctx) = CtxBuilder::new()
        .client_addr(
            req.headers()
                .get(http::header::HOST)
                .and_then(|h| h.to_str().ok())
                .map(|h| h.to_string()),
        )
        .auth_with_token(
            req.headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|auth| auth.to_str().ok())
                .map(|auth| auth.to_string())
                .unwrap_or_default(),
        )
        .build()
    {
        req.extensions_mut().insert(ctx);
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
