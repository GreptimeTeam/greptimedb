use std::net::SocketAddr;

use axum::{response::Json, routing::get, Router};
use snafu::ResultExt;

use crate::error::{HyperSnafu, Result};

/// Http server
pub struct HttpServer {}

impl HttpServer {
    pub async fn start(&self) -> Result<()> {
        let app = Router::new().route("/", get(handler));

        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        println!("listening on {}", addr);
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .context(HyperSnafu)?;

        Ok(())
    }
}

async fn handler() -> Json<&'static str> {
    Json("test")
}
