use std::collections::HashMap;

use tonic::codegen::http;

use super::HttpHandler;
use crate::error::Result;

const HTTP_OK: &str = "OK\n";

pub struct HealthHandler;

#[async_trait::async_trait]
impl HttpHandler for HealthHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(HTTP_OK.to_owned())
            .unwrap())
    }
}
