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

use std::collections::HashMap;

use tonic::codegen::http;

use crate::error::Result;
use crate::service::admin::HttpHandler;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_handle() {
        let health_handler = HealthHandler {};
        let path = "any";
        let params = HashMap::default();
        let res = health_handler.handle(path, &params).await.unwrap();

        assert!(res.status().is_success());
        assert_eq!(HTTP_OK.to_owned(), res.body().clone());
    }
}
