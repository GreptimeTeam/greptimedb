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

use axum::http;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
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
