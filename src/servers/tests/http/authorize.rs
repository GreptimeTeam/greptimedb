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

use std::sync::Arc;

use auth::tests::MockUserProvider;
use auth::UserProvider;
use axum::body::BoxBody;
use axum::http;
use hyper::Request;
use servers::http::authorize::HttpAuth;
use session::context::QueryContextRef;
use tower_http::auth::AsyncAuthorizeRequest;

#[tokio::test]
async fn test_http_auth() {
    let mut http_auth: HttpAuth<BoxBody> = HttpAuth::new(None);

    // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
    let req = mock_http_request(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();
    let req = http_auth.authorize(req).await.unwrap();
    let ctx: &QueryContextRef = req.extensions().get().unwrap();
    let user_info = ctx.current_user().unwrap();
    let default = auth::userinfo_by_name(None);
    assert_eq!(default.username(), user_info.username());

    // In mock user provider, right username:password == "greptime:greptime"
    let mock_user_provider = Some(Arc::new(MockUserProvider::default()) as Arc<dyn UserProvider>);
    let mut http_auth: HttpAuth<BoxBody> = HttpAuth::new(mock_user_provider);

    // base64encode("greptime:greptime") == "Z3JlcHRpbWU6Z3JlcHRpbWU="
    let req = mock_http_request(Some("Basic Z3JlcHRpbWU6Z3JlcHRpbWU="), None).unwrap();
    let req = http_auth.authorize(req).await.unwrap();
    let ctx: &QueryContextRef = req.extensions().get().unwrap();
    let user_info = ctx.current_user().unwrap();
    let default = auth::userinfo_by_name(None);
    assert_eq!(default.username(), user_info.username());

    let req = mock_http_request(None, None).unwrap();
    let auth_res = http_auth.authorize(req).await;
    assert!(auth_res.is_err());

    // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
    let wrong_req = mock_http_request(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();
    let auth_res = http_auth.authorize(wrong_req).await;
    assert!(auth_res.is_err());
}

#[tokio::test]
async fn test_schema_validating() {
    // In mock user provider, right username:password == "greptime:greptime"
    let provider = MockUserProvider::default();
    let mock_user_provider = Some(Arc::new(provider) as Arc<dyn UserProvider>);
    let mut http_auth: HttpAuth<BoxBody> = HttpAuth::new(mock_user_provider);

    // base64encode("greptime:greptime") == "Z3JlcHRpbWU6Z3JlcHRpbWU="
    // http://localhost/{http_api_version}/sql?db=greptime
    let version = servers::http::HTTP_API_VERSION;
    let req = mock_http_request(
        Some("Basic Z3JlcHRpbWU6Z3JlcHRpbWU="),
        Some(format!("http://localhost/{version}/sql?db=public").as_str()),
    )
    .unwrap();
    let req = http_auth.authorize(req).await.unwrap();
    let ctx: &QueryContextRef = req.extensions().get().unwrap();
    let user_info = ctx.current_user().unwrap();
    let default = auth::userinfo_by_name(None);
    assert_eq!(default.username(), user_info.username());

    // wrong database
    let req = mock_http_request(
        Some("Basic Z3JlcHRpbWU6Z3JlcHRpbWU="),
        Some(format!("http://localhost/{version}/sql?db=wrong").as_str()),
    )
    .unwrap();
    let result = http_auth.authorize(req).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_whitelist_no_auth() {
    // In mock user provider, right username:password == "greptime:greptime"
    let mock_user_provider = Some(Arc::new(MockUserProvider::default()) as Arc<dyn UserProvider>);
    let mut http_auth: HttpAuth<BoxBody> = HttpAuth::new(mock_user_provider);

    // base64encode("greptime:greptime") == "Z3JlcHRpbWU6Z3JlcHRpbWU="
    // try auth path first
    let req = mock_http_request(None, None).unwrap();
    let req = http_auth.authorize(req).await;
    assert!(req.is_err());

    // try whitelist path
    let req = mock_http_request(None, Some("http://localhost/health")).unwrap();
    let req = http_auth.authorize(req).await;
    let _ = req.unwrap();
}

// copy from http::authorize
fn mock_http_request(
    auth_header: Option<&str>,
    uri: Option<&str>,
) -> servers::error::Result<Request<()>> {
    let http_api_version = servers::http::HTTP_API_VERSION;
    let mut req = Request::builder()
        .uri(uri.unwrap_or(format!("http://localhost/{http_api_version}/sql?db=public").as_str()));
    if let Some(auth_header) = auth_header {
        req = req.header(http::header::AUTHORIZATION, auth_header);
    }

    Ok(req.body(()).unwrap())
}
