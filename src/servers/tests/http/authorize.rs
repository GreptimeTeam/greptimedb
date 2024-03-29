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
use axum::http;
use http_body::Body;
use hyper::{Request, StatusCode};
use servers::http::authorize::inner_auth;
use session::context::QueryContextRef;

#[tokio::test]
async fn test_http_auth() {
    // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
    let req = mock_http_request(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();
    let req = inner_auth(None, req).await.unwrap();
    let ctx: &QueryContextRef = req.extensions().get().unwrap();
    let user_info = ctx.current_user().unwrap();
    let default = auth::userinfo_by_name(None);
    assert_eq!(default.username(), user_info.username());

    // In mock user provider, right username:password == "greptime:greptime"
    let mock_user_provider = Some(Arc::new(MockUserProvider::default()) as Arc<dyn UserProvider>);

    // base64encode("greptime:greptime") == "Z3JlcHRpbWU6Z3JlcHRpbWU="
    let req = mock_http_request(Some("Basic Z3JlcHRpbWU6Z3JlcHRpbWU="), None).unwrap();
    let req = inner_auth(mock_user_provider.clone(), req).await.unwrap();
    let ctx: &QueryContextRef = req.extensions().get().unwrap();
    let user_info = ctx.current_user().unwrap();
    let default = auth::userinfo_by_name(None);
    assert_eq!(default.username(), user_info.username());

    let req = mock_http_request(None, None).unwrap();
    let auth_res = inner_auth(mock_user_provider.clone(), req).await;
    assert!(auth_res.is_err());
    let mut resp = auth_res.unwrap_err();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        b"{\"code\":7003,\"error\":\"Not found http or grpc authorization header\",\"execution_time_ms\":0}",
        resp.data().await.unwrap().unwrap().as_ref()
    );

    // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
    let wrong_req = mock_http_request(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();
    let auth_res = inner_auth(mock_user_provider, wrong_req).await;
    assert!(auth_res.is_err());
    let mut resp = auth_res.unwrap_err();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        b"{\"code\":7000,\"error\":\"User not found, username: username\",\"execution_time_ms\":0}",
        resp.data().await.unwrap().unwrap().as_ref(),
    );
}

#[tokio::test]
async fn test_schema_validating() {
    // In mock user provider, right username:password == "greptime:greptime"
    let mock_user_provider = Some(Arc::new(MockUserProvider::default()) as Arc<dyn UserProvider>);

    // base64encode("greptime:greptime") == "Z3JlcHRpbWU6Z3JlcHRpbWU="
    // http://localhost/{http_api_version}/sql?db=greptime
    let version = servers::http::HTTP_API_VERSION;
    let req = mock_http_request(
        Some("Basic Z3JlcHRpbWU6Z3JlcHRpbWU="),
        Some(format!("http://localhost/{version}/sql?db=public").as_str()),
    )
    .unwrap();
    let req = inner_auth(mock_user_provider.clone(), req).await.unwrap();
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
    let result = inner_auth(mock_user_provider, req).await;
    assert!(result.is_err());
    let mut resp = result.unwrap_err();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        b"{\"code\":7005,\"error\":\"Access denied for user 'greptime' to database 'greptime-wrong'\",\"execution_time_ms\":0}",
        resp.data().await.unwrap().unwrap().as_ref()
    );
}

#[tokio::test]
async fn test_whitelist_no_auth() {
    // In mock user provider, right username:password == "greptime:greptime"
    let mock_user_provider = Some(Arc::new(MockUserProvider::default()) as Arc<dyn UserProvider>);

    // base64encode("greptime:greptime") == "Z3JlcHRpbWU6Z3JlcHRpbWU="
    // try auth path first
    let req = mock_http_request(None, None).unwrap();
    let auth_res = inner_auth(mock_user_provider.clone(), req).await;
    assert!(auth_res.is_err());
    let mut resp = auth_res.unwrap_err();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        b"{\"code\":7003,\"error\":\"Not found http or grpc authorization header\",\"execution_time_ms\":0}",
        resp.data().await.unwrap().unwrap().as_ref()
    );

    // try whitelist path
    let req = mock_http_request(None, Some("http://localhost/health")).unwrap();
    let req = inner_auth(mock_user_provider, req).await;
    assert!(req.is_ok());
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
