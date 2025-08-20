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

use std::pin::Pin;
use std::result::Result as StdResult;
use std::task::{Context, Poll};

use auth::UserProviderRef;
use session::context::{Channel, QueryContext};
use tonic::body::Body;
use tonic::server::NamedService;
use tower::{Layer, Service};

use crate::http::authorize::{extract_catalog_and_schema, extract_username_and_password};

#[derive(Clone)]
pub struct AuthMiddlewareLayer {
    user_provider: Option<UserProviderRef>,
}

impl<S> Layer<S> for AuthMiddlewareLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthMiddleware {
            inner: service,
            user_provider: self.user_provider.clone(),
        }
    }
}

/// This middleware is responsible for authenticating the user and setting the user
/// info in the request extension.
///
/// Detail: Authorization information is passed in through the Authorization request
/// header.
#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    user_provider: Option<UserProviderRef>,
}

impl<S> NamedService for AuthMiddleware<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<S> Service<http::Request<Body>> for AuthMiddleware<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<StdResult<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let user_provider = self.user_provider.clone();

        Box::pin(async move {
            if let Err(status) = do_auth(&mut req, user_provider).await {
                return Ok(status.into_http());
            }
            inner.call(req).await
        })
    }
}

async fn do_auth<T>(
    req: &mut http::Request<T>,
    user_provider: Option<UserProviderRef>,
) -> Result<(), tonic::Status> {
    let (catalog, schema) = extract_catalog_and_schema(req);

    let query_ctx = QueryContext::with_channel(&catalog, &schema, Channel::Grpc);

    let Some(user_provider) = user_provider else {
        query_ctx.set_current_user(auth::userinfo_by_name(None));
        let _ = req.extensions_mut().insert(query_ctx);
        return Ok(());
    };

    let (username, password) = extract_username_and_password(req)
        .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

    let id = auth::Identity::UserId(&username, None);
    let pwd = auth::Password::PlainText(password);

    let user_info = user_provider
        .auth(id, pwd, &catalog, &schema)
        .await
        .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

    query_ctx.set_current_user(user_info);
    let _ = req.extensions_mut().insert(query_ctx);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use auth::tests::MockUserProvider;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use headers::Header;
    use hyper::Request;
    use session::context::QueryContext;

    use crate::grpc::authorize::do_auth;
    use crate::http::header::GreptimeDbName;

    #[tokio::test]
    async fn test_do_auth_with_user_provider() {
        let user_provider = Arc::new(MockUserProvider::default());

        // auth success
        let authorization_val = format!("Basic {}", STANDARD.encode("greptime:greptime"));
        let mut req = Request::new(());
        req.headers_mut()
            .insert("authorization", authorization_val.parse().unwrap());

        let auth_result = do_auth(&mut req, Some(user_provider.clone())).await;

        assert!(auth_result.is_ok());
        check_req(&req, "greptime", "public", "greptime");

        // auth failed, err: user not exist.
        let authorization_val = format!("Basic {}", STANDARD.encode("greptime2:greptime2"));
        let mut req = Request::new(());
        req.headers_mut()
            .insert("authorization", authorization_val.parse().unwrap());

        let auth_result = do_auth(&mut req, Some(user_provider)).await;
        assert!(auth_result.is_err());
    }

    #[tokio::test]
    async fn test_do_auth_without_user_provider() {
        let mut req = Request::new(());
        req.headers_mut()
            .insert("authentication", "pwd".parse().unwrap());
        let auth_result = do_auth(&mut req, None).await;
        assert!(auth_result.is_ok());
        check_req(&req, "greptime", "public", "greptime");

        let mut req = Request::new(());
        let auth_result = do_auth(&mut req, None).await;
        assert!(auth_result.is_ok());
        check_req(&req, "greptime", "public", "greptime");

        let mut req = Request::new(());
        req.headers_mut()
            .insert(GreptimeDbName::name(), "catalog-schema".parse().unwrap());
        let auth_result = do_auth(&mut req, None).await;
        assert!(auth_result.is_ok());
        check_req(&req, "catalog", "schema", "greptime");
    }

    fn check_req<T>(
        req: &Request<T>,
        expected_catalog: &str,
        expected_schema: &str,
        expected_user_name: &str,
    ) {
        let ctx = req.extensions().get::<QueryContext>().unwrap();
        assert_eq!(expected_catalog, ctx.current_catalog());
        assert_eq!(expected_schema, ctx.current_schema());

        let user_info = ctx.current_user();
        assert_eq!(expected_user_name, user_info.username());
    }
}
