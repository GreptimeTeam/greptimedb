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

use std::marker::PhantomData;

use axum::http::{self, Request, StatusCode};
use axum::response::Response;
use futures::future::BoxFuture;
use http_body::Body;
use snafu::{OptionExt, ResultExt};
use tower_http::auth::AsyncAuthorizeRequest;

use crate::auth::{Identity, UserInfo, UserProviderRef};
use crate::error::{self, Result};

pub struct HttpAuth<RespBody> {
    user_provider: Option<UserProviderRef>,
    _ty: PhantomData<RespBody>,
}

impl<RespBody> HttpAuth<RespBody> {
    pub fn new(user_provider: Option<UserProviderRef>) -> Self {
        Self {
            user_provider,
            _ty: PhantomData,
        }
    }
}

impl<RespBody> Clone for HttpAuth<RespBody> {
    fn clone(&self) -> Self {
        Self {
            user_provider: self.user_provider.clone(),
            _ty: PhantomData,
        }
    }
}

impl<B, RespBody> AsyncAuthorizeRequest<B> for HttpAuth<RespBody>
where
    B: Send + Sync + 'static,
    RespBody: Body + Default,
{
    type RequestBody = B;
    type ResponseBody = RespBody;
    type Future = BoxFuture<'static, std::result::Result<Request<B>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: Request<B>) -> Self::Future {
        let user_provider = self.user_provider.clone();
        Box::pin(async move {
            request.extensions_mut().insert(UserInfo::default());
            let user_provider = if let Some(user_provider) = &user_provider {
                user_provider
            } else {
                return Ok(request);
            };

            let (scheme, credential) = match auth_header(&request) {
                Ok(auth_header) => auth_header,
                Err(_) => return Err(unauthorized_resp()),
            };

            match scheme {
                AuthScheme::Basic => {
                    let (username, pwd) = decode_basic(credential).unwrap();
                    match user_provider
                        .auth(
                            Identity::UserId(&username, None),
                            crate::auth::Password::PlainText(pwd.as_bytes()),
                        )
                        .await
                    {
                        Ok(user_info) => {
                            request.extensions_mut().insert(user_info);
                            Ok(request)
                        }
                        Err(_) => Err(unauthorized_resp()),
                    }
                }
            }
        })
    }
}

fn unauthorized_resp<RespBody>() -> Response<RespBody>
where
    RespBody: Body + Default,
{
    let mut res = Response::new(RespBody::default());
    *res.status_mut() = StatusCode::UNAUTHORIZED;
    res
}

#[derive(Debug)]
pub enum AuthScheme {
    Basic,
}

impl TryFrom<&str> for AuthScheme {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "basic" => Ok(AuthScheme::Basic),
            other => error::UnsupportedAuthSchemeSnafu { name: other }.fail(),
        }
    }
}

type Credential<'a> = &'a str;

fn auth_header<B>(req: &Request<B>) -> Result<(AuthScheme, Credential)> {
    let auth_header = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .context(error::NotFoundAuthHeaderSnafu)?
        .to_str()
        .context(error::InvisibleASCIISnafu)?;

    let (auth_scheme, encoded_credentials) = auth_header
        .split_once(' ')
        .context(error::InvalidAuthorizationHeaderSnafu)?;

    if encoded_credentials.contains(' ') {
        return error::InvalidAuthorizationHeaderSnafu {}.fail();
    }

    Ok((auth_scheme.try_into()?, encoded_credentials))
}

type Username = String;
type Password = String;

fn decode_basic(credential: Credential) -> Result<(Username, Password)> {
    let decoded = base64::decode(credential).context(error::InvalidBase64ValueSnafu)?;
    let as_utf8 = String::from_utf8(decoded).context(error::InvalidUtf8ValueSnafu)?;

    if let Some((user_id, password)) = as_utf8.split_once(':') {
        return Ok((user_id.to_string(), password.to_string()));
    }

    error::InvalidAuthorizationHeaderSnafu {}.fail()
}

#[cfg(test)]
mod tests {
    use axum::http;
    use hyper::Request;

    use super::{auth_header, decode_basic, AuthScheme};
    use crate::error;
    use crate::error::Result;

    #[test]
    fn test_decode_basic() {
        let credential = "dXNlcm5hbWU6cGFzc3dvcmQ=";
        let (username, pwd) = decode_basic(credential).unwrap();
        assert_eq!("username", username);
        assert_eq!("password", pwd);

        let credential = "dXNlcm5hbWU6cG Fzc3dvcmQ=";
        let result = decode_basic(credential);
        matches!(result.err(), Some(error::Error::InvalidBase64Value { .. }));
    }

    #[test]
    fn test_try_into_auth_scheme() {
        let auth_scheme_str = "basic";
        let auth_scheme: AuthScheme = auth_scheme_str.try_into().unwrap();
        matches!(auth_scheme, AuthScheme::Basic);

        let unsupported = "digest";
        let auth_scheme: Result<AuthScheme> = unsupported.try_into();
        assert!(auth_scheme.is_err());
    }

    #[test]
    fn test_split_auth_header() {
        let req = mock_http_request("Basic dXNlcm5hbWU6cGFzc3dvcmQ=").unwrap();

        let (auth_scheme, credential) = auth_header(&req).unwrap();
        matches!(auth_scheme, AuthScheme::Basic);
        assert_eq!("dXNlcm5hbWU6cGFzc3dvcmQ=", credential);

        let req = mock_http_request("Basic dXNlcm5hbWU6 cGFzc3dvcmQ=").unwrap();
        let res = auth_header(&req);
        matches!(
            res.err(),
            Some(error::Error::InvalidAuthorizationHeader { .. })
        );

        let req = mock_http_request("Digest dXNlcm5hbWU6cGFzc3dvcmQ=").unwrap();
        let res = auth_header(&req);
        matches!(res.err(), Some(error::Error::UnsupportedAuthScheme { .. }));
    }

    fn mock_http_request(auth_header: &str) -> Result<Request<()>> {
        Ok(Request::builder()
            .uri("https://www.rust-lang.org/")
            .header(http::header::AUTHORIZATION, auth_header)
            .body(())
            .unwrap())
    }
}
