use std::collections::HashMap;
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
use std::marker::PhantomData;

use axum::http::{self, Request, StatusCode};
use axum::response::Response;
use common_telemetry::error;
use futures::future::BoxFuture;
use http_body::Body;
use session::context::UserInfo;
use snafu::{OptionExt, ResultExt};
use tower_http::auth::AsyncAuthorizeRequest;

use crate::auth::Error::IllegalParam;
use crate::auth::{Identity, IllegalParamSnafu, InternalStateSnafu, UserProviderRef};
use crate::error::{self, Result};
use crate::http::HTTP_API_PREFIX;

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
            let need_auth = request.uri().path().starts_with(HTTP_API_PREFIX);
            let user_provider = if let Some(user_provider) = user_provider.filter(|_| need_auth) {
                user_provider
            } else {
                request.extensions_mut().insert(UserInfo::default());
                return Ok(request);
            };

            // do authenticate
            match authenticate(&user_provider, &request).await {
                Ok(user_info) => {
                    request.extensions_mut().insert(user_info);
                }
                Err(e) => {
                    error!("authenticate failed: {}", e);
                    return Err(unauthorized_resp());
                }
            }

            match authorize(&user_provider, &request).await {
                Ok(_) => Ok(request),
                Err(e) => {
                    error!("authorize failed: {}", e);
                    Err(unauthorized_resp())
                }
            }
        })
    }
}

async fn authorize<B: Send + Sync + 'static>(
    user_provider: &UserProviderRef,
    request: &Request<B>,
) -> crate::auth::Result<()> {
    // try get database name
    let query = request.uri().query().unwrap_or_default();
    let input_database = match serde_urlencoded::from_str::<HashMap<String, String>>(query) {
        Ok(query_map) => query_map
            .get("db")
            .context(IllegalParamSnafu {
                msg: "fail to get valid database from http query",
            })?
            .to_owned(),
        Err(e) => IllegalParamSnafu {
            msg: format!("fail to parse http query: {e}"),
        }
        .fail()?,
    };

    let (catalog, database) =
        crate::parse_catalog_and_schema_from_client_database_name(&input_database);

    let user_info = request
        .extensions()
        .get::<UserInfo>()
        .context(InternalStateSnafu {
            msg: "no user info provided while authorizing",
        })?;

    user_provider.authorize(catalog, database, user_info).await
}

async fn authenticate<B: Send + Sync + 'static>(
    user_provider: &UserProviderRef,
    request: &Request<B>,
) -> crate::auth::Result<UserInfo> {
    let (scheme, credential) = auth_header(request).map_err(|e| IllegalParam {
        msg: format!("failed to get http authorize header, err: {e:?}"),
    })?;

    match scheme {
        AuthScheme::Basic => {
            let (username, password) = decode_basic(credential).map_err(|e| IllegalParam {
                msg: format!("failed to decode basic authorize, err: {e:?}"),
            })?;

            Ok(user_provider
                .authenticate(
                    Identity::UserId(&username, None),
                    crate::auth::Password::PlainText(&password),
                )
                .await?)
        }
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
    use super::*;

    #[test]
    fn test_decode_basic() {
        // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
        let credential = "dXNlcm5hbWU6cGFzc3dvcmQ=";
        let (username, pwd) = decode_basic(credential).unwrap();
        assert_eq!("username", username);
        assert_eq!("password", pwd);

        let wrong_credential = "dXNlcm5hbWU6cG Fzc3dvcmQ=";
        let result = decode_basic(wrong_credential);
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
    fn test_auth_header() {
        // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
        let req = mock_http_request(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();

        let (auth_scheme, credential) = auth_header(&req).unwrap();
        matches!(auth_scheme, AuthScheme::Basic);
        assert_eq!("dXNlcm5hbWU6cGFzc3dvcmQ=", credential);

        let wrong_req = mock_http_request(Some("Basic dXNlcm5hbWU6 cGFzc3dvcmQ="), None).unwrap();
        let res = auth_header(&wrong_req);
        matches!(
            res.err(),
            Some(error::Error::InvalidAuthorizationHeader { .. })
        );

        let wrong_req = mock_http_request(Some("Digest dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();
        let res = auth_header(&wrong_req);
        matches!(res.err(), Some(error::Error::UnsupportedAuthScheme { .. }));
    }

    fn mock_http_request(auth_header: Option<&str>, uri: Option<&str>) -> Result<Request<()>> {
        let http_api_version = crate::http::HTTP_API_VERSION;
        let mut req = Request::builder()
            .uri(uri.unwrap_or(format!("http://localhost/{http_api_version}/sql").as_str()));
        if let Some(auth_header) = auth_header {
            req = req.header(http::header::AUTHORIZATION, auth_header);
        }

        Ok(req.body(()).unwrap())
    }
}
