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
use std::marker::PhantomData;

use axum::http::{self, Request, StatusCode};
use axum::response::Response;
use common_telemetry::error;
use futures::future::BoxFuture;
use http_body::Body;
use session::context::UserInfo;
use snafu::{ensure, OptionExt, ResultExt};
use tower_http::auth::AsyncAuthorizeRequest;

use super::PUBLIC_APIS;
use crate::auth::Error::IllegalParam;
use crate::auth::{Identity, IllegalParamSnafu, InternalStateSnafu, UserProviderRef};
use crate::error::{
    self, InvalidAuthorizationHeaderSnafu, InvisibleASCIISnafu, NotFoundInfluxAuthSnafu, Result,
    UnsupportedAuthSchemeSnafu,
};
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
            let need_auth = need_auth(&request);

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

fn get_influxdb_credentials<B: Send + Sync + 'static>(
    request: &Request<B>,
) -> Result<Option<(Username, Password)>> {
    // compat with influxdb v2 and v1
    if let Some(header) = request.headers().get(http::header::AUTHORIZATION) {
        // try v2 first
        let (auth_scheme, credential) = header
            .to_str()
            .context(InvisibleASCIISnafu)?
            .split_once(' ')
            .context(InvalidAuthorizationHeaderSnafu)?;
        ensure!(
            auth_scheme.to_lowercase() == "token",
            UnsupportedAuthSchemeSnafu { name: auth_scheme }
        );

        let (username, password) = credential
            .split_once(':')
            .context(InvalidAuthorizationHeaderSnafu)?;

        Ok(Some((username.to_string(), password.to_string())))
    } else {
        // try v1
        let query_str = request.uri().query();
        if query_str.is_none() {
            return Ok(None);
        }
        // TODO(shuiyisong): remove this for performance optimization
        // `authorize` would deserialize query from urlencoded again
        let query = match serde_urlencoded::from_str::<HashMap<String, String>>(query_str.unwrap())
        {
            Ok(query_map) => query_map,
            Err(e) => IllegalParamSnafu {
                msg: format!("fail to parse http query: {e}"),
            }
            .fail()?,
        };

        let username = query.get("u");
        let password = query.get("p");
        if username.is_none() && password.is_none() {
            Ok(None)
        } else if username.is_some() && password.is_some() {
            Ok(Some((username.unwrap().clone(), password.unwrap().clone())))
        } else {
            IllegalParamSnafu {
                msg: "influxdb v1 auth: username and password must be provided together",
            }
            .fail()?
        }
    }
}

async fn authenticate<B: Send + Sync + 'static>(
    user_provider: &UserProviderRef,
    request: &Request<B>,
) -> Result<UserInfo> {
    let (username, password) = if request.uri().path().contains("influxdb") {
        // compatible with influxdb auth
        get_influxdb_credentials(request)?.context(NotFoundInfluxAuthSnafu)?
    } else {
        // normal http auth
        let (scheme, credential) = auth_header(request)?;
        match scheme {
            AuthScheme::Basic => decode_basic(credential)?,
        }
    };

    Ok(user_provider
        .authenticate(
            Identity::UserId(&username, None),
            crate::auth::Password::PlainText(&password),
        )
        .await?)
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
            other => UnsupportedAuthSchemeSnafu { name: other }.fail(),
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
        .context(InvisibleASCIISnafu)?;

    let (auth_scheme, encoded_credentials) = auth_header
        .split_once(' ')
        .context(InvalidAuthorizationHeaderSnafu)?;

    if encoded_credentials.contains(' ') {
        return InvalidAuthorizationHeaderSnafu {}.fail();
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

    InvalidAuthorizationHeaderSnafu {}.fail()
}

fn need_auth<B>(req: &Request<B>) -> bool {
    let path = req.uri().path();

    for api in PUBLIC_APIS {
        if path.starts_with(api) {
            return false;
        }
    }

    path.starts_with(HTTP_API_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_need_auth() {
        let req = Request::builder()
            .uri("http://127.0.0.1/v1/influxdb/ping")
            .body(())
            .unwrap();

        assert!(!need_auth(&req));

        let req = Request::builder()
            .uri("http://127.0.0.1/v1/influxdb/health")
            .body(())
            .unwrap();

        assert!(!need_auth(&req));

        let req = Request::builder()
            .uri("http://127.0.0.1/v1/influxdb/write")
            .body(())
            .unwrap();

        assert!(need_auth(&req));
    }

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
