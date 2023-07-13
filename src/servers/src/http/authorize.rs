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
use common_error::ext::ErrorExt;
use common_telemetry::warn;
use futures::future::BoxFuture;
use http_body::Body;
use metrics::increment_counter;
use secrecy::SecretString;
use session::context::UserInfo;
use snafu::{ensure, IntoError, OptionExt, ResultExt};
use tower_http::auth::AsyncAuthorizeRequest;

use super::PUBLIC_APIS;
use crate::auth::Error::IllegalParam;
use crate::auth::{Identity, IllegalParamSnafu, UserProviderRef};
use crate::error::{
    self, AuthSnafu, InvalidAuthorizationHeaderSnafu, InvisibleASCIISnafu, NotFoundInfluxAuthSnafu,
    Result, UnsupportedAuthSchemeSnafu,
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
                let _ = request.extensions_mut().insert(UserInfo::default());
                return Ok(request);
            };

            let (username, password) = match extract_username_and_password(&request) {
                Ok((username, password)) => (username, password),
                Err(e) => {
                    warn!("extract username and password failed: {}", e);
                    increment_counter!(
                        crate::metrics::METRIC_AUTH_FAILURE,
                        &[(
                            crate::metrics::METRIC_CODE_LABEL,
                            format!("{}", e.status_code())
                        )]
                    );
                    return Err(unauthorized_resp());
                }
            };

            let (catalog, schema) = match extract_catalog_and_schema(&request) {
                Ok((catalog, schema)) => (catalog, schema),
                Err(e) => {
                    warn!("extract catalog and schema failed: {}", e);
                    increment_counter!(
                        crate::metrics::METRIC_AUTH_FAILURE,
                        &[(
                            crate::metrics::METRIC_CODE_LABEL,
                            format!("{}", e.status_code())
                        )]
                    );
                    return Err(unauthorized_resp());
                }
            };

            match user_provider
                .auth(
                    Identity::UserId(username.as_str(), None),
                    crate::auth::Password::PlainText(password),
                    catalog,
                    schema,
                )
                .await
            {
                Ok(userinfo) => {
                    let _ = request.extensions_mut().insert(userinfo);
                    Ok(request)
                }
                Err(e) => {
                    warn!("authenticate failed: {}", e);
                    increment_counter!(
                        crate::metrics::METRIC_AUTH_FAILURE,
                        &[(
                            crate::metrics::METRIC_CODE_LABEL,
                            format!("{}", e.status_code())
                        )]
                    );
                    Err(unauthorized_resp())
                }
            }
        })
    }
}

fn extract_catalog_and_schema<B: Send + Sync + 'static>(
    request: &Request<B>,
) -> crate::auth::Result<(&str, &str)> {
    // try get database name
    let query = request.uri().query().unwrap_or_default();
    let input_database = extract_db_from_query(query).context(IllegalParamSnafu {
        msg: "db not provided or corrupted",
    })?;

    Ok(crate::parse_catalog_and_schema_from_client_database_name(
        input_database,
    ))
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

        Ok(Some((username.to_string(), password.to_string().into())))
    } else {
        // try v1
        let Some(query_str) = request.uri().query() else { return Ok(None) };

        match extract_influxdb_user_from_query(query_str) {
            (None, None) => Ok(None),
            (Some(username), Some(password)) => {
                Ok(Some((username.to_string(), password.to_string().into())))
            }
            _ => Err(AuthSnafu.into_error(IllegalParam {
                msg: "influxdb auth: username and password must be provided together".to_string(),
            })),
        }
    }
}

fn extract_username_and_password<B: Send + Sync + 'static>(
    request: &Request<B>,
) -> Result<(Username, Password)> {
    Ok(if request.uri().path().contains("influxdb") {
        // compatible with influxdb auth
        get_influxdb_credentials(request)?.context(NotFoundInfluxAuthSnafu)?
    } else {
        // normal http auth
        let scheme = auth_header(request)?;
        match scheme {
            AuthScheme::Basic(username, password) => (username, password),
        }
    })
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
    Basic(Username, Password),
}

type Username = String;
type Password = SecretString;

impl TryFrom<&str> for AuthScheme {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self> {
        let (scheme, encoded_credentials) = value
            .split_once(' ')
            .context(InvalidAuthorizationHeaderSnafu)?;
        ensure!(
            !encoded_credentials.contains(' '),
            InvalidAuthorizationHeaderSnafu
        );

        match scheme.to_lowercase().as_str() {
            "basic" => decode_basic(encoded_credentials)
                .map(|(username, password)| AuthScheme::Basic(username, password)),
            other => UnsupportedAuthSchemeSnafu { name: other }.fail(),
        }
    }
}

type Credential<'a> = &'a str;

fn auth_header<B>(req: &Request<B>) -> Result<AuthScheme> {
    let auth_header = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .context(error::NotFoundAuthHeaderSnafu)?
        .to_str()
        .context(InvisibleASCIISnafu)?;

    auth_header.try_into()
}

fn decode_basic(credential: Credential) -> Result<(Username, Password)> {
    let decoded = base64::decode(credential).context(error::InvalidBase64ValueSnafu)?;
    let as_utf8 = String::from_utf8(decoded).context(error::InvalidUtf8ValueSnafu)?;

    if let Some((user_id, password)) = as_utf8.split_once(':') {
        return Ok((user_id.to_string(), password.to_string().into()));
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

fn extract_db_from_query(query: &str) -> Option<&str> {
    for pair in query.split('&') {
        if let Some(db) = pair.strip_prefix("db=") {
            return if db.is_empty() { None } else { Some(db) };
        }
    }
    None
}

fn extract_influxdb_user_from_query(query: &str) -> (Option<&str>, Option<&str>) {
    let mut username = None;
    let mut password = None;

    for pair in query.split('&') {
        if pair.starts_with("u=") && pair.len() > 2 {
            username = Some(&pair[2..]);
        } else if pair.starts_with("p=") && pair.len() > 2 {
            password = Some(&pair[2..]);
        }
    }
    (username, password)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use secrecy::ExposeSecret;

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
        assert_eq!("password", pwd.expose_secret());

        let wrong_credential = "dXNlcm5hbWU6cG Fzc3dvcmQ=";
        let result = decode_basic(wrong_credential);
        assert_matches!(result.err(), Some(error::Error::InvalidBase64Value { .. }));
    }

    #[test]
    fn test_try_into_auth_scheme() {
        let auth_scheme_str = "basic";
        let re: Result<AuthScheme> = auth_scheme_str.try_into();
        assert!(re.is_err());

        let auth_scheme_str = "basic dGVzdDp0ZXN0";
        let scheme: AuthScheme = auth_scheme_str.try_into().unwrap();
        assert_matches!(scheme, AuthScheme::Basic(username, pwd) if username == "test" && pwd.expose_secret() == "test");

        let unsupported = "digest";
        let auth_scheme: Result<AuthScheme> = unsupported.try_into();
        assert!(auth_scheme.is_err());
    }

    #[test]
    fn test_auth_header() {
        // base64encode("username:password") == "dXNlcm5hbWU6cGFzc3dvcmQ="
        let req = mock_http_request(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();

        let auth_scheme = auth_header(&req).unwrap();
        assert_matches!(auth_scheme, AuthScheme::Basic(username, pwd) if username == "username" && pwd.expose_secret() == "password");

        let wrong_req = mock_http_request(Some("Basic dXNlcm5hbWU6 cGFzc3dvcmQ="), None).unwrap();
        let res = auth_header(&wrong_req);
        assert_matches!(
            res.err(),
            Some(error::Error::InvalidAuthorizationHeader { .. })
        );

        let wrong_req = mock_http_request(Some("Digest dXNlcm5hbWU6cGFzc3dvcmQ="), None).unwrap();
        let res = auth_header(&wrong_req);
        assert_matches!(res.err(), Some(error::Error::UnsupportedAuthScheme { .. }));
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

    #[test]
    fn test_extract_db() {
        assert_matches!(extract_db_from_query(""), None);
        assert_matches!(extract_db_from_query("&"), None);
        assert_matches!(extract_db_from_query("db="), None);
        assert_matches!(extract_db_from_query("db=foo"), Some("foo"));
        assert_matches!(extract_db_from_query("name=bar"), None);
        assert_matches!(extract_db_from_query("db=&name=bar"), None);
        assert_matches!(extract_db_from_query("db=foo&name=bar"), Some("foo"));
        assert_matches!(extract_db_from_query("name=bar&db="), None);
        assert_matches!(extract_db_from_query("name=bar&db=foo"), Some("foo"));
        assert_matches!(extract_db_from_query("name=bar&db=&name=bar"), None);
        assert_matches!(
            extract_db_from_query("name=bar&db=foo&name=bar"),
            Some("foo")
        );
    }

    #[test]
    fn test_extract_user() {
        assert_matches!(extract_influxdb_user_from_query(""), (None, None));
        assert_matches!(extract_influxdb_user_from_query("u="), (None, None));
        assert_matches!(
            extract_influxdb_user_from_query("u=123"),
            (Some("123"), None)
        );
        assert_matches!(
            extract_influxdb_user_from_query("u=123&p="),
            (Some("123"), None)
        );
        assert_matches!(
            extract_influxdb_user_from_query("u=123&p=4"),
            (Some("123"), Some("4"))
        );
        assert_matches!(extract_influxdb_user_from_query("p="), (None, None));
        assert_matches!(extract_influxdb_user_from_query("p=4"), (None, Some("4")));
        assert_matches!(
            extract_influxdb_user_from_query("p=4&u="),
            (None, Some("4"))
        );
        assert_matches!(
            extract_influxdb_user_from_query("p=4&u=123"),
            (Some("123"), Some("4"))
        );
    }
}
