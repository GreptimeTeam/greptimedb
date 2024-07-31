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

use ::auth::UserProviderRef;
use axum::extract::State;
use axum::http::{self, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use common_base::secrets::SecretString;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_telemetry::warn;
use common_time::timezone::parse_timezone;
use common_time::Timezone;
use headers::Header;
use session::context::QueryContextBuilder;
use snafu::{ensure, OptionExt, ResultExt};

use super::header::{GreptimeDbName, GREPTIME_TIMEZONE_HEADER_NAME};
use super::PUBLIC_APIS;
use crate::error::{
    self, InvalidAuthHeaderInvisibleASCIISnafu, InvalidAuthHeaderSnafu, InvalidParameterSnafu,
    NotFoundInfluxAuthSnafu, Result, UnsupportedAuthSchemeSnafu, UrlDecodeSnafu,
};
use crate::http::error_result::ErrorResponse;
use crate::http::HTTP_API_PREFIX;
use crate::influxdb::{is_influxdb_request, is_influxdb_v2_request};

/// AuthState is a holder state for [`UserProviderRef`]
/// during [`check_http_auth`] function in axum's middleware
#[derive(Clone)]
pub struct AuthState {
    user_provider: Option<UserProviderRef>,
}

impl AuthState {
    pub fn new(user_provider: Option<UserProviderRef>) -> Self {
        Self { user_provider }
    }
}

pub async fn inner_auth<B>(
    user_provider: Option<UserProviderRef>,
    mut req: Request<B>,
) -> std::result::Result<Request<B>, Response> {
    // 1. prepare
    let (catalog, schema) = extract_catalog_and_schema(&req);
    // TODO(ruihang): move this out of auth module
    let timezone = extract_timezone(&req);
    let query_ctx_builder = QueryContextBuilder::default()
        .current_catalog(catalog.clone())
        .current_schema(schema.clone())
        .timezone(timezone);

    let query_ctx = query_ctx_builder.build();
    let need_auth = need_auth(&req);

    // 2. check if auth is needed
    let user_provider = if let Some(user_provider) = user_provider.filter(|_| need_auth) {
        user_provider
    } else {
        query_ctx.set_current_user(auth::userinfo_by_name(None));
        let _ = req.extensions_mut().insert(query_ctx);
        return Ok(req);
    };

    // 3. get username and pwd
    let (username, password) = match extract_username_and_password(&req) {
        Ok((username, password)) => (username, password),
        Err(e) => {
            warn!(e; "extract username and password failed");
            crate::metrics::METRIC_AUTH_FAILURE
                .with_label_values(&[e.status_code().as_ref()])
                .inc();
            return Err(err_response(e));
        }
    };

    // 4. auth
    match user_provider
        .auth(
            auth::Identity::UserId(&username, None),
            auth::Password::PlainText(password),
            &catalog,
            &schema,
        )
        .await
    {
        Ok(userinfo) => {
            query_ctx.set_current_user(userinfo);
            let _ = req.extensions_mut().insert(query_ctx);
            Ok(req)
        }
        Err(e) => {
            warn!(e; "authenticate failed");
            crate::metrics::METRIC_AUTH_FAILURE
                .with_label_values(&[e.status_code().as_ref()])
                .inc();
            Err(err_response(e))
        }
    }
}

pub async fn check_http_auth<B>(
    State(auth_state): State<AuthState>,
    req: Request<B>,
    next: Next<B>,
) -> Response {
    match inner_auth(auth_state.user_provider, req).await {
        Ok(req) => next.run(req).await,
        Err(resp) => resp,
    }
}

fn err_response(err: impl ErrorExt) -> Response {
    (StatusCode::UNAUTHORIZED, ErrorResponse::from_error(err)).into_response()
}

pub fn extract_catalog_and_schema<B>(request: &Request<B>) -> (String, String) {
    // parse database from header
    let dbname = request
        .headers()
        .get(GreptimeDbName::name())
        // eat this invalid ascii error and give user the final IllegalParam error
        .and_then(|header| header.to_str().ok())
        .or_else(|| {
            let query = request.uri().query().unwrap_or_default();
            if is_influxdb_v2_request(request) {
                extract_db_from_query(query).or_else(|| extract_bucket_from_query(query))
            } else {
                extract_db_from_query(query)
            }
        })
        .unwrap_or(DEFAULT_SCHEMA_NAME);

    parse_catalog_and_schema_from_db_string(dbname)
}

fn extract_timezone<B>(request: &Request<B>) -> Timezone {
    // parse timezone from header
    let timezone = request
        .headers()
        .get(&GREPTIME_TIMEZONE_HEADER_NAME)
        // eat this invalid ascii error and give user the final IllegalParam error
        .and_then(|header| header.to_str().ok())
        .unwrap_or("");
    parse_timezone(Some(timezone))
}

fn get_influxdb_credentials<B>(request: &Request<B>) -> Result<Option<(Username, Password)>> {
    // compat with influxdb v2 and v1
    if let Some(header) = request.headers().get(http::header::AUTHORIZATION) {
        // try header
        let (auth_scheme, credential) = header
            .to_str()
            .context(InvalidAuthHeaderInvisibleASCIISnafu)?
            .split_once(' ')
            .context(InvalidAuthHeaderSnafu)?;

        let (username, password) = match auth_scheme.to_lowercase().as_str() {
            "token" => {
                let (u, p) = credential.split_once(':').context(InvalidAuthHeaderSnafu)?;
                (u.to_string(), p.to_string().into())
            }
            "basic" => decode_basic(credential)?,
            _ => UnsupportedAuthSchemeSnafu { name: auth_scheme }.fail()?,
        };

        Ok(Some((username, password)))
    } else {
        // try u and p in query
        let Some(query_str) = request.uri().query() else {
            return Ok(None);
        };

        let query_str = urlencoding::decode(query_str).context(UrlDecodeSnafu)?;

        match extract_influxdb_user_from_query(&query_str) {
            (None, None) => Ok(None),
            (Some(username), Some(password)) => {
                Ok(Some((username.to_string(), password.to_string().into())))
            }
            _ => InvalidParameterSnafu {
                reason: "influxdb auth: username and password must be provided together"
                    .to_string(),
            }
            .fail(),
        }
    }
}

pub fn extract_username_and_password<B>(request: &Request<B>) -> Result<(Username, Password)> {
    Ok(if is_influxdb_request(request) {
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

#[derive(Debug)]
pub enum AuthScheme {
    Basic(Username, Password),
}

type Username = String;
type Password = SecretString;

impl TryFrom<&str> for AuthScheme {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self> {
        let (scheme, encoded_credentials) =
            value.split_once(' ').context(InvalidAuthHeaderSnafu)?;

        ensure!(!encoded_credentials.contains(' '), InvalidAuthHeaderSnafu);

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
        .context(InvalidAuthHeaderInvisibleASCIISnafu)?;

    auth_header.try_into()
}

fn decode_basic(credential: Credential) -> Result<(Username, Password)> {
    let decoded = BASE64_STANDARD
        .decode(credential)
        .context(error::InvalidBase64ValueSnafu)?;
    let as_utf8 =
        String::from_utf8(decoded).context(error::InvalidAuthHeaderInvalidUtf8ValueSnafu)?;

    if let Some((user_id, password)) = as_utf8.split_once(':') {
        return Ok((user_id.to_string(), password.to_string().into()));
    }

    InvalidAuthHeaderSnafu {}.fail()
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

fn extract_param_from_query<'a>(query: &'a str, param: &'a str) -> Option<&'a str> {
    let prefix = format!("{}=", param);
    for pair in query.split('&') {
        if let Some(param) = pair.strip_prefix(&prefix) {
            return if param.is_empty() { None } else { Some(param) };
        }
    }
    None
}

fn extract_db_from_query(query: &str) -> Option<&str> {
    extract_param_from_query(query, "db")
}

/// InfluxDB v2 uses "bucket" instead of "db"
/// https://docs.influxdata.com/influxdb/v1/tools/api/#apiv2write-http-endpoint
fn extract_bucket_from_query(query: &str) -> Option<&str> {
    extract_param_from_query(query, "bucket")
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

    use common_base::secrets::ExposeSecret;

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
        assert_matches!(res.err(), Some(error::Error::InvalidAuthHeader { .. }));

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
    fn test_db_name_header() {
        let http_api_version = crate::http::HTTP_API_VERSION;
        let req = Request::builder()
            .uri(format!("http://localhost/{http_api_version}/sql").as_str())
            .header(GreptimeDbName::name(), "greptime-tomcat")
            .body(())
            .unwrap();

        let db = extract_catalog_and_schema(&req);
        assert_eq!(db, ("greptime".to_string(), "tomcat".to_string()));
    }

    #[test]
    fn test_extract_db() {
        assert_matches!(extract_db_from_query(""), None);
        assert_matches!(extract_db_from_query("&"), None);
        assert_matches!(extract_db_from_query("db="), None);
        assert_matches!(extract_bucket_from_query("bucket="), None);
        assert_matches!(extract_bucket_from_query("db=foo"), None);
        assert_matches!(extract_db_from_query("db=foo"), Some("foo"));
        assert_matches!(extract_bucket_from_query("bucket=foo"), Some("foo"));
        assert_matches!(extract_db_from_query("name=bar"), None);
        assert_matches!(extract_db_from_query("db=&name=bar"), None);
        assert_matches!(extract_db_from_query("db=foo&name=bar"), Some("foo"));
        assert_matches!(extract_bucket_from_query("db=foo&bucket=bar"), Some("bar"));
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
