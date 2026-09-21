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

use api::v1::auth_header::AuthScheme;
use api::v1::{AuthHeader, RequestHeader};
use auth::{Identity, Password, UserInfoRef, UserProviderRef};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use session::context::{Channel, QueryContextBuilder, QueryContextRef};
use session::hints::INSERT_SKIP_WAL_HINT;
use snafu::{OptionExt, ResultExt};
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::error::Error::UnsupportedAuthScheme;
use crate::error::{AuthSnafu, InvalidParameterSnafu, NotFoundAuthHeaderSnafu, Result};
use crate::grpc::TonicResult;
use crate::hint_headers;
use crate::http::AUTHORIZATION_HEADER;
use crate::http::header::constants::GREPTIME_DB_HEADER_NAME;
use crate::metrics::METRIC_AUTH_FAILURE;

/// Create a query context from the grpc metadata.
pub fn create_query_context_from_grpc_metadata(
    headers: &MetadataMap,
) -> TonicResult<QueryContextRef> {
    let (catalog, schema) = if let Some(db) = extract_header(headers, &[GREPTIME_DB_HEADER_NAME])? {
        parse_catalog_and_schema_from_db_string(db)
    } else {
        (
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        )
    };

    let ctx = QueryContextBuilder::default()
        .current_catalog(catalog)
        .current_schema(schema)
        .channel(Channel::Grpc)
        .build();
    // OTEL Arrow uses ordinary inserts. Accept only its request-level WAL hint,
    // leaving unrelated hints and reserved internal extensions unchanged.
    if let Some((key, value)) = hint_headers::extract_hints(headers)
        .into_iter()
        .find(|(key, _)| key == INSERT_SKIP_WAL_HINT)
    {
        let skip_wal = value.parse::<bool>().map_err(|_| {
            InvalidParameterSnafu {
                reason: format!("Invalid {key} hint: expected true or false, got {value:?}"),
            }
            .build()
        })?;
        ctx.set_skip_wal(skip_wal);
    }
    Ok(Arc::new(ctx))
}

/// Helper function to extract a header from the metadata map.
/// Can be multiple keys, and the first one found will be returned.
pub fn extract_header<'a>(headers: &'a MetadataMap, keys: &[&str]) -> TonicResult<Option<&'a str>> {
    let mut value = None;
    for key in keys {
        if let Some(v) = headers.get(*key) {
            value = Some(v);
            break;
        }
    }

    let Some(v) = value else {
        return Ok(None);
    };
    let Ok(v) = std::str::from_utf8(v.as_bytes()) else {
        return Err(InvalidParameterSnafu {
            reason: "expect valid UTF-8 value",
        }
        .build()
        .into());
    };
    Ok(Some(v))
}

/// Helper function to extract the header from the metadata and authenticate the user.
pub async fn check_auth(
    user_provider: Option<UserProviderRef>,
    headers: &MetadataMap,
    query_ctx: QueryContextRef,
) -> TonicResult<bool> {
    if user_provider.is_none() {
        return Ok(true);
    }

    let auth_schema = extract_header(
        headers,
        &[AUTHORIZATION_HEADER, http::header::AUTHORIZATION.as_str()],
    )?
    .map(|x| {
        if x.len() > 5 && x[0..5].eq_ignore_ascii_case("Basic") {
            x.try_into()
        } else {
            // compatible with old version
            format!("Basic {}", x).as_str().try_into()
        }
    })
    .transpose()?
    .map(|x: crate::http::authorize::AuthScheme| x.into());

    let auth_schema = auth_schema.context(NotFoundAuthHeaderSnafu)?;
    let header = RequestHeader {
        authorization: Some(AuthHeader {
            auth_scheme: Some(auth_schema),
        }),
        catalog: query_ctx.current_catalog().to_string(),
        schema: query_ctx.current_schema(),
        ..Default::default()
    };

    match auth(user_provider, Some(&header), &query_ctx).await {
        Ok(user_info) => {
            query_ctx.set_current_user(user_info);
            Ok(true)
        }
        Err(_) => Err(Status::unauthenticated("auth failed")),
    }
}

/// Authenticate the user based on the header and query context.
pub async fn auth(
    user_provider: Option<UserProviderRef>,
    header: Option<&RequestHeader>,
    query_ctx: &QueryContextRef,
) -> Result<UserInfoRef> {
    let Some(user_provider) = user_provider else {
        return Ok(auth::userinfo_by_name(None));
    };

    let auth_scheme = header
        .and_then(|header| {
            header
                .authorization
                .as_ref()
                .and_then(|x| x.auth_scheme.clone())
        })
        .context(NotFoundAuthHeaderSnafu)?;

    match auth_scheme {
        AuthScheme::Basic(api::v1::Basic { username, password }) => user_provider
            .auth(
                Identity::UserId(&username, None),
                Password::PlainText(password.into()),
                query_ctx.current_catalog(),
                &query_ctx.current_schema(),
            )
            .await
            .context(AuthSnafu),
        AuthScheme::Token(_) => Err(UnsupportedAuthScheme {
            name: "Token AuthScheme".to_string(),
        }),
    }
    .inspect_err(|e| {
        METRIC_AUTH_FAILURE
            .with_label_values(&[e.status_code().as_ref()])
            .inc();
    })
}

#[cfg(test)]
mod tests {
    use session::hints::{HINTS_KEY, REMOTE_QUERY_ID_EXTENSION_KEY, RESERVED_EXTENSION_KEYS};

    use super::*;

    #[test]
    fn test_arrow_insert_hint_does_not_accept_reserved_extensions() {
        let mut headers = MetadataMap::new();
        assert_eq!(
            create_query_context_from_grpc_metadata(&headers)
                .unwrap()
                .extension(INSERT_SKIP_WAL_HINT),
            None
        );
        for (value, expected) in [("true", true), ("false", false)] {
            let mut hints = format!("insert_skip_wal={value},ttl=7d");
            for key in RESERVED_EXTENSION_KEYS {
                hints.push_str(&format!(",{key}=external"));
            }
            headers.insert(HINTS_KEY, hints.parse().unwrap());
            let ctx = create_query_context_from_grpc_metadata(&headers).unwrap();
            assert_eq!(ctx.skip_wal(), expected);
            assert_eq!(ctx.extension(INSERT_SKIP_WAL_HINT), None);
            assert_eq!(ctx.extension("ttl"), None);
            for key in RESERVED_EXTENSION_KEYS {
                if key == REMOTE_QUERY_ID_EXTENSION_KEY {
                    // The builder generates this ID; external hints must not replace it.
                    assert!(ctx.remote_query_id().is_some());
                    assert_ne!(ctx.extension(key), Some("external"));
                } else {
                    assert_eq!(ctx.extension(key), None);
                }
            }
        }
        // Only the first matching hint is parsed and applied.
        for (hints, expected) in [
            ("insert_skip_wal=true,insert_skip_wal=false", true),
            ("insert_skip_wal=false,insert_skip_wal=true", false),
            ("insert_skip_wal=true,insert_skip_wal=invalid", true),
        ] {
            headers.insert(HINTS_KEY, hints.parse().unwrap());
            let ctx = create_query_context_from_grpc_metadata(&headers).unwrap();
            assert_eq!(ctx.skip_wal(), expected);
        }
        for value in ["", "TRUE", "1", "invalid"] {
            headers.insert(
                HINTS_KEY,
                format!("insert_skip_wal={value}").parse().unwrap(),
            );
            assert!(create_query_context_from_grpc_metadata(&headers).is_err());
        }
    }
}
