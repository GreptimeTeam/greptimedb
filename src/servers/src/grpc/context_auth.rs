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
use snafu::{OptionExt, ResultExt};
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::error::Error::UnsupportedAuthScheme;
use crate::error::{AuthSnafu, InvalidParameterSnafu, NotFoundAuthHeaderSnafu, Result};
use crate::grpc::TonicResult;
use crate::hint_headers;
use crate::hints::protocol::{Endpoint, Protocol};
use crate::http::AUTHORIZATION_HEADER;
use crate::http::header::constants::GREPTIME_DB_HEADER_NAME;
use crate::metrics::METRIC_AUTH_FAILURE;

/// Create a query context from gRPC metadata and server-owned request extensions.
pub fn create_query_context_from_grpc_metadata(
    headers: &MetadataMap,
    extensions: &http::Extensions,
) -> TonicResult<QueryContextRef> {
    create_query_context_from_grpc_metadata_for_protocol(headers, extensions, Protocol::OtelArrow)
}

pub(crate) fn create_query_context_from_grpc_metadata_for_protocol(
    headers: &MetadataMap,
    extensions: &http::Extensions,
    protocol: Protocol,
) -> TonicResult<QueryContextRef> {
    let (catalog, schema) = if let Some(db) = extract_header(headers, &[GREPTIME_DB_HEADER_NAME])? {
        parse_catalog_and_schema_from_db_string(db)
    } else {
        (
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        )
    };

    let mut ctx = QueryContextBuilder::default()
        .current_catalog(catalog)
        .current_schema(schema)
        .channel(
            extensions
                .get::<Channel>()
                .copied()
                .unwrap_or(Channel::Grpc),
        )
        .build();
    crate::hints::parse_endpoint(
        Endpoint::Grpc(protocol),
        hint_headers::extract_hints(headers),
    )?
    .apply(&mut ctx);
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
    use session::hints::INSERT_SKIP_WAL_HINT;
    use session::hints::{HINTS_KEY, REMOTE_QUERY_ID_EXTENSION_KEY, RESERVED_EXTENSION_KEYS};

    use super::*;

    #[test]
    fn test_channel_comes_from_server_extensions() {
        let mut headers = MetadataMap::new();
        headers.insert(
            "x-greptime-flow-extensions",
            r#"[["flow.return_region_seq","true"]]"#.parse().unwrap(),
        );
        headers.insert(HINTS_KEY, "channel=internal".parse().unwrap());
        let mut extensions = http::Extensions::new();
        let ctx = create_query_context_from_grpc_metadata(&headers, &extensions).unwrap();
        assert_eq!(ctx.channel(), Channel::Grpc);

        extensions.insert(Channel::Internal);
        let ctx = create_query_context_from_grpc_metadata(&headers, &extensions).unwrap();
        assert_eq!(ctx.channel(), Channel::Internal);
    }

    fn metadata_context(protocol: Protocol, hints: &str) -> TonicResult<QueryContextRef> {
        let mut headers = MetadataMap::new();
        headers.insert(HINTS_KEY, hints.parse().unwrap());
        create_query_context_from_grpc_metadata_for_protocol(
            &headers,
            &http::Extensions::new(),
            protocol,
        )
    }

    #[test]
    fn test_metadata_endpoints_select_supported_hints_and_protect_reserved_keys() {
        for protocol in [Protocol::OtelArrow, Protocol::BulkInsert] {
            let empty = metadata_context(protocol, "").unwrap();
            assert!(!empty.skip_wal());
            assert_eq!(empty.extension(INSERT_SKIP_WAL_HINT), None);
            for (value, expected) in [("true", true), ("false", false)] {
                let mut hints = format!(
                    "insert_skip_wal={value},auto_create_table=true,auto_create_table=false,ttl=invalid,query.parallelism=invalid,datafusion.unknown=invalid"
                );
                for key in RESERVED_EXTENSION_KEYS {
                    hints.push_str(&format!(",{key}=external"));
                }
                let context = metadata_context(protocol, &hints).unwrap();
                assert_eq!(context.skip_wal(), expected);
                assert_eq!(context.extension(INSERT_SKIP_WAL_HINT), None);
                assert_eq!(
                    context.extension(table::requests::AUTO_CREATE_TABLE_KEY),
                    (protocol == Protocol::BulkInsert).then_some("false")
                );
                for key in ["ttl", "query.parallelism", "datafusion.unknown"] {
                    assert_eq!(context.extension(key), None, "{protocol:?}: {key}");
                }
                for key in RESERVED_EXTENSION_KEYS {
                    if key == REMOTE_QUERY_ID_EXTENSION_KEY {
                        assert!(context.remote_query_id().is_some());
                        assert_ne!(context.extension(key), Some("external"));
                    } else {
                        assert_eq!(context.extension(key), None, "{protocol:?}: {key}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_metadata_wal_duplicates_validate_every_value_and_apply_last() {
        for protocol in [Protocol::OtelArrow, Protocol::BulkInsert] {
            for (hints, expected) in [
                ("insert_skip_wal=true,insert_skip_wal=false", false),
                ("insert_skip_wal=false,insert_skip_wal=true", true),
            ] {
                let context = metadata_context(protocol, hints).unwrap();
                assert_eq!(context.skip_wal(), expected, "{protocol:?}: {hints}");
            }
            for value in [
                "",
                "TRUE",
                "1",
                "invalid",
                "true,insert_skip_wal=invalid",
                "invalid,insert_skip_wal=true",
            ] {
                let hints = format!("insert_skip_wal={value}");
                assert!(
                    metadata_context(protocol, &hints).is_err(),
                    "{protocol:?}: {hints}"
                );
            }
        }
    }
}
