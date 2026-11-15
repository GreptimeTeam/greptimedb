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

//! Shared parsing and application for public protocol hints.

pub(crate) mod compatibility;
pub(crate) mod protocol;
pub(crate) mod registry;

use session::context::QueryContext;
use session::hint_registry::ParsedHint;
use session::query_options::{self, QueryParseOptions};
use session::session_config;

use crate::error::{Error, InvalidParameterSnafu, Result, UnknownHintSnafu};
use crate::hint_headers;
use crate::hints::protocol::{Endpoint, Transport};

/// A complete validated batch. Applying it cannot fail.
pub(crate) struct ParsedHints {
    extensions: Vec<(String, String)>,
    registered: Vec<ParsedHint<QueryContext>>,
}

impl ParsedHints {
    /// Applies the validated batch to the request context.
    pub(crate) fn apply(self, context: &mut QueryContext) {
        for (name, value) in self.extensions {
            context.set_extension(name, value);
        }
        for hint in self.registered {
            hint.apply(context);
        }
    }
}

/// Parses each supported hint once, using identical rules for both transports.
/// Metadata-only services retain their deliberately restricted input set.
pub(crate) fn parse_endpoint(
    endpoint: Endpoint,
    hints: Vec<(String, String)>,
) -> Result<ParsedHints> {
    let hints = if endpoint.is_metadata_only() {
        registry::select_metadata_hints(endpoint, hints)
    } else {
        hints
    };
    let batch = registry::for_endpoint(endpoint)
        .parse_grouped(
            hint_headers::filter_public_hints(hints),
            QueryParseOptions {
                max_parallelism: query_options::MAX_PUBLIC_PARALLELISM,
            },
        )
        .map_err(|error| map_error(endpoint.transport(), error))?;
    let (registered, extensions) = batch.into_parts();
    // Public query namespaces remain validated even on routes which do not
    // consume them. Unrelated extension namespaces stay available to plugins.
    let extensions = if extensions.is_empty() {
        extensions
    } else {
        query_options::normalize_query_option_batch(
            extensions,
            query_options::MAX_PUBLIC_PARALLELISM,
        )
        .map_err(|error| map_error(endpoint.transport(), error))?
    };
    Ok(ParsedHints {
        extensions,
        registered,
    })
}

fn map_error(transport: Transport, error: session_config::Error) -> Error {
    match transport {
        Transport::Http => InvalidParameterSnafu {
            reason: error.to_string(),
        }
        .build(),
        Transport::Grpc => UnknownHintSnafu {
            hint: error.to_string(),
        }
        .build(),
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};
    use session::hints::HINTS_KEY;
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::*;
    use crate::hints::protocol::Protocol;

    #[test]
    fn test_missing_values_use_registered_parsers_on_both_transports() {
        for name in ["query.parallelism", "insert_skip_wal", "read_preference"] {
            let mut headers = HeaderMap::new();
            headers.insert(HINTS_KEY, HeaderValue::from_static(name));
            let mut metadata = MetadataMap::new();
            metadata.insert(HINTS_KEY, MetadataValue::from_static(name));
            for (endpoint, hints) in [
                (
                    Endpoint::Http(Protocol::Sql),
                    hint_headers::extract_hints(&headers),
                ),
                (
                    Endpoint::Grpc(Protocol::Sql),
                    hint_headers::extract_hints(&metadata),
                ),
            ] {
                assert!(
                    parse_endpoint(endpoint, hints).is_err(),
                    "{endpoint:?}: {name}"
                );
            }
        }
        for endpoint in [Endpoint::Http(Protocol::Sql), Endpoint::Grpc(Protocol::Sql)] {
            let mut context = QueryContext::with("greptime", "public");
            parse_endpoint(endpoint, vec![("custom.option".to_string(), String::new())])
                .unwrap()
                .apply(&mut context);
            assert_eq!(context.extension("custom.option"), Some(""));
        }
    }
}
