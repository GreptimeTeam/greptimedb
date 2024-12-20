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

use std::fmt::Display;

use common_telemetry::{debug, error, trace};
use futures::TryStreamExt;
use opendal::layers::{LoggingInterceptor, LoggingLayer, TracingLayer};
use opendal::raw::{AccessorInfo, Operation};
use opendal::{Entry, ErrorKind, Lister};

use crate::layers::PrometheusMetricsLayer;
use crate::ObjectStore;

/// Collect all entries from the [Lister].
pub async fn collect(stream: Lister) -> Result<Vec<Entry>, opendal::Error> {
    stream.try_collect::<Vec<_>>().await
}

/// Join two paths and normalize the output dir.
///
/// The output dir is always ends with `/`. e.g.
/// - `/a/b` join `c` => `/a/b/c/`
/// - `/a/b` join `/c/` => `/a/b/c/`
///
/// All internal `//` will be replaced by `/`.
pub fn join_dir(parent: &str, child: &str) -> String {
    // Always adds a `/` to the output path.
    let output = format!("{parent}/{child}/");
    normalize_dir(&output)
}

/// Modified from the `opendal::raw::normalize_root`
///
/// # The different
///
/// It doesn't always append `/` ahead of the path,
/// It only keeps `/` ahead if the original path starts with `/`.
///
/// Make sure the directory is normalized to style like `abc/def/`.
///
/// # Normalize Rules
///
/// - All whitespace will be trimmed: ` abc/def ` => `abc/def`
/// - All leading / will be trimmed: `///abc` => `abc`
/// - Internal // will be replaced by /: `abc///def` => `abc/def`
/// - Empty path will be `/`: `` => `/`
/// - **(Removed❗️)** ~~Add leading `/` if not starts with: `abc/` => `/abc/`~~
/// - Add trailing `/` if not ends with: `/abc` => `/abc/`
///
/// Finally, we will got path like `/path/to/root/`.
pub fn normalize_dir(v: &str) -> String {
    let has_root = v.starts_with('/');
    let mut v = v
        .split('/')
        .filter(|v| !v.is_empty())
        .collect::<Vec<&str>>()
        .join("/");
    if has_root {
        v.insert(0, '/');
    }
    if !v.ends_with('/') {
        v.push('/')
    }
    v
}

/// Push `child` to `parent` dir and normalize the output path.
///
/// - Path endswith `/` means it's a dir path.
/// - Otherwise, it's a file path.
pub fn join_path(parent: &str, child: &str) -> String {
    let output = format!("{parent}/{child}");
    normalize_path(&output)
}

/// Make sure all operation are constructed by normalized path:
///
/// - Path endswith `/` means it's a dir path.
/// - Otherwise, it's a file path.
///
/// # Normalize Rules
///
/// - All whitespace will be trimmed: ` abc/def ` => `abc/def`
/// - Repeated leading / will be trimmed: `///abc` => `/abc`
/// - Internal // will be replaced by /: `abc///def` => `abc/def`
/// - Empty path will be `/`: `` => `/`
pub fn normalize_path(path: &str) -> String {
    // - all whitespace has been trimmed.
    let path = path.trim();

    // Fast line for empty path.
    if path.is_empty() {
        return "/".to_string();
    }

    let has_leading = path.starts_with('/');
    let has_trailing = path.ends_with('/');

    let mut p = path
        .split('/')
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>()
        .join("/");

    // If path is not starting with `/` but it should
    if !p.starts_with('/') && has_leading {
        p.insert(0, '/');
    }

    // If path is not ending with `/` but it should
    if !p.ends_with('/') && has_trailing {
        p.push('/');
    }

    p
}

// This logical tries to extract parent path from the object storage operation
// the function also relies on assumption that the region path is built from
// pattern `<data|index>/catalog/schema/table_id/....`
//
// this implementation tries to extract at most 3 levels of parent path
pub(crate) fn extract_parent_path(path: &str) -> &str {
    // split the path into `catalog`, `schema` and others
    path.char_indices()
        .filter(|&(_, c)| c == '/')
        // we get the data/catalog/schema from path, split at the 3rd /
        .nth(2)
        .map_or(path, |(i, _)| &path[..i])
}

/// Attaches instrument layers to the object store.
pub fn with_instrument_layers(object_store: ObjectStore, path_label: bool) -> ObjectStore {
    object_store
        .layer(LoggingLayer::new(DefaultLoggingInterceptor))
        .layer(TracingLayer)
        .layer(PrometheusMetricsLayer::new(path_label))
}

static LOGGING_TARGET: &str = "opendal::services";

struct LoggingContext<'a>(&'a [(&'a str, &'a str)]);

impl Display for LoggingContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, (k, v)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, " {}={}", k, v)?;
            } else {
                write!(f, "{}={}", k, v)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct DefaultLoggingInterceptor;

impl LoggingInterceptor for DefaultLoggingInterceptor {
    #[inline]
    fn log(
        &self,
        info: &AccessorInfo,
        operation: Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&opendal::Error>,
    ) {
        if let Some(err) = err {
            // Print error if it's unexpected, otherwise in error.
            if err.kind() == ErrorKind::Unexpected {
                error!(
                    target: LOGGING_TARGET,
                    "service={} name={} {}: {operation} {message} {err:#?}",
                    info.scheme(),
                    info.name(),
                    LoggingContext(context),
                );
            } else {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} name={} {}: {operation} {message} {err}",
                    info.scheme(),
                    info.name(),
                    LoggingContext(context),
                );
            };
        }

        // Print debug message if operation is oneshot, otherwise in trace.
        if operation.is_oneshot() {
            debug!(
                target: LOGGING_TARGET,
                "service={} name={} {}: {operation} {message}",
                info.scheme(),
                info.name(),
                LoggingContext(context),
            );
        } else {
            trace!(
                target: LOGGING_TARGET,
                "service={} name={} {}: {operation} {message}",
                info.scheme(),
                info.name(),
                LoggingContext(context),
            );
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_dir() {
        assert_eq!("/", normalize_dir("/"));
        assert_eq!("/", normalize_dir(""));
        assert_eq!("/test/", normalize_dir("/test"));
    }

    #[test]
    fn test_join_dir() {
        assert_eq!("/", join_dir("", ""));
        assert_eq!("/", join_dir("/", ""));
        assert_eq!("/", join_dir("", "/"));
        assert_eq!("/", join_dir("/", "/"));
        assert_eq!("/a/", join_dir("/a", ""));
        assert_eq!("a/b/c/", join_dir("a/b", "c"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "c"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "c/"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "/c/"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "//c"));
    }

    #[test]
    fn test_join_path() {
        assert_eq!("/", join_path("", ""));
        assert_eq!("/", join_path("/", ""));
        assert_eq!("/", join_path("", "/"));
        assert_eq!("/", join_path("/", "/"));
        assert_eq!("a/", join_path("a", ""));
        assert_eq!("/a", join_path("/", "a"));
        assert_eq!("a/b/c.txt", join_path("a/b", "c.txt"));
        assert_eq!("/a/b/c.txt", join_path("/a/b", "c.txt"));
        assert_eq!("/a/b/c/", join_path("/a/b", "c/"));
        assert_eq!("/a/b/c/", join_path("/a/b", "/c/"));
        assert_eq!("/a/b/c.txt", join_path("/a/b", "//c.txt"));
        assert_eq!("abc/def", join_path(" abc", "/def "));
        assert_eq!("/abc", join_path("//", "/abc"));
        assert_eq!("abc/def", join_path("abc/", "//def"));
    }

    #[test]
    fn test_path_extraction() {
        assert_eq!(
            "data/greptime/public",
            extract_parent_path("data/greptime/public/1024/1024_0000000000/")
        );

        assert_eq!(
            "data/greptime/public",
            extract_parent_path("data/greptime/public/1/")
        );

        assert_eq!(
            "data/greptime/public",
            extract_parent_path("data/greptime/public")
        );

        assert_eq!("data/greptime/", extract_parent_path("data/greptime/"));

        assert_eq!("data/", extract_parent_path("data/"));

        assert_eq!("/", extract_parent_path("/"));
    }
}
