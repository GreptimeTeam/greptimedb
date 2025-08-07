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
use std::path;
use std::time::Duration;

use common_telemetry::{debug, error, info, warn};
use opendal::layers::{LoggingInterceptor, LoggingLayer, RetryInterceptor, TracingLayer};
use opendal::raw::{AccessorInfo, HttpClient, Operation};
use opendal::{Error, ErrorKind};
use snafu::ResultExt;

use crate::config::HttpClientConfig;
use crate::{error, ObjectStore};

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

/// Attaches instrument layers to the object store.
pub fn with_instrument_layers(object_store: ObjectStore, path_label: bool) -> ObjectStore {
    object_store
        .layer(LoggingLayer::new(DefaultLoggingInterceptor))
        .layer(TracingLayer)
        .layer(crate::layers::build_prometheus_metrics_layer(path_label))
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

        debug!(
            target: LOGGING_TARGET,
            "service={} name={} {}: {operation} {message}",
            info.scheme(),
            info.name(),
            LoggingContext(context),
        );
    }
}

pub(crate) fn build_http_client(config: &HttpClientConfig) -> error::Result<HttpClient> {
    if config.skip_ssl_validation {
        common_telemetry::warn!("Skipping SSL validation for object storage HTTP client. Please ensure the environment is trusted.");
    }

    let client = reqwest::ClientBuilder::new()
        .pool_max_idle_per_host(config.pool_max_idle_per_host as usize)
        .connect_timeout(config.connect_timeout)
        .pool_idle_timeout(config.pool_idle_timeout)
        .timeout(config.timeout)
        .danger_accept_invalid_certs(config.skip_ssl_validation)
        .build()
        .context(error::BuildHttpClientSnafu)?;
    Ok(HttpClient::with(client))
}

pub fn clean_temp_dir(dir: &str) -> error::Result<()> {
    if path::Path::new(&dir).exists() {
        info!("Begin to clean temp storage directory: {}", dir);
        std::fs::remove_dir_all(dir).context(error::RemoveDirSnafu { dir })?;
        info!("Cleaned temp storage directory: {}", dir);
    }

    Ok(())
}

/// PrintDetailedError is a retry interceptor that prints error in Debug format in retrying.
pub struct PrintDetailedError;

// PrintDetailedError is a retry interceptor that prints error in Debug format in retrying.
impl RetryInterceptor for PrintDetailedError {
    fn intercept(&self, err: &Error, dur: Duration) {
        warn!("Retry after {}s, error: {:#?}", dur.as_secs_f64(), err);
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
}
