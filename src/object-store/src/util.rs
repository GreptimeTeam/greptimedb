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

use futures::TryStreamExt;
use opendal::layers::{LoggingLayer, TracingLayer};
use opendal::{Entry, Lister};

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
    opendal::raw::normalize_path(&output)
}

/// Attaches instrument layers to the object store.
pub fn with_instrument_layers(object_store: ObjectStore) -> ObjectStore {
    object_store
        .layer(
            LoggingLayer::default()
                // Print the expected error only in DEBUG level.
                // See https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html#method.with_error_level
                .with_error_level(Some("debug"))
                .expect("input error level must be valid"),
        )
        .layer(TracingLayer)
        .layer(PrometheusMetricsLayer)
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
        assert_eq!("a/b/c.txt", join_path("a/b", "c.txt"));
        assert_eq!("a/b/c.txt", join_path("/a/b", "c.txt"));
        assert_eq!("a/b/c/", join_path("/a/b", "c/"));
        assert_eq!("a/b/c/", join_path("/a/b", "/c/"));
        assert_eq!("a/b/c.txt", join_path("/a/b", "//c.txt"));
    }
}
