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
use opendal::{Entry, Lister};

pub async fn collect(stream: Lister) -> Result<Vec<Entry>, opendal::Error> {
    stream.try_collect::<Vec<_>>().await
}

/// Normalize a directory path, ensure it is ends with '/'
pub fn normalize_dir(dir: &str) -> String {
    let mut dir = dir.to_string();
    if !dir.ends_with('/') {
        dir.push('/')
    }

    dir
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
    // We call opendal's normalize_dir which doesn't push `/` to
    // the end of path.
    opendal::raw::normalize_root(&output)
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
    fn test_join_paths() {
        assert_eq!("/", join_dir("", ""));
        assert_eq!("/", join_dir("/", ""));
        assert_eq!("/", join_dir("", "/"));
        assert_eq!("/", join_dir("/", "/"));
        assert_eq!("/a/", join_dir("/a", ""));
        assert_eq!("/a/b/c/", join_dir("a/b", "c"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "c"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "c/"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "/c/"));
        assert_eq!("/a/b/c/", join_dir("/a/b", "//c"));
    }
}
