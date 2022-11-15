// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::TryStreamExt;

use crate::{DirEntry, DirStreamer};

pub async fn collect(stream: DirStreamer) -> Result<Vec<DirEntry>, std::io::Error> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_dir() {
        assert_eq!("/", normalize_dir("/"));
        assert_eq!("/", normalize_dir(""));
        assert_eq!("/test/", normalize_dir("/test"));
    }
}
