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

use std::env;
use std::path::Path;

use snafu::ResultExt;

use crate::error::{GetCurrentDirSnafu, Result};

/// Resolves the relative path to an absolute path.
pub fn resolve_relative_path(current_dir: impl AsRef<Path>, path_str: &str) -> String {
    let path = Path::new(path_str);
    if path.is_relative() {
        let path = current_dir.as_ref().join(path);
        common_telemetry::debug!("Resolved relative path: {}", path.to_string_lossy());
        path.to_string_lossy().to_string()
    } else {
        path_str.to_string()
    }
}

/// Resolves the relative path to an absolute path.
pub fn resolve_relative_path_with_current_dir(path_str: &str) -> Result<String> {
    let current_dir = env::current_dir().context(GetCurrentDirSnafu)?;
    Ok(resolve_relative_path(current_dir, path_str))
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_resolve_relative_path_absolute() {
        let abs_path = if cfg!(windows) {
            "C:\\foo\\bar"
        } else {
            "/foo/bar"
        };
        let current_dir = PathBuf::from("/tmp");
        let result = resolve_relative_path(&current_dir, abs_path);
        assert_eq!(result, abs_path);
    }

    #[test]
    fn test_resolve_relative_path_relative() {
        let current_dir = PathBuf::from("/tmp");
        let rel_path = "foo/bar";
        let expected = "/tmp/foo/bar";
        let result = resolve_relative_path(&current_dir, rel_path);
        // On Windows, the separator is '\', so normalize for comparison
        // '/' is as a normal character in Windows paths
        if cfg!(windows) {
            assert!(result.ends_with("foo/bar"));
            assert!(result.contains("/tmp\\"));
        } else {
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_resolve_relative_path_with_current_dir_absolute() {
        let abs_path = if cfg!(windows) {
            "C:\\foo\\bar"
        } else {
            "/foo/bar"
        };
        let result = resolve_relative_path_with_current_dir(abs_path).unwrap();
        assert_eq!(result, abs_path);
    }

    #[test]
    fn test_resolve_relative_path_with_current_dir_relative() {
        let rel_path = "foo/bar";
        let current_dir = env::current_dir().unwrap();
        let expected = current_dir.join(rel_path).to_string_lossy().to_string();
        let result = resolve_relative_path_with_current_dir(rel_path).unwrap();
        assert_eq!(result, expected);
    }
}
