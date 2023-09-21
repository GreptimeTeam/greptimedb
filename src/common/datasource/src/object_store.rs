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

pub mod fs;
pub mod s3;
use std::collections::HashMap;

use lazy_static::lazy_static;
use object_store::ObjectStore;
use regex::Regex;
use snafu::{OptionExt, ResultExt};
use url::{ParseError, Url};

use self::fs::build_fs_backend;
use self::s3::build_s3_backend;
use crate::error::{self, Result};
use crate::util::find_dir_and_filename;

pub const FS_SCHEMA: &str = "FS";
pub const S3_SCHEMA: &str = "S3";

/// Returns `(schema, Option<host>, path)`
pub fn parse_url(url: &str) -> Result<(String, Option<String>, String)> {
    #[cfg(windows)]
    {
        // On Windows, the url may start with `C:/`.
        if let Some(_) = handle_windows_path(url) {
            return Ok((FS_SCHEMA.to_string(), None, url.to_string()));
        }
    }
    let parsed_url = Url::parse(url);
    match parsed_url {
        Ok(url) => Ok((
            url.scheme().to_string(),
            url.host_str().map(|s| s.to_string()),
            url.path().to_string(),
        )),
        Err(ParseError::RelativeUrlWithoutBase) => {
            Ok((FS_SCHEMA.to_string(), None, url.to_string()))
        }
        Err(err) => Err(err).context(error::InvalidUrlSnafu { url }),
    }
}

pub fn build_backend(url: &str, connection: &HashMap<String, String>) -> Result<ObjectStore> {
    let (schema, host, path) = parse_url(url)?;
    let (root, _) = find_dir_and_filename(&path);

    match schema.to_uppercase().as_str() {
        S3_SCHEMA => {
            let host = host.context(error::EmptyHostPathSnafu {
                url: url.to_string(),
            })?;
            Ok(build_s3_backend(&host, &root, connection)?)
        }
        FS_SCHEMA => Ok(build_fs_backend(&root)?),

        _ => error::UnsupportedBackendProtocolSnafu {
            protocol: schema,
            url,
        }
        .fail(),
    }
}

lazy_static! {
    static ref DISK_SYMBOL_PATTERN: Regex = Regex::new("^([A-Za-z]:/)").unwrap();
}

pub fn handle_windows_path(url: &str) -> Option<String> {
    DISK_SYMBOL_PATTERN
        .captures(url)
        .map(|captures| captures[0].to_string())
}

#[cfg(test)]
mod tests {
    use super::handle_windows_path;

    #[test]
    fn test_handle_windows_path() {
        assert_eq!(
            handle_windows_path("C:/to/path/file"),
            Some("C:/".to_string())
        );
        assert_eq!(handle_windows_path("https://google.com"), None);
        assert_eq!(handle_windows_path("s3://bucket/path/to"), None);
    }
}
