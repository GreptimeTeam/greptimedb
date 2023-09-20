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

use object_store::ObjectStore;
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

        _ => {
            #[cfg(windows)]
            {
                // Disk symbol
                if schema.len() == 1 {
                    return Ok(build_fs_backend(&url)?);
                }
            }

            error::UnsupportedBackendProtocolSnafu {
                protocol: schema,
                path: url,
            }
            .fail()
        }
    }
}
