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

use std::io::Write;

use serde::Serialize;
use serde_json::to_writer;
use snafu::ResultExt;
use store_api::manifest::action::VersionHeader;
use store_api::manifest::ManifestVersion;

use crate::error::{EncodeJsonSnafu, Result};

pub const NEWLINE: &[u8] = b"\n";

pub fn encode_actions<T: Serialize>(
    prev_version: ManifestVersion,
    actions: &[T],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::default();
    {
        // Encode prev_version
        let v = VersionHeader { prev_version };

        to_writer(&mut bytes, &v).context(EncodeJsonSnafu)?;
        // unwrap is fine here, because we write into a buffer.
        bytes.write_all(NEWLINE).unwrap();
    }

    for action in actions {
        to_writer(&mut bytes, action).context(EncodeJsonSnafu)?;
        bytes.write_all(NEWLINE).unwrap();
    }

    Ok(bytes)
}
