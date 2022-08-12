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
