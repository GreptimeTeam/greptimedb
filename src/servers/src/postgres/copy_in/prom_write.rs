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

//! The `promwrite` format for COPY FROM STDIN: one Prometheus Remote
//! Write request body (v1 or v2), snappy- or zstd-compressed protobuf.
//!
//! `COPY FROM STDIN WITH (FORMAT promwrite [, version 2] [, <options>])`
//! is a GreptimeDB-specific, **binary** format (wire format code 1).
//! The payload is byte-for-byte the body a Prometheus remote-write
//! client POSTs to `/v1/prometheus/write` — compression and protobuf
//! framing included — buffered until `CopyDone` and then completed
//! through the exact ingestion core that endpoint runs: the same
//! decode, validation, conversion, batched/prepared write paths and
//! context extensions, under the connection's regular postgres query
//! context.
//!
//! The `WITH` options carry what the HTTP endpoint takes as its
//! content-type/-encoding and query parameters; values already provided
//! by the postgres session (database, user) are not repeated:
//!
//! - `version 1|2` — the remote-write protocol version, matching the
//!   HTTP content-type's proto (default `1`).
//! - `zstd` — the body uses zstd instead of the default snappy
//!   compression, matching `Content-Encoding: zstd`.
//! - `physical_table 'name'` — the `?physical_table=` query parameter.
//! - `pipeline_name`/`pipeline_version` — the pipeline headers (v1
//!   only; for v2 they are accepted and ignored, exactly like the HTTP
//!   route ignores them).
//!
//! One COPY statement is one bounded ingestion request (see
//! [`crate::postgres::copy_in::COPY_IN_MAX_PAYLOAD_BYTES`]); the row
//! count reports written rows (v1) or written samples plus histograms
//! (v2).

use pgwire::error::PgWireResult;

use crate::postgres::copy_in::bad_copy_data;
use crate::postgres::copy_in::codec::CopyFormat;
use crate::postgres::copy_in::options::CopyOptionSet;

/// The format's validated options, carried to the flush that completes
/// the copy through the shared remote-write ingestion core.
#[derive(Debug, Clone)]
pub(crate) struct PromWriteOptions {
    pub(crate) version: PromWriteVersion,
    /// The `?physical_table=` query-parameter equivalent.
    pub(crate) physical_table: Option<String>,
    /// Pipeline headers (v1 only).
    pub(crate) pipeline_name: Option<String>,
    pub(crate) pipeline_version: Option<String>,
    /// `Content-Encoding: zstd` equivalent; snappy by default.
    pub(crate) zstd: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PromWriteVersion {
    #[default]
    V1,
    V2,
}

pub(crate) fn build_prom_write_codec(mut options: CopyOptionSet) -> PgWireResult<CopyFormat> {
    let version = match options.take_value("version") {
        Some(value) => match &value {
            crate::postgres::copy_in::options::CopyOptionValue::Number(n) if n == "2" => {
                PromWriteVersion::V2
            }
            crate::postgres::copy_in::options::CopyOptionValue::Number(n) if n == "1" => {
                PromWriteVersion::V1
            }
            crate::postgres::copy_in::options::CopyOptionValue::String(s)
                if s == "2" || s.eq_ignore_ascii_case("v2") =>
            {
                PromWriteVersion::V2
            }
            crate::postgres::copy_in::options::CopyOptionValue::String(s)
                if s == "1" || s.eq_ignore_ascii_case("v1") =>
            {
                PromWriteVersion::V1
            }
            other => {
                return Err(bad_copy_data(format!(
                    "invalid promwrite version {:?}, expected 1 or 2",
                    other.kind()
                )));
            }
        },
        None => PromWriteVersion::V1,
    };
    let physical_table = options.take_string("physical_table")?;
    let pipeline_name = options.take_string("pipeline_name")?;
    let pipeline_version = options.take_string("pipeline_version")?;
    let zstd = options.take_flag("zstd")?.unwrap_or(false);
    options.reject_leftovers("promwrite")?;
    // The pipeline options are accepted and ignored for version 2,
    // exactly like the HTTP route ignores them there.
    let (pipeline_name, pipeline_version) = match version {
        PromWriteVersion::V1 => (pipeline_name, pipeline_version),
        PromWriteVersion::V2 => (None, None),
    };
    Ok(CopyFormat::PromWrite(PromWriteOptions {
        version,
        physical_table,
        pipeline_name,
        pipeline_version,
        zstd,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::copy_in::options::CopyOptionValue;

    fn opts(entries: &[(&str, CopyOptionValue)]) -> CopyOptionSet {
        let mut set = CopyOptionSet::default();
        for (k, v) in entries {
            set.insert((*k).into(), v.clone()).unwrap();
        }
        set
    }

    #[test]
    fn test_defaults_and_versions() {
        let CopyFormat::PromWrite(options) =
            build_prom_write_codec(CopyOptionSet::default()).unwrap()
        else {
            panic!("expected promwrite");
        };
        assert_eq!(options.version, PromWriteVersion::V1);
        assert!(!options.zstd);
        assert!(options.physical_table.is_none());

        for (entry, expected) in [
            (CopyOptionValue::Number("2".into()), PromWriteVersion::V2),
            (CopyOptionValue::String("v2".into()), PromWriteVersion::V2),
            (CopyOptionValue::Number("1".into()), PromWriteVersion::V1),
        ] {
            let CopyFormat::PromWrite(options) =
                build_prom_write_codec(opts(&[("version", entry)])).unwrap()
            else {
                panic!("expected promwrite");
            };
            assert_eq!(options.version, expected);
        }

        let set = opts(&[
            ("version", CopyOptionValue::Number("2".into())),
            ("zstd", CopyOptionValue::Flag),
            ("physical_table", CopyOptionValue::String("phys".into())),
        ]);
        let CopyFormat::PromWrite(options) = build_prom_write_codec(set).unwrap() else {
            panic!("expected promwrite");
        };
        assert_eq!(options.version, PromWriteVersion::V2);
        assert!(options.zstd);
        assert_eq!(options.physical_table.as_deref(), Some("phys"));
    }

    #[test]
    fn test_rejects_bad_options() {
        let set = opts(&[("version", CopyOptionValue::Number("3".into()))]);
        assert!(build_prom_write_codec(set).is_err());

        let set = opts(&[("unknown", CopyOptionValue::Flag)]);
        assert!(build_prom_write_codec(set).is_err());
    }

    #[test]
    fn test_v2_accepts_and_ignores_pipeline_options() {
        // The HTTP v2 route deliberately ignores pipeline information;
        // COPY accepts the options and ignores them too.
        let CopyFormat::PromWrite(options) = build_prom_write_codec(opts(&[
            ("version", CopyOptionValue::Number("2".into())),
            ("pipeline_name", CopyOptionValue::String("p".into())),
            ("pipeline_version", CopyOptionValue::String("v1".into())),
        ]))
        .unwrap() else {
            panic!("expected promwrite");
        };
        assert_eq!(options.version, PromWriteVersion::V2);
        assert!(options.pipeline_name.is_none());
        assert!(options.pipeline_version.is_none());
    }
}
