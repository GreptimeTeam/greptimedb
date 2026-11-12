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

//! The `influxdb` line protocol format for COPY FROM STDIN.
//!
//! `COPY FROM STDIN WITH (FORMAT influxdb, precision 'ns')` ingests
//! [InfluxDB line protocol](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/)
//! text. The format is table-less — each line names its own measurement
//! — and does not parse here at all: the COPY layer buffers the whole
//! payload (bounded by [`crate::postgres::copy_in::COPY_IN_MAX_PAYLOAD_BYTES`])
//! and completes the copy by handing it to the frontend's line-protocol
//! write pipeline as one body, exactly like the HTTP
//! `/v1/influxdb/v1/write` endpoint receives one request body:
//! conversion, permissions, interceptors, timestamp alignment, merge
//! mode and inserts are the same path, and record boundaries (including
//! newlines inside quoted field values and after escapes) are decided
//! by the line-protocol parser alone — the COPY layer never splits
//! records.
//!
//! One COPY statement is therefore one bounded ingestion request; a
//! larger import is multiple COPY statements.
//!
//! The `precision` option carries the same values as the HTTP
//! `precision` query parameter (`ns`, `us`, `ms`, `s`, ...), defaults to
//! nanoseconds, and is resolved by the shared `parse_time_precision`.

use common_grpc::precision::Precision;
use pgwire::error::PgWireResult;

use crate::postgres::copy_in::bad_copy_data;
use crate::postgres::copy_in::codec::CopyFormat;
use crate::postgres::copy_in::options::CopyOptionSet;

/// The format's validated options. A `None` precision carries the
/// pipeline's default (nanoseconds), exactly like an HTTP write without
/// a `precision` parameter.
#[derive(Debug, Clone)]
pub(super) struct InfluxdbOptions {
    pub(super) precision: Option<Precision>,
}

pub(super) fn build_influxdb_codec(mut options: CopyOptionSet) -> PgWireResult<CopyFormat> {
    let precision = match options.take_string("precision")? {
        Some(value) => Some(
            crate::influxdb::parse_time_precision(&value)
                .map_err(|e| bad_copy_data(format!("invalid precision option: {e}")))?,
        ),
        None => None,
    };
    options.reject_leftovers("influxdb")?;
    Ok(CopyFormat::Influxdb(InfluxdbOptions { precision }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::copy_in::options::CopyOptionValue;

    #[test]
    fn test_builder_options() {
        let format = build_influxdb_codec(CopyOptionSet::default()).unwrap();
        assert!(matches!(format, CopyFormat::Influxdb(_)));
        assert!(matches!(format, CopyFormat::Influxdb(_)));

        let mut set = CopyOptionSet::default();
        set.insert("precision".into(), CopyOptionValue::String("ms".into()))
            .unwrap();
        assert!(matches!(
            build_influxdb_codec(set).unwrap(),
            CopyFormat::Influxdb(InfluxdbOptions {
                precision: Some(Precision::Millisecond)
            })
        ));

        let mut set = CopyOptionSet::default();
        set.insert("precision".into(), CopyOptionValue::String("bogus".into()))
            .unwrap();
        assert!(build_influxdb_codec(set).is_err());

        let mut set = CopyOptionSet::default();
        set.insert("unknown".into(), CopyOptionValue::Flag).unwrap();
        assert!(build_influxdb_codec(set).is_err());
    }
}
