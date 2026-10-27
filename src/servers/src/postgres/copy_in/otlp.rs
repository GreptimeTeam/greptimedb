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

//! The `otlp` format for COPY FROM STDIN: one binary OTLP
//! `ExportMetricsServiceRequest` payload.
//!
//! `COPY FROM STDIN WITH (FORMAT otlp)` is a GreptimeDB-specific,
//! **binary** format — the only one that advertises wire format code 1
//! in the `CopyInResponse` header. Like the influxdb format it is
//! table-less (the payload's resource metrics route to their own
//! tables) and does not decode here: the whole payload is buffered
//! until `CopyDone` — exactly like an OTLP/HTTP request body, which is
//! also consumed as one message — then decoded and completed through
//! the frontend's OTLP metrics handler, the same path the
//! `/v1/otlp/v1/metrics` endpoint uses (permissions, interceptors,
//! preparation, per-table permission targets and inserts), under the
//! connection's regular postgres query context.
//!
//! One COPY statement is one bounded ingestion request (see
//! [`crate::postgres::copy_in::COPY_IN_MAX_PAYLOAD_BYTES`]); OTLP
//! carries no COPY-specific options.

use pgwire::error::PgWireResult;

use crate::postgres::copy_in::codec::CopyFormat;
use crate::postgres::copy_in::options::CopyOptionSet;

pub(super) fn build_otlp_codec(options: CopyOptionSet) -> PgWireResult<CopyFormat> {
    options.reject_leftovers("otlp")?;
    Ok(CopyFormat::Otlp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::copy_in::options::CopyOptionValue;

    #[test]
    fn test_builder_rejects_options() {
        assert!(matches!(
            build_otlp_codec(CopyOptionSet::default()).unwrap(),
            CopyFormat::Otlp
        ));

        let mut set = CopyOptionSet::default();
        set.insert("precision".into(), CopyOptionValue::String("ns".into()))
            .unwrap();
        assert!(build_otlp_codec(set).is_err());
    }
}
