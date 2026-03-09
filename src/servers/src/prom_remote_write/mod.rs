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

//! Prometheus remote write support.
//!
//! This module groups validation, row building, and protobuf decoding for
//! the Prometheus remote write API.

pub mod decode;
pub(crate) mod row_builder;
pub(crate) mod types;
pub mod validation;

use bytes::Bytes;
use lazy_static::lazy_static;
use object_pool::Pool;
use snafu::ResultExt;

use crate::error;
use crate::prom_remote_write::decode::{PromSeriesProcessor, PromWriteRequest};
use crate::prom_remote_write::row_builder::TablesBuilder;
use crate::prom_remote_write::validation::PromValidationMode;
use crate::prom_store::{snappy_decompress, zstd_decompress};

lazy_static! {
    static ref PROM_WRITE_REQUEST_POOL: Pool<PromWriteRequest<'static>> =
        Pool::new(256, PromWriteRequest::default);
}

pub fn try_decompress(is_zstd: bool, body: &[u8]) -> crate::error::Result<Vec<u8>> {
    if is_zstd {
        zstd_decompress(body)
    } else {
        snappy_decompress(body)
    }
}

pub fn decode_remote_write_request(
    is_zstd: bool,
    body: Bytes,
    prom_validation_mode: PromValidationMode,
    processor: &mut PromSeriesProcessor,
) -> crate::error::Result<TablesBuilder<'static>> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();

    // due to vmagent's limitation, there is a chance that vmagent is
    // sending content type wrong so we have to apply a fallback with decoding
    // the content in another method.
    //
    // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5301
    // see https://github.com/GreptimeTeam/greptimedb/issues/3929
    let buf = if let Ok(buf) = try_decompress(is_zstd, &body[..]) {
        buf
    } else {
        // fallback to the other compression method
        try_decompress(!is_zstd, &body[..])?
    };

    let mut request = PROM_WRITE_REQUEST_POOL.pull(PromWriteRequest::default);

    request
        .decode(buf, prom_validation_mode, processor)
        .context(error::DecodePromRemoteRequestSnafu)?;
    Ok(std::mem::take(&mut request.table_data))
}
