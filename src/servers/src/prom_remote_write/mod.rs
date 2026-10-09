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
#[cfg(any(test, feature = "testing"))]
pub mod v2;
#[cfg(not(any(test, feature = "testing")))]
pub(crate) mod v2;
pub mod validation;

use bytes::Bytes;
use common_memory_manager::MemoryGuard;
use lazy_static::lazy_static;
use object_pool::Pool;
use snafu::ResultExt;

use crate::error;
use crate::prom_remote_write::decode::{PromSeriesProcessor, PromWriteRequest};
use crate::prom_remote_write::row_builder::TablesBuilder;
use crate::prom_remote_write::validation::PromValidationMode;
use crate::prom_store::{
    ChargedBuffer, MAX_DECOMPRESSED_REQUEST_SIZE, snappy_decompress_limited,
    zstd_decompress_limited,
};
use crate::request_memory_limiter::ServerMemoryLimiter;
use crate::request_memory_metrics::RequestMemoryMetrics;

/// Prometheus remote write protocol versions, also used as the `version` label
/// of the remote write metrics.
pub const REMOTE_WRITE_V1_VERSION: &str = "1.0";
pub const REMOTE_WRITE_V2_VERSION: &str = "2.0";

lazy_static! {
    static ref PROM_WRITE_REQUEST_POOL: Pool<PromWriteRequest<'static>> =
        Pool::new(256, PromWriteRequest::default);
}

/// Decompresses a remote-write body with the codec selected by `is_zstd`,
/// bounded by the hard decoded-size cap and charged to the aggregate
/// request-memory limiter.
///
/// Due to vmagent's limitation there is a chance that vmagent sends the
/// content-encoding header wrong, so decoding falls back to the other codec
/// when the first attempt fails on malformed input. Size-limit and quota
/// errors are final: the body either expanded beyond the cap under the
/// declared codec, or it could not be admitted, and the other codec cannot
/// produce a different outcome.
///
/// see <https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5301>
/// see <https://github.com/GreptimeTeam/greptimedb/issues/3929>
pub(crate) async fn decompress_remote_write_body(
    is_zstd: bool,
    body: &[u8],
    limiter: &ServerMemoryLimiter,
) -> crate::error::Result<ChargedBuffer> {
    let decompress = |is_zstd: bool| async move {
        if is_zstd {
            zstd_decompress_limited(body, MAX_DECOMPRESSED_REQUEST_SIZE, limiter).await
        } else {
            snappy_decompress_limited(body, MAX_DECOMPRESSED_REQUEST_SIZE, limiter).await
        }
    };

    match decompress(is_zstd).await {
        Ok(buf) => Ok(buf),
        Err(e) => {
            if matches!(
                e,
                crate::error::Error::DecompressSnappyPromRemoteRequest { .. }
                    | crate::error::Error::DecompressZstdPromRemoteRequest { .. }
            ) {
                decompress(!is_zstd).await
            } else {
                Err(e)
            }
        }
    }
}

pub async fn decode_remote_write_request(
    is_zstd: bool,
    body: Bytes,
    prom_validation_mode: PromValidationMode,
    processor: &mut PromSeriesProcessor,
    limiter: &ServerMemoryLimiter,
) -> crate::error::Result<(
    TablesBuilder<'static>,
    Vec<MemoryGuard<RequestMemoryMetrics>>,
)> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_CODEC_ELAPSED
        .with_label_values(&["decode", REMOTE_WRITE_V1_VERSION])
        .start_timer();

    // Holds the memory permits for the decompressed bytes; the caller must
    // keep them alive as long as the returned `TablesBuilder`, which retains
    // the decompressed buffer as its raw data.
    let buf = decompress_remote_write_body(is_zstd, &body[..], limiter).await?;
    // Decompression copied the payload out, so the compressed body is no longer needed.
    drop(body);
    let (data, guards) = buf.into_parts();
    let mut request = PROM_WRITE_REQUEST_POOL.pull(PromWriteRequest::default);

    request
        .decode(data, prom_validation_mode, processor)
        .context(error::DecodePromRemoteRequestSnafu)?;
    let tables = std::mem::take(&mut request.table_data);
    Ok((tables, guards))
}
