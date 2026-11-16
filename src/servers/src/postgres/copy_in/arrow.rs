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

//! Standard Arrow IPC streams over COPY, using Flight's bulk ingestion core.
//!
//! Only the IPC encapsulation changes: schema and record-batch metadata/bodies
//! are forwarded intact to the Flight decoder, preserving its single-region
//! fast path, compression, schema evolution and write semantics. COPY returns
//! one final row count, not Flight's per-batch request IDs or timings.

use std::sync::Arc;

use arrow_flight::{FlightData, FlightDescriptor};
use arrow_ipc::MessageHeader;
use bytes::{Buf, Bytes};
use common_grpc::flight::do_put::DoPutResponse;
use common_memory_manager::MemoryGuard;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use pgwire::error::PgWireResult;
use session::context::QueryContextRef;
use snafu::IntoError;
use table::requests::AUTO_CREATE_TABLE_KEY;

use crate::grpc::flight::PutRecordBatchRequestStream;
use crate::postgres::copy_in::codec::CopyFormat;
use crate::postgres::copy_in::options::CopyOptionSet;
use crate::postgres::copy_in::{bad_copy_data, unsupported};
use crate::postgres::utils::convert_err;
use crate::query_handler::CopyInHandlerRef;
use crate::request_memory_limiter::ServerMemoryLimiter;
use crate::request_memory_metrics::RequestMemoryMetrics;

/// Flight's ingestion hints, scoped to one COPY rather than the connection.
#[derive(Debug, Clone, Default)]
pub(crate) struct ArrowOptions {
    auto_create_table: Option<bool>,
    insert_skip_wal: Option<bool>,
}

pub(crate) fn build_arrow_codec(mut options: CopyOptionSet) -> PgWireResult<CopyFormat> {
    let result = ArrowOptions {
        auto_create_table: options.take_flag("auto_create_table")?,
        insert_skip_wal: options.take_flag("insert_skip_wal")?,
    };
    options.reject_leftovers("arrow")?;
    Ok(CopyFormat::Arrow(result))
}

impl ArrowOptions {
    pub(crate) fn query_context(&self, ctx: &QueryContextRef) -> QueryContextRef {
        let mut ctx = ctx.fork();
        if let Some(value) = self.auto_create_table {
            ctx.set_extension(AUTO_CREATE_TABLE_KEY, value.to_string());
        }
        if let Some(value) = self.insert_skip_wal {
            ctx.set_skip_wal(value);
        }
        Arc::new(ctx)
    }
}

/// One IPC stream and one lazy Flight ingestion stream. No background task is
/// needed: each completed batch drives the handler to its acknowledgement before
/// accepting the next batch. Dropping COPY drops the decoder and pending writes.
pub(crate) struct ArrowCopyState {
    table: String,
    buffer: Vec<u8>,
    seen_schema: bool,
    ended: bool,
    input: mpsc::UnboundedSender<Result<FlightData, tonic::Status>>,
    output: BoxStream<'static, crate::error::Result<DoPutResponse>>,
    limiter: ServerMemoryLimiter,
    guards: Vec<MemoryGuard<RequestMemoryMetrics>>,
    charged: u64,
    // Schema/dictionaries live in FlightDecoder across batches. Retain their
    // reservations until COPY ends, including replaced dictionaries (conservative).
    retained_guards: Vec<MemoryGuard<RequestMemoryMetrics>>,
    retained_bytes: u64,
}

impl ArrowCopyState {
    pub(crate) async fn new(
        table: String,
        handler: &CopyInHandlerRef,
        ctx: QueryContextRef,
        limiter: ServerMemoryLimiter,
    ) -> PgWireResult<Self> {
        let (input, receiver) = mpsc::unbounded();
        // COPY reserves raw frames and decompression memory before forwarding;
        // avoid charging the same body again in the Flight transport adapter.
        let stream = PutRecordBatchRequestStream::new(
            receiver,
            ctx.current_catalog().to_string(),
            ctx.current_schema(),
            None,
        )
        .await
        .map_err(|e| bad_copy_data(e.to_string()))?;
        let output = handler.copy_in_arrow(stream, ctx).map_err(convert_err)?;
        Ok(Self {
            table,
            buffer: Vec::new(),
            seen_schema: false,
            ended: false,
            input,
            output,
            limiter,
            guards: Vec::new(),
            charged: 0,
            retained_guards: Vec::new(),
            retained_bytes: 0,
        })
    }

    /// Reserves before buffering or decompressing, with Flight's fail-fast
    /// admission policy. Waiting while retaining partial frames can deadlock
    /// multiple connections growing their requests against the same budget.
    fn reserve(&mut self, bytes: u64) -> PgWireResult<()> {
        let additional = bytes.saturating_sub(self.charged);
        if additional == 0 {
            return Ok(());
        }
        let granted = additional
            .checked_add(1023)
            .map(|bytes| bytes / 1024 * 1024)
            .ok_or_else(|| bad_copy_data("Arrow memory reservation overflow"))?;
        let held = self
            .charged
            .checked_add(self.retained_bytes)
            .ok_or_else(|| bad_copy_data("Arrow memory reservation overflow"))?;
        self.limiter
            .check_request_fits(granted, held)
            .map_err(convert_err)?;
        let guard = self.limiter.try_acquire(additional).ok_or_else(|| {
            convert_err(crate::error::MemoryLimitExceededSnafu.into_error(
                common_memory_manager::Error::MemoryLimitExceeded {
                    requested_bytes: additional,
                    limit_bytes: self.limiter.limit_bytes(),
                },
            ))
        })?;
        self.guards.push(guard);
        self.charged += granted;
        Ok(())
    }

    /// Consumes arbitrary CopyData boundaries, retaining at most one incomplete
    /// frame plus the stream schema/dictionaries. The import itself has no total
    /// byte limit; every completed record batch is written immediately.
    pub(crate) async fn feed(&mut self, mut data: &[u8]) -> PgWireResult<u64> {
        let mut rows = 0;
        while !data.is_empty() {
            if self.ended {
                return Err(bad_copy_data("data after Arrow IPC end-of-stream marker"));
            }
            let target = frame_target(&self.buffer)?;
            // Allow for the encoded frame and decoder-owned copies/alignment.
            self.reserve((target as u64).saturating_mul(2))?;
            let take = (target - self.buffer.len()).min(data.len());
            self.buffer.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.buffer.len() < target {
                continue;
            }
            let Some((prefix, metadata_len)) = frame_prefix(&self.buffer)? else {
                continue;
            };
            if metadata_len == 0 {
                if !self.seen_schema {
                    return Err(bad_copy_data("Arrow IPC stream requires a schema"));
                }
                self.ended = true;
                self.buffer.clear();
                self.guards.clear();
                self.charged = 0;
                continue;
            }
            if self.buffer.len() < prefix + metadata_len {
                continue;
            }
            let message = arrow_ipc::root_as_message(&self.buffer[prefix..prefix + metadata_len])
                .map_err(|e| bad_copy_data(format!("invalid Arrow IPC metadata: {e}")))?;
            let body_len = usize::try_from(message.bodyLength())
                .map_err(|_| bad_copy_data("invalid Arrow IPC body length"))?;
            let total = (prefix + metadata_len)
                .checked_add(body_len)
                .ok_or_else(|| bad_copy_data("Arrow IPC frame length overflow"))?;
            if self.buffer.len() < total {
                continue;
            }
            let kind = message.header_type();
            if !self.seen_schema && kind != MessageHeader::Schema {
                return Err(bad_copy_data("first Arrow IPC message must be a schema"));
            }
            match kind {
                MessageHeader::Schema if !self.seen_schema => {}
                MessageHeader::DictionaryBatch | MessageHeader::RecordBatch => {}
                _ => return Err(unsupported("expected one Arrow schema followed by batches")),
            }
            let expanded = decompressed_size(&message, &self.buffer[prefix + metadata_len..])?;
            self.reserve((total as u64).saturating_mul(2).saturating_add(expanded))?;
            let bytes = Bytes::from(std::mem::take(&mut self.buffer));
            let flight_data = FlightData {
                flight_descriptor: (kind == MessageHeader::Schema)
                    .then(|| FlightDescriptor::new_path(vec![self.table.clone()])),
                data_header: bytes.slice(prefix..prefix + metadata_len),
                data_body: bytes.slice(prefix + metadata_len..),
                ..Default::default()
            };
            self.input
                .unbounded_send(Ok(flight_data))
                .map_err(|_| bad_copy_data("Arrow ingestion stream closed"))?;
            if kind == MessageHeader::RecordBatch {
                let response = self
                    .output
                    .next()
                    .await
                    .ok_or_else(|| {
                        bad_copy_data("Arrow ingestion stream ended before acknowledgement")
                    })?
                    .map_err(convert_err)?;
                rows += response.affected_rows() as u64;
                self.guards.clear();
                self.charged = 0;
            } else {
                self.seen_schema = true;
                self.retained_guards.append(&mut self.guards);
                self.retained_bytes += std::mem::take(&mut self.charged);
            }
        }
        Ok(rows)
    }

    pub(crate) async fn finish(&mut self) -> PgWireResult<u64> {
        if !self.seen_schema || !self.buffer.is_empty() {
            return Err(bad_copy_data("incomplete Arrow IPC stream"));
        }
        // IPC permits clean EOF without the optional end-of-stream marker.
        self.input.close_channel();
        let mut rows = 0;
        while let Some(response) = self.output.next().await {
            rows += response.map_err(convert_err)?.affected_rows() as u64;
        }
        Ok(rows)
    }
}

/// Reads both modern continuation-prefixed and legacy IPC message lengths.
fn frame_prefix(buffer: &[u8]) -> PgWireResult<Option<(usize, usize)>> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let mut input = buffer;
    let first = input.get_i32_le();
    let (prefix, length) = if first == -1 {
        if input.len() < 4 {
            return Ok(None);
        }
        (8, input.get_i32_le())
    } else {
        (4, first)
    };
    let length =
        usize::try_from(length).map_err(|_| bad_copy_data("invalid Arrow IPC metadata length"))?;
    Ok(Some((prefix, length)))
}

/// The next framing boundary, so a large CopyData never queues many batches.
fn frame_target(buffer: &[u8]) -> PgWireResult<usize> {
    if buffer.len() < 4 {
        return Ok(4);
    }
    let Some((prefix, length)) = frame_prefix(buffer)? else {
        return Ok(8);
    };
    let metadata_end = prefix + length;
    if buffer.len() < metadata_end || length == 0 {
        return Ok(metadata_end);
    }
    let message = arrow_ipc::root_as_message(&buffer[prefix..metadata_end])
        .map_err(|e| bad_copy_data(format!("invalid Arrow IPC metadata: {e}")))?;
    let body = usize::try_from(message.bodyLength())
        .map_err(|_| bad_copy_data("invalid Arrow IPC body length"))?;
    metadata_end
        .checked_add(body)
        .ok_or_else(|| bad_copy_data("Arrow IPC frame length overflow"))
}

/// Compressed IPC buffers carry their uncompressed length in an i64 prefix.
/// Reserve the expansion before FlightDecoder is allowed to allocate it.
fn decompressed_size(message: &arrow_ipc::Message<'_>, body: &[u8]) -> PgWireResult<u64> {
    let batch = match message.header_type() {
        MessageHeader::RecordBatch => message.header_as_record_batch(),
        MessageHeader::DictionaryBatch => {
            message.header_as_dictionary_batch().and_then(|b| b.data())
        }
        _ => None,
    };
    let Some(batch) = batch else {
        return Ok(0);
    };
    if batch.compression().is_none() {
        return Ok(0);
    }
    let mut total = 0u64;
    if let Some(buffers) = batch.buffers() {
        for buffer in buffers {
            if buffer.length() == 0 {
                continue;
            }
            if buffer.length() < 8 {
                return Err(bad_copy_data("truncated compressed Arrow buffer"));
            }
            let start = usize::try_from(buffer.offset())
                .map_err(|_| bad_copy_data("invalid Arrow buffer offset"))?;
            let length = usize::try_from(buffer.length())
                .map_err(|_| bad_copy_data("invalid Arrow buffer length"))?;
            if start.checked_add(length).is_none_or(|end| end > body.len()) {
                return Err(bad_copy_data("truncated compressed Arrow buffer"));
            }
            let end = start
                .checked_add(8)
                .ok_or_else(|| bad_copy_data("invalid Arrow buffer offset"))?;
            let mut prefix = body
                .get(start..end)
                .ok_or_else(|| bad_copy_data("truncated compressed Arrow buffer"))?;
            let length = prefix.get_i64_le();
            if length < -1 {
                return Err(bad_copy_data("invalid compressed Arrow buffer length"));
            }
            total = total
                .checked_add(length.max(0) as u64)
                .ok_or_else(|| bad_copy_data("Arrow decompression size overflow"))?;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_handler::CopyInHandler;
    use arrow::array::{Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use async_trait::async_trait;
    use common_query::Output;

    struct Handler;

    #[async_trait]
    impl CopyInHandler for Handler {
        async fn copy_in_table(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: QueryContextRef,
        ) -> crate::error::Result<Option<table::metadata::TableInfoRef>> {
            Ok(None)
        }

        async fn copy_in_insert(
            &self,
            _: api::v1::RowInsertRequests,
            _: QueryContextRef,
        ) -> crate::error::Result<Output> {
            panic!("Arrow must not use row ingestion")
        }

        fn copy_in_arrow(
            &self,
            stream: PutRecordBatchRequestStream,
            _: QueryContextRef,
        ) -> crate::error::Result<BoxStream<'static, crate::error::Result<DoPutResponse>>> {
            Ok(stream
                .map(|request| {
                    let request = request.map_err(|e| {
                        crate::error::InvalidParameterSnafu {
                            reason: e.to_string(),
                        }
                        .build()
                    })?;
                    assert_eq!(request.table_name.table_name, "test");
                    assert_eq!(request.record_batch, batch());
                    Ok(DoPutResponse::new(0, request.record_batch.num_rows(), 0.0))
                })
                .boxed())
        }
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["a", "b"])) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn ipc(compression: Option<arrow_ipc::CompressionType>, batches: usize) -> Vec<u8> {
        let batch = batch();
        let options = IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
        for _ in 0..batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        writer.into_inner().unwrap()
    }

    async fn state(limit: u64) -> ArrowCopyState {
        ArrowCopyState::new(
            "test".into(),
            &(Arc::new(Handler) as CopyInHandlerRef),
            session::context::QueryContext::arc(),
            ServerMemoryLimiter::new(limit, Default::default()),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn chunked_ipc_and_compression() {
        for compression in [
            None,
            Some(arrow_ipc::CompressionType::LZ4_FRAME),
            Some(arrow_ipc::CompressionType::ZSTD),
        ] {
            let bytes = ipc(compression, 3);
            for chunk in [1, 2, 7, 64, bytes.len()] {
                let mut state = state(16 * 1024 * 1024).await;
                let mut rows = 0;
                for data in bytes.chunks(chunk) {
                    rows += state.feed(data).await.unwrap();
                }
                rows += state.finish().await.unwrap();
                assert_eq!(rows, 6, "chunk={chunk}, compression={compression:?}");
                assert_eq!(state.charged, 0);
            }
        }
    }

    #[tokio::test]
    async fn legacy_ipc_framing() {
        let batch = batch();
        let options = IpcWriteOptions::try_new(8, true, arrow_ipc::MetadataVersion::V4).unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let bytes = writer.into_inner().unwrap();
        let mut copy = state(1024 * 1024).await;
        let mut rows = 0;
        for chunk in bytes.chunks(3) {
            rows += copy.feed(chunk).await.unwrap();
        }
        rows += copy.finish().await.unwrap();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn empty_truncated_and_trailing_data() {
        let mut empty = state(1024 * 1024).await;
        assert!(empty.finish().await.is_err());
        let bytes = ipc(None, 0);
        let mut schema_only = state(1024 * 1024).await;
        assert_eq!(schema_only.feed(&bytes).await.unwrap(), 0);
        assert_eq!(schema_only.finish().await.unwrap(), 0);
        assert!(schema_only.feed(b"extra").await.is_err());
        let bytes = ipc(None, 1);
        let mut truncated = state(1024 * 1024).await;
        truncated.feed(&bytes[..bytes.len() - 10]).await.unwrap();
        assert!(truncated.finish().await.is_err());
        let mut eof = state(1024 * 1024).await;
        assert_eq!(eof.feed(&bytes[..bytes.len() - 8]).await.unwrap(), 2);
        assert_eq!(eof.finish().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn malformed_lengths_and_memory_quota() {
        // Even an unlimited limiter must reject arithmetic overflow.
        let mut unlimited = state(0).await;
        assert!(unlimited.reserve(u64::MAX).is_err());
        let mut copy = state(1024).await;
        let error = copy.feed(&ipc(None, 1)).await.unwrap_err();
        assert!(!error.to_string().is_empty());
        let mut copy = state(1024).await;
        assert!(
            copy.feed(&[255, 255, 255, 255, 254, 255, 255, 255])
                .await
                .is_err()
        );
        let mut copy = state(1024).await;
        assert!(
            copy.feed(&[255, 255, 255, 255, 255, 255, 255, 127])
                .await
                .is_ok()
        );
        // Declared metadata length is admitted before reading/allocating it.
        assert!(copy.feed(&[0]).await.is_err());
    }

    #[tokio::test]
    async fn streaming_releases_batch_memory() {
        let mut copy = state(16 * 1024).await;
        let limiter = copy.limiter.clone();
        let bytes = ipc(None, 100);
        assert!(bytes.len() > limiter.limit_bytes() as usize);
        let mut rows = 0;
        for chunk in bytes.chunks(37) {
            rows += copy.feed(chunk).await.unwrap();
        }
        rows += copy.finish().await.unwrap();
        assert_eq!(rows, 200);
        drop(copy);
        assert_eq!(limiter.used_bytes(), 0);

        let mut incomplete = state(16 * 1024).await;
        let limiter = incomplete.limiter.clone();
        incomplete.feed(&bytes[..100]).await.unwrap();
        assert!(limiter.used_bytes() > 0);
        drop(incomplete);
        assert_eq!(limiter.used_bytes(), 0);
    }

    #[tokio::test]
    async fn decompression_is_admitted_before_decode() {
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
            .unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let bytes = writer.into_inner().unwrap();
        assert!(bytes.len() < 64 * 1024);
        let mut copy = state(64 * 1024).await;
        let limiter = copy.limiter.clone();
        assert!(copy.feed(&bytes).await.is_err());
        drop(copy);
        assert_eq!(limiter.used_bytes(), 0);
    }

    #[test]
    fn options_are_scoped_to_copy() {
        use crate::postgres::copy_in::copy_from_stdin_from_sql;
        let stmt = copy_from_stdin_from_sql(
            "COPY t FROM STDIN WITH (FORMAT arrow, auto_create_table false, insert_skip_wal true)",
        )
        .unwrap()
        .unwrap();
        let CopyFormat::Arrow(options) = stmt.format else {
            panic!("expected arrow")
        };
        let original = session::context::QueryContext::arc();
        let ctx = options.query_context(&original);
        assert_eq!(ctx.extension(AUTO_CREATE_TABLE_KEY), Some("false"));
        assert!(ctx.skip_wal());
        assert!(!original.skip_wal());
        assert!(original.extension(AUTO_CREATE_TABLE_KEY).is_none());
        for option in [
            "header",
            "pipeline_name 'p'",
            "auto_create_table 'false'",
            "insert_skip_wal 1",
        ] {
            assert!(
                copy_from_stdin_from_sql(&format!(
                    "COPY t FROM STDIN WITH (FORMAT arrow, {option})"
                ))
                .is_err()
            );
        }
    }
}
