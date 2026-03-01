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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::slice;

use api::prom_store::remote::Sample;
use bytes::{Buf, Bytes};
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_telemetry::warn;
use pipeline::{ContextReq, GreptimePipelineParams, PipelineContext, PipelineDefinition};
use prost::DecodeError;
use prost::encoding::message::merge;
use prost::encoding::{WireType, decode_key, decode_varint};
use session::context::QueryContextRef;
use snafu::OptionExt;
use vrl::prelude::NotNan;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::InternalSnafu;
use crate::http::PromValidationMode;
use crate::http::event::PipelineIngestRequest;
use crate::pipeline::run_pipeline;
use crate::prom_row_builder::{PromCtx, TablesBuilder};
use crate::prom_store::{
    DATABASE_LABEL_ALT_BYTES, DATABASE_LABEL_BYTES, METRIC_NAME_LABEL_BYTES,
    PHYSICAL_TABLE_LABEL_ALT_BYTES, PHYSICAL_TABLE_LABEL_BYTES,
};
use crate::query_handler::PipelineHandlerRef;
use crate::repeated_field::{Clear, RepeatedField};

pub type RawBytes = &'static [u8];

impl Clear for Sample {
    fn clear(&mut self) {
        self.timestamp = 0;
        self.value = 0.0;
    }
}

#[derive(Default, Clone, Debug)]
pub struct PromLabel {
    pub name: RawBytes,
    pub value: RawBytes,
}

impl Clear for PromLabel {
    fn clear(&mut self) {
        self.name.clear();
        self.value.clear();
    }
}

impl PromLabel {
    pub fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut Bytes,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromLabel";
        match tag {
            1u32 => {
                // decode label name
                let value = &mut self.name;
                merge_bytes(value, buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "name");
                    error
                })
            }
            2u32 => {
                // decode label value
                let value = &mut self.value;
                merge_bytes(value, buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "value");
                    error
                })
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, Default::default()),
        }
    }
}

/// Reads a variable-length encoded bytes field from `src` and assign it to `dst`.
/// # Safety
/// Callers must ensure `src` outlives `dst`.
#[inline(always)]
fn merge_bytes(dst: &mut RawBytes, src: &mut Bytes) -> Result<(), DecodeError> {
    let len = decode_varint(src)? as usize;
    if len > src.remaining() {
        return Err(DecodeError::new(format!(
            "buffer underflow, len: {}, remaining: {}",
            len,
            src.remaining()
        )));
    }
    *dst = unsafe { slice::from_raw_parts(src.as_ptr(), len) };
    src.advance(len);
    Ok(())
}

#[derive(Default, Debug)]
pub struct PromTimeSeries {
    pub table_name: String,
    // specified using `__database__` label
    pub schema: Option<String>,
    // specified using `__physical_table__` label
    pub physical_table: Option<String>,

    pub labels: RepeatedField<PromLabel>,
    pub samples: RepeatedField<Sample>,
}

impl Clear for PromTimeSeries {
    fn clear(&mut self) {
        self.table_name.clear();
        self.labels.clear();
        self.samples.clear();
    }
}

impl PromTimeSeries {
    pub fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut Bytes,
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromTimeSeries";
        match tag {
            1u32 => {
                // decode labels
                let label = self.labels.push_default();

                let len = decode_varint(buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "labels");
                    error
                })?;
                let remaining = buf.remaining();
                if len > remaining as u64 {
                    return Err(DecodeError::new("buffer underflow"));
                }

                let limit = remaining - len as usize;
                while buf.remaining() > limit {
                    let (tag, wire_type) = decode_key(buf)?;
                    label.merge_field(tag, wire_type, buf)?;
                }
                if buf.remaining() != limit {
                    return Err(DecodeError::new("delimited length exceeded"));
                }

                match label.name {
                    METRIC_NAME_LABEL_BYTES => {
                        self.table_name = prom_validation_mode.decode_string(label.value)?;
                        self.labels.truncate(self.labels.len() - 1); // remove last label
                    }
                    #[allow(deprecated)]
                    crate::prom_store::SCHEMA_LABEL_BYTES => {
                        self.schema = Some(prom_validation_mode.decode_string(label.value)?);
                        self.labels.truncate(self.labels.len() - 1); // remove last label
                    }
                    DATABASE_LABEL_BYTES | DATABASE_LABEL_ALT_BYTES => {
                        // Only set schema from __database__ if __schema__ hasn't been set yet
                        if self.schema.is_none() {
                            self.schema = Some(prom_validation_mode.decode_string(label.value)?);
                        }
                        self.labels.truncate(self.labels.len() - 1); // remove last label
                    }
                    PHYSICAL_TABLE_LABEL_BYTES | PHYSICAL_TABLE_LABEL_ALT_BYTES => {
                        self.physical_table =
                            Some(prom_validation_mode.decode_string(label.value)?);
                        self.labels.truncate(self.labels.len() - 1); // remove last label
                    }
                    _ => {}
                }

                Ok(())
            }
            2u32 => {
                let sample = self.samples.push_default();
                merge(WireType::LengthDelimited, sample, buf, Default::default()).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "samples");
                        error
                    },
                )?;
                Ok(())
            }
            // todo(hl): exemplars are skipped temporarily
            3u32 => prost::encoding::skip_field(wire_type, tag, buf, Default::default()),
            _ => prost::encoding::skip_field(wire_type, tag, buf, Default::default()),
        }
    }

    fn add_to_table_data(
        &mut self,
        table_builders: &mut TablesBuilder,
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        let label_num = self.labels.len();
        let row_num = self.samples.len();

        let prom_ctx = PromCtx {
            schema: self.schema.take(),
            physical_table: self.physical_table.take(),
        };

        let table_data = table_builders.get_or_create_table_builder(
            prom_ctx,
            std::mem::take(&mut self.table_name),
            label_num,
            row_num,
        );
        table_data.add_labels_and_samples(
            self.labels.as_slice(),
            self.samples.as_slice(),
            prom_validation_mode,
        )?;

        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct PromWriteRequest {
    pub(crate) table_data: TablesBuilder,
    series: PromTimeSeries,
}

impl Clear for PromWriteRequest {
    fn clear(&mut self) {
        self.table_data.clear();
    }
}

impl PromWriteRequest {
    pub fn as_row_insert_requests(&mut self) -> ContextReq {
        self.table_data.as_insert_requests()
    }

    // todo(hl): maybe use &[u8] can reduce the overhead introduced with Bytes.
    pub fn merge(
        &mut self,
        mut buf: Bytes,
        prom_validation_mode: PromValidationMode,
        processor: &mut PromSeriesProcessor,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromWriteRequest";
        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            assert_eq!(WireType::LengthDelimited, wire_type);
            match tag {
                1u32 => {
                    // decode TimeSeries
                    let len = decode_varint(&mut buf).map_err(|mut e| {
                        e.push(STRUCT_NAME, "timeseries");
                        e
                    })?;
                    let remaining = buf.remaining();
                    if len > remaining as u64 {
                        return Err(DecodeError::new("buffer underflow"));
                    }

                    let limit = remaining - len as usize;
                    while buf.remaining() > limit {
                        let (tag, wire_type) = decode_key(&mut buf)?;
                        self.series
                            .merge_field(tag, wire_type, &mut buf, prom_validation_mode)?;
                    }
                    if buf.remaining() != limit {
                        return Err(DecodeError::new("delimited length exceeded"));
                    }

                    if processor.use_pipeline {
                        processor.consume_series_to_pipeline_map(
                            &mut self.series,
                            prom_validation_mode,
                        )?;
                    } else {
                        self.series
                            .add_to_table_data(&mut self.table_data, prom_validation_mode)?;
                    }

                    // clear state
                    self.series.labels.clear();
                    self.series.samples.clear();
                }
                3u32 => {
                    // todo(hl): metadata are skipped.
                    prost::encoding::skip_field(wire_type, tag, &mut buf, Default::default())?;
                }
                _ => prost::encoding::skip_field(wire_type, tag, &mut buf, Default::default())?,
            }
        }

        Ok(())
    }
}

/// A hook to be injected into the PromWriteRequest decoding process.
/// It was originally designed with two usage:
/// 1. consume one series to desired type, in this case, the pipeline map
/// 2. convert itself to RowInsertRequests
///
/// Since the origin conversion is coupled with PromWriteRequest,
/// let's keep it that way for now.
pub struct PromSeriesProcessor {
    pub(crate) use_pipeline: bool,
    pub(crate) table_values: BTreeMap<String, Vec<VrlValue>>,

    // optional fields for pipeline
    pub(crate) pipeline_handler: Option<PipelineHandlerRef>,
    pub(crate) query_ctx: Option<QueryContextRef>,
    pub(crate) pipeline_def: Option<PipelineDefinition>,
}

impl PromSeriesProcessor {
    pub fn default_processor() -> Self {
        Self {
            use_pipeline: false,
            table_values: BTreeMap::new(),
            pipeline_handler: None,
            query_ctx: None,
            pipeline_def: None,
        }
    }

    pub fn set_pipeline(
        &mut self,
        handler: PipelineHandlerRef,
        query_ctx: QueryContextRef,
        pipeline_def: PipelineDefinition,
    ) {
        self.use_pipeline = true;
        self.pipeline_handler = Some(handler);
        self.query_ctx = Some(query_ctx);
        self.pipeline_def = Some(pipeline_def);
    }

    // convert one series to pipeline map
    pub(crate) fn consume_series_to_pipeline_map(
        &mut self,
        series: &mut PromTimeSeries,
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        let mut vec_pipeline_map = Vec::new();
        let mut pipeline_map = BTreeMap::new();
        for l in series.labels.iter() {
            let name = prom_validation_mode.decode_string(l.name)?;
            let value = prom_validation_mode.decode_string(l.value)?;
            pipeline_map.insert(KeyString::from(name), VrlValue::Bytes(value.into()));
        }

        let one_sample = series.samples.len() == 1;

        for s in series.samples.iter() {
            let Ok(value) = NotNan::new(s.value) else {
                warn!("Invalid float value: {}", s.value);
                continue;
            };

            let timestamp = s.timestamp;
            pipeline_map.insert(
                KeyString::from(greptime_timestamp()),
                VrlValue::Integer(timestamp),
            );
            pipeline_map.insert(KeyString::from(greptime_value()), VrlValue::Float(value));
            if one_sample {
                vec_pipeline_map.push(VrlValue::Object(pipeline_map));
                break;
            } else {
                vec_pipeline_map.push(VrlValue::Object(pipeline_map.clone()));
            }
        }

        let table_name = std::mem::take(&mut series.table_name);
        match self.table_values.entry(table_name) {
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().append(&mut vec_pipeline_map);
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(vec_pipeline_map);
            }
        }

        Ok(())
    }

    pub(crate) async fn exec_pipeline(&mut self) -> crate::error::Result<ContextReq> {
        // prepare params
        let handler = self.pipeline_handler.as_ref().context(InternalSnafu {
            err_msg: "pipeline handler is not set",
        })?;
        let pipeline_def = self.pipeline_def.as_ref().context(InternalSnafu {
            err_msg: "pipeline definition is not set",
        })?;
        let pipeline_param = GreptimePipelineParams::default();
        let query_ctx = self.query_ctx.as_ref().context(InternalSnafu {
            err_msg: "query context is not set",
        })?;

        let pipeline_ctx = PipelineContext::new(pipeline_def, &pipeline_param, query_ctx.channel());

        // run pipeline
        let mut req = ContextReq::default();
        let table_values = std::mem::take(&mut self.table_values);
        for (table_name, pipeline_maps) in table_values.into_iter() {
            let pipeline_req = PipelineIngestRequest {
                table: table_name,
                values: pipeline_maps,
            };
            let row_req =
                run_pipeline(handler, &pipeline_ctx, pipeline_req, query_ctx, true).await?;
            req.merge(row_req);
        }

        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::prom_store::remote::WriteRequest;
    use api::v1::{Row, RowInsertRequests, Rows};
    use bytes::Bytes;
    use prost::Message;

    use crate::http::PromValidationMode;
    use crate::prom_store::to_grpc_row_insert_requests;
    use crate::proto::{PromSeriesProcessor, PromWriteRequest};
    use crate::repeated_field::Clear;

    fn sort_rows(rows: Rows) -> Rows {
        let permutation =
            permutation::sort_by_key(&rows.schema, |schema| schema.column_name.clone());
        let schema = permutation.apply_slice(&rows.schema);
        let mut inner_rows = vec![];
        for row in rows.rows {
            let values = permutation.apply_slice(&row.values);
            inner_rows.push(Row { values });
        }
        Rows {
            schema,
            rows: inner_rows,
        }
    }

    fn check_deserialized(
        prom_write_request: &mut PromWriteRequest,
        data: &Bytes,
        expected_samples: usize,
        expected_rows: &RowInsertRequests,
    ) {
        let mut p = PromSeriesProcessor::default_processor();
        prom_write_request.clear();
        prom_write_request
            .merge(data.clone(), PromValidationMode::Strict, &mut p)
            .unwrap();

        let req = prom_write_request.as_row_insert_requests();

        let samples = req
            .ref_all_req()
            .filter_map(|r| r.rows.as_ref().map(|r| r.rows.len()))
            .sum::<usize>();
        let prom_rows = RowInsertRequests {
            inserts: req.all_req().collect::<Vec<_>>(),
        };

        assert_eq!(expected_samples, samples);
        assert_eq!(expected_rows.inserts.len(), prom_rows.inserts.len());

        let expected_rows_map = expected_rows
            .inserts
            .iter()
            .map(|insert| (insert.table_name.clone(), insert.rows.clone().unwrap()))
            .collect::<HashMap<_, _>>();

        for r in &prom_rows.inserts {
            // check value
            let expected_rows = expected_rows_map.get(&r.table_name).unwrap().clone();
            assert_eq!(sort_rows(expected_rows), sort_rows(r.rows.clone().unwrap()));
        }
    }

    // Ensures `WriteRequest` and `PromWriteRequest` produce the same gRPC request.
    #[test]
    fn test_decode_write_request() {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("benches");
        d.push("write_request.pb.data");
        let data = Bytes::from(std::fs::read(d).unwrap());

        let (expected_rows, expected_samples) =
            to_grpc_row_insert_requests(&WriteRequest::decode(data.clone()).unwrap()).unwrap();

        let mut prom_write_request = PromWriteRequest::default();
        for _ in 0..3 {
            check_deserialized(
                &mut prom_write_request,
                &data,
                expected_samples,
                &expected_rows,
            );
        }
    }

    #[test]
    fn test_decode_string_strict_mode_valid_utf8() {
        let valid_utf8 = Bytes::from("hello world");
        let result = PromValidationMode::Strict.decode_string(&valid_utf8);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world");
    }

    #[test]
    fn test_decode_string_strict_mode_empty() {
        let empty = Bytes::new();
        let result = PromValidationMode::Strict.decode_string(&empty);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_decode_string_strict_mode_unicode() {
        let unicode = Bytes::from("Hello 世界 🌍");
        let result = PromValidationMode::Strict.decode_string(&unicode);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello 世界 🌍");
    }

    #[test]
    fn test_decode_string_strict_mode_invalid_utf8() {
        // Invalid UTF-8 sequence
        let invalid_utf8 = Bytes::from(vec![0xFF, 0xFE, 0xFD]);
        let result = PromValidationMode::Strict.decode_string(&invalid_utf8);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "failed to decode Protobuf message: invalid utf-8"
        );
    }

    #[test]
    fn test_decode_string_strict_mode_incomplete_utf8() {
        // Incomplete UTF-8 sequence (missing continuation bytes)
        let incomplete_utf8 = Bytes::from(vec![0xC2]); // Start of 2-byte sequence but missing second byte
        let result = PromValidationMode::Strict.decode_string(&incomplete_utf8);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "failed to decode Protobuf message: invalid utf-8"
        );
    }

    #[test]
    fn test_decode_string_lossy_mode_valid_utf8() {
        let valid_utf8 = Bytes::from("hello world");
        let result = PromValidationMode::Lossy.decode_string(&valid_utf8);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world");
    }

    #[test]
    fn test_decode_string_lossy_mode_empty() {
        let empty = Bytes::new();
        let result = PromValidationMode::Lossy.decode_string(&empty);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_decode_string_lossy_mode_unicode() {
        let unicode = Bytes::from("Hello 世界 🌍");
        let result = PromValidationMode::Lossy.decode_string(&unicode);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello 世界 🌍");
    }

    #[test]
    fn test_decode_string_lossy_mode_invalid_utf8() {
        // Invalid UTF-8 sequence - should be replaced with replacement character
        let invalid_utf8 = Bytes::from(vec![0xFF, 0xFE, 0xFD]);
        let result = PromValidationMode::Lossy.decode_string(&invalid_utf8);
        assert!(result.is_ok());
        // Each invalid byte should be replaced with the Unicode replacement character
        assert_eq!(result.unwrap(), "���");
    }

    #[test]
    fn test_decode_string_lossy_mode_mixed_valid_invalid() {
        // Mix of valid and invalid UTF-8
        let mut mixed = Vec::new();
        mixed.extend_from_slice(b"hello");
        mixed.push(0xFF); // Invalid byte
        mixed.extend_from_slice(b"world");
        let mixed_utf8 = Bytes::from(mixed);

        let result = PromValidationMode::Lossy.decode_string(&mixed_utf8);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello�world");
    }

    #[test]
    fn test_decode_string_unchecked_mode_valid_utf8() {
        let valid_utf8 = Bytes::from("hello world");
        let result = PromValidationMode::Unchecked.decode_string(&valid_utf8);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world");
    }

    #[test]
    fn test_decode_string_unchecked_mode_empty() {
        let empty = Bytes::new();
        let result = PromValidationMode::Unchecked.decode_string(&empty);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_decode_string_unchecked_mode_unicode() {
        let unicode = Bytes::from("Hello 世界 🌍");
        let result = PromValidationMode::Unchecked.decode_string(&unicode);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello 世界 🌍");
    }

    #[test]
    fn test_decode_string_unchecked_mode_invalid_utf8() {
        // Invalid UTF-8 sequence - unchecked mode doesn't validate
        let invalid_utf8 = Bytes::from(vec![0xFF, 0xFE, 0xFD]);
        let result = PromValidationMode::Unchecked.decode_string(&invalid_utf8);
        // This should succeed but the resulting string may contain invalid UTF-8
        assert!(result.is_ok());
        // We can't easily test the exact content since it's invalid UTF-8,
        // but we can verify it doesn't panic and returns something
        let _string = result.unwrap();
    }

    #[test]
    fn test_decode_string_all_modes_ascii() {
        let ascii = Bytes::from("simple_ascii_123");

        // All modes should handle ASCII identically
        let strict_result = PromValidationMode::Strict.decode_string(&ascii).unwrap();
        let lossy_result = PromValidationMode::Lossy.decode_string(&ascii).unwrap();
        let unchecked_result = PromValidationMode::Unchecked.decode_string(&ascii).unwrap();

        assert_eq!(strict_result, "simple_ascii_123");
        assert_eq!(lossy_result, "simple_ascii_123");
        assert_eq!(unchecked_result, "simple_ascii_123");
        assert_eq!(strict_result, lossy_result);
        assert_eq!(lossy_result, unchecked_result);
    }
}
