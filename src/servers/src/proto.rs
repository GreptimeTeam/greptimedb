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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::slice;

use api::prom_store::remote::Sample;
use api::v1::RowInsertRequests;
use bytes::{Buf, Bytes};
use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use pipeline::{GreptimePipelineParams, PipelineContext, PipelineDefinition, PipelineMap, Value};
use prost::encoding::message::merge;
use prost::encoding::{decode_key, decode_varint, WireType};
use prost::DecodeError;
use session::context::QueryContextRef;
use snafu::OptionExt;

use crate::error::InternalSnafu;
use crate::http::event::PipelineIngestRequest;
use crate::pipeline::run_pipeline;
use crate::prom_row_builder::TablesBuilder;
use crate::prom_store::METRIC_NAME_LABEL_BYTES;
use crate::query_handler::PipelineHandlerRef;
use crate::repeated_field::{Clear, RepeatedField};

impl Clear for Sample {
    fn clear(&mut self) {
        self.timestamp = 0;
        self.value = 0.0;
    }
}

#[derive(Default, Clone, Debug)]
pub struct PromLabel {
    pub name: Bytes,
    pub value: Bytes,
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

#[inline(always)]
fn copy_to_bytes(data: &mut Bytes, len: usize) -> Bytes {
    if len == data.remaining() {
        std::mem::replace(data, Bytes::new())
    } else {
        let ret = split_to(data, len);
        data.advance(len);
        ret
    }
}

/// Similar to `Bytes::split_to`, but directly operates on underlying memory region.
/// # Safety
/// This function is safe as long as `data` is backed by a consecutive region of memory,
/// for example `Vec<u8>` or `&[u8]`, and caller must ensure that `buf` outlives
/// the `Bytes` returned.
#[inline(always)]
fn split_to(buf: &mut Bytes, end: usize) -> Bytes {
    let len = buf.len();
    assert!(
        end <= len,
        "range end out of bounds: {:?} <= {:?}",
        end,
        len,
    );

    if end == 0 {
        return Bytes::new();
    }

    let ptr = buf.as_ptr();
    let x = unsafe { slice::from_raw_parts(ptr, end) };
    // `Bytes::drop` does nothing when it's built via `from_static`.
    Bytes::from_static(x)
}

/// Reads a variable-length encoded bytes field from `buf` and assign it to `value`.
/// # Safety
/// Callers must ensure `buf` outlives `value`.
#[inline(always)]
fn merge_bytes(value: &mut Bytes, buf: &mut Bytes) -> Result<(), DecodeError> {
    let len = decode_varint(buf)?;
    if len > buf.remaining() as u64 {
        return Err(DecodeError::new(format!(
            "buffer underflow, len: {}, remaining: {}",
            len,
            buf.remaining()
        )));
    }
    *value = copy_to_bytes(buf, len as usize);
    Ok(())
}

#[derive(Default, Debug)]
pub struct PromTimeSeries {
    pub table_name: String,
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
        is_strict_mode: bool,
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
                if label.name.deref() == METRIC_NAME_LABEL_BYTES {
                    let table_name = if is_strict_mode {
                        match String::from_utf8(label.value.to_vec()) {
                            Ok(s) => s,
                            Err(_) => return Err(DecodeError::new("invalid utf-8")),
                        }
                    } else {
                        unsafe { String::from_utf8_unchecked(label.value.to_vec()) }
                    };
                    self.table_name = table_name;
                    self.labels.truncate(self.labels.len() - 1); // remove last label
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
        is_strict_mode: bool,
    ) -> Result<(), DecodeError> {
        let label_num = self.labels.len();
        let row_num = self.samples.len();
        let table_data = table_builders.get_or_create_table_builder(
            std::mem::take(&mut self.table_name),
            label_num,
            row_num,
        );
        table_data.add_labels_and_samples(
            self.labels.as_slice(),
            self.samples.as_slice(),
            is_strict_mode,
        )?;

        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct PromWriteRequest {
    table_data: TablesBuilder,
    series: PromTimeSeries,
}

impl Clear for PromWriteRequest {
    fn clear(&mut self) {
        self.table_data.clear();
    }
}

impl PromWriteRequest {
    pub fn as_row_insert_requests(&mut self) -> (RowInsertRequests, usize) {
        self.table_data.as_insert_requests()
    }

    // todo(hl): maybe use &[u8] can reduce the overhead introduced with Bytes.
    pub fn merge(
        &mut self,
        mut buf: Bytes,
        is_strict_mode: bool,
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
                            .merge_field(tag, wire_type, &mut buf, is_strict_mode)?;
                    }
                    if buf.remaining() != limit {
                        return Err(DecodeError::new("delimited length exceeded"));
                    }

                    if processor.use_pipeline {
                        processor.consume_series_to_pipeline_map(&mut self.series)?;
                    } else {
                        self.series
                            .add_to_table_data(&mut self.table_data, is_strict_mode)?;
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
    pub(crate) table_values: BTreeMap<String, Vec<PipelineMap>>,

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
    ) -> Result<(), DecodeError> {
        let mut vec_pipeline_map: Vec<PipelineMap> = Vec::new();
        let mut pipeline_map = PipelineMap::new();
        for l in series.labels.iter() {
            let name = String::from_utf8(l.name.to_vec())
                .map_err(|_| DecodeError::new("invalid utf-8"))?;
            let value = String::from_utf8(l.value.to_vec())
                .map_err(|_| DecodeError::new("invalid utf-8"))?;
            pipeline_map.insert(name, Value::String(value));
        }

        let one_sample = series.samples.len() == 1;

        for s in series.samples.iter() {
            let timestamp = s.timestamp;
            pipeline_map.insert(GREPTIME_TIMESTAMP.to_string(), Value::Int64(timestamp));
            pipeline_map.insert(GREPTIME_VALUE.to_string(), Value::Float64(s.value));
            if one_sample {
                vec_pipeline_map.push(pipeline_map);
                break;
            } else {
                vec_pipeline_map.push(pipeline_map.clone());
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

    pub(crate) async fn exec_pipeline(
        &mut self,
    ) -> crate::error::Result<(RowInsertRequests, usize)> {
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
        let mut size = 0;

        // run pipeline
        let mut inserts = Vec::with_capacity(self.table_values.len());
        for (table_name, pipeline_maps) in self.table_values.iter_mut() {
            let pipeline_req = PipelineIngestRequest {
                table: table_name.clone(),
                values: pipeline_maps.clone(),
            };
            let row_req =
                run_pipeline(handler, &pipeline_ctx, pipeline_req, query_ctx, true).await?;
            size += row_req
                .iter()
                .map(|rq| rq.rows.as_ref().map(|r| r.rows.len()).unwrap_or(0))
                .sum::<usize>();
            inserts.extend(row_req);
        }

        let row_insert_requests = RowInsertRequests { inserts };

        Ok((row_insert_requests, size))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::prom_store::remote::WriteRequest;
    use api::v1::{Row, RowInsertRequests, Rows};
    use bytes::Bytes;
    use prost::Message;

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
            .merge(data.clone(), true, &mut p)
            .unwrap();
        let (prom_rows, samples) = prom_write_request.as_row_insert_requests();

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
}
