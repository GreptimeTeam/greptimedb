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

//! Decoding of Prometheus remote write protobuf payloads.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use api::prom_store::remote::Sample;
use bytes::Buf;
use common_query::prelude::{greptime_timestamp, greptime_value};
use pipeline::{ContextReq, GreptimePipelineParams, PipelineContext, PipelineDefinition};
use prost::DecodeError;
use prost::encoding::message::merge;
use prost::encoding::{WireType, decode_key, decode_varint};
use session::context::QueryContextRef;
use snafu::OptionExt;
use vrl::prelude::NotNan;
use vrl::value::{KeyString, Value as VrlValue};

use super::row_builder::{PromCtx, TablesBuilder};
use super::types::PromLabel;
use super::validation::PromValidationMode;
use crate::error::InternalSnafu;
use crate::http::event::PipelineIngestRequest;
use crate::pipeline::run_pipeline;
#[allow(deprecated)]
use crate::prom_store::{
    DATABASE_LABEL_ALT_BYTES, DATABASE_LABEL_BYTES, METRIC_NAME_LABEL_BYTES,
    PHYSICAL_TABLE_LABEL_ALT_BYTES, PHYSICAL_TABLE_LABEL_BYTES, SCHEMA_LABEL_BYTES,
};
use crate::query_handler::PipelineHandlerRef;
use crate::repeated_field::{Clear, RepeatedField};

#[derive(Default, Debug)]
pub struct PromTimeSeries {
    pub table_name: String,
    pub schema: Option<String>,
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
        buf: &mut &[u8],
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromTimeSeries";
        match tag {
            1u32 => {
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

                #[allow(deprecated)]
                match label.name {
                    METRIC_NAME_LABEL_BYTES => {
                        self.table_name = prom_validation_mode.decode_string(label.value)?;
                        self.labels.truncate(self.labels.len() - 1);
                    }
                    SCHEMA_LABEL_BYTES => {
                        self.schema = Some(prom_validation_mode.decode_string(label.value)?);
                        self.labels.truncate(self.labels.len() - 1);
                    }
                    DATABASE_LABEL_BYTES | DATABASE_LABEL_ALT_BYTES => {
                        if self.schema.is_none() {
                            self.schema = Some(prom_validation_mode.decode_string(label.value)?);
                        }
                        self.labels.truncate(self.labels.len() - 1);
                    }
                    PHYSICAL_TABLE_LABEL_BYTES | PHYSICAL_TABLE_LABEL_ALT_BYTES => {
                        self.physical_table =
                            Some(prom_validation_mode.decode_string(label.value)?);
                        self.labels.truncate(self.labels.len() - 1);
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
            3u32 => prost::encoding::skip_field(wire_type, tag, buf, Default::default()),
            _ => prost::encoding::skip_field(wire_type, tag, buf, Default::default()),
        }
    }

    fn add_to_table_data<'a>(
        &mut self,
        table_builders: &mut TablesBuilder<'a>,
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
pub struct PromWriteRequest<'a> {
    pub(crate) table_data: TablesBuilder<'a>,
    series: PromTimeSeries,
}

impl<'a> Clear for PromWriteRequest<'a> {
    fn clear(&mut self) {
        self.table_data.clear();
    }
}

impl<'a> PromWriteRequest<'a> {
    pub fn as_row_insert_requests(&mut self) -> ContextReq {
        self.table_data.as_insert_requests()
    }

    pub fn decode(
        &mut self,
        buf: Vec<u8>,
        prom_validation_mode: PromValidationMode,
        processor: &mut PromSeriesProcessor,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromWriteRequest";
        self.table_data.set_raw_data(buf);
        let mut offset = 0;
        while offset < self.table_data.raw_data.len() {
            let mut should_add_to_table_data = false;
            let mut decoded_timeseries = false;
            {
                let raw_data = &self.table_data.raw_data;
                let buf = &mut &raw_data[offset..];
                let (tag, wire_type) = decode_key(buf)?;
                assert_eq!(WireType::LengthDelimited, wire_type);
                match tag {
                    1u32 => {
                        let len = decode_varint(buf).map_err(|mut e| {
                            e.push(STRUCT_NAME, "timeseries");
                            e
                        })?;
                        let remaining = buf.remaining();
                        if len > remaining as u64 {
                            return Err(DecodeError::new("buffer underflow"));
                        }

                        let limit = remaining - len as usize;
                        while buf.remaining() > limit {
                            let (tag, wire_type) = decode_key(buf)?;
                            self.series
                                .merge_field(tag, wire_type, buf, prom_validation_mode)?;
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
                            should_add_to_table_data = true;
                        }

                        decoded_timeseries = true;
                    }
                    3u32 => {
                        prost::encoding::skip_field(wire_type, tag, buf, Default::default())?;
                    }
                    _ => prost::encoding::skip_field(wire_type, tag, buf, Default::default())?,
                }
                offset = raw_data.len() - buf.remaining();
            }

            if should_add_to_table_data {
                self.series
                    .add_to_table_data(&mut self.table_data, prom_validation_mode)?;
            }

            if decoded_timeseries {
                self.series.labels.clear();
                self.series.samples.clear();
            }
        }

        Ok(())
    }
}

/// Hook injected into the PromWriteRequest decoding process.
pub struct PromSeriesProcessor {
    pub(crate) use_pipeline: bool,
    pub(crate) table_values: BTreeMap<String, Vec<VrlValue>>,

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

    pub(crate) fn consume_series_to_pipeline_map(
        &mut self,
        series: &mut PromTimeSeries,
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        let mut vec_pipeline_map = Vec::new();
        let mut pipeline_map = BTreeMap::new();
        for l in series.labels.iter() {
            let name = prom_validation_mode.decode_label_name(l.name)?;
            let value = prom_validation_mode.decode_string(l.value)?;
            pipeline_map.insert(KeyString::from(name), VrlValue::Bytes(value.into()));
        }

        let one_sample = series.samples.len() == 1;

        for s in series.samples.iter() {
            let Ok(value) = NotNan::new(s.value) else {
                common_telemetry::warn!("Invalid float value: {}", s.value);
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

    use super::*;
    use crate::prom_store::to_grpc_row_insert_requests;
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
        data: &[u8],
        expected_samples: usize,
        expected_rows: &RowInsertRequests,
    ) {
        let mut p = PromSeriesProcessor::default_processor();
        prom_write_request.clear();
        prom_write_request
            .decode(data.to_owned(), PromValidationMode::Strict, &mut p)
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
            let expected_rows = expected_rows_map.get(&r.table_name).unwrap().clone();
            assert_eq!(sort_rows(expected_rows), sort_rows(r.rows.clone().unwrap()));
        }
    }

    #[test]
    fn test_decode_write_request() {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("benches");
        d.push("write_request.pb.data");
        let data = std::fs::read(d).unwrap();

        let (expected_rows, expected_samples) =
            to_grpc_row_insert_requests(&WriteRequest::decode(&data[..]).unwrap()).unwrap();

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
    fn test_decode_string_all_modes_ascii() {
        let ascii = Bytes::from("simple_ascii_123");
        let strict_result = PromValidationMode::Strict.decode_string(&ascii).unwrap();
        let lossy_result = PromValidationMode::Lossy.decode_string(&ascii).unwrap();
        let unchecked_result = PromValidationMode::Unchecked.decode_string(&ascii).unwrap();
        assert_eq!(strict_result, "simple_ascii_123");
        assert_eq!(lossy_result, "simple_ascii_123");
        assert_eq!(unchecked_result, "simple_ascii_123");
    }
}
