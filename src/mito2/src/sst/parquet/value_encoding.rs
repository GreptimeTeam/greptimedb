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

//! Parquet per-column write overrides and MetricEngine value encoding policy.

use std::collections::HashMap;

use common_query::prelude::greptime_value;
use datatypes::arrow::array::{Array, Float32Array, Float64Array};
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, METRIC_DATA_REGION_GROUP,
};
use strum::EnumString;

/// Operation kind for generic Parquet write policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParquetWriteOperation {
    /// Memtable flush.
    Flush,
    /// Compaction/rewrite.
    Compaction,
    /// Other write path.
    #[default]
    Other,
}

const MAX_PROFILE_SAMPLES: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetricValueEncodingMode {
    #[default]
    Disabled,
    Auto,
    Plain,
    ByteStreamSplit,
}

impl MetricValueEncodingMode {
    pub fn is_auto(self) -> bool {
        matches!(self, Self::Auto)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricValueEncodingDecision {
    DictionaryPlain,
    ByteStreamSplit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricValueEncodingPlan {
    pub decision: MetricValueEncodingDecision,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricValueEncodingProfile {
    pub valid_count: usize,
    pub adjacent_equal_ratio: f64,
    pub distinct_ratio: f64,
    pub max_run: usize,
}

#[derive(Debug, Default)]
pub struct MetricValueEncodingProfileBuilder {
    samples: Vec<u64>,
    unsupported: bool,
}

impl MetricValueEncodingProfileBuilder {
    pub fn update_batch(&mut self, batch: &RecordBatch) {
        if self.samples.len() >= MAX_PROFILE_SAMPLES || self.unsupported {
            return;
        }
        let Some(array) = batch.column_by_name(greptime_value()) else {
            self.unsupported = true;
            return;
        };
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..array.len() {
                    if array.is_valid(i) {
                        self.samples.push(array.value(i).to_bits() as u64);
                        if self.samples.len() >= MAX_PROFILE_SAMPLES {
                            break;
                        }
                    }
                }
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..array.len() {
                    if array.is_valid(i) {
                        self.samples.push(array.value(i).to_bits());
                        if self.samples.len() >= MAX_PROFILE_SAMPLES {
                            break;
                        }
                    }
                }
            }
            _ => self.unsupported = true,
        }
    }

    pub fn finish(&self) -> Option<MetricValueEncodingProfile> {
        if self.unsupported {
            return None;
        }
        let valid_count = self.samples.len();
        if valid_count == 0 {
            return Some(MetricValueEncodingProfile {
                valid_count,
                adjacent_equal_ratio: 0.0,
                distinct_ratio: 0.0,
                max_run: 0,
            });
        }
        let mut adjacent_equal = 0usize;
        let mut max_run = 1usize;
        let mut run = 1usize;
        for pair in self.samples.windows(2) {
            if pair[0] == pair[1] {
                adjacent_equal += 1;
                run += 1;
                max_run = max_run.max(run);
            } else {
                run = 1;
            }
        }
        let mut distinct_samples = self.samples.clone();
        distinct_samples.sort_unstable();
        distinct_samples.dedup();
        let distinct = distinct_samples.len();
        Some(MetricValueEncodingProfile {
            valid_count,
            adjacent_equal_ratio: if valid_count > 1 {
                adjacent_equal as f64 / (valid_count - 1) as f64
            } else {
                0.0
            },
            distinct_ratio: distinct as f64 / valid_count as f64,
            max_run,
        })
    }
}

pub fn decide_profile(profile: &MetricValueEncodingProfile) -> MetricValueEncodingPlan {
    let decision = if profile.valid_count < 2
        || profile.adjacent_equal_ratio >= 0.20
        || profile.distinct_ratio <= 0.20
    {
        MetricValueEncodingDecision::DictionaryPlain
    } else {
        MetricValueEncodingDecision::ByteStreamSplit
    };
    MetricValueEncodingPlan { decision }
}

pub fn metric_engine_value_column_encoding(
    metadata: &RegionMetadataRef,
    mode: MetricValueEncodingMode,
) -> Option<MetricValueEncodingPlan> {
    if mode == MetricValueEncodingMode::Disabled
        || metadata.region_id.region_group() != METRIC_DATA_REGION_GROUP
        || metadata
            .column_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .is_none()
        || metadata
            .column_by_name(DATA_SCHEMA_TSID_COLUMN_NAME)
            .is_none()
    {
        return None;
    }
    let value_column = metadata.column_by_name(greptime_value())?;
    let data_type = &value_column.column_schema.data_type;
    if data_type != &ConcreteDataType::float32_datatype()
        && data_type != &ConcreteDataType::float64_datatype()
    {
        return None;
    }
    match mode {
        MetricValueEncodingMode::Disabled => None,
        MetricValueEncodingMode::Plain => Some(default_plan()),
        MetricValueEncodingMode::ByteStreamSplit => Some(MetricValueEncodingPlan {
            decision: MetricValueEncodingDecision::ByteStreamSplit,
        }),
        MetricValueEncodingMode::Auto => None,
    }
}

pub fn metric_value_encoding_plan_for_batch(
    metadata: &RegionMetadataRef,
    mode: MetricValueEncodingMode,
    batch: &RecordBatch,
) -> Option<MetricValueEncodingPlan> {
    if mode != MetricValueEncodingMode::Auto {
        return metric_engine_value_column_encoding(metadata, mode);
    }
    metric_engine_value_column_encoding(metadata, MetricValueEncodingMode::Plain)?;
    let mut builder = MetricValueEncodingProfileBuilder::default();
    builder.update_batch(batch);
    builder.finish().map(|profile| decide_profile(&profile))
}

pub fn default_plan() -> MetricValueEncodingPlan {
    MetricValueEncodingPlan {
        decision: MetricValueEncodingDecision::DictionaryPlain,
    }
}

/// Per-column Parquet write options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetColumnWriteOptions {
    /// Optional column encoding override.
    pub encoding: Option<Encoding>,
    /// Optional dictionary setting override.
    pub dictionary_enabled: Option<bool>,
}

impl ParquetColumnWriteOptions {
    /// Overrides the column encoding.
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Overrides whether dictionary encoding is enabled.
    pub fn with_dictionary_enabled(mut self, enabled: bool) -> Self {
        self.dictionary_enabled = Some(enabled);
        self
    }
}

/// Generic Parquet write options shared by SST writers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetWriteOptions {
    /// Column path/name to Parquet write overrides.
    pub column_options: HashMap<String, ParquetColumnWriteOptions>,
}

impl ParquetWriteOptions {
    /// Adds/updates an override for a top-level column name.
    pub fn with_column_options(
        mut self,
        column: impl Into<String>,
        options: ParquetColumnWriteOptions,
    ) -> Self {
        self.column_options.insert(column.into(), options);
        self
    }

    /// Overlays another set of overrides. Overrides from `other` win on conflicts.
    pub fn overlay(mut self, other: ParquetWriteOptions) -> Self {
        self.column_options.extend(other.column_options);
        self
    }

    /// Applies generic column overrides to an existing builder.
    pub fn apply_to_builder(
        &self,
        mut builder: WriterPropertiesBuilder,
    ) -> WriterPropertiesBuilder {
        for (column, options) in &self.column_options {
            let column_path = ColumnPath::new(vec![column.clone()]);
            if let Some(encoding) = options.encoding {
                builder = builder.set_column_encoding(column_path.clone(), encoding);
            }
            if let Some(enabled) = options.dictionary_enabled {
                builder = builder.set_column_dictionary_enabled(column_path, enabled);
            }
        }
        builder
    }
}

pub fn parquet_options_for_plan(plan: MetricValueEncodingPlan) -> ParquetWriteOptions {
    match plan.decision {
        MetricValueEncodingDecision::ByteStreamSplit => ParquetWriteOptions::default()
            .with_column_options(
                greptime_value(),
                ParquetColumnWriteOptions::default()
                    .with_encoding(Encoding::BYTE_STREAM_SPLIT)
                    .with_dictionary_enabled(false),
            ),
        MetricValueEncodingDecision::DictionaryPlain => ParquetWriteOptions::default(),
    }
}

/// Builds the default Mito SST Parquet writer properties and applies generic overrides.
pub fn sst_writer_properties_builder_with_metadata(
    key_value_meta: Option<Vec<KeyValue>>,
    row_group_size: usize,
    parquet_options: &ParquetWriteOptions,
) -> WriterPropertiesBuilder {
    let builder = WriterProperties::builder()
        .set_key_value_metadata(key_value_meta)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_encoding(Encoding::PLAIN)
        .set_max_row_group_row_count(Some(row_group_size))
        .set_column_index_truncate_length(None)
        .set_statistics_truncate_length(None);
    parquet_options.apply_to_builder(builder)
}

/// Builds the default Mito SST Parquet writer properties and applies generic overrides.
pub fn sst_writer_properties(
    key_value_meta: KeyValue,
    row_group_size: usize,
    parquet_options: &ParquetWriteOptions,
    customize: impl FnOnce(WriterPropertiesBuilder) -> WriterPropertiesBuilder,
) -> WriterProperties {
    customize(sst_writer_properties_builder_with_metadata(
        Some(vec![key_value_meta]),
        row_group_size,
        parquet_options,
    ))
    .build()
}

/// Builds the default Mito SST Parquet writer properties with optional metadata.
pub fn sst_writer_properties_with_metadata(
    key_value_meta: Option<Vec<KeyValue>>,
    row_group_size: usize,
    parquet_options: &ParquetWriteOptions,
    customize: impl FnOnce(WriterPropertiesBuilder) -> WriterPropertiesBuilder,
) -> WriterProperties {
    customize(sst_writer_properties_builder_with_metadata(
        key_value_meta,
        row_group_size,
        parquet_options,
    ))
    .build()
}
