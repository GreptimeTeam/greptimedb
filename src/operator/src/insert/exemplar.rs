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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ahash::HashMap;
use api::helper::ColumnDataTypeWrapper;
use api::v1::helper::tag_column_schema;
use api::v1::region::InsertRequest as RegionInsertRequest;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnOptions, ColumnSchema, Row, Rows, SemanticType, Value};
use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use common_catalog::consts::METRICS_EXEMPLARS_TABLE;
use common_error::ext::BoxedError;
use common_query::prelude::{
    EXEMPLAR_METRIC_COLUMN, greptime_exemplar_labels, greptime_timestamp, greptime_value,
};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::data_type::ConcreteDataType;
use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
use datatypes::json::{self, JsonSettings};
use datatypes::types::json_type::JsonNativeType;
use datatypes::value::Value as DataValue;
use mito_codec::row_converter::TsidGenerator;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::is_internal_column;
use store_api::storage::{RegionId, TableId};
use table::metadata::TableInfo;

use crate::error::{ColumnDataTypeSnafu, ExternalSnafu, InvalidInsertRequestSnafu, Result};

/// One observation belonging to a query-visible metric series.
/// Metric names and inherited labels are shared until sparse batch construction.
#[derive(Debug, Clone, PartialEq)]
pub struct Exemplar {
    pub metric: Arc<str>,
    pub series_labels: Arc<BTreeMap<String, String>>,
    pub timestamp: i64,
    pub value: f64,
    pub labels: BTreeMap<String, String>,
}

/// Validated batch of exemplars kept in sparse normalized form through schema reconciliation.
#[derive(Debug, Clone, PartialEq)]
pub struct ExemplarBatch {
    exemplars: Vec<Exemplar>,
    sources: BTreeSet<String>,
    distinct_tags: BTreeSet<String>,
}

impl ExemplarBatch {
    /// Validates exemplar invariants and extracts source table names and distinct inherited tags.
    pub fn try_new(exemplars: Vec<Exemplar>) -> Result<Self> {
        let mut sources = BTreeSet::new();
        let mut distinct_tags = BTreeSet::new();
        let mut last_series: Option<&BTreeMap<String, String>> = None;

        for exemplar in &exemplars {
            ensure!(
                !exemplar.metric.is_empty() && exemplar.metric.as_ref() != METRICS_EXEMPLARS_TABLE,
                InvalidInsertRequestSnafu {
                    reason: "invalid or reserved exemplar source metric name",
                }
            );
            if !sources.contains(exemplar.metric.as_ref()) {
                sources.insert(exemplar.metric.to_string());
            }

            let series_ptr: &BTreeMap<String, String> = &exemplar.series_labels;
            if !last_series.is_some_and(|prev| std::ptr::eq(prev, series_ptr)) {
                for name in series_ptr.keys() {
                    ensure!(
                        !is_reserved_exemplar_tag(name),
                        InvalidInsertRequestSnafu {
                            reason: format!(
                                "inherited exemplar label collides with reserved column: {name}"
                            ),
                        }
                    );
                    if !distinct_tags.contains(name) {
                        distinct_tags.insert(name.clone());
                    }
                }
                last_series = Some(series_ptr);
            }

            for name in exemplar.labels.keys() {
                ensure!(
                    name != json::JSON2_REMAINDER_FIELD_NAME,
                    InvalidInsertRequestSnafu {
                        reason: "exemplar label name is reserved by JSON2",
                    }
                );
            }
        }

        Ok(Self {
            exemplars,
            sources,
            distinct_tags,
        })
    }

    pub fn len(&self) -> usize {
        self.exemplars.len()
    }

    pub fn is_empty(&self) -> bool {
        self.exemplars.is_empty()
    }

    pub fn exemplars(&self) -> &[Exemplar] {
        &self.exemplars
    }

    pub fn sources(&self) -> &BTreeSet<String> {
        &self.sources
    }

    pub fn distinct_tags(&self) -> &BTreeSet<String> {
        &self.distinct_tags
    }

    /// Builds the 0-row request schema used for schema reconciliation on `greptime_metrics_exemplars`.
    pub(crate) fn request_schema(&self) -> Result<Vec<ColumnSchema>> {
        let mut schema = Vec::with_capacity(self.distinct_tags.len() + 6);
        schema.push(tag_column_schema(
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
            ColumnDataType::Uint32,
        ));
        schema.push(tag_column_schema(
            DATA_SCHEMA_TSID_COLUMN_NAME,
            ColumnDataType::Uint64,
        ));
        schema.push(tag_column_schema(
            EXEMPLAR_METRIC_COLUMN,
            ColumnDataType::String,
        ));
        for tag in &self.distinct_tags {
            schema.push(tag_column_schema(tag, ColumnDataType::String));
        }
        schema.push(ColumnSchema {
            column_name: greptime_value().to_string(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        });
        schema.push(ColumnSchema {
            column_name: greptime_timestamp().to_string(),
            datatype: ColumnDataType::TimestampNanosecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        });
        schema.push(build_json2_column_schema(greptime_exemplar_labels())?);
        Ok(schema)
    }

    pub(crate) fn validate_destination_tags(
        &self,
        table_name: &str,
        has_tag: impl Fn(&str) -> bool,
    ) -> Result<()> {
        for tag in &self.distinct_tags {
            ensure!(
                has_tag(tag),
                InvalidInsertRequestSnafu {
                    reason: format!("exemplar destination {table_name} is missing tag {tag}"),
                }
            );
        }
        Ok(())
    }

    /// Validates exemplar source IDs and timestamp precision without building protobuf rows.
    pub(crate) fn validate_for_destination(
        &self,
        resolved_table_ids: &HashMap<String, TableId>,
        destination: &TableInfo,
    ) -> Result<()> {
        let target_unit = destination
            .meta
            .schema
            .timestamp_column()
            .and_then(|c| c.data_type.as_timestamp())
            .map(|ts| ts.unit())
            .unwrap_or(TimeUnit::Nanosecond);
        for exemplar in &self.exemplars {
            resolved_table_ids
                .get(exemplar.metric.as_ref())
                .copied()
                .with_context(|| InvalidInsertRequestSnafu {
                    reason: format!(
                        "exemplar source {} table ID was not resolved",
                        exemplar.metric
                    ),
                })?;
            Timestamp::new_nanosecond(exemplar.timestamp)
                .convert_to(target_unit)
                .with_context(|| InvalidInsertRequestSnafu {
                    reason: format!(
                        "exemplar timestamp {} overflows when converting to unit {target_unit:?}",
                        exemplar.timestamp
                    ),
                })?;
        }
        Ok(())
    }

    /// Groups exemplars by present tag key-sets for WCU metering and Flow mirroring
    /// without padding unrelated schema tags across rows.
    pub(crate) fn to_unpadded_region_requests(
        &self,
        resolved_table_ids: &HashMap<String, TableId>,
        destination: &TableInfo,
    ) -> Result<Vec<RegionInsertRequest>> {
        if self.exemplars.is_empty() {
            return Ok(Vec::new());
        }
        let target_unit = destination
            .meta
            .schema
            .timestamp_column()
            .and_then(|c| c.data_type.as_timestamp())
            .map(|ts| ts.unit())
            .unwrap_or(TimeUnit::Nanosecond);
        let ts_datatype = api::helper::timestamp_datatype(target_unit);
        let region_id = RegionId::new(destination.table_id(), 0).as_u64();

        let mut groups: BTreeMap<Vec<&str>, Vec<&Exemplar>> = BTreeMap::new();
        for exemplar in &self.exemplars {
            let key: Vec<&str> = exemplar.series_labels.keys().map(String::as_str).collect();
            groups.entry(key).or_default().push(exemplar);
        }

        let labels_column = build_json2_column_schema(greptime_exemplar_labels())?;
        let mut requests = Vec::with_capacity(groups.len());
        for (tag_names, group) in groups {
            let mut schema = Vec::with_capacity(tag_names.len() + 6);
            schema.push(tag_column_schema(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ColumnDataType::Uint32,
            ));
            schema.push(tag_column_schema(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ColumnDataType::Uint64,
            ));
            schema.push(tag_column_schema(
                EXEMPLAR_METRIC_COLUMN,
                ColumnDataType::String,
            ));
            for tag in &tag_names {
                schema.push(tag_column_schema(tag, ColumnDataType::String));
            }
            schema.push(ColumnSchema {
                column_name: greptime_value().to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            });
            schema.push(ColumnSchema {
                column_name: greptime_timestamp().to_string(),
                datatype: ts_datatype as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            });
            schema.push(labels_column.clone());

            let mut rows = Vec::with_capacity(group.len());
            for exemplar in group {
                let table_id = resolved_table_ids
                    .get(exemplar.metric.as_ref())
                    .copied()
                    .with_context(|| InvalidInsertRequestSnafu {
                        reason: format!(
                            "exemplar source {} table ID was not resolved",
                            exemplar.metric
                        ),
                    })?;
                let tsid = TsidGenerator::from_sorted_tags(
                    exemplar
                        .series_labels
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str())),
                );
                let timestamp = Timestamp::new_nanosecond(exemplar.timestamp)
                    .convert_to(target_unit)
                    .with_context(|| InvalidInsertRequestSnafu {
                        reason: format!(
                            "exemplar timestamp {} overflows when converting to unit {target_unit:?}",
                            exemplar.timestamp
                        ),
                    })?;

                let mut values = Vec::with_capacity(schema.len());
                values.push(ValueData::U32Value(table_id).into());
                values.push(ValueData::U64Value(tsid).into());
                values.push(ValueData::StringValue(exemplar.metric.to_string()).into());
                for tag_value in exemplar.series_labels.values() {
                    values.push(ValueData::StringValue(tag_value.clone()).into());
                }
                values.push(ValueData::F64Value(exemplar.value).into());
                values.push(api::helper::to_grpc_value(DataValue::Timestamp(timestamp)));
                values.push(Value {
                    value_data: Some(encode_json2_value(&exemplar.labels)?),
                });
                rows.push(Row { values });
            }

            requests.push(RegionInsertRequest {
                region_id,
                rows: Some(Rows { schema, rows }),
                ..Default::default()
            });
        }
        Ok(requests)
    }
}

fn is_reserved_exemplar_tag(name: &str) -> bool {
    [
        greptime_timestamp(),
        greptime_value(),
        EXEMPLAR_METRIC_COLUMN,
        greptime_exemplar_labels(),
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
        DATA_SCHEMA_TSID_COLUMN_NAME,
    ]
    .contains(&name)
        || is_internal_column(name)
}

fn build_json2_column_schema(name: &str) -> Result<ColumnSchema> {
    let extension = Json2ExtensionType::new(Arc::new(JsonMetadata::new(JsonSettings::new_v2())));
    let mut options = ColumnOptions::default();
    options.options.insert(
        EXTENSION_TYPE_NAME_KEY.to_string(),
        Json2ExtensionType::NAME.to_string(),
    );
    if let Some(metadata) = extension.serialize_metadata() {
        options
            .options
            .insert(EXTENSION_TYPE_METADATA_KEY.to_string(), metadata);
    }

    Ok(ColumnSchema {
        column_name: name.to_string(),
        datatype: ColumnDataType::Json as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension: ColumnDataTypeWrapper::try_from(ConcreteDataType::json2(
            JsonNativeType::Null,
        ))
        .context(ColumnDataTypeSnafu)?
        .into_parts()
        .1,
        options: Some(options),
    })
}

pub(crate) fn encode_json2_data_value(labels: &BTreeMap<String, String>) -> Result<DataValue> {
    let map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    JsonSettings::new_v2()
        .encode(serde_json::Value::Object(map))
        .map_err(BoxedError::new)
        .context(ExternalSnafu)
}

fn encode_json2_value(labels: &BTreeMap<String, String>) -> Result<ValueData> {
    let value = encode_json2_data_value(labels)?;
    api::helper::to_grpc_value(value)
        .value_data
        .context(InvalidInsertRequestSnafu {
            reason: "JSON2 encoding returned an empty value",
        })
}
