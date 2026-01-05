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

pub mod coerce;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use api::helper::{ColumnDataTypeWrapper, encode_json_value};
use api::v1::column_def::{collect_column_options, options_from_column_schema};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, SemanticType};
use arrow_schema::extension::ExtensionType;
use coerce::{coerce_columns, coerce_value};
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_telemetry::warn;
use datatypes::data_type::ConcreteDataType;
use datatypes::extension::json::JsonExtensionType;
use datatypes::value::Value;
use greptime_proto::v1::{ColumnSchema, Row, Rows, Value as GreptimeValue};
use itertools::Itertools;
use jsonb::Number;
use once_cell::sync::OnceCell;
use serde_json as serde_json_crate;
use session::context::Channel;
use snafu::OptionExt;
use table::Table;
use vrl::prelude::{Bytes, VrlValueConvert};
use vrl::value::value::StdError;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    ArrayElementMustBeObjectSnafu, CoerceIncompatibleTypesSnafu,
    IdentifyPipelineColumnTypeMismatchSnafu, InvalidTimestampSnafu, Result,
    TimeIndexMustBeNonNullSnafu, TransformColumnNameMustBeUniqueSnafu,
    TransformMultipleTimestampIndexSnafu, TransformTimestampIndexCountSnafu, ValueMustBeMapSnafu,
};
use crate::etl::PipelineDocVersion;
use crate::etl::ctx_req::ContextOpt;
use crate::etl::field::{Field, Fields};
use crate::etl::transform::index::Index;
use crate::etl::transform::{Transform, Transforms};
use crate::{PipelineContext, truthy, unwrap_or_continue_if_err};

const DEFAULT_MAX_NESTED_LEVELS_FOR_JSON_FLATTENING: usize = 10;

/// Row with potentially designated table suffix.
pub type RowWithTableSuffix = (Row, Option<String>);

/// fields not in the columns will be discarded
/// to prevent automatic column creation in GreptimeDB
#[derive(Debug, Clone)]
pub struct GreptimeTransformer {
    transforms: Transforms,
    schema: Vec<ColumnSchema>,
}

/// Parameters that can be used to configure the greptime pipelines.
#[derive(Debug, Default)]
pub struct GreptimePipelineParams {
    /// The original options for configuring the greptime pipelines.
    /// This should not be used directly, instead, use the parsed shortcut option values.
    options: HashMap<String, String>,

    /// Whether to skip error when processing the pipeline.
    pub skip_error: OnceCell<bool>,
    /// Max nested levels when flattening JSON object. Defaults to
    /// `DEFAULT_MAX_NESTED_LEVELS_FOR_JSON_FLATTENING` when not provided.
    pub max_nested_levels: OnceCell<usize>,
}

impl GreptimePipelineParams {
    /// Create a `GreptimePipelineParams` from params string which is from the http header with key `x-greptime-pipeline-params`
    /// The params is in the format of `key1=value1&key2=value2`,for example:
    /// x-greptime-pipeline-params: max_nested_levels=5
    pub fn from_params(params: Option<&str>) -> Self {
        let options = Self::parse_header_str_to_map(params);

        Self {
            options,
            skip_error: OnceCell::new(),
            max_nested_levels: OnceCell::new(),
        }
    }

    pub fn from_map(options: HashMap<String, String>) -> Self {
        Self {
            options,
            skip_error: OnceCell::new(),
            max_nested_levels: OnceCell::new(),
        }
    }

    pub fn parse_header_str_to_map(params: Option<&str>) -> HashMap<String, String> {
        if let Some(params) = params {
            if params.is_empty() {
                HashMap::new()
            } else {
                params
                    .split('&')
                    .filter_map(|s| s.split_once('='))
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<HashMap<String, String>>()
            }
        } else {
            HashMap::new()
        }
    }

    /// Whether to skip error when processing the pipeline.
    pub fn skip_error(&self) -> bool {
        *self
            .skip_error
            .get_or_init(|| self.options.get("skip_error").map(truthy).unwrap_or(false))
    }

    /// Max nested levels for JSON flattening. If not provided or invalid,
    /// falls back to `DEFAULT_MAX_NESTED_LEVELS_FOR_JSON_FLATTENING`.
    pub fn max_nested_levels(&self) -> usize {
        *self.max_nested_levels.get_or_init(|| {
            self.options
                .get("max_nested_levels")
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_MAX_NESTED_LEVELS_FOR_JSON_FLATTENING)
        })
    }
}

impl GreptimeTransformer {
    /// Add a default timestamp column to the transforms
    fn add_greptime_timestamp_column(transforms: &mut Transforms) {
        let type_ = ColumnDataType::TimestampNanosecond;
        let default = None;

        let transform = Transform {
            fields: Fields::one(Field::new(greptime_timestamp().to_string(), None)),
            type_,
            default,
            index: Some(Index::Time),
            on_failure: Some(crate::etl::transform::OnFailure::Default),
            tag: false,
        };
        transforms.push(transform);
    }

    /// Generate the schema for the GreptimeTransformer
    fn init_schemas(transforms: &Transforms) -> Result<Vec<ColumnSchema>> {
        let mut schema = vec![];
        for transform in transforms.iter() {
            schema.extend(coerce_columns(transform)?);
        }
        Ok(schema)
    }
}

impl GreptimeTransformer {
    pub fn new(mut transforms: Transforms, doc_version: &PipelineDocVersion) -> Result<Self> {
        // empty check is done in the caller
        let mut column_names_set = HashSet::new();
        let mut timestamp_columns = vec![];

        for transform in transforms.iter() {
            let target_fields_set = transform
                .fields
                .iter()
                .map(|f| f.target_or_input_field())
                .collect::<HashSet<_>>();

            let intersections: Vec<_> = column_names_set.intersection(&target_fields_set).collect();
            if !intersections.is_empty() {
                let duplicates = intersections.iter().join(",");
                return TransformColumnNameMustBeUniqueSnafu { duplicates }.fail();
            }

            column_names_set.extend(target_fields_set);

            if let Some(idx) = transform.index
                && idx == Index::Time
            {
                match transform.fields.len() {
                    //Safety unwrap is fine here because we have checked the length of real_fields
                    1 => timestamp_columns.push(transform.fields.first().unwrap().input_field()),
                    _ => {
                        return TransformMultipleTimestampIndexSnafu {
                            columns: transform.fields.iter().map(|x| x.input_field()).join(", "),
                        }
                        .fail();
                    }
                }
            }
        }

        let schema = match timestamp_columns.len() {
            0 if doc_version == &PipelineDocVersion::V1 => {
                // compatible with v1, add a default timestamp column
                GreptimeTransformer::add_greptime_timestamp_column(&mut transforms);
                GreptimeTransformer::init_schemas(&transforms)?
            }
            1 => GreptimeTransformer::init_schemas(&transforms)?,
            count => {
                let columns = timestamp_columns.iter().join(", ");
                return TransformTimestampIndexCountSnafu { count, columns }.fail();
            }
        };
        Ok(GreptimeTransformer { transforms, schema })
    }

    pub fn transform_mut(
        &self,
        pipeline_map: &mut VrlValue,
        is_v1: bool,
    ) -> Result<Vec<GreptimeValue>> {
        let mut values = vec![GreptimeValue { value_data: None }; self.schema.len()];
        let mut output_index = 0;
        for transform in self.transforms.iter() {
            for field in transform.fields.iter() {
                let column_name = field.input_field();

                let pipeline_map = pipeline_map.as_object_mut().context(ValueMustBeMapSnafu)?;
                // let keep us `get` here to be compatible with v1
                match pipeline_map.get(column_name) {
                    Some(v) => {
                        let value_data = coerce_value(v, transform)?;
                        // every transform fields has only one output field
                        values[output_index] = GreptimeValue { value_data };
                    }
                    None => {
                        let value_data = match transform.on_failure {
                            Some(crate::etl::transform::OnFailure::Default) => {
                                match transform.get_default() {
                                    Some(default) => Some(default.clone()),
                                    None => transform.get_default_value_when_data_is_none(),
                                }
                            }
                            Some(crate::etl::transform::OnFailure::Ignore) => None,
                            None => None,
                        };
                        if transform.is_timeindex() && value_data.is_none() {
                            return TimeIndexMustBeNonNullSnafu.fail();
                        }
                        values[output_index] = GreptimeValue { value_data };
                    }
                }
                output_index += 1;
                if !is_v1 {
                    // remove the column from the pipeline_map
                    // so that the auto-transform can use the rest fields
                    pipeline_map.remove(column_name);
                }
            }
        }
        Ok(values)
    }

    pub fn transforms(&self) -> &Transforms {
        &self.transforms
    }

    pub fn schemas(&self) -> &Vec<greptime_proto::v1::ColumnSchema> {
        &self.schema
    }

    pub fn transforms_mut(&mut self) -> &mut Transforms {
        &mut self.transforms
    }
}

#[derive(Clone)]
pub struct ColumnMetadata {
    column_schema: datatypes::schema::ColumnSchema,
    semantic_type: SemanticType,
}

impl From<ColumnSchema> for ColumnMetadata {
    fn from(value: ColumnSchema) -> Self {
        let datatype = value.datatype();
        let semantic_type = value.semantic_type();
        let ColumnSchema {
            column_name,
            datatype: _,
            semantic_type: _,
            datatype_extension,
            options,
        } = value;

        let column_schema = datatypes::schema::ColumnSchema::new(
            column_name,
            ColumnDataTypeWrapper::new(datatype, datatype_extension).into(),
            semantic_type != SemanticType::Timestamp,
        );

        let metadata = collect_column_options(options.as_ref());
        let column_schema = column_schema.with_metadata(metadata);

        Self {
            column_schema,
            semantic_type,
        }
    }
}

impl TryFrom<ColumnMetadata> for ColumnSchema {
    type Error = api::error::Error;

    fn try_from(value: ColumnMetadata) -> std::result::Result<Self, Self::Error> {
        let ColumnMetadata {
            column_schema,
            semantic_type,
        } = value;

        let options = options_from_column_schema(&column_schema);

        let (datatype, datatype_extension) =
            ColumnDataTypeWrapper::try_from(column_schema.data_type).map(|x| x.into_parts())?;

        Ok(ColumnSchema {
            column_name: column_schema.name,
            datatype: datatype as _,
            semantic_type: semantic_type as _,
            datatype_extension,
            options,
        })
    }
}

/// This is used to record the current state schema information and a sequential cache of field names.
/// As you traverse the user input JSON, this will change.
/// It will record a superset of all user input schemas.
#[derive(Default)]
pub struct SchemaInfo {
    /// schema info
    pub schema: Vec<ColumnMetadata>,
    /// index of the column name
    pub index: HashMap<String, usize>,
    /// The pipeline's corresponding table (if already created). Useful to retrieve column schemas.
    table: Option<Arc<Table>>,
}

impl SchemaInfo {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            schema: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
            table: None,
        }
    }

    pub fn from_schema_list(schema_list: Vec<ColumnSchema>) -> Self {
        let mut index = HashMap::new();
        for (i, schema) in schema_list.iter().enumerate() {
            index.insert(schema.column_name.clone(), i);
        }
        Self {
            schema: schema_list.into_iter().map(Into::into).collect(),
            index,
            table: None,
        }
    }

    pub fn set_table(&mut self, table: Option<Arc<Table>>) {
        self.table = table;
    }

    fn find_column_schema_in_table(&self, column_name: &str) -> Option<ColumnMetadata> {
        if let Some(table) = &self.table
            && let Some(i) = table.schema_ref().column_index_by_name(column_name)
        {
            let column_schema = table.schema_ref().column_schemas()[i].clone();

            let semantic_type = if column_schema.is_time_index() {
                SemanticType::Timestamp
            } else if table.table_info().meta.primary_key_indices.contains(&i) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            };

            Some(ColumnMetadata {
                column_schema,
                semantic_type,
            })
        } else {
            None
        }
    }

    pub fn column_schemas(&self) -> api::error::Result<Vec<ColumnSchema>> {
        self.schema
            .iter()
            .map(|x| x.clone().try_into())
            .collect::<api::error::Result<Vec<_>>>()
    }
}

fn resolve_schema(
    index: Option<usize>,
    pipeline_context: &PipelineContext,
    column: &str,
    value_type: &ConcreteDataType,
    schema_info: &mut SchemaInfo,
) -> Result<()> {
    if let Some(index) = index {
        let column_type = &mut schema_info.schema[index].column_schema.data_type;
        match (column_type, value_type) {
            (column_type, value_type) if column_type == value_type => Ok(()),
            (ConcreteDataType::Json(column_type), ConcreteDataType::Json(value_type))
                if column_type.is_include(value_type) =>
            {
                Ok(())
            }
            (column_type, value_type) => IdentifyPipelineColumnTypeMismatchSnafu {
                column,
                expected: column_type.to_string(),
                actual: value_type.to_string(),
            }
            .fail(),
        }
    } else {
        let column_schema = schema_info
            .find_column_schema_in_table(column)
            .unwrap_or_else(|| {
                let semantic_type = decide_semantic(pipeline_context, column);
                let column_schema = datatypes::schema::ColumnSchema::new(
                    column,
                    value_type.clone(),
                    semantic_type != SemanticType::Timestamp,
                );
                ColumnMetadata {
                    column_schema,
                    semantic_type,
                }
            });
        let key = column.to_string();
        schema_info.schema.push(column_schema);
        schema_info.index.insert(key, schema_info.schema.len() - 1);
        Ok(())
    }
}

fn calc_ts(p_ctx: &PipelineContext, values: &VrlValue) -> Result<Option<ValueData>> {
    match p_ctx.channel {
        Channel::Prometheus => {
            let ts = values
                .as_object()
                .and_then(|m| m.get(greptime_timestamp()))
                .and_then(|ts| ts.try_into_i64().ok())
                .unwrap_or_default();
            Ok(Some(ValueData::TimestampMillisecondValue(ts)))
        }
        _ => {
            let custom_ts = p_ctx.pipeline_definition.get_custom_ts();
            match custom_ts {
                Some(ts) => {
                    let ts_field = values.as_object().and_then(|m| m.get(ts.get_column_name()));
                    Some(ts.get_timestamp_value(ts_field)).transpose()
                }
                None => Ok(Some(ValueData::TimestampNanosecondValue(
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                ))),
            }
        }
    }
}

/// Converts VRL values to Greptime rows grouped by their ContextOpt.
/// # Returns
/// A HashMap where keys are `ContextOpt` and values are vectors of (row, table_suffix) pairs.
/// Single object input produces one ContextOpt group with one row.
/// Array input groups rows by their per-element ContextOpt values.
///
/// # Errors
/// - `ArrayElementMustBeObject` if an array element is not an object
pub(crate) fn values_to_rows(
    schema_info: &mut SchemaInfo,
    mut values: VrlValue,
    pipeline_ctx: &PipelineContext<'_>,
    row: Option<Vec<GreptimeValue>>,
    need_calc_ts: bool,
    tablesuffix_template: Option<&crate::tablesuffix::TableSuffixTemplate>,
) -> Result<std::collections::HashMap<ContextOpt, Vec<RowWithTableSuffix>>> {
    let skip_error = pipeline_ctx.pipeline_param.skip_error();
    let VrlValue::Array(arr) = values else {
        // Single object: extract ContextOpt and table_suffix
        let mut result = std::collections::HashMap::new();

        let mut opt = match ContextOpt::from_pipeline_map_to_opt(&mut values) {
            Ok(r) => r,
            Err(e) => return if skip_error { Ok(result) } else { Err(e) },
        };

        let table_suffix = opt.resolve_table_suffix(tablesuffix_template, &values);
        let row = match values_to_row(schema_info, values, pipeline_ctx, row, need_calc_ts) {
            Ok(r) => r,
            Err(e) => return if skip_error { Ok(result) } else { Err(e) },
        };
        result.insert(opt, vec![(row, table_suffix)]);
        return Ok(result);
    };

    let mut rows_by_context: std::collections::HashMap<ContextOpt, Vec<RowWithTableSuffix>> =
        std::collections::HashMap::new();
    for (index, mut value) in arr.into_iter().enumerate() {
        if !value.is_object() {
            unwrap_or_continue_if_err!(
                ArrayElementMustBeObjectSnafu {
                    index,
                    actual_type: value.kind_str().to_string(),
                }
                .fail(),
                skip_error
            );
        }

        // Extract ContextOpt and table_suffix for this element
        let mut opt = unwrap_or_continue_if_err!(
            ContextOpt::from_pipeline_map_to_opt(&mut value),
            skip_error
        );
        let table_suffix = opt.resolve_table_suffix(tablesuffix_template, &value);
        let transformed_row = unwrap_or_continue_if_err!(
            values_to_row(schema_info, value, pipeline_ctx, row.clone(), need_calc_ts),
            skip_error
        );
        rows_by_context
            .entry(opt)
            .or_default()
            .push((transformed_row, table_suffix));
    }
    Ok(rows_by_context)
}

/// `need_calc_ts` happens in two cases:
/// 1. full greptime_identity
/// 2. auto-transform without transformer
///
/// if transform is present in custom pipeline in v2 mode
/// we dont need to calc ts again, nor do we need to check ts column name
pub(crate) fn values_to_row(
    schema_info: &mut SchemaInfo,
    values: VrlValue,
    pipeline_ctx: &PipelineContext<'_>,
    row: Option<Vec<GreptimeValue>>,
    need_calc_ts: bool,
) -> Result<Row> {
    let mut row: Vec<GreptimeValue> =
        row.unwrap_or_else(|| Vec::with_capacity(schema_info.schema.len()));
    let custom_ts = pipeline_ctx.pipeline_definition.get_custom_ts();

    if need_calc_ts {
        // calculate timestamp value based on the channel
        let ts = calc_ts(pipeline_ctx, &values)?;
        row.push(GreptimeValue { value_data: ts });
    }

    row.resize(schema_info.schema.len(), GreptimeValue { value_data: None });

    // skip ts column
    let ts_column_name = custom_ts
        .as_ref()
        .map_or(greptime_timestamp(), |ts| ts.get_column_name());

    let values = values.into_object().context(ValueMustBeMapSnafu)?;

    for (column_name, value) in values {
        if need_calc_ts && column_name.as_str() == ts_column_name {
            continue;
        }

        resolve_value(
            value,
            column_name.into(),
            &mut row,
            schema_info,
            pipeline_ctx,
        )?;
    }
    Ok(Row { values: row })
}

fn decide_semantic(p_ctx: &PipelineContext, column_name: &str) -> SemanticType {
    if p_ctx.channel == Channel::Prometheus && column_name != greptime_value() {
        SemanticType::Tag
    } else {
        SemanticType::Field
    }
}

fn resolve_value(
    value: VrlValue,
    column_name: String,
    row: &mut Vec<GreptimeValue>,
    schema_info: &mut SchemaInfo,
    p_ctx: &PipelineContext,
) -> Result<()> {
    let index = schema_info.index.get(&column_name).copied();

    let value_data = match value {
        VrlValue::Null => None,

        VrlValue::Integer(v) => {
            // safe unwrap after type matched
            resolve_schema(
                index,
                p_ctx,
                &column_name,
                &ConcreteDataType::int64_datatype(),
                schema_info,
            )?;
            Some(ValueData::I64Value(v))
        }

        VrlValue::Float(v) => {
            // safe unwrap after type matched
            resolve_schema(
                index,
                p_ctx,
                &column_name,
                &ConcreteDataType::float64_datatype(),
                schema_info,
            )?;
            Some(ValueData::F64Value(v.into()))
        }

        VrlValue::Boolean(v) => {
            resolve_schema(
                index,
                p_ctx,
                &column_name,
                &ConcreteDataType::boolean_datatype(),
                schema_info,
            )?;
            Some(ValueData::BoolValue(v))
        }

        VrlValue::Bytes(v) => {
            resolve_schema(
                index,
                p_ctx,
                &column_name,
                &ConcreteDataType::string_datatype(),
                schema_info,
            )?;
            Some(ValueData::StringValue(String::from_utf8_lossy_owned(
                v.to_vec(),
            )))
        }

        VrlValue::Regex(v) => {
            warn!(
                "Persisting regex value in the table, this should not happen, column_name: {}",
                column_name
            );
            resolve_schema(
                index,
                p_ctx,
                &column_name,
                &ConcreteDataType::string_datatype(),
                schema_info,
            )?;
            Some(ValueData::StringValue(v.to_string()))
        }

        VrlValue::Timestamp(ts) => {
            let ns = ts.timestamp_nanos_opt().context(InvalidTimestampSnafu {
                input: ts.to_rfc3339(),
            })?;
            resolve_schema(
                index,
                p_ctx,
                &column_name,
                &ConcreteDataType::timestamp_nanosecond_datatype(),
                schema_info,
            )?;
            Some(ValueData::TimestampNanosecondValue(ns))
        }

        VrlValue::Array(_) | VrlValue::Object(_) => {
            let is_json_native_type = schema_info
                .find_column_schema_in_table(&column_name)
                .is_some_and(|x| {
                    if let ConcreteDataType::Json(column_type) = &x.column_schema.data_type {
                        column_type.is_native_type()
                    } else {
                        false
                    }
                });

            let value = if is_json_native_type {
                let json_extension_type: Option<JsonExtensionType> =
                    if let Some(x) = schema_info.find_column_schema_in_table(&column_name) {
                        x.column_schema.extension_type()?
                    } else {
                        None
                    };
                let settings = json_extension_type
                    .and_then(|x| x.metadata().json_structure_settings.clone())
                    .unwrap_or_default();
                let value: serde_json::Value = value.try_into().map_err(|e: StdError| {
                    CoerceIncompatibleTypesSnafu { msg: e.to_string() }.build()
                })?;
                let value = settings.encode(value)?;

                resolve_schema(index, p_ctx, &column_name, &value.data_type(), schema_info)?;

                let Value::Json(value) = value else {
                    unreachable!()
                };
                ValueData::JsonValue(encode_json_value(*value))
            } else {
                resolve_schema(
                    index,
                    p_ctx,
                    &column_name,
                    &ConcreteDataType::binary_datatype(),
                    schema_info,
                )?;

                let value = vrl_value_to_jsonb_value(&value);
                ValueData::BinaryValue(value.to_vec())
            };
            Some(value)
        }
    };

    let value = GreptimeValue { value_data };
    if let Some(index) = index {
        row[index] = value;
    } else {
        row.push(value);
    }
    Ok(())
}

fn vrl_value_to_jsonb_value<'a>(value: &'a VrlValue) -> jsonb::Value<'a> {
    match value {
        VrlValue::Bytes(bytes) => jsonb::Value::String(String::from_utf8_lossy(bytes)),
        VrlValue::Regex(value_regex) => jsonb::Value::String(Cow::Borrowed(value_regex.as_str())),
        VrlValue::Integer(i) => jsonb::Value::Number(Number::Int64(*i)),
        VrlValue::Float(not_nan) => jsonb::Value::Number(Number::Float64(not_nan.into_inner())),
        VrlValue::Boolean(b) => jsonb::Value::Bool(*b),
        VrlValue::Timestamp(date_time) => jsonb::Value::String(Cow::Owned(date_time.to_rfc3339())),
        VrlValue::Object(btree_map) => jsonb::Value::Object(
            btree_map
                .iter()
                .map(|(key, value)| (key.to_string(), vrl_value_to_jsonb_value(value)))
                .collect(),
        ),
        VrlValue::Array(values) => jsonb::Value::Array(
            values
                .iter()
                .map(|value| vrl_value_to_jsonb_value(value))
                .collect(),
        ),
        VrlValue::Null => jsonb::Value::Null,
    }
}

fn identity_pipeline_inner(
    pipeline_maps: Vec<VrlValue>,
    pipeline_ctx: &PipelineContext<'_>,
) -> Result<(SchemaInfo, HashMap<ContextOpt, Vec<Row>>)> {
    let skip_error = pipeline_ctx.pipeline_param.skip_error();
    let mut schema_info = SchemaInfo::default();
    let custom_ts = pipeline_ctx.pipeline_definition.get_custom_ts();

    // set time index column schema first
    let column_schema = datatypes::schema::ColumnSchema::new(
        custom_ts
            .map(|ts| ts.get_column_name().to_string())
            .unwrap_or_else(|| greptime_timestamp().to_string()),
        custom_ts
            .map(|c| ConcreteDataType::from(ColumnDataTypeWrapper::new(c.get_datatype(), None)))
            .unwrap_or_else(|| {
                if pipeline_ctx.channel == Channel::Prometheus {
                    ConcreteDataType::timestamp_millisecond_datatype()
                } else {
                    ConcreteDataType::timestamp_nanosecond_datatype()
                }
            }),
        false,
    );
    schema_info.schema.push(ColumnMetadata {
        column_schema,
        semantic_type: SemanticType::Timestamp,
    });

    let mut opt_map = HashMap::new();
    let len = pipeline_maps.len();

    for mut pipeline_map in pipeline_maps {
        let opt = unwrap_or_continue_if_err!(
            ContextOpt::from_pipeline_map_to_opt(&mut pipeline_map),
            skip_error
        );
        let row = unwrap_or_continue_if_err!(
            values_to_row(&mut schema_info, pipeline_map, pipeline_ctx, None, true),
            skip_error
        );

        opt_map
            .entry(opt)
            .or_insert_with(|| Vec::with_capacity(len))
            .push(row);
    }

    let column_count = schema_info.schema.len();
    for (_, row) in opt_map.iter_mut() {
        for row in row.iter_mut() {
            let diff = column_count - row.values.len();
            for _ in 0..diff {
                row.values.push(GreptimeValue { value_data: None });
            }
        }
    }

    Ok((schema_info, opt_map))
}

/// Identity pipeline for Greptime
/// This pipeline will convert the input JSON array to Greptime Rows
/// params table is used to set the semantic type of the row key column to Tag
/// 1. The pipeline will add a default timestamp column to the schema
/// 2. The pipeline not resolve NULL value
/// 3. The pipeline assumes that the json format is fixed
/// 4. The pipeline will return an error if the same column datatype is mismatched
/// 5. The pipeline will analyze the schema of each json record and merge them to get the final schema.
pub fn identity_pipeline(
    array: Vec<VrlValue>,
    table: Option<Arc<table::Table>>,
    pipeline_ctx: &PipelineContext<'_>,
) -> Result<HashMap<ContextOpt, Rows>> {
    let skip_error = pipeline_ctx.pipeline_param.skip_error();
    let max_nested_levels = pipeline_ctx.pipeline_param.max_nested_levels();
    // Always flatten JSON objects and stringify arrays
    let mut input = Vec::with_capacity(array.len());
    for item in array.into_iter() {
        let result =
            unwrap_or_continue_if_err!(flatten_object(item, max_nested_levels), skip_error);
        input.push(result);
    }

    identity_pipeline_inner(input, pipeline_ctx).and_then(|(mut schema, opt_map)| {
        if let Some(table) = table {
            let table_info = table.table_info();
            for tag_name in table_info.meta.row_key_column_names() {
                if let Some(index) = schema.index.get(tag_name) {
                    schema.schema[*index].semantic_type = SemanticType::Tag;
                }
            }
        }

        let column_schemas = schema.column_schemas()?;
        Ok(opt_map
            .into_iter()
            .map(|(opt, rows)| {
                (
                    opt,
                    Rows {
                        schema: column_schemas.clone(),
                        rows,
                    },
                )
            })
            .collect::<HashMap<ContextOpt, Rows>>())
    })
}

/// Consumes the JSON object and consumes it into a single-level object.
///
/// The `max_nested_levels` parameter is used to limit how deep to flatten nested JSON objects.
/// When the maximum level is reached, the remaining nested structure is serialized to a JSON
/// string and stored at the current flattened key.
pub fn flatten_object(object: VrlValue, max_nested_levels: usize) -> Result<VrlValue> {
    let mut flattened = BTreeMap::new();
    let object = object.into_object().context(ValueMustBeMapSnafu)?;

    if !object.is_empty() {
        // it will use recursion to flatten the object.
        do_flatten_object(&mut flattened, None, object, 1, max_nested_levels);
    }

    Ok(VrlValue::Object(flattened))
}

fn vrl_value_to_serde_json(value: &VrlValue) -> serde_json_crate::Value {
    match value {
        VrlValue::Null => serde_json_crate::Value::Null,
        VrlValue::Boolean(b) => serde_json_crate::Value::Bool(*b),
        VrlValue::Integer(i) => serde_json_crate::Value::Number((*i).into()),
        VrlValue::Float(not_nan) => serde_json_crate::Number::from_f64(not_nan.into_inner())
            .map(serde_json_crate::Value::Number)
            .unwrap_or(serde_json_crate::Value::Null),
        VrlValue::Bytes(bytes) => {
            serde_json_crate::Value::String(String::from_utf8_lossy(bytes).into_owned())
        }
        VrlValue::Regex(re) => serde_json_crate::Value::String(re.as_str().to_string()),
        VrlValue::Timestamp(ts) => serde_json_crate::Value::String(ts.to_rfc3339()),
        VrlValue::Array(arr) => {
            serde_json_crate::Value::Array(arr.iter().map(vrl_value_to_serde_json).collect())
        }
        VrlValue::Object(map) => serde_json_crate::Value::Object(
            map.iter()
                .map(|(k, v)| (k.to_string(), vrl_value_to_serde_json(v)))
                .collect(),
        ),
    }
}

fn do_flatten_object(
    dest: &mut BTreeMap<KeyString, VrlValue>,
    base: Option<&str>,
    object: BTreeMap<KeyString, VrlValue>,
    current_level: usize,
    max_nested_levels: usize,
) {
    for (key, value) in object {
        let new_key = base.map_or_else(
            || key.clone(),
            |base_key| format!("{base_key}.{key}").into(),
        );

        match value {
            VrlValue::Object(object) => {
                if current_level >= max_nested_levels {
                    // Reached the maximum level; stringify the remaining object.
                    let json_string = serde_json_crate::to_string(&vrl_value_to_serde_json(
                        &VrlValue::Object(object),
                    ))
                    .unwrap_or_else(|_| String::from("{}"));
                    dest.insert(new_key, VrlValue::Bytes(Bytes::from(json_string)));
                } else {
                    do_flatten_object(
                        dest,
                        Some(&new_key),
                        object,
                        current_level + 1,
                        max_nested_levels,
                    );
                }
            }
            // Arrays are stringified to ensure no JSON column types in the result.
            VrlValue::Array(_) => {
                let json_string = serde_json_crate::to_string(&vrl_value_to_serde_json(&value))
                    .unwrap_or_else(|_| String::from("[]"));
                dest.insert(new_key, VrlValue::Bytes(Bytes::from(json_string)));
            }
            // Other leaf types are inserted as-is.
            _ => {
                dest.insert(new_key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;

    use super::*;
    use crate::{PipelineDefinition, identity_pipeline};

    #[test]
    fn test_identify_pipeline() {
        let params = GreptimePipelineParams::default();
        let pipeline_ctx = PipelineContext::new(
            &PipelineDefinition::GreptimeIdentityPipeline(None),
            &params,
            Channel::Unknown,
        );
        {
            let array = [
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": "88.5",
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let array = array.iter().map(|v| v.into()).collect();
            let rows = identity_pipeline(array, None, &pipeline_ctx);
            assert!(rows.is_err());
            assert_eq!(
                rows.err().unwrap().to_string(),
                "Column datatype mismatch. For column: score, expected datatype: Float64, actual datatype: String".to_string(),
            );
        }
        {
            let array = [
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": 88,
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let array = array.iter().map(|v| v.into()).collect();
            let rows = identity_pipeline(array, None, &pipeline_ctx);
            assert!(rows.is_err());
            assert_eq!(
                rows.err().unwrap().to_string(),
                "Column datatype mismatch. For column: score, expected datatype: Float64, actual datatype: Int64".to_string(),
            );
        }
        {
            let array = [
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": 88.5,
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let array = array.iter().map(|v| v.into()).collect();
            let rows = identity_pipeline(array, None, &pipeline_ctx);
            assert!(rows.is_ok());
            let mut rows = rows.unwrap();
            assert!(rows.len() == 1);
            let rows = rows.remove(&ContextOpt::default()).unwrap();
            assert_eq!(rows.schema.len(), 8);
            assert_eq!(rows.rows.len(), 2);
            assert_eq!(8, rows.rows[0].values.len());
            assert_eq!(8, rows.rows[1].values.len());
        }
        {
            let array = [
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": 88.5,
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let tag_column_names = ["name".to_string(), "address".to_string()];

            let rows =
                identity_pipeline_inner(array.iter().map(|v| v.into()).collect(), &pipeline_ctx)
                    .map(|(mut schema, mut rows)| {
                        for name in tag_column_names {
                            if let Some(index) = schema.index.get(&name) {
                                schema.schema[*index].semantic_type = SemanticType::Tag;
                            }
                        }

                        assert!(rows.len() == 1);
                        let rows = rows.remove(&ContextOpt::default()).unwrap();

                        Rows {
                            schema: schema.column_schemas().unwrap(),
                            rows,
                        }
                    });

            assert!(rows.is_ok());
            let rows = rows.unwrap();
            assert_eq!(rows.schema.len(), 8);
            assert_eq!(rows.rows.len(), 2);
            assert_eq!(8, rows.rows[0].values.len());
            assert_eq!(8, rows.rows[1].values.len());
            assert_eq!(
                rows.schema
                    .iter()
                    .find(|x| x.column_name == "name")
                    .unwrap()
                    .semantic_type,
                SemanticType::Tag as i32
            );
            assert_eq!(
                rows.schema
                    .iter()
                    .find(|x| x.column_name == "address")
                    .unwrap()
                    .semantic_type,
                SemanticType::Tag as i32
            );
            assert_eq!(
                rows.schema
                    .iter()
                    .filter(|x| x.semantic_type == SemanticType::Tag as i32)
                    .count(),
                2
            );
        }
    }

    #[test]
    fn test_flatten() {
        let test_cases = vec![
            // Basic case.
            (
                serde_json::json!(
                    {
                        "a": {
                            "b": {
                                "c": [1, 2, 3]
                            }
                        },
                        "d": [
                            "foo",
                            "bar"
                        ],
                        "e": {
                            "f": [7, 8, 9],
                            "g": {
                                "h": 123,
                                "i": "hello",
                                "j": {
                                    "k": true
                                }
                            }
                        }
                    }
                ),
                10,
                Some(serde_json::json!(
                    {
                        "a.b.c": "[1,2,3]",
                        "d": "[\"foo\",\"bar\"]",
                        "e.f": "[7,8,9]",
                        "e.g.h": 123,
                        "e.g.i": "hello",
                        "e.g.j.k": true
                    }
                )),
            ),
            // Test the case where the object has more than 3 nested levels.
            (
                serde_json::json!(
                    {
                        "a": {
                            "b": {
                                "c": {
                                    "d": [1, 2, 3]
                                }
                            }
                        },
                        "e": [
                            "foo",
                            "bar"
                        ]
                    }
                ),
                3,
                Some(serde_json::json!(
                    {
                        "a.b.c": "{\"d\":[1,2,3]}",
                        "e": "[\"foo\",\"bar\"]"
                    }
                )),
            ),
        ];

        for (input, max_depth, expected) in test_cases {
            let input = input.into();
            let expected = expected.map(|e| e.into());

            let flattened_object = flatten_object(input, max_depth).ok();
            assert_eq!(flattened_object, expected);
        }
    }

    use ahash::HashMap as AHashMap;
    #[test]
    fn test_values_to_rows_skip_error_handling() {
        let table_suffix_template: Option<crate::tablesuffix::TableSuffixTemplate> = None;

        // Case 1: skip_error=true, mixed valid/invalid elements
        {
            let schema_info = &mut SchemaInfo::default();
            let input_array = vec![
                // Valid object
                serde_json::json!({"name": "Alice", "age": 25}).into(),
                // Invalid element (string)
                VrlValue::Bytes("invalid_string".into()),
                // Valid object
                serde_json::json!({"name": "Bob", "age": 30}).into(),
                // Invalid element (number)
                VrlValue::Integer(42),
                // Valid object
                serde_json::json!({"name": "Charlie", "age": 35}).into(),
            ];

            let params = GreptimePipelineParams::from_map(AHashMap::from_iter([(
                "skip_error".to_string(),
                "true".to_string(),
            )]));

            let pipeline_ctx = PipelineContext::new(
                &PipelineDefinition::GreptimeIdentityPipeline(None),
                &params,
                Channel::Unknown,
            );

            let result = values_to_rows(
                schema_info,
                VrlValue::Array(input_array),
                &pipeline_ctx,
                None,
                true,
                table_suffix_template.as_ref(),
            );

            // Should succeed and only process valid objects
            assert!(result.is_ok());
            let rows_by_context = result.unwrap();
            // Count total rows across all ContextOpt groups
            let total_rows: usize = rows_by_context.values().map(|v| v.len()).sum();
            assert_eq!(total_rows, 3); // Only 3 valid objects
        }

        // Case 2: skip_error=false, invalid elements present
        {
            let schema_info = &mut SchemaInfo::default();
            let input_array = vec![
                serde_json::json!({"name": "Alice", "age": 25}).into(),
                VrlValue::Bytes("invalid_string".into()), // This should cause error
            ];

            let params = GreptimePipelineParams::default(); // skip_error = false

            let pipeline_ctx = PipelineContext::new(
                &PipelineDefinition::GreptimeIdentityPipeline(None),
                &params,
                Channel::Unknown,
            );

            let result = values_to_rows(
                schema_info,
                VrlValue::Array(input_array),
                &pipeline_ctx,
                None,
                true,
                table_suffix_template.as_ref(),
            );

            // Should fail with ArrayElementMustBeObject error
            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(error_msg.contains("Array element at index 1 must be an object for one-to-many transformation, got string"));
        }
    }

    /// Test that values_to_rows correctly groups rows by per-element ContextOpt
    #[test]
    fn test_values_to_rows_per_element_context_opt() {
        let table_suffix_template: Option<crate::tablesuffix::TableSuffixTemplate> = None;
        let schema_info = &mut SchemaInfo::default();

        // Create array with elements having different TTL values (ContextOpt)
        let input_array = vec![
            serde_json::json!({"name": "Alice", "greptime_ttl": "1h"}).into(),
            serde_json::json!({"name": "Bob", "greptime_ttl": "1h"}).into(),
            serde_json::json!({"name": "Charlie", "greptime_ttl": "24h"}).into(),
        ];

        let params = GreptimePipelineParams::default();
        let pipeline_ctx = PipelineContext::new(
            &PipelineDefinition::GreptimeIdentityPipeline(None),
            &params,
            Channel::Unknown,
        );

        let result = values_to_rows(
            schema_info,
            VrlValue::Array(input_array),
            &pipeline_ctx,
            None,
            true,
            table_suffix_template.as_ref(),
        );

        assert!(result.is_ok());
        let rows_by_context = result.unwrap();

        // Should have 2 different ContextOpt groups (1h TTL and 24h TTL)
        assert_eq!(rows_by_context.len(), 2);

        // Count rows per group
        let total_rows: usize = rows_by_context.values().map(|v| v.len()).sum();
        assert_eq!(total_rows, 3);

        // Verify that rows are correctly grouped by TTL
        let mut ttl_1h_count = 0;
        let mut ttl_24h_count = 0;
        for rows in rows_by_context.values() {
            // ContextOpt doesn't expose ttl directly, but we can count by group size
            if rows.len() == 2 {
                ttl_1h_count = rows.len();
            } else if rows.len() == 1 {
                ttl_24h_count = rows.len();
            }
        }
        assert_eq!(ttl_1h_count, 2); // Alice and Bob with 1h TTL
        assert_eq!(ttl_24h_count, 1); // Charlie with 24h TTL
    }
}
