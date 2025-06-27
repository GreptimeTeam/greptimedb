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

use common_telemetry::debug;
use ordered_float::NotNan;
use snafu::{OptionExt, ResultExt};
use vrl::prelude::Bytes;
use vrl::value::{KeyString, Value as VrlValue};
use yaml_rust::Yaml;

use crate::error::{
    Error, FieldRequiredForDispatcherSnafu, FloatIsNanSnafu, Result,
    TableSuffixRequiredForDispatcherRuleSnafu, ValueParseFloatSnafu,
    ValueRequiredForDispatcherRuleSnafu, ValueUnsupportedYamlTypeSnafu,
    ValueYamlKeyMustBeStringSnafu,
};
use crate::etl::ctx_req::TABLE_SUFFIX_KEY;

const FIELD: &str = "field";
const PIPELINE: &str = "pipeline";
const VALUE: &str = "value";
const RULES: &str = "rules";

/// The dispatcher configuration.
///
/// Dispatcher in a pipeline allows user to call another pipeline and specify
/// table name based on field matching.
///
/// ```yaml
/// dispatcher:
///   field: type
///   rules:
///     - value: http
///       pipeline: http_pipeline
///       table_suffix: http_log
///     - value: db
///       pipeline: db_pipeline
///       table_suffix: db_log
/// ```
///
/// If none of the rules match the value, this pipeline will continue to process
/// current log entry
#[derive(Debug, PartialEq)]
pub(crate) struct Dispatcher {
    pub field: String,
    pub rules: Vec<Rule>,
}

/// The rule definition for dispatcher
///
/// - `value`: for pattern matching
/// - `pipeline`: the pipeline to call, if it's unspecified, we use default
///   `greptime_identity`
/// - `table_suffix`: the table name segment that we use to construct full table
///   name
#[derive(Debug, PartialEq)]
pub(crate) struct Rule {
    pub value: VrlValue,
    pub table_suffix: String,
    pub pipeline: Option<String>,
}

impl TryFrom<&Yaml> for Dispatcher {
    type Error = Error;

    fn try_from(value: &Yaml) -> Result<Self> {
        let field = value[FIELD]
            .as_str()
            .map(|s| s.to_string())
            .context(FieldRequiredForDispatcherSnafu)?;

        let rules = if let Some(rules) = value[RULES].as_vec() {
            rules
                .iter()
                .map(|rule| {
                    let table_part = rule[TABLE_SUFFIX_KEY]
                        .as_str()
                        .map(|s| s.to_string())
                        .context(TableSuffixRequiredForDispatcherRuleSnafu)?;

                    let pipeline = rule[PIPELINE].as_str().map(|s| s.to_string());

                    if rule[VALUE].is_badvalue() {
                        ValueRequiredForDispatcherRuleSnafu.fail()?;
                    }

                    let value = yaml_to_vrl_value(&rule[VALUE])?;

                    Ok(Rule {
                        value,
                        table_suffix: table_part,
                        pipeline,
                    })
                })
                .collect::<Result<Vec<Rule>>>()?
        } else {
            vec![]
        };

        Ok(Dispatcher { field, rules })
    }
}

pub(crate) fn yaml_to_vrl_value(v: &yaml_rust::Yaml) -> Result<VrlValue> {
    match v {
        yaml_rust::Yaml::Null => Ok(VrlValue::Null),
        yaml_rust::Yaml::Boolean(v) => Ok(VrlValue::Boolean(*v)),
        yaml_rust::Yaml::Integer(v) => Ok(VrlValue::Integer(*v)),
        yaml_rust::Yaml::Real(v) => {
            let f = v
                .parse::<f64>()
                .context(ValueParseFloatSnafu { ty: "float64", v })?;
            NotNan::new(f).map(VrlValue::Float).context(FloatIsNanSnafu)
        }
        yaml_rust::Yaml::String(v) => Ok(VrlValue::Bytes(Bytes::from(v.to_string()))),
        yaml_rust::Yaml::Array(arr) => {
            let mut values = vec![];
            for v in arr {
                values.push(yaml_to_vrl_value(v)?);
            }
            Ok(VrlValue::Array(values))
        }
        yaml_rust::Yaml::Hash(v) => {
            let mut values = BTreeMap::new();
            for (k, v) in v {
                let key = k
                    .as_str()
                    .with_context(|| ValueYamlKeyMustBeStringSnafu { value: v.clone() })?;
                values.insert(KeyString::from(key), yaml_to_vrl_value(v)?);
            }
            Ok(VrlValue::Object(values))
        }
        _ => ValueUnsupportedYamlTypeSnafu { value: v.clone() }.fail(),
    }
}

impl Dispatcher {
    /// execute dispatcher and returns matched rule if any
    pub(crate) fn exec(&self, data: &VrlValue) -> Option<&Rule> {
        let data = data.as_object()?;
        if let Some(value) = data.get(self.field.as_str()) {
            for rule in &self.rules {
                if rule.value == *value {
                    return Some(rule);
                }
            }

            None
        } else {
            debug!("field {} not found in keys {:?}", &self.field, data);
            None
        }
    }
}
