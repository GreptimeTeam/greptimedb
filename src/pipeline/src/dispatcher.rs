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

use snafu::OptionExt;
use yaml_rust::Yaml;

use crate::etl::error::{Error, Result};
use crate::etl_error::{
    FieldRequiredForDispatcherSnafu, TablePartRequiredForDispatcherRuleSnafu,
    ValueRequiredForDispatcherRuleSnafu,
};
use crate::Value;

const FIELD: &str = "field";
const TABLE_PARTIAL: &str = "table_part";
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
///       table_part: http_log
///     - value: db
///       pipeline: db_pipeline
///       table_part: db_log
///
///
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
/// - `table_part`: the table name segment that we use to construct full table
///   name
#[derive(Debug, PartialEq)]
pub(crate) struct Rule {
    pub value: Value,
    pub table_part: String,
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
                    let table_part = rule[TABLE_PARTIAL]
                        .as_str()
                        .map(|s| s.to_string())
                        .context(TablePartRequiredForDispatcherRuleSnafu)?;
                    let pipeline = rule[PIPELINE].as_str().map(|s| s.to_string());

                    if rule[VALUE].is_badvalue() {
                        ValueRequiredForDispatcherRuleSnafu.fail()?;
                    }
                    let value = Value::try_from(&rule[VALUE])?;

                    Ok(Rule {
                        value,
                        table_part,
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
