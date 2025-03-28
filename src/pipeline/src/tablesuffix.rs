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

use dyn_fmt::AsStrFormatExt;
use regex::Regex;
use snafu::{ensure, OptionExt};
use yaml_rust::Yaml;

use crate::error::{
    Error, InvalidTableSuffixTemplateSnafu, RequiredTableSuffixTemplateSnafu, Result,
};
use crate::{PipelineMap, Value};

const REPLACE_KEY: &str = "{}";

lazy_static::lazy_static! {
    static ref NAME_TPL: Regex = Regex::new(r"\$\{([^}]+)\}").unwrap();
}

/// TableSuffixTemplate is used to generate suffix for the table name, so that the input data can be written to multiple tables.
/// The config should be placed at the end of the pipeline.
/// Use `${variable}` to refer to the variable in the pipeline context, the viarable can be from input data or be a processed result.
/// Note the variable should be an integer number or a string.
/// In case of any error occurs during runtime, no suffix will be added to the table name.
///
/// ```yaml
/// table_suffix: _${xxx}_${b}
/// ```
///
/// For example, if the template is `_${xxx}_${b}`, and the pipeline context is
/// `{"xxx": "123", "b": "456"}`, the generated table name will be `_123_456`.
#[derive(Debug, PartialEq)]
pub(crate) struct TableSuffixTemplate {
    pub template: String,
    pub keys: Vec<String>,
}

impl TableSuffixTemplate {
    pub fn apply(&self, val: &PipelineMap) -> Option<String> {
        let values = self
            .keys
            .iter()
            .filter_map(|key| {
                let v = val.get(key)?;
                match v {
                    Value::Int8(v) => Some(v.to_string()),
                    Value::Int16(v) => Some(v.to_string()),
                    Value::Int32(v) => Some(v.to_string()),
                    Value::Int64(v) => Some(v.to_string()),
                    Value::Uint8(v) => Some(v.to_string()),
                    Value::Uint16(v) => Some(v.to_string()),
                    Value::Uint32(v) => Some(v.to_string()),
                    Value::Uint64(v) => Some(v.to_string()),
                    Value::String(v) => Some(v.clone()),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        if values.len() != self.keys.len() {
            return None;
        }
        Some(self.template.format(&values))
    }
}

impl TryFrom<&Yaml> for TableSuffixTemplate {
    type Error = Error;

    fn try_from(value: &Yaml) -> Result<Self> {
        let name_template = value
            .as_str()
            .context(RequiredTableSuffixTemplateSnafu)?
            .to_string();

        let mut keys = Vec::new();
        for cap in NAME_TPL.captures_iter(&name_template) {
            ensure!(
                cap.len() >= 2,
                InvalidTableSuffixTemplateSnafu {
                    input: name_template.clone(),
                }
            );
            let key = cap[1].trim().to_string();
            keys.push(key);
        }

        let template = NAME_TPL
            .replace_all(&name_template, REPLACE_KEY)
            .to_string();

        Ok(TableSuffixTemplate { template, keys })
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use crate::tablesuffix::TableSuffixTemplate;

    #[test]
    fn test_table_suffix_parsing() {
        let yaml = r#"
        table_suffix: _${xxx}_${b}
        "#;
        let config = YamlLoader::load_from_str(yaml);
        assert!(config.is_ok());
        let config = config.unwrap()[0]["table_suffix"].clone();
        let name_template = TableSuffixTemplate::try_from(&config);
        assert!(name_template.is_ok());
        let name_template = name_template.unwrap();
        assert_eq!(name_template.template, "_{}_{}");
        assert_eq!(name_template.keys, vec!["xxx", "b"]);
    }
}
