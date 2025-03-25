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

use regex::Regex;
use snafu::{ensure, OptionExt};
use yaml_rust::Yaml;

use crate::error::{Error, InvalidTableNameTemplateSnafu, RequiredTableNameTemplateSnafu, Result};

const REPLACE_KEY: &str = "{}";

lazy_static::lazy_static! {
    static ref RE: Regex = Regex::new(r"\$\{([^}]+)\}").unwrap();
}

#[derive(Debug, PartialEq)]
pub(crate) struct TableNameTemplate {
    pub template: String,
    pub keys: Vec<String>,
}

impl TryFrom<&Yaml> for TableNameTemplate {
    type Error = Error;

    fn try_from(value: &Yaml) -> Result<Self> {
        let name_template = value
            .as_str()
            .context(RequiredTableNameTemplateSnafu)?
            .to_string();

        let mut keys = Vec::new();
        for cap in RE.captures_iter(&name_template) {
            ensure!(
                cap.len() >= 2,
                InvalidTableNameTemplateSnafu {
                    input: name_template.clone(),
                }
            );
            let key = cap[1].trim().to_string();
            keys.push(key);
        }

        let template = RE.replace_all(&name_template, REPLACE_KEY).to_string();

        Ok(TableNameTemplate { template, keys })
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use crate::tablename::TableNameTemplate;

    #[test]
    fn test_table_name_parsing() {
        let yaml = r#"
        tablename: a_${xxx}_${b}
        "#;
        let config = YamlLoader::load_from_str(yaml);
        assert!(config.is_ok());
        let config = config.unwrap()[0]["tablename"].clone();
        let name_tamplate = TableNameTemplate::try_from(&config);
        assert!(name_tamplate.is_ok());
        let name_tamplate = name_tamplate.unwrap();
        assert_eq!(name_tamplate.template, "a_{}_{}");
        assert_eq!(name_tamplate.keys, vec!["xxx", "b"]);
    }
}
