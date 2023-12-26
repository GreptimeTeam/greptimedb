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

use std::fs;
use std::path::Path;

use serde::Serialize;
use strum::{Display, EnumString};
use tinytemplate::TinyTemplate;

use crate::find_workspace_path;

#[derive(EnumString, Display)]
#[strum(serialize_all = "lowercase")]
pub enum ConfigTemplate {
    Datanode,
    Metasrv,
    Standalone,
}

impl ConfigTemplate {
    fn render(&self, config_values: ConfigValues) -> String {
        let template_name = self.to_string();

        let template = fs::read_to_string(find_workspace_path(&format!(
            "/src/common/test-util/conf/{template_name}-test.toml.template"
        )))
        .unwrap();

        let mut tt = TinyTemplate::new();
        tt.add_template(&template_name, &template).unwrap();
        tt.render(&template_name, &config_values).unwrap()
    }
}

#[derive(Serialize, Default)]
pub struct ConfigValues {
    pub wal_dir: String,
    pub data_home: String,
    pub procedure_dir: String,
    pub is_raft_engine: bool,
    pub kafka_wal_broker_endpoints: String,
    pub grpc_addr: String,
}

/// Generate a config file from template (determined by parameter `config_template`), with provided
/// config values (in parameter `config_values`), and stores the file under the directory specified
/// by parameter `target_dir`, returns the target file name.
pub fn generate_config_file(
    config_template: ConfigTemplate,
    config_values: ConfigValues,
    target_dir: &Path,
) -> String {
    let rendered = config_template.render(config_values);

    let target_file = format!(
        "{config_template}-{}.toml",
        common_time::util::current_time_millis()
    );
    fs::write(target_dir.join(&target_file), rendered).unwrap();

    target_file
}
