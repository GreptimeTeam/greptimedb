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

use std::path::PathBuf;

use common_telemetry::tracing::info;
use serde::Serialize;
use snafu::ResultExt;
use tinytemplate::TinyTemplate;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::error;
use crate::error::Result;

/// Get the path of config dir `tests-fuzz/conf`.
pub fn get_conf_path() -> PathBuf {
    let mut root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root_path.push("conf");
    root_path
}

/// Returns rendered config file.
pub fn render_config_file<C: Serialize>(template_path: &str, context: &C) -> String {
    let mut tt = TinyTemplate::new();
    let template = std::fs::read_to_string(template_path).unwrap();
    tt.add_template(template_path, &template).unwrap();
    tt.render(template_path, context).unwrap()
}

// Writes config file to `output_path`.
pub async fn write_config_file<C: Serialize>(
    template_path: &str,
    context: &C,
    output_path: &str,
) -> Result<()> {
    info!("template_path: {template_path}, output_path: {output_path}");
    let content = render_config_file(template_path, context);
    let mut config_file = File::create(output_path)
        .await
        .context(error::CreateFileSnafu { path: output_path })?;
    config_file
        .write_all(content.as_bytes())
        .await
        .context(error::WriteFileSnafu { path: output_path })?;
    Ok(())
}
