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

use datafusion::config::{ConfigExtension, ExtensionOptions};

#[derive(Debug, Clone)]
pub struct AdaptiveVectorTopKOptions {
    pub max_rounds: usize,
    pub max_k: Option<usize>,
    pub max_rows: usize,
}

impl Default for AdaptiveVectorTopKOptions {
    fn default() -> Self {
        Self {
            max_rounds: 8,
            max_k: None,
            max_rows: 100_000,
        }
    }
}

impl ConfigExtension for AdaptiveVectorTopKOptions {
    const PREFIX: &'static str = "vector_topk";
}

impl ExtensionOptions for AdaptiveVectorTopKOptions {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion_common::Result<()> {
        match key {
            "max_rounds" => {
                self.max_rounds = value.parse().map_err(|_| {
                    datafusion_common::DataFusionError::Plan(format!("Invalid max_rounds: {value}"))
                })?;
            }
            "max_k" => {
                if value.eq_ignore_ascii_case("none") {
                    self.max_k = None;
                } else {
                    self.max_k = Some(value.parse().map_err(|_| {
                        datafusion_common::DataFusionError::Plan(format!("Invalid max_k: {value}"))
                    })?);
                }
            }
            "max_rows" => {
                self.max_rows = value.parse().map_err(|_| {
                    datafusion_common::DataFusionError::Plan(format!("Invalid max_rows: {value}"))
                })?;
            }
            _ => {
                return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                    "AdaptiveVectorTopKOptions does not support set key: {key}"
                )));
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        vec![
            datafusion::config::ConfigEntry {
                key: "max_rounds".to_string(),
                value: Some(self.max_rounds.to_string()),
                description: "Maximum dynamic overfetch rounds",
            },
            datafusion::config::ConfigEntry {
                key: "max_k".to_string(),
                value: self.max_k.map(|v| v.to_string()),
                description: "Maximum k for dynamic overfetch",
            },
            datafusion::config::ConfigEntry {
                key: "max_rows".to_string(),
                value: Some(self.max_rows.to_string()),
                description: "Maximum rows collected per adaptive round",
            },
        ]
    }
}
