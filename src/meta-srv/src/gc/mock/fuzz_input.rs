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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Phase2MockFuzzInput {
    pub seed: u64,
    pub action_count: usize,
    pub dropped_region_bias: u8,
    pub route_override_bias: u8,
    pub retry_bias: u8,
    pub full_listing_bias: u8,
}

impl Phase2MockFuzzInput {
    pub(crate) const fn smoke(seed: u64) -> Self {
        Self {
            seed,
            action_count: 8,
            dropped_region_bias: 35,
            route_override_bias: 45,
            retry_bias: 50,
            full_listing_bias: 60,
        }
    }

    pub(crate) fn summary(&self) -> String {
        format!(
            "seed={} action_count={} dropped_region_bias={} route_override_bias={} retry_bias={} full_listing_bias={}",
            self.seed,
            self.action_count,
            self.dropped_region_bias,
            self.route_override_bias,
            self.retry_bias,
            self.full_listing_bias,
        )
    }

    pub(crate) fn from_seed_metadata(input: &str) -> Result<Self, String> {
        let mut seed = None;
        let mut action_count = None;
        let mut dropped_region_bias = None;
        let mut route_override_bias = None;
        let mut retry_bias = None;
        let mut full_listing_bias = None;

        for raw_line in input.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("invalid seed metadata line: {line}"))?;
            let value = value.trim();

            match key.trim() {
                "seed" => seed = Some(value.parse::<u64>().map_err(|e| e.to_string())?),
                "action_count" => {
                    action_count = Some(value.parse::<usize>().map_err(|e| e.to_string())?)
                }
                "dropped_region_bias" => {
                    dropped_region_bias = Some(value.parse::<u8>().map_err(|e| e.to_string())?)
                }
                "route_override_bias" => {
                    route_override_bias = Some(value.parse::<u8>().map_err(|e| e.to_string())?)
                }
                "retry_bias" => retry_bias = Some(value.parse::<u8>().map_err(|e| e.to_string())?),
                "full_listing_bias" => {
                    full_listing_bias = Some(value.parse::<u8>().map_err(|e| e.to_string())?)
                }
                other => return Err(format!("unknown seed metadata key: {other}")),
            }
        }

        Ok(Self {
            seed: seed.ok_or_else(|| "missing seed".to_string())?,
            action_count: action_count.ok_or_else(|| "missing action_count".to_string())?,
            dropped_region_bias: dropped_region_bias
                .ok_or_else(|| "missing dropped_region_bias".to_string())?,
            route_override_bias: route_override_bias
                .ok_or_else(|| "missing route_override_bias".to_string())?,
            retry_bias: retry_bias.ok_or_else(|| "missing retry_bias".to_string())?,
            full_listing_bias: full_listing_bias
                .ok_or_else(|| "missing full_listing_bias".to_string())?,
        })
    }
}
