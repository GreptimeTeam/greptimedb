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

use ahash::HashSet;

/// Protocol specific context
/// for carrying options(like HTTP header options) within the query context
#[derive(Debug, Clone, Default)]
pub enum ProtocolCtx {
    #[default]
    None,
    OtlpMetric(OtlpMetricCtx),
}

impl ProtocolCtx {
    pub fn get_otlp_metric_ctx(&self) -> Option<&OtlpMetricCtx> {
        match self {
            ProtocolCtx::None => None,
            ProtocolCtx::OtlpMetric(opt) => Some(opt),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OtlpMetricCtx {
    pub promote_all_resource_attrs: bool,
    pub resource_attrs: HashSet<String>,
    pub promote_scope_attrs: bool,
    pub with_metric_engine: bool,
    // If the user uses OTLP metrics ingestion before v0.16, it uses the old path.
    // So we call this path 'legacy'.
    // After v0.16, we store the OTLP metrics using prometheus compatible format, the new path.
    // The difference is how we convert the input data into the final table schema.
    pub is_legacy: bool,
}
