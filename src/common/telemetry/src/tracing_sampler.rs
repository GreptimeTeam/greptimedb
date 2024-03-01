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

use std::collections::HashSet;

use opentelemetry::trace::{
    Link, SamplingDecision, SamplingResult, SpanKind, TraceContextExt, TraceId, TraceState,
};
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::{Sampler, ShouldSample};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TracingSampleOptions {
    pub default_ratio: f64,
    pub rules: Vec<TracingSampleRule>,
}

impl Default for TracingSampleOptions {
    fn default() -> Self {
        Self {
            default_ratio: 1.0,
            rules: vec![],
        }
    }
}

/// Determine the sampling rate of a span according to the `rules` provided in `RuleSampler`.
/// For spans that do not hit any `rules`, the `default_ratio` is used.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TracingSampleRule {
    pub protocol: String,
    pub request_types: HashSet<String>,
    pub ratio: f64,
}

impl TracingSampleRule {
    pub fn match_rule(&self, protocol: &str, request_type: Option<&str>) -> Option<f64> {
        if protocol == self.protocol {
            if self.request_types.is_empty() {
                Some(self.ratio)
            } else if let Some(t) = request_type
                && self.request_types.contains(t)
            {
                Some(self.ratio)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl PartialEq for TracingSampleOptions {
    fn eq(&self, other: &Self) -> bool {
        self.default_ratio == other.default_ratio && self.rules == other.rules
    }
}
impl PartialEq for TracingSampleRule {
    fn eq(&self, other: &Self) -> bool {
        self.protocol == other.protocol
            && self.request_types == other.request_types
            && self.ratio == other.ratio
    }
}

impl Eq for TracingSampleOptions {}
impl Eq for TracingSampleRule {}

pub fn create_sampler(opt: &TracingSampleOptions) -> Box<dyn ShouldSample> {
    if opt.rules.is_empty() {
        Box::new(Sampler::TraceIdRatioBased(opt.default_ratio))
    } else {
        Box::new(opt.clone())
    }
}

impl ShouldSample for TracingSampleOptions {
    fn should_sample(
        &self,
        parent_context: Option<&opentelemetry::Context>,
        trace_id: TraceId,
        _name: &str,
        _span_kind: &SpanKind,
        attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let (mut protocol, mut request_type) = (None, None);
        for kv in attributes {
            match kv.key.as_str() {
                "protocol" => protocol = Some(kv.value.as_str()),
                "request_type" => request_type = Some(kv.value.as_str()),
                _ => (),
            }
        }
        let ratio = protocol
            .and_then(|p| {
                self.rules
                    .iter()
                    .find_map(|rule| rule.match_rule(p.as_ref(), request_type.as_deref()))
            })
            .unwrap_or(self.default_ratio);
        SamplingResult {
            decision: sample_based_on_probability(ratio, trace_id),
            // No extra attributes ever set by the SDK samplers.
            attributes: Vec::new(),
            // all sampler in SDK will not modify trace state.
            trace_state: match parent_context {
                Some(ctx) => ctx.span().span_context().trace_state().clone(),
                None => TraceState::default(),
            },
        }
    }
}

/// The code here mainly refers to the relevant implementation of
/// [opentelemetry](https://github.com/open-telemetry/opentelemetry-rust/blob/ef4701055cc39d3448d5e5392812ded00cdd4476/opentelemetry-sdk/src/trace/sampler.rs#L229),
/// and determines whether the span needs to be collected based on the `TraceId` and sampling rate (i.e. `prob`).
fn sample_based_on_probability(prob: f64, trace_id: TraceId) -> SamplingDecision {
    if prob >= 1.0 {
        SamplingDecision::RecordAndSample
    } else {
        let prob_upper_bound = (prob.max(0.0) * (1u64 << 63) as f64) as u64;
        let bytes = trace_id.to_bytes();
        let (_, low) = bytes.split_at(8);
        let trace_id_low = u64::from_be_bytes(low.try_into().unwrap());
        let rnd_from_trace_id = trace_id_low >> 1;

        if rnd_from_trace_id < prob_upper_bound {
            SamplingDecision::RecordAndSample
        } else {
            SamplingDecision::Drop
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::tracing_sampler::TracingSampleRule;

    #[test]
    fn test_rule() {
        let rule = TracingSampleRule {
            protocol: "http".to_string(),
            request_types: HashSet::new(),
            ratio: 1.0,
        };
        assert_eq!(rule.match_rule("not_http", None), None);
        assert_eq!(rule.match_rule("http", None), Some(1.0));
        assert_eq!(rule.match_rule("http", Some("abc")), Some(1.0));
        let rule1 = TracingSampleRule {
            protocol: "http".to_string(),
            request_types: HashSet::from(["mysql".to_string()]),
            ratio: 1.0,
        };
        assert_eq!(rule1.match_rule("http", None), None);
        assert_eq!(rule1.match_rule("http", Some("abc")), None);
        assert_eq!(rule1.match_rule("http", Some("mysql")), Some(1.0));
    }
}
