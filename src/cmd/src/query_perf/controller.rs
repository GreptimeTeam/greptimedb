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

//! Local-controller contracts only.
//!
//! Process lifecycle, port allocation, cache/materialization, and integration
//! deliberately live in later disjoint modules. Do not add them here before the
//! serial controller-contract checkpoint is integrated.

pub mod cache;
pub mod model;
pub mod process;

pub use model::*;

use crate::query_perf::error::{Error, Result};

/// Validates the controller run specification and reserves orchestration for the
/// later process/cache/integration lanes.
pub async fn run_suite(args: RunSuiteArgs) -> Result<ControllerRunOutcome> {
    let _spec = LocalControllerRunSpec::from_args(args)?;
    Err(Error::new(
        "query_regression_local_controller run-suite is NotImplemented: lifecycle orchestration is not in the scaffold checkpoint",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scaffold_validates_inputs_then_returns_typed_not_implemented_error() {
        let error = run_suite(RunSuiteArgs {
            suite: "suite.toml".into(),
            base_binary: "base".into(),
            candidate_binary: "candidate".into(),
            base_attestation: "base.json".into(),
            candidate_attestation: "candidate.json".into(),
            endpoint_runner: "runner".into(),
            work_root: "work".into(),
            fixture_cache_root: None,
            report: "report.json".into(),
            summary: "summary.md".into(),
            artifact: "artifact.json".into(),
        })
        .await
        .expect_err("scaffold must not orchestrate");
        assert!(error.to_string().contains("NotImplemented"));
    }
}
