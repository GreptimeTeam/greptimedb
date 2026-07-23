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

#![allow(clippy::print_stderr)]

use std::path::PathBuf;
use std::process::ExitCode;

use async_trait::async_trait;
use clap::{Args, Parser, Subcommand};
use cmd::query_perf::controller::{self, ControllerRunOutcome, RunSuiteArgs};
use cmd::query_perf::error::Result;

#[derive(Parser, Debug)]
#[command(
    name = "query_regression_local_controller",
    about = "Local-only query regression lifecycle controller"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    RunSuite(RunSuite),
}

/// Explicit future controller inputs. The scaffold intentionally has no
/// defaults for suite-derived semantics, binaries, roots, or outputs.
#[derive(Args, Debug)]
struct RunSuite {
    #[arg(long)]
    suite: PathBuf,
    #[arg(long)]
    base_binary: PathBuf,
    #[arg(long)]
    candidate_binary: PathBuf,
    #[arg(long)]
    base_attestation: PathBuf,
    #[arg(long)]
    candidate_attestation: PathBuf,
    #[arg(long)]
    endpoint_runner: PathBuf,
    #[arg(long)]
    work_root: PathBuf,
    #[arg(long)]
    fixture_cache_root: Option<PathBuf>,
    #[arg(long)]
    report: PathBuf,
    #[arg(long)]
    summary: PathBuf,
    #[arg(long)]
    artifact: PathBuf,
}

impl From<RunSuite> for RunSuiteArgs {
    fn from(value: RunSuite) -> Self {
        Self {
            suite: value.suite,
            base_binary: value.base_binary,
            candidate_binary: value.candidate_binary,
            base_attestation: value.base_attestation,
            candidate_attestation: value.candidate_attestation,
            endpoint_runner: value.endpoint_runner,
            work_root: value.work_root,
            fixture_cache_root: value.fixture_cache_root,
            report: value.report,
            summary: value.summary,
            artifact: value.artifact,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    run(Cli::parse().command).await
}

#[async_trait]
trait ControllerExecutor: Sync {
    async fn run_suite(&self, args: RunSuiteArgs) -> Result<ControllerRunOutcome>;
}
struct LocalControllerExecutor;
#[async_trait]
impl ControllerExecutor for LocalControllerExecutor {
    async fn run_suite(&self, args: RunSuiteArgs) -> Result<ControllerRunOutcome> {
        controller::run_suite(args).await
    }
}
async fn run(command: Command) -> ExitCode {
    run_with_executor(&LocalControllerExecutor, command).await
}
async fn run_with_executor(executor: &dyn ControllerExecutor, command: Command) -> ExitCode {
    let result = match command {
        Command::RunSuite(args) => executor.run_suite(args.into()).await,
    };
    match result {
        Ok(ControllerRunOutcome {
            outcome: controller::ControllerOutcome::Succeeded,
            ..
        }) => ExitCode::SUCCESS,
        Ok(outcome) => {
            eprintln!(
                "query_regression_local_controller completed with {:?}",
                outcome.outcome
            );
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("query_regression_local_controller failed: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use cmd::query_perf::error::Error;

    use super::*;
    struct TestExecutor {
        result: Mutex<Option<Result<ControllerRunOutcome>>>,
        args: Mutex<Vec<RunSuiteArgs>>,
    }
    impl TestExecutor {
        fn new(result: Result<ControllerRunOutcome>) -> Self {
            Self {
                result: Mutex::new(Some(result)),
                args: Mutex::new(vec![]),
            }
        }
    }
    #[async_trait]
    impl ControllerExecutor for TestExecutor {
        async fn run_suite(&self, args: RunSuiteArgs) -> Result<ControllerRunOutcome> {
            self.args
                .lock()
                .unwrap_or_else(|_| panic!("args"))
                .push(args);
            self.result
                .lock()
                .unwrap_or_else(|_| panic!("result"))
                .take()
                .unwrap_or_else(|| Err(Error::new("unexpected invocation")))
        }
    }
    fn command() -> Command {
        Command::RunSuite(RunSuite {
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
    }
    fn outcome(outcome: controller::ControllerOutcome) -> ControllerRunOutcome {
        ControllerRunOutcome {
            run_id: "run".into(),
            outcome,
            target_manifest: None,
            fixture_manifest: None,
            commands: vec![],
        }
    }
    #[tokio::test]
    async fn cli_dispatches_run_suite_without_process_exit() {
        let executor = TestExecutor::new(Ok(outcome(controller::ControllerOutcome::Succeeded)));
        assert_eq!(
            run_with_executor(&executor, command()).await,
            ExitCode::SUCCESS
        );
        assert_eq!(
            executor
                .args
                .lock()
                .unwrap_or_else(|_| panic!("args"))
                .len(),
            1
        );
    }
    #[tokio::test]
    async fn cli_maps_non_success_outcomes_and_errors_to_nonzero() {
        let executor =
            TestExecutor::new(Ok(outcome(controller::ControllerOutcome::NotImplemented)));
        assert_ne!(
            run_with_executor(&executor, command()).await,
            ExitCode::SUCCESS
        );
        let executor = TestExecutor::new(Ok(outcome(controller::ControllerOutcome::Failed)));
        assert_ne!(
            run_with_executor(&executor, command()).await,
            ExitCode::SUCCESS
        );
        let executor = TestExecutor::new(Err(Error::new("failed")));
        assert_ne!(
            run_with_executor(&executor, command()).await,
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn cli_requires_every_run_suite_input() {
        assert!(Cli::try_parse_from(["controller", "run-suite", "--suite", "suite.toml"]).is_err());
        assert!(
            Cli::try_parse_from([
                "controller",
                "run-suite",
                "--suite",
                "suite.toml",
                "--base-binary",
                "base",
                "--candidate-binary",
                "candidate",
                "--base-attestation",
                "base.json",
                "--candidate-attestation",
                "candidate.json",
                "--endpoint-runner",
                "runner",
                "--work-root",
                "work",
                "--report",
                "report",
                "--summary",
                "summary",
                "--artifact",
                "artifact"
            ])
            .is_ok()
        );
    }
}
