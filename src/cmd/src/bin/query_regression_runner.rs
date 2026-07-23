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
use cmd::query_perf::error::Result;
use cmd::query_perf::runner::{self, FinalizeArgs, MeasureArgs};

#[derive(Parser)]
#[command(
    name = "query_regression_runner",
    about = "Endpoint-only query regression runner"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Measure(Measure),
    Finalize(Finalize),
}
#[derive(Args)]
struct Measure {
    #[arg(long)]
    case: PathBuf,
    #[arg(long)]
    targets: PathBuf,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    out: PathBuf,
}
#[derive(Args)]
struct Finalize {
    #[arg(long)]
    case: PathBuf,
    #[arg(long)]
    targets: PathBuf,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    measurements: PathBuf,
    #[arg(long)]
    observations: PathBuf,
    #[arg(long)]
    report: PathBuf,
    #[arg(long)]
    summary: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    run(Cli::parse().command).await
}

#[async_trait]
trait CommandExecutor: Sync {
    async fn measure(&self, args: MeasureArgs) -> Result<()>;
    async fn finalize(&self, args: FinalizeArgs) -> Result<()>;
}

struct RunnerCommandExecutor;

#[async_trait]
impl CommandExecutor for RunnerCommandExecutor {
    async fn measure(&self, args: MeasureArgs) -> Result<()> {
        runner::measure(args).await
    }

    async fn finalize(&self, args: FinalizeArgs) -> Result<()> {
        runner::finalize(args)
    }
}

async fn run(command: Command) -> ExitCode {
    run_with_executor(&RunnerCommandExecutor, command).await
}

async fn run_with_executor(executor: &dyn CommandExecutor, command: Command) -> ExitCode {
    let result = match command {
        Command::Measure(value) => {
            executor
                .measure(MeasureArgs {
                    case: value.case,
                    targets: value.targets,
                    fixture: value.fixture,
                    out: value.out,
                })
                .await
        }
        Command::Finalize(value) => {
            executor
                .finalize(FinalizeArgs {
                    case: value.case,
                    targets: value.targets,
                    fixture: value.fixture,
                    measurements: value.measurements,
                    observations: value.observations,
                    report: value.report,
                    summary: value.summary,
                })
                .await
        }
    };
    if let Err(err) = result {
        eprintln!("query_regression_runner failed: {err}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use cmd::query_perf::error::Error;

    use super::*;

    #[derive(Debug)]
    enum Invocation {
        Measure(MeasureArgs),
        Finalize(FinalizeArgs),
    }

    struct TestCommandExecutor {
        measure_result: Mutex<Option<Result<()>>>,
        finalize_result: Mutex<Option<Result<()>>>,
        invocations: Mutex<Vec<Invocation>>,
    }

    impl TestCommandExecutor {
        fn new(measure_result: Result<()>, finalize_result: Result<()>) -> Self {
            Self {
                measure_result: Mutex::new(Some(measure_result)),
                finalize_result: Mutex::new(Some(finalize_result)),
                invocations: Mutex::new(vec![]),
            }
        }

        fn invocations(&self) -> Vec<Invocation> {
            self.invocations
                .lock()
                .unwrap_or_else(|_| panic!("command invocations"))
                .drain(..)
                .collect()
        }
    }

    #[async_trait]
    impl CommandExecutor for TestCommandExecutor {
        async fn measure(&self, args: MeasureArgs) -> Result<()> {
            self.invocations
                .lock()
                .unwrap_or_else(|_| panic!("command invocations"))
                .push(Invocation::Measure(args));
            self.measure_result
                .lock()
                .unwrap_or_else(|_| panic!("measure result"))
                .take()
                .unwrap_or_else(|| Err(Error::new("unexpected measure invocation")))
        }

        async fn finalize(&self, args: FinalizeArgs) -> Result<()> {
            self.invocations
                .lock()
                .unwrap_or_else(|_| panic!("command invocations"))
                .push(Invocation::Finalize(args));
            self.finalize_result
                .lock()
                .unwrap_or_else(|_| panic!("finalize result"))
                .take()
                .unwrap_or_else(|| Err(Error::new("unexpected finalize invocation")))
        }
    }

    fn measure_command() -> Command {
        Command::Measure(Measure {
            case: PathBuf::from("case.toml"),
            targets: PathBuf::from("targets.json"),
            fixture: PathBuf::from("fixture.json"),
            out: PathBuf::from("measurements.json"),
        })
    }

    fn finalize_command() -> Command {
        Command::Finalize(Finalize {
            case: PathBuf::from("case.toml"),
            targets: PathBuf::from("targets.json"),
            fixture: PathBuf::from("fixture.json"),
            measurements: PathBuf::from("measurements.json"),
            observations: PathBuf::from("observations.json"),
            report: PathBuf::from("report.json"),
            summary: PathBuf::from("summary.md"),
        })
    }

    #[tokio::test]
    async fn cli_exit_mapping_success_dispatches_measure_once() {
        let executor = TestCommandExecutor::new(Ok(()), Ok(()));

        assert_eq!(
            run_with_executor(&executor, measure_command()).await,
            ExitCode::SUCCESS
        );
        let invocations = executor.invocations();
        assert_eq!(invocations.len(), 1);
        match &invocations[0] {
            Invocation::Measure(args) => {
                assert_eq!(args.case, PathBuf::from("case.toml"));
                assert_eq!(args.targets, PathBuf::from("targets.json"));
                assert_eq!(args.fixture, PathBuf::from("fixture.json"));
                assert_eq!(args.out, PathBuf::from("measurements.json"));
            }
            Invocation::Finalize(_) => panic!("measure command dispatched finalize"),
        }
    }

    #[tokio::test]
    async fn cli_exit_mapping_operational_error_is_nonzero() {
        let executor = TestCommandExecutor::new(Err(Error::new("I/O failed")), Ok(()));

        assert_ne!(
            run_with_executor(&executor, measure_command()).await,
            ExitCode::SUCCESS
        );
        assert!(matches!(
            executor.invocations().as_slice(),
            [Invocation::Measure(_)]
        ));
    }

    #[tokio::test]
    async fn cli_exit_mapping_threshold_failed_finalize_is_nonzero() {
        let executor = TestCommandExecutor::new(
            Ok(()),
            Err(Error::new("query regression thresholds failed")),
        );

        assert_ne!(
            run_with_executor(&executor, finalize_command()).await,
            ExitCode::SUCCESS
        );
        let invocations = executor.invocations();
        assert_eq!(invocations.len(), 1);
        match &invocations[0] {
            Invocation::Finalize(args) => {
                assert_eq!(args.case, PathBuf::from("case.toml"));
                assert_eq!(args.targets, PathBuf::from("targets.json"));
                assert_eq!(args.fixture, PathBuf::from("fixture.json"));
                assert_eq!(args.measurements, PathBuf::from("measurements.json"));
                assert_eq!(args.observations, PathBuf::from("observations.json"));
                assert_eq!(args.report, PathBuf::from("report.json"));
                assert_eq!(args.summary, PathBuf::from("summary.md"));
            }
            Invocation::Measure(_) => panic!("finalize command dispatched measure"),
        }
    }
}
