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

use std::collections::HashMap;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_telemetry::{info, warn};
use nix::sys::signal::Signal;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::{ensure, ResultExt};
use tokio::fs::OpenOptions;
use tokio::process::Child;

use crate::error::{self, Result};
use crate::utils::health::HealthChecker;

pub type Pid = u32;

/// The state of a process.
#[derive(Debug, Clone)]
pub struct Process {
    pub(crate) exit_status: Option<ExitStatus>,
    pub(crate) exited: bool,
}

/// ProcessManager provides the ability to spawn/wait/kill a child process.
#[derive(Debug, Clone)]
pub struct ProcessManager {
    processes: Arc<Mutex<HashMap<Pid, Process>>>,
}

/// The callback while the child process exits.
pub type OnChildExitResult = std::result::Result<ExitStatus, std::io::Error>;

impl Default for ProcessManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessManager {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Default::default()),
        }
    }

    pub fn get(&self, pid: Pid) -> Option<Process> {
        self.processes.lock().unwrap().get(&pid).cloned()
    }

    fn wait<F>(&self, mut child: Child, f: F)
    where
        F: FnOnce(Pid, OnChildExitResult) + Send + 'static,
    {
        let processes = self.processes.clone();
        tokio::spawn(async move {
            // Safety: caller checked
            let pid = child.id().unwrap();
            let result = child.wait().await;

            match result {
                Ok(code) => {
                    warn!("pid: {pid} exited with status: {}", code);
                    f(pid, Ok(code));
                    processes.lock().unwrap().entry(pid).and_modify(|process| {
                        process.exit_status = Some(code);
                        process.exited = true;
                    });
                }
                Err(err) => {
                    warn!("pid: {pid} exited with error: {}", err);
                    f(pid, Err(err));
                    processes.lock().unwrap().entry(pid).and_modify(|process| {
                        process.exited = true;
                    });
                }
            }
        });
    }

    /// Spawns a new process.
    pub fn spawn<T: Into<Stdio>, F>(
        &self,
        binary: &str,
        args: &[String],
        stdout: T,
        stderr: T,
        on_exit: F,
    ) -> Result<Pid>
    where
        F: FnOnce(Pid, OnChildExitResult) + Send + 'static,
    {
        info!("starting {} with {:?}", binary, args);
        let child = tokio::process::Command::new(binary)
            .args(args)
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .context(error::SpawnChildSnafu)?;
        let pid = child.id();

        if let Some(pid) = pid {
            self.processes.lock().unwrap().insert(
                pid,
                Process {
                    exit_status: None,
                    exited: false,
                },
            );

            self.wait(child, on_exit);
            Ok(pid)
        } else {
            error::UnexpectedExitedSnafu {}.fail()
        }
    }

    /// Kills a process via [Pid].
    pub fn kill<T: Into<Option<Signal>>>(pid: Pid, signal: T) -> Result<()> {
        let signal: Option<Signal> = signal.into();
        info!("kill pid :{} signal: {:?}", pid, signal);
        // Safety: checked.
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), signal)
            .context(error::KillProcessSnafu { pid })?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    NotSpawn,
    Spawning,
    HealthChecking(Pid),
    Health(Pid),
    Killing(Pid),
    Exited(Pid),
}

impl ProcessState {
    /// Returns true if it's [ProcessState::Health].
    pub fn health(&self) -> bool {
        matches!(self, ProcessState::Health(_))
    }
}

/// The controller of an unstable process.
pub struct UnstableProcessController {
    pub binary_path: String,
    pub args: Vec<String>,
    pub root_dir: String,
    pub seed: u64,
    pub process_manager: ProcessManager,
    pub health_check: Box<dyn HealthChecker>,
    pub sender: tokio::sync::watch::Sender<ProcessState>,
    pub running: Arc<AtomicBool>,
}

async fn path_to_stdio(path: &str) -> Result<std::fs::File> {
    Ok(OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .await
        .context(error::CreateFileSnafu { path })?
        .into_std()
        .await)
}

impl UnstableProcessController {
    /// Start the unstable processes.
    pub async fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
        let mut rng = ChaChaRng::seed_from_u64(self.seed);
        while self.running.load(Ordering::Relaxed) {
            let min = rng.gen_range(50..100);
            let max = rng.gen_range(300..600);
            let ms = rng.gen_range(min..max);
            let pid = self
                .start_process_with_retry(3)
                .await
                .expect("Failed to start process");
            tokio::time::sleep(Duration::from_millis(ms)).await;
            warn!("After {ms}ms, killing pid: {pid}");
            self.sender.send(ProcessState::Killing(pid)).unwrap();
            ProcessManager::kill(pid, Signal::SIGKILL).expect("Failed to kill");
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    async fn start_process_with_retry(&self, max_retry: usize) -> Result<Pid> {
        for _ in 0..max_retry {
            let pid = self.start_process().await.unwrap();
            let wait_timeout = self.health_check.wait_timeout();
            let result = tokio::time::timeout(wait_timeout, async {
                self.sender.send(ProcessState::HealthChecking(pid)).unwrap();
                self.health_check.check().await;
            })
            .await;
            match result {
                Ok(_) => {
                    self.sender.send(ProcessState::Health(pid)).unwrap();
                    return Ok(pid);
                }
                Err(_) => {
                    ensure!(
                        self.process_manager.get(pid).unwrap().exited,
                        error::UnexpectedSnafu {
                            violated: format!("Failed to start process: pid: {pid}")
                        }
                    );
                    self.sender.send(ProcessState::Exited(pid)).unwrap();
                    // Retry alter
                    warn!("Wait for health checking timeout, retry later...");
                }
            }
        }

        error::UnexpectedSnafu {
            violated: "Failed to start process",
        }
        .fail()
    }

    async fn start_process(&self) -> Result<Pid> {
        let on_exit = move |pid, result| {
            info!("The pid: {pid} exited, result: {result:?}");
        };
        let now = common_time::util::current_time_millis();
        let stdout = format!("{}stdout-{}", self.root_dir, now);
        let stderr = format!("{}stderr-{}", self.root_dir, now);
        let stdout = path_to_stdio(&stdout).await?;
        let stderr = path_to_stdio(&stderr).await?;
        self.sender.send(ProcessState::Spawning).unwrap();
        self.process_manager.spawn(
            &self.binary_path,
            &self.args.clone(),
            stdout,
            stderr,
            on_exit,
        )
    }
}
