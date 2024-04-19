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
use std::sync::{Arc, Mutex};

use common_telemetry::{info, warn};
use nix::sys::signal::Signal;
use snafu::ResultExt;
use tokio::process::Child;

use crate::error::{self, Result};

pub(crate) type Pid = u32;

/// The state of a process.
#[derive(Debug, Clone)]
pub(crate) struct Process {
    pub(crate) exit_status: Option<ExitStatus>,
    pub(crate) exited: bool,
}

/// ProcessManager provides the ability to spawn/wait/kill a child process.
#[derive(Debug, Clone)]
pub(crate) struct ProcessManager {
    processes: Arc<Mutex<HashMap<Pid, Process>>>,
}

/// The callback while the child process exits.
pub type OnChildExitResult = std::result::Result<ExitStatus, std::io::Error>;

impl ProcessManager {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Default::default()),
        }
    }

    pub(crate) fn get(&self, pid: Pid) -> Option<Process> {
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
        info!("kill pid :{} siganl: {:?}", pid, signal);
        // Safety: checked.
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), signal)
            .context(error::KillProcessSnafu)?;

        Ok(())
    }
}
