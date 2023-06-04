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
use std::path::PathBuf;
use std::sync::Arc;

use procfs::net::TcpState;
use procfs::process::{FDTarget, Process};
use snafu::ResultExt;

use crate::error::{self, Error, Result};

type ProcessRef = Arc<Process>;

#[derive(Debug, Clone)]
/// A fundamental (physical) unit in a local cluster.
pub struct ProcessInfo {
    pub pid: i32,
    pub name: String,
    pub cwd: PathBuf,
    pub cmdline: Vec<String>,
    pub listen_addr: Vec<String>,
    pub process_ref: ProcessRef,
}

impl TryFrom<Process> for ProcessInfo {
    type Error = Error;

    fn try_from(proc: Process) -> Result<Self> {
        ProcessInfo::try_new(proc)
    }
}

impl ProcessInfo {
    pub fn try_new(process: Process) -> Result<Self> {
        let process_ref = Arc::new(process);

        let tcpnet_entries = process_ref.tcp().context(error::GetProcInfoSnafu)?;
        let fds = process_ref.fd().context(error::GetProcInfoSnafu)?;
        let inodes = fds
            .filter_map(|fd| {
                if let FDTarget::Socket(inode) = fd.unwrap().target {
                    Some(inode)
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        let listen_addr = tcpnet_entries
            .into_iter()
            .filter_map(|entry| {
                if entry.state == TcpState::Listen && inodes.contains(&entry.inode) {
                    Some(entry.local_address.to_string())
                } else {
                    None
                }
            })
            .collect();

        let comm = process_ref.stat().context(error::GetProcInfoSnafu)?.comm;

        Ok(Self {
            pid: process_ref.pid,
            name: comm,
            cwd: process_ref.cwd().context(error::GetProcInfoSnafu)?,
            cmdline: process_ref.cmdline().context(error::GetProcInfoSnafu)?,
            listen_addr,
            process_ref,
        })
    }
}

pub fn list_processes_by_name(name: String) -> Result<Vec<Process>> {
    let all_procs = procfs::process::all_processes().context(error::GetProcInfoSnafu)?;
    let mut processes = Vec::new();
    for proc in all_procs.flatten() {
        if let Ok(stat) = proc.stat() {
            if stat.comm == name {
                processes.push(proc)
            }
        }
    }

    Ok(processes)
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    const ENABLE_DEBUG_CLUSTER: &str = "enable_debug_cluster";
    #[test]
    fn test_list_greptime() {
        if env::var(ENABLE_DEBUG_CLUSTER).is_ok() {
            let procs = list_processes_by_name("greptime".to_string()).unwrap();
            let procs = procs
                .into_iter()
                .filter_map(|proc| match ProcessInfo::try_new(proc) {
                    Ok(proc) => Some(proc),
                    Err(_) => None,
                })
                .collect::<Vec<_>>();

            for proc in procs {
                // println!("{:?}", proc);
                assert!(!proc.listen_addr.is_empty());
            }
        }
    }
}
