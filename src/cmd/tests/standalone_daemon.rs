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

use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Stops the orphaned background daemon when the test finishes (including on
/// panic). The daemon's command line contains the unique `--data-home` path, so
/// we match on it to locate the process.
struct DaemonGuard(String);

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = Command::new("pkill")
            .args(["-f", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[test]
#[cfg(unix)]
fn test_standalone_daemon_mode_detaches_from_shell() {
    let bin = env!("CARGO_BIN_EXE_greptime");
    let data_home = tempfile::tempdir().unwrap();
    let data_home_path = data_home.path().to_str().unwrap().to_string();

    let http_addr = format!("127.0.0.1:{}", common_test_util::ports::get_port());
    let grpc_addr = format!("127.0.0.1:{}", common_test_util::ports::get_port());
    let mysql_addr = format!("127.0.0.1:{}", common_test_util::ports::get_port());
    let postgres_addr = format!("127.0.0.1:{}", common_test_util::ports::get_port());

    let mut child = Command::new(bin)
        .args([
            "standalone",
            "start",
            "--daemon",
            "--data-home",
            &data_home_path,
            "--http-addr",
            &http_addr,
            "--grpc-bind-addr",
            &grpc_addr,
            "--mysql-addr",
            &mysql_addr,
            "--postgres-addr",
            &postgres_addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let _guard = DaemonGuard(data_home_path);

    // When daemonization works, the parent forks and exits right away. When it
    // does not, the process stays in the foreground and never returns.
    let detach_timeout = Duration::from_secs(30);
    let started = Instant::now();
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                assert!(
                    status.success(),
                    "daemon start should exit successfully, got {status:?}"
                );
                break;
            }
            None => {
                assert!(
                    started.elapsed() < detach_timeout,
                    "daemon mode should detach and return quickly, but it blocked for over {detach_timeout:?}"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }

    // A successful detach is not enough: a broken implementation could fork,
    // have the parent exit, then crash the child. Poll the HTTP address to
    // confirm the daemon actually came up and is listening.
    let ready_timeout = Duration::from_secs(30);
    let started = Instant::now();
    loop {
        if TcpStream::connect(&http_addr).is_ok() {
            break;
        }
        assert!(
            started.elapsed() < ready_timeout,
            "daemon should come up and listen on {http_addr}, but it was unreachable for {ready_timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}
