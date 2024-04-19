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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::time;

/// Check port every 0.1 second.
const PORT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Get the dir of test cases. This function only works when the runner is run
/// under the project's dir because it depends on some envs set by cargo.
pub fn get_case_dir(case_dir: Option<PathBuf>) -> String {
    let runner_path = match case_dir {
        Some(path) => path,
        None => {
            // retrieve the manifest runner (./tests/runner)
            let mut runner_crate_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            // change directory to cases' dir from runner's (should be runner/../cases)
            let _ = runner_crate_path.pop();
            runner_crate_path.push("cases");
            runner_crate_path
        }
    };

    runner_path.into_os_string().into_string().unwrap()
}

/// Get the dir that contains workspace manifest (the top-level Cargo.toml).
pub fn get_workspace_root() -> String {
    // retrieve the manifest runner (./tests/runner)
    let mut runner_crate_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // change directory to workspace's root (runner/../..)
    let _ = runner_crate_path.pop();
    let _ = runner_crate_path.pop();

    runner_crate_path.into_os_string().into_string().unwrap()
}

pub fn get_binary_dir(mode: &str) -> PathBuf {
    // first go to the workspace root.
    let mut workspace_root = PathBuf::from(get_workspace_root());

    // change directory to target dir (workspace/target/<build mode>/)
    workspace_root.push("target");
    workspace_root.push(mode);

    workspace_root
}

/// Spin-waiting a socket address is available, or timeout.
/// Returns whether the addr is up.
pub async fn check_port(ip_addr: SocketAddr, timeout: Duration) -> bool {
    let check_task = async {
        loop {
            let socket = TcpSocket::new_v4().expect("Cannot create v4 socket");
            match socket.connect(ip_addr).await {
                Ok(mut stream) => {
                    let _ = stream.shutdown().await;
                    break;
                }
                Err(_) => time::sleep(PORT_CHECK_INTERVAL).await,
            }
        }
    };

    tokio::time::timeout(timeout, check_task).await.is_ok()
}

/// Get the path of sqlness config dir `tests/conf`.
pub fn sqlness_conf_path() -> PathBuf {
    let mut sqlness_root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    sqlness_root_path.pop();
    sqlness_root_path.push("conf");
    sqlness_root_path
}

/// Start kafka cluster if needed. Config file is `conf/kafka-cluster.yml`.
///
/// ```shell
/// docker compose -f kafka-cluster.yml up kafka -d --wait
/// ```
pub fn setup_wal() {
    let mut sqlness_root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    sqlness_root_path.pop();
    sqlness_root_path.push("conf");

    Command::new("docker")
        .current_dir(sqlness_conf_path())
        .args([
            "compose",
            "-f",
            "kafka-cluster.yml",
            "up",
            "kafka",
            "-d",
            "--wait",
        ])
        .output()
        .expect("Failed to start kafka cluster");

    println!("kafka cluster is up");
}

/// Stop kafka cluster if needed. Config file is `conf/kafka-cluster.yml`.
///
/// ```shell
/// docker compose -f docker-compose-standalone.yml down kafka
/// ```
pub fn teardown_wal() {
    let mut sqlness_root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    sqlness_root_path.pop();
    sqlness_root_path.push("conf");

    Command::new("docker")
        .current_dir(sqlness_conf_path())
        .args(["compose", "-f", "kafka-cluster.yml", "down", "kafka"])
        .output()
        .expect("Failed to stop kafka cluster");

    println!("kafka cluster is down");
}
