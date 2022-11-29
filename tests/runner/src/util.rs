// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use client::api::v1::column::Values;
use client::api::v1::ColumnDataType;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::time;

/// Check port every 0.1 second.
const PORT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

pub fn values_to_string(data_type: ColumnDataType, values: Values) -> Vec<String> {
    match data_type {
        ColumnDataType::Int64 => values
            .i64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Float64 => values
            .f64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::String => values.string_values,
        ColumnDataType::Boolean => values
            .bool_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int8 => values
            .i8_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int16 => values
            .i16_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int32 => values
            .i32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint8 => values
            .u8_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint16 => values
            .u16_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint32 => values
            .u32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint64 => values
            .u64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Float32 => values
            .f32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Binary => values
            .binary_values
            .into_iter()
            .map(|val| format!("{:?}", val))
            .collect(),
        ColumnDataType::Datetime => values
            .i64_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
        ColumnDataType::Date => values
            .i32_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
        ColumnDataType::Timestamp => values
            .ts_millis_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
    }
}

/// Get the dir of test cases. This function only works when the runner is run
/// under the project's dir because it depends on some envs set by cargo.
pub fn get_case_dir() -> String {
    // retrieve the manifest runner (./tests/runner)
    let mut runner_crate_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // change directory to cases' dir from runner's (should be runner/../cases)
    runner_crate_path.pop();
    runner_crate_path.push("cases");

    runner_crate_path.into_os_string().into_string().unwrap()
}

/// Get the dir that contains workspace manifest (the top-level Cargo.toml).
pub fn get_workspace_root() -> String {
    // retrieve the manifest runner (./tests/runner)
    let mut runner_crate_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // change directory to workspace's root (runner/../..)
    runner_crate_path.pop();
    runner_crate_path.pop();

    runner_crate_path.into_os_string().into_string().unwrap()
}

pub fn get_binary_dir(mode: &str) -> String {
    // first go to the workspace root.
    let mut workspace_root = PathBuf::from(get_workspace_root());

    // change directory to target dir (workspace/target/<build mode>/)
    workspace_root.push("target");
    workspace_root.push(mode);

    workspace_root.into_os_string().into_string().unwrap()
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

    tokio::select! {
        _ = check_task => {
            true
        },
        _ = time::sleep(timeout) => {
            false
        }
    }
}
