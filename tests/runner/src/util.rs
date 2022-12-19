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

use std::fmt::Display;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use client::api::v1::column::Values;
use client::api::v1::ColumnDataType;
use common_base::BitVec;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::time;

/// Check port every 0.1 second.
const PORT_CHECK_INTERVAL: Duration = Duration::from_millis(100);
const NULL_DATA_PLACEHOLDER: &str = "__NULL__";

/// Helper struct for iterate over column with null_mask
struct NullableColumnIter<N, B, D, T>
where
    N: Iterator<Item = B>,
    B: AsRef<bool>,
    D: Iterator<Item = T>,
    T: Display,
{
    null_iter: N,
    data_iter: D,
}

impl<N, B, D, T> Iterator for NullableColumnIter<N, B, D, T>
where
    N: Iterator<Item = B>,
    B: AsRef<bool>,
    D: Iterator<Item = T>,
    T: Display,
{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(is_null) = self.null_iter.next() {
            if *is_null.as_ref() {
                Some(NULL_DATA_PLACEHOLDER.to_string())
            } else {
                self.data_iter.next().map(|data| data.to_string())
            }
        } else {
            None
        }
    }
}

macro_rules! build_nullable_iter {
    ($null_iter:ident, $data_iter:expr) => {
        NullableColumnIter {
            null_iter: $null_iter,
            data_iter: $data_iter,
        }
        .collect()
    };
}

pub fn values_to_string(
    data_type: ColumnDataType,
    values: Values,
    null_mask: Vec<u8>,
) -> Vec<String> {
    let bit_vec = BitVec::from_vec(null_mask);
    let null_iter = bit_vec.iter();
    match data_type {
        ColumnDataType::Int64 => build_nullable_iter!(null_iter, values.i64_values.into_iter()),
        ColumnDataType::Float64 => build_nullable_iter!(null_iter, values.f64_values.into_iter()),
        ColumnDataType::String => build_nullable_iter!(null_iter, values.string_values.into_iter()),
        ColumnDataType::Boolean => build_nullable_iter!(null_iter, values.bool_values.into_iter()),
        ColumnDataType::Int8 => build_nullable_iter!(null_iter, values.i8_values.into_iter()),
        ColumnDataType::Int16 => build_nullable_iter!(null_iter, values.i16_values.into_iter()),
        ColumnDataType::Int32 => build_nullable_iter!(null_iter, values.i32_values.into_iter()),
        ColumnDataType::Uint8 => build_nullable_iter!(null_iter, values.u8_values.into_iter()),
        ColumnDataType::Uint16 => build_nullable_iter!(null_iter, values.u16_values.into_iter()),
        ColumnDataType::Uint32 => build_nullable_iter!(null_iter, values.u32_values.into_iter()),
        ColumnDataType::Uint64 => build_nullable_iter!(null_iter, values.u64_values.into_iter()),
        ColumnDataType::Float32 => build_nullable_iter!(null_iter, values.f32_values.into_iter()),
        ColumnDataType::Binary => build_nullable_iter!(
            null_iter,
            values
                .binary_values
                .into_iter()
                .map(|val| format!("{:?}", val))
        ),
        ColumnDataType::Datetime => build_nullable_iter!(null_iter, values.i64_values.into_iter()),
        ColumnDataType::Date => build_nullable_iter!(null_iter, values.i32_values.into_iter()),
        ColumnDataType::Timestamp => {
            build_nullable_iter!(null_iter, values.ts_millis_values.into_iter())
        }
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

    tokio::time::timeout(timeout, check_task).await.is_ok()
}
