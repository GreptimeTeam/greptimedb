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

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::time;
use tokio_stream::StreamExt;

/// Check port every 0.1 second.
const PORT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

fn http_proxy() -> Option<String> {
    for proxy in ["http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"] {
        if let Ok(proxy_addr) = std::env::var(proxy) {
            println!("Setting Proxy from env: {}={}", proxy, proxy_addr);
            return Some(proxy_addr);
        }
    }
    None
}

fn https_proxy() -> Option<String> {
    for proxy in ["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"] {
        if let Ok(proxy_addr) = std::env::var(proxy) {
            println!("Setting Proxy from env: {}={}", proxy, proxy_addr);
            return Some(proxy_addr);
        }
    }
    None
}

async fn download_files(url: &str, path: &str) {
    let proxy = if url.starts_with("http://") {
        http_proxy().map(|proxy| reqwest::Proxy::http(proxy).unwrap())
    } else if url.starts_with("https://") {
        https_proxy().map(|proxy| reqwest::Proxy::https(proxy).unwrap())
    } else {
        None
    };

    let client = proxy
        .map(|proxy| reqwest::Client::builder().proxy(proxy).build().unwrap())
        .unwrap_or(reqwest::Client::new());

    let mut file = tokio::fs::File::create(path).await.unwrap();
    println!("Downloading {}...", url);

    let mut stream = client.get(url).send().await.unwrap().bytes_stream();
    let mut size_downloaded = 0;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.unwrap();
        size_downloaded += chunk.len();
        print!("\rDownloaded {} bytes", size_downloaded);
        file.write_all(&chunk).await.unwrap();
    }

    file.flush().await.unwrap();

    println!("\nDownloaded {}", url);
}

fn decompress(archive: &str, dest: &str) {
    let tar = std::fs::File::open(archive).unwrap();
    let dec = flate2::read::GzDecoder::new(tar);
    let mut a = tar::Archive::new(dec);
    a.unpack(dest).unwrap();
}

/// Use curl to download the binary from the release page.
///
/// # Arguments
///
/// * `version` - The version of the binary to download. i.e. "v0.9.5"
pub async fn pull_binary(version: &str) {
    let os = std::env::consts::OS;
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => panic!("Unsupported arch: {}", std::env::consts::ARCH),
    };
    let triple = format!("greptime-{}-{}-{}", os, arch, version);
    let filename = format!("{triple}.tar.gz");

    let url = format!(
        "https://github.com/GreptimeTeam/greptimedb/releases/download/{version}/{filename}"
    );
    println!("Downloading {version} binary from {}", url);

    // mkdir {version}
    std::fs::create_dir(version).unwrap();

    let archive = Path::new(version).join(filename);
    let folder_path = Path::new(version);

    // download the binary to the version directory
    download_files(&url, &archive.to_string_lossy()).await;

    let checksum_file = format!("{triple}.sha256sum");
    let checksum_url = format!(
        "https://github.com/GreptimeTeam/greptimedb/releases/download/{version}/{checksum_file}"
    );
    download_files(
        &checksum_url,
        &PathBuf::from_iter([version, &checksum_file]).to_string_lossy(),
    )
    .await;

    // verify the checksum
    let mut file = std::fs::File::open(&archive).unwrap();
    let mut sha256 = Sha256::new();
    std::io::copy(&mut file, &mut sha256).unwrap();
    let checksum: Vec<u8> = sha256.finalize().to_vec();

    let mut expected_checksum =
        std::fs::File::open(PathBuf::from_iter([version, &checksum_file])).unwrap();
    let mut buf = String::new();
    expected_checksum.read_to_string(&mut buf).unwrap();
    let expected_checksum = hex::decode(buf.lines().next().unwrap()).unwrap();

    assert_eq!(
        checksum, expected_checksum,
        "Checksum mismatched, downloaded file is corrupted"
    );

    decompress(&archive.to_string_lossy(), &folder_path.to_string_lossy());
    println!("Downloaded and extracted {version} binary to {folder_path:?}");

    // move the binary to the version directory
    std::fs::rename(
        PathBuf::from_iter([version, &triple, "greptime"]),
        PathBuf::from_iter([version, "greptime"]),
    )
    .unwrap();

    // remove the archive and inner folder
    std::fs::remove_file(&archive).unwrap();
    std::fs::remove_dir(PathBuf::from_iter([version, &triple])).unwrap();
}

/// Pull the binary if it does not exist and `pull_version_on_need` is true.
pub async fn maybe_pull_binary(version: &str, pull_version_on_need: bool) {
    let exist = Path::new(version).is_dir();
    match (exist, pull_version_on_need){
        (true, _) => println!("Binary {version} exists"),
        (false, false) => panic!("Binary {version} does not exist, please run with --pull-version-on-need or manually download it"),
        (false, true) => { pull_binary(version).await; },
    }
}

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
