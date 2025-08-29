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

#[cfg(not(windows))]
pub const PROGRAM: &str = "./greptime";
#[cfg(windows)]
pub const PROGRAM: &str = "greptime.exe";

fn http_proxy() -> Option<String> {
    for proxy in ["http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"] {
        if let Ok(proxy_addr) = std::env::var(proxy) {
            println!("Getting Proxy from env var: {}={}", proxy, proxy_addr);
            return Some(proxy_addr);
        }
    }
    None
}

fn https_proxy() -> Option<String> {
    for proxy in ["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"] {
        if let Ok(proxy_addr) = std::env::var(proxy) {
            println!("Getting Proxy from env var: {}={}", proxy, proxy_addr);
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
        .map(|proxy| {
            reqwest::Client::builder()
                .proxy(proxy)
                .build()
                .expect("Failed to build client")
        })
        .unwrap_or(reqwest::Client::new());

    let mut file = tokio::fs::File::create(path)
        .await
        .unwrap_or_else(|_| panic!("Failed to create file in {path}"));
    println!("Downloading {}...", url);

    let resp = client
        .get(url)
        .send()
        .await
        .expect("Failed to send download request");
    let len = resp.content_length();
    let mut stream = resp.bytes_stream();
    let mut size_downloaded = 0;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.unwrap();
        size_downloaded += chunk.len();
        if let Some(len) = len {
            print!("\rDownloading {}/{} bytes", size_downloaded, len);
        } else {
            print!("\rDownloaded {} bytes", size_downloaded);
        }

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
    let _ = std::fs::create_dir(version);

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
    let exist = Path::new(version).join(PROGRAM).is_file();
    match (exist, pull_version_on_need){
        (true, _) => println!("Binary {version} exists"),
        (false, false) => panic!("Binary {version} does not exist, please run with --pull-version-on-need or manually download it"),
        (false, true) => { pull_binary(version).await; },
    }
}

/// Set up a standalone etcd in docker.
pub fn setup_etcd(client_ports: Vec<u16>, peer_port: Option<u16>, etcd_version: Option<&str>) {
    if std::process::Command::new("docker")
        .args(["-v"])
        .status()
        .is_err()
    {
        panic!("Docker is not installed");
    }
    let peer_port = peer_port.unwrap_or(2380);
    let exposed_port: Vec<_> = client_ports.iter().chain(Some(&peer_port)).collect();
    let exposed_port_str = exposed_port
        .iter()
        .flat_map(|p| ["-p".to_string(), format!("{p}:{p}")])
        .collect::<Vec<_>>();
    let etcd_version = etcd_version.unwrap_or("v3.5.17");
    let etcd_image = format!("quay.io/coreos/etcd:{etcd_version}");
    let peer_url = format!("http://0.0.0.0:{peer_port}");
    let my_local_ip = local_ip_address::local_ip().unwrap();

    let my_local_ip_str = my_local_ip.to_string();

    let mut arg_list = vec![];
    arg_list.extend([
        "run",
        "-d",
        "-v",
        "/usr/share/ca-certificates/:/etc/ssl/certs",
    ]);
    arg_list.extend(exposed_port_str.iter().map(std::ops::Deref::deref));
    arg_list.extend([
        "--name",
        "etcd",
        &etcd_image,
        "etcd",
        "-name",
        "etcd0",
        "-advertise-client-urls",
    ]);

    let adv_client_urls = client_ports
        .iter()
        .map(|p| format!("http://{my_local_ip_str}:{p}"))
        .collect::<Vec<_>>()
        .join(",");

    arg_list.push(&adv_client_urls);

    arg_list.extend(["-listen-client-urls"]);

    let client_ports_fmt = client_ports
        .iter()
        .map(|p| format!("http://0.0.0.0:{p}"))
        .collect::<Vec<_>>()
        .join(",");

    arg_list.push(&client_ports_fmt);

    arg_list.push("-initial-advertise-peer-urls");
    let advertise_peer_url = format!("http://{my_local_ip_str}:{peer_port}");
    arg_list.push(&advertise_peer_url);

    arg_list.extend(["-listen-peer-urls", &peer_url]);

    arg_list.extend(["-initial-cluster-token", "etcd-cluster-1"]);

    arg_list.push("-initial-cluster");

    let init_cluster_url = format!("etcd0=http://{my_local_ip_str}:{peer_port}");

    arg_list.push(&init_cluster_url);

    arg_list.extend(["-initial-cluster-state", "new"]);

    let mut cmd = std::process::Command::new("docker");

    cmd.args(arg_list);

    println!("Starting etcd with command: {:?}", cmd);

    let status = cmd.status();
    if status.is_err() {
        panic!("Failed to start etcd: {:?}", status);
    } else if let Ok(status) = status {
        if status.success() {
            println!(
                "Started etcd with client ports {:?} and peer port {}, statues:{status:?}",
                client_ports, peer_port
            );
        } else {
            panic!("Failed to start etcd: {:?}", status);
        }
    }
}

/// Stop and remove the etcd container
pub fn stop_rm_etcd() {
    let status = std::process::Command::new("docker")
        .args(["container", "stop", "etcd"])
        .status();
    if status.is_err() {
        panic!("Failed to stop etcd: {:?}", status);
    } else {
        println!("Stopped etcd");
    }
    // rm the container
    let status = std::process::Command::new("docker")
        .args(["container", "rm", "etcd"])
        .status();
    if status.is_err() {
        panic!("Failed to remove etcd container: {:?}", status);
    } else {
        println!("Removed etcd container");
    }
}

/// Set up a PostgreSQL server in docker.
pub fn setup_pg(pg_port: u16, pg_version: Option<&str>) {
    if std::process::Command::new("docker")
        .args(["-v"])
        .status()
        .is_err()
    {
        panic!("Docker is not installed");
    }

    let pg_image = if let Some(pg_version) = pg_version {
        format!("postgres:{pg_version}")
    } else {
        "postgres:latest".to_string()
    };
    let pg_password = "admin";
    let pg_user = "greptimedb";

    let mut arg_list = vec![];
    arg_list.extend(["run", "-d"]);

    let pg_password_env = format!("POSTGRES_PASSWORD={pg_password}");
    let pg_user_env = format!("POSTGRES_USER={pg_user}");
    let pg_port_forward = format!("{pg_port}:5432");
    arg_list.extend(["-e", &pg_password_env, "-e", &pg_user_env]);
    arg_list.extend(["-p", &pg_port_forward]);

    arg_list.extend(["--name", "greptimedb_pg", &pg_image]);

    let mut cmd = std::process::Command::new("docker");

    cmd.args(arg_list);

    println!("Starting PostgreSQL with command: {:?}", cmd);

    let status = cmd.status();
    if status.is_err() {
        panic!("Failed to start PostgreSQL: {:?}", status);
    } else if let Ok(status) = status {
        if status.success() {
            println!("Started PostgreSQL with port {}", pg_port);
        } else {
            panic!("Failed to start PostgreSQL: {:?}", status);
        }
    }
}

/// Set up a MySql server in docker.
pub fn setup_mysql(mysql_port: u16, mysql_version: Option<&str>) {
    if std::process::Command::new("docker")
        .args(["-v"])
        .status()
        .is_err()
    {
        panic!("Docker is not installed");
    }

    let mysql_image = if let Some(mysql_version) = mysql_version {
        format!("public.ecr.aws/i8k6a5e1/bitnami/mysql:{mysql_version}")
    } else {
        "public.ecr.aws/i8k6a5e1/bitnami/mysql:5.7".to_string()
    };
    let mysql_password = "admin";
    let mysql_user = "greptimedb";

    let mut arg_list = vec![];
    arg_list.extend(["run", "-d"]);

    let mysql_password_env = format!("MYSQL_PASSWORD={mysql_password}");
    let mysql_user_env = format!("MYSQL_USER={mysql_user}");
    let mysql_root_password_env = format!("MYSQL_ROOT_PASSWORD={mysql_password}");
    let mysql_port_forward = format!("{mysql_port}:3306");
    arg_list.extend([
        "-e",
        &mysql_password_env,
        "-e",
        &mysql_user_env,
        "-e",
        &mysql_root_password_env,
        "-e",
        "MYSQL_DATABASE=mysql",
    ]);
    arg_list.extend(["-p", &mysql_port_forward]);

    arg_list.extend(["--name", "greptimedb_mysql", &mysql_image]);

    let mut cmd = std::process::Command::new("docker");

    cmd.args(arg_list);

    println!("Starting MySQL with command: {:?}", cmd);

    let status = cmd.status();
    if status.is_err() {
        panic!("Failed to start MySQL: {:?}", status);
    } else if let Ok(status) = status {
        if status.success() {
            println!("Started MySQL with port {}", mysql_port);
        } else {
            panic!("Failed to start MySQL: {:?}", status);
        }
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

/// Get a random available port by binding to port 0
pub fn get_random_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
}
