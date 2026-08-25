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

use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const READINESS_TIMEOUT: Duration = Duration::from_secs(60);

struct Endpoints {
    metasrv_rpc: u16,
    metasrv_http: u16,
    datanode_rpc: u16,
    datanode_http: u16,
    frontend_rpc: u16,
    frontend_http: u16,
}

struct ChildProcesses {
    children: Vec<Child>,
}

impl Drop for ChildProcesses {
    fn drop(&mut self) {
        for child in &mut self.children {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires GREPTIMEDB_BIN pointing to a real greptimedb binary"]
async fn test_distributed_workload_scheduler_toggle() {
    let binary = PathBuf::from(std::env::var("GREPTIMEDB_BIN").unwrap_or_else(|_| {
        panic!("GREPTIMEDB_BIN is not set; set it to the real greptimedb binary to run this E2E")
    }));
    let binary = fs::canonicalize(&binary).unwrap_or_else(|error| {
        panic!(
            "GREPTIMEDB_BIN must point to an existing greptime binary: {} ({error})",
            binary.display()
        )
    });
    assert!(
        binary.is_file(),
        "GREPTIMEDB_BIN must point to an existing greptime binary: {}",
        binary.display()
    );

    let temp = tempfile::tempdir().expect("create workload scheduler E2E temporary directory");
    let endpoints = allocate_endpoints();
    let mut processes = ChildProcesses { children: vec![] };

    let metasrv = spawn_metasrv(&binary, &temp, &endpoints);
    processes.children.push(metasrv);
    wait_for_health(endpoints.metasrv_http, "metasrv");

    let datanode = spawn_datanode(&binary, &temp, &endpoints);
    processes.children.push(datanode);
    wait_for_health(endpoints.datanode_http, "datanode");
    wait_for_registration(endpoints.metasrv_http);

    let frontend = spawn_frontend(&binary, &temp, &endpoints);
    processes.children.push(frontend);
    wait_for_health(endpoints.frontend_http, "frontend");

    let client = format!("127.0.0.1:{}", endpoints.datanode_http);
    let initial = get_json(&client, "/debug/workload_scheduler");
    assert_scheduler_status(&initial, true);

    let frontend = format!("127.0.0.1:{}", endpoints.frontend_http);
    sql(
        &frontend,
        "CREATE TABLE scheduler_e2e (ts TIMESTAMP TIME INDEX, v INT)",
    );
    sql(
        &frontend,
        "INSERT INTO scheduler_e2e VALUES (1000, 10), (2000, 20)",
    );
    assert_values(
        &sql(&frontend, "SELECT v FROM scheduler_e2e ORDER BY v"),
        &[10, 20],
    );

    post_json(&client, "/debug/workload_scheduler/enabled", "false");
    let disabled = get_json(&client, "/debug/workload_scheduler");
    assert_scheduler_status(&disabled, false);

    sql(&frontend, "INSERT INTO scheduler_e2e VALUES (3000, 30)");
    assert_values(
        &sql(&frontend, "SELECT v FROM scheduler_e2e ORDER BY v"),
        &[10, 20, 30],
    );

    post_json(&client, "/debug/workload_scheduler/enabled", "true");
    let reenabled = get_json(&client, "/debug/workload_scheduler");
    assert_scheduler_status(&reenabled, true);

    sql(&frontend, "INSERT INTO scheduler_e2e VALUES (4000, 40)");
    assert_values(
        &sql(&frontend, "SELECT v FROM scheduler_e2e ORDER BY v"),
        &[10, 20, 30, 40],
    );

    drop(processes);
    drop(temp);
}

fn allocate_endpoints() -> Endpoints {
    let listeners: Vec<TcpListener> = (0..6)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).expect("allocate localhost port"))
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("read allocated port").port())
        .collect();
    drop(listeners);
    Endpoints {
        metasrv_rpc: ports[0],
        metasrv_http: ports[1],
        datanode_rpc: ports[2],
        datanode_http: ports[3],
        frontend_rpc: ports[4],
        frontend_http: ports[5],
    }
}

fn spawn_metasrv(binary: &Path, temp: &TempDir, endpoints: &Endpoints) -> Child {
    let dir = temp.path().join("metasrv");
    spawn(
        binary,
        &dir,
        [
            "metasrv",
            "start",
            "--grpc-bind-addr",
            &address(endpoints.metasrv_rpc),
            "--grpc-server-addr",
            &address(endpoints.metasrv_rpc),
            "--http-addr",
            &address(endpoints.metasrv_http),
            "--backend",
            "memory-store",
            "--enable-region-failover",
            "false",
            "--data-home",
            dir.to_str().expect("metasrv path is UTF-8"),
            "--log-dir",
            dir.join("logs")
                .to_str()
                .expect("metasrv log path is UTF-8"),
        ],
    )
}

fn spawn_datanode(binary: &Path, temp: &TempDir, endpoints: &Endpoints) -> Child {
    let dir = temp.path().join("datanode");
    fs::create_dir_all(&dir).expect("create datanode directory");
    let config = dir.join("datanode.toml");
    let config_text = format!(
        r#"
        [runtime.experimental_workload_scheduler]
        enable = true
        max_concurrent_polls = 2
        query_weight = 1
        write_weight = 1

        [wal]
        provider = "raft_engine"
        dir = "{}"

        [storage]
        data_home = "{}"
        "#,
        dir.join("wal").display(),
        dir.display(),
    );
    fs::write(&config, config_text).expect("write datanode configuration");
    spawn(
        binary,
        &dir,
        [
            "datanode",
            "start",
            "--config-file",
            config.to_str().expect("datanode config path is UTF-8"),
            "--node-id",
            "1",
            "--grpc-bind-addr",
            &address(endpoints.datanode_rpc),
            "--grpc-server-addr",
            &address(endpoints.datanode_rpc),
            "--http-addr",
            &address(endpoints.datanode_http),
            "--metasrv-addrs",
            &address(endpoints.metasrv_rpc),
            "--data-home",
            dir.to_str().expect("datanode path is UTF-8"),
            "--log-dir",
            dir.join("logs")
                .to_str()
                .expect("datanode log path is UTF-8"),
        ],
    )
}

fn spawn_frontend(binary: &Path, temp: &TempDir, endpoints: &Endpoints) -> Child {
    let dir = temp.path().join("frontend");
    spawn(
        binary,
        &dir,
        [
            "frontend",
            "start",
            "--metasrv-addrs",
            &address(endpoints.metasrv_rpc),
            "--http-addr",
            &address(endpoints.frontend_http),
            "--grpc-bind-addr",
            &address(endpoints.frontend_rpc),
            "--grpc-server-addr",
            &address(endpoints.frontend_rpc),
            "--log-dir",
            dir.join("logs")
                .to_str()
                .expect("frontend log path is UTF-8"),
        ],
    )
}

fn spawn<const N: usize>(binary: &Path, dir: &Path, args: [&str; N]) -> Child {
    fs::create_dir_all(dir).expect("create process directory");
    let stdout = File::create(dir.join("stdout.log")).expect("create stdout log");
    let stderr = File::create(dir.join("stderr.log")).expect("create stderr log");
    Command::new(binary)
        .args(args)
        .current_dir(dir)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {}: {error}", binary.display()))
}

fn address(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn wait_for_health(port: u16, component: &str) {
    wait_until(&format!("{component} TCP /health"), || {
        match http_request("GET", &format!("127.0.0.1:{port}"), "/health", &[]) {
            Ok(response) if response.status == 200 => Ok(()),
            Ok(response) => Err(format!("HTTP {}", response.status)),
            Err(error) => Err(error),
        }
    });
}

fn wait_for_registration(metasrv_http: u16) {
    wait_until(
        "datanode registration at metasrv /admin/node-lease",
        || match http_request(
            "GET",
            &format!("127.0.0.1:{metasrv_http}"),
            "/admin/node-lease",
            &[],
        ) {
            Ok(response) if response.status == 200 => {
                let json: Value = serde_json::from_slice(&response.body)
                    .map_err(|error| format!("invalid registration JSON: {error}"))?;
                if json.as_array().is_some_and(|leases| !leases.is_empty()) {
                    Ok(())
                } else {
                    Err(format!("no datanode lease yet: {json}"))
                }
            }
            Ok(response) => Err(format!("HTTP {}", response.status)),
            Err(error) => Err(error),
        },
    );
}

fn wait_until<F>(what: &str, mut check: F)
where
    F: FnMut() -> Result<(), String>,
{
    let deadline = Instant::now() + READINESS_TIMEOUT;
    let mut last_error = String::from("not attempted");
    while Instant::now() < deadline {
        match check() {
            Ok(()) => return,
            Err(error) => last_error = error,
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("timed out waiting for {what} after {READINESS_TIMEOUT:?}: {last_error}");
}

fn get_json(host: &str, path: &str) -> Value {
    let response = http_request("GET", host, path, &[])
        .unwrap_or_else(|error| panic!("GET http://{host}{path} failed: {error}"));
    assert_eq!(
        response.status,
        200,
        "GET http://{host}{path} body: {}",
        body_text(&response)
    );
    serde_json::from_slice(&response.body)
        .unwrap_or_else(|error| panic!("invalid JSON from http://{host}{path}: {error}"))
}

fn post_json(host: &str, path: &str, body: &str) {
    let response = http_request("POST", host, path, body.as_bytes())
        .unwrap_or_else(|error| panic!("POST http://{host}{path} failed: {error}"));
    assert_eq!(
        response.status,
        200,
        "POST http://{host}{path} body: {}",
        body_text(&response)
    );
}

fn sql(host: &str, statement: &str) -> Value {
    let body = format!("sql={}&db=public", form_encode(statement));
    let response = http_request("POST", host, "/v1/sql", body.as_bytes())
        .unwrap_or_else(|error| panic!("SQL {statement:?} via {host} failed: {error}"));
    assert_eq!(
        response.status,
        200,
        "SQL {statement:?} returned HTTP {}: {}",
        response.status,
        body_text(&response)
    );
    let json: Value = serde_json::from_slice(&response.body)
        .unwrap_or_else(|error| panic!("SQL {statement:?} returned invalid JSON: {error}"));
    assert!(
        json.get("error").is_none(),
        "SQL {statement:?} failed: {json}"
    );
    json
}

fn assert_scheduler_status(status: &Value, enabled: bool) {
    assert_eq!(status["enabled"], enabled);
    assert_eq!(status["max_concurrent_polls"], 2);
    let classes = status["classes"].as_object().expect("scheduler classes");
    assert!(
        classes.contains_key("query"),
        "scheduler query class: {status}"
    );
    assert!(
        classes.contains_key("write"),
        "scheduler write class: {status}"
    );
    assert_eq!(classes["query"]["weight"], 1);
    assert_eq!(classes["write"]["weight"], 1);
}

fn assert_values(output: &Value, expected: &[i64]) {
    let rows = output["output"][0]["records"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("SQL response has no rows: {output}"));
    let values: Vec<i64> = rows
        .iter()
        .map(|row| row[0].as_i64().expect("integer value in SQL row"))
        .collect();
    assert_eq!(values, expected);
}

fn form_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            b' ' => "+".to_string(),
            byte => format!("%{byte:02X}"),
        })
        .collect()
}

fn http_request(method: &str, host: &str, path: &str, body: &[u8]) -> Result<HttpResponse, String> {
    let mut stream = TcpStream::connect(host).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(REQUEST_TIMEOUT))
        .map_err(|error| format!("set read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(REQUEST_TIMEOUT))
        .map_err(|error| format!("set write timeout: {error}"))?;
    let content_type = if body.is_empty() {
        ""
    } else if method == "POST" && path == "/v1/sql" {
        "Content-Type: application/x-www-form-urlencoded\r\n"
    } else {
        "Content-Type: application/json\r\n"
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n{content_type}Content-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|error| format!("write request: {error}"))?;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|error| format!("read response: {error}"))?;
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| format!("malformed HTTP response: {}", body_text_bytes(&raw)))?;
    let headers = &raw[..header_end];
    let status_line = headers
        .split(|byte| *byte == b'\n')
        .next()
        .ok_or_else(|| "HTTP response has no status line".to_string())?;
    let status = status_line
        .split(|byte| *byte == b' ')
        .nth(1)
        .ok_or_else(|| "HTTP response has no status code".to_string())?
        .iter()
        .copied()
        .map(char::from)
        .collect::<String>()
        .parse::<u16>()
        .map_err(|error| format!("invalid HTTP status: {error}"))?;
    Ok(HttpResponse {
        status,
        body: raw[header_end + 4..].to_vec(),
    })
}

fn body_text(response: &HttpResponse) -> String {
    body_text_bytes(&response.body)
}

fn body_text_bytes(body: &[u8]) -> String {
    String::from_utf8_lossy(body).chars().take(2000).collect()
}
