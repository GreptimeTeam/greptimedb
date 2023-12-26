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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use once_cell::sync::OnceCell;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;

const PORT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

static PORTS: OnceCell<AtomicUsize> = OnceCell::new();

/// Return a unique port(in runtime) for test
pub fn get_port() -> usize {
    PORTS
        .get_or_init(|| AtomicUsize::new(rand::thread_rng().gen_range(3000..3800)))
        .fetch_add(1, Ordering::Relaxed)
}

/// Spin awaits a socket address until it is connectable, or timeout.
pub async fn check_connectable(ip_addr: &str, timeout: Duration) -> bool {
    let ip_addr = ip_addr.parse().unwrap();

    let check_task = async {
        loop {
            let socket = TcpSocket::new_v4().unwrap();
            match socket.connect(ip_addr).await {
                Ok(mut stream) => {
                    let _ = stream.shutdown().await;
                    break;
                }
                Err(_) => tokio::time::sleep(PORT_CHECK_INTERVAL).await,
            }
        }
    };

    tokio::time::timeout(timeout, check_task).await.is_ok()
}
