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

use crate::{ObjectStore, Result};

/// Temp folder for object store test
pub struct TempFolder {
    store: ObjectStore,
    // The path under root.
    path: String,
}

impl TempFolder {
    pub fn new(store: &ObjectStore, path: &str) -> Self {
        Self {
            store: store.clone(),
            path: path.to_string(),
        }
    }

    pub async fn remove_all(&self) -> Result<()> {
        self.store.delete_with(&self.path).recursive(true).await
    }
}

/// A minimal HTTP/1.1 server for object storage tests. Every connection serves
/// one request; the handler builds the raw response from the request head.
pub struct MockHttpServer {
    pub port: u16,
    request_heads: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl MockHttpServer {
    pub async fn start(handler: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let request_heads = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = request_heads.clone();
        let handler = std::sync::Arc::new(handler);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let recorded = recorded.clone();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let content_length = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + content_length {
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    recorded.lock().unwrap().push(head.clone());
                    let response = handler(&head);
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self {
            port,
            request_heads,
        }
    }

    /// Request lines and headers of the requests received so far.
    pub fn request_heads(&self) -> Vec<String> {
        self.request_heads.lock().unwrap().clone()
    }

    pub fn request_count(&self) -> usize {
        self.request_heads.lock().unwrap().len()
    }
}

/// Builds a raw HTTP/1.1 response that closes the connection.
pub fn http_response(status: &str, headers: &[(&str, String)], body: &str) -> String {
    let mut out = format!("HTTP/1.1 {status}\r\nconnection: close\r\n");
    for (name, value) in headers {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    out.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
    out
}
