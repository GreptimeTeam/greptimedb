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

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use client::Client;
    use common_grpc::channel_manager::ChannelManager;
    use futures_util::future::BoxFuture;
    use http::Uri;
    use hyper_util::rt::TokioIo;
    use servers::grpc::GrpcServerConfig;
    use servers::server::Server;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tower::Service;

    use crate::test_util::{StorageType, setup_grpc_server_with};

    struct NetworkTrafficMonitorableConnector {
        interested_tx: mpsc::Sender<String>,
    }

    impl Service<Uri> for NetworkTrafficMonitorableConnector {
        type Response = TokioIo<CollectGrpcResponseFrameTypeStream>;
        type Error = String;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, uri: Uri) -> Self::Future {
            let frame_types = self.interested_tx.clone();

            Box::pin(async move {
                let addr = format!(
                    "{}:{}",
                    uri.host().unwrap_or("localhost"),
                    uri.port_u16().unwrap_or(4001),
                );
                let inner = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
                Ok(TokioIo::new(CollectGrpcResponseFrameTypeStream {
                    inner,
                    frame_types,
                }))
            })
        }
    }

    struct CollectGrpcResponseFrameTypeStream {
        inner: TcpStream,
        frame_types: mpsc::Sender<String>,
    }

    impl AsyncRead for CollectGrpcResponseFrameTypeStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let before_len = buf.filled().len();

            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = &result {
                let after_len = buf.filled().len();

                let new_data = &buf.filled()[before_len..after_len];
                if let Some(frame_type) = maybe_decode_frame_type(new_data)
                    && let Err(_) = self.frame_types.try_send(frame_type.to_string())
                {
                    return Poll::Ready(Err(io::Error::other("interested party has gone")));
                }
            }
            result
        }
    }

    fn maybe_decode_frame_type(data: &[u8]) -> Option<&str> {
        (data.len() >= 9).then(|| match data[3] {
            0x0 => "DATA",
            0x1 => "HEADERS",
            0x2 => "PRIORITY",
            0x3 => "RST_STREAM",
            0x4 => "SETTINGS",
            0x5 => "PUSH_PROMISE",
            0x6 => "PING",
            0x7 => "GOAWAY",
            0x8 => "WINDOW_UPDATE",
            0x9 => "CONTINUATION",
            _ => "UNKNOWN",
        })
    }

    impl AsyncWrite for CollectGrpcResponseFrameTypeStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_grpc_max_connection_age() {
        let config = GrpcServerConfig {
            max_connection_age: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let (_db, server) = setup_grpc_server_with(
            StorageType::File,
            "test_grpc_max_connection_age",
            None,
            Some(config),
            None,
        )
        .await;
        let addr = server.bind_addr().unwrap().to_string();

        let channel_manager = ChannelManager::new();
        let client = Client::with_manager_and_urls(channel_manager.clone(), vec![&addr]);

        let (tx, mut rx) = mpsc::channel(1024);
        channel_manager
            .reset_with_connector(
                &addr,
                NetworkTrafficMonitorableConnector { interested_tx: tx },
            )
            .unwrap();

        let recv = tokio::spawn(async move {
            let sleep = tokio::time::sleep(Duration::from_secs(3));
            tokio::pin!(sleep);

            let mut frame_types = vec![];
            loop {
                tokio::select! {
                    x = rx.recv() => {
                        if let Some(x) = x {
                            frame_types.push(x);
                        } else {
                            break;
                        }
                    }
                    _ = &mut sleep => {
                        break;
                    }
                }
            }
            frame_types
        });

        // Drive the gRPC connection, has no special meaning for this keep-alive test.
        let _ = client.health_check().await;

        let frame_types = recv.await.unwrap();
        // If "max_connection_age" has taken effects, server will return a "GOAWAY" message.
        assert!(
            frame_types.iter().any(|x| x == "GOAWAY"),
            "{:?}",
            frame_types
        );

        server.shutdown().await.unwrap();
    }
}
