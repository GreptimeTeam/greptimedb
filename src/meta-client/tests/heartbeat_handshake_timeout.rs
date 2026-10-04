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

use std::pin::Pin;
use std::time::Duration;

use api::v1::meta::heartbeat_server::{Heartbeat, HeartbeatServer};
use api::v1::meta::{
    AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse, Peer, ResponseHeader,
};
use common_error::ext::{ErrorExt, RetryHint};
use common_meta::distributed_time_constants::HEARTBEAT_TIMEOUT;
use futures::Stream;
use meta_client::error::Error;
use meta_client::{MetaClientOptions, MetaClientRef, MetaClientType, create_meta_client};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status, Streaming};

type HeartbeatStream =
    Pin<Box<dyn Stream<Item = Result<HeartbeatResponse, Status>> + Send + 'static>>;

struct MockMetasrv {
    addr: String,
    send_first_heartbeat_response: bool,
}

#[async_trait::async_trait]
impl Heartbeat for MockMetasrv {
    type HeartbeatStream = HeartbeatStream;

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let send_first_heartbeat_response = self.send_first_heartbeat_response;
        let stream = futures::stream::once(async move {
            let mut request = request.into_inner();
            request
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("missing heartbeat request"))?;

            if send_first_heartbeat_response {
                Ok(HeartbeatResponse::default())
            } else {
                futures::future::pending::<Result<HeartbeatResponse, Status>>().await
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn ask_leader(
        &self,
        _request: Request<AskLeaderRequest>,
    ) -> Result<Response<AskLeaderResponse>, Status> {
        Ok(Response::new(AskLeaderResponse {
            header: Some(ResponseHeader::default()),
            leader: Some(Peer {
                id: 1,
                addr: self.addr.clone(),
            }),
        }))
    }
}

async fn create_client(send_first_heartbeat_response: bool) -> MetaClientRef {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = MockMetasrv {
        addr: addr.to_string(),
        send_first_heartbeat_response,
    };

    tokio::spawn(async move {
        let service = HeartbeatServer::new(mock)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let options = MetaClientOptions {
        metasrv_addrs: vec![addr.to_string()],
        connect_timeout: Duration::from_millis(500),
        ..Default::default()
    };
    create_meta_client(
        MetaClientType::Datanode { member_id: 1 },
        &options,
        None,
        None,
    )
    .await
    .expect("the mock answers leader discovery")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_handshake_succeeds_when_the_server_responds() {
    let client = create_client(true).await;

    tokio::time::timeout(HEARTBEAT_TIMEOUT, client.heartbeat())
        .await
        .expect("heartbeat handshake must complete before its deadline")
        .expect("heartbeat handshake must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_handshake_times_out_when_the_stream_stays_silent() {
    let client = create_client(false).await;

    let result = tokio::time::timeout(2 * HEARTBEAT_TIMEOUT, client.heartbeat())
        .await
        .expect("heartbeat handshake must be bounded");
    let error = match result {
        Ok(_) => panic!("a silent heartbeat stream must fail the handshake"),
        Err(error) => error,
    };
    assert!(matches!(
        &error,
        Error::HeartbeatHandshakeTimeout { timeout, .. } if *timeout == HEARTBEAT_TIMEOUT
    ));
    assert_eq!(error.retry_hint(), RetryHint::Retryable);
}
