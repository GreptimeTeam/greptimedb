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

use api::v1::greptime_database_client::GreptimeDatabaseClient;
use api::v1::greptime_request::Request;
use api::v1::{
    AuthHeader, GreptimeRequest, GreptimeResponse, InsertRequest, InsertRequests, RequestHeader,
    RowInsertRequest, RowInsertRequests,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Response, Status};

use crate::error::{self, Result};
use crate::from_grpc_response;

/// A structure that provides some methods for streaming data insert.
///
/// [`StreamInserter`] cannot be constructed via the `StreamInserter::new` method.
/// You can use the following way to obtain [`StreamInserter`].
///
/// ```ignore
/// let grpc_client = Client::with_urls(vec!["127.0.0.1:4002"]);
/// let client = Database::new_with_dbname("db_name", grpc_client);
/// let stream_inserter = client.streaming_inserter().unwrap();
/// ```
///
/// If you want to see a concrete usage example, please see
/// [stream_inserter.rs](https://github.com/GreptimeTeam/greptimedb/blob/main/src/client/examples/stream_ingest.rs).
pub struct StreamInserter {
    sender: mpsc::Sender<GreptimeRequest>,

    auth_header: Option<AuthHeader>,

    dbname: String,

    join: JoinHandle<std::result::Result<Response<GreptimeResponse>, Status>>,
}

impl StreamInserter {
    pub(crate) fn new(
        mut client: GreptimeDatabaseClient<Channel>,
        dbname: String,
        auth_header: Option<AuthHeader>,
        channel_size: usize,
    ) -> StreamInserter {
        let (send, recv) = tokio::sync::mpsc::channel(channel_size);

        let join: JoinHandle<std::result::Result<Response<GreptimeResponse>, Status>> =
            tokio::spawn(async move {
                let recv_stream = ReceiverStream::new(recv);
                client.handle_requests(recv_stream).await
            });

        StreamInserter {
            sender: send,
            auth_header,
            dbname,
            join,
        }
    }

    pub async fn insert(&self, requests: Vec<InsertRequest>) -> Result<()> {
        let inserts = InsertRequests { inserts: requests };
        let request = self.to_rpc_request(Request::Inserts(inserts));

        self.sender.send(request).await.map_err(|e| {
            error::ClientStreamingSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }

    pub async fn row_insert(&self, requests: Vec<RowInsertRequest>) -> Result<()> {
        let inserts = RowInsertRequests { inserts: requests };
        let request = self.to_rpc_request(Request::RowInserts(inserts));

        self.sender.send(request).await.map_err(|e| {
            error::ClientStreamingSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }

    pub async fn finish(self) -> Result<u32> {
        drop(self.sender);

        let response = self.join.await.unwrap()?;
        let response = response.into_inner();
        from_grpc_response(response)
    }

    fn to_rpc_request(&self, request: Request) -> GreptimeRequest {
        GreptimeRequest {
            header: Some(RequestHeader {
                authorization: self.auth_header.clone(),
                dbname: self.dbname.clone(),
                ..Default::default()
            }),
            request: Some(request),
        }
    }
}
