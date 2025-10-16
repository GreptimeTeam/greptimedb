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

use std::collections::HashMap;
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::region::RegionRequest;
use api::v1::region::region_request::Body;
use common_error::ext::{BoxedError, ErrorExt, StackError};
use common_error::status_code::StatusCode;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::debug;
use snafu::{ResultExt, Snafu};
use store_api::metadata::RegionMetadata;
use store_api::storage::RegionId;
use tokio::sync::mpsc;

use crate::error::{self, Error, Result};
use crate::peer::Peer;
use crate::test_util::MockDatanodeHandler;

#[async_trait::async_trait]
impl MockDatanodeHandler for () {
    async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
        Ok(RegionResponse {
            affected_rows: 0,
            extensions: Default::default(),
            metadata: Vec::new(),
        })
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

type RegionRequestHandler =
    Arc<dyn Fn(Peer, RegionRequest) -> Result<RegionResponse> + Send + Sync>;

#[derive(Clone)]
pub struct DatanodeWatcher {
    sender: mpsc::Sender<(Peer, RegionRequest)>,
    handler: Option<RegionRequestHandler>,
}

impl DatanodeWatcher {
    pub fn new(sender: mpsc::Sender<(Peer, RegionRequest)>) -> Self {
        Self {
            sender,
            handler: None,
        }
    }

    pub fn with_handler(
        mut self,
        user_handler: impl Fn(Peer, RegionRequest) -> Result<RegionResponse> + Send + Sync + 'static,
    ) -> Self {
        self.handler = Some(Arc::new(user_handler));
        self
    }
}

#[async_trait::async_trait]
impl MockDatanodeHandler for DatanodeWatcher {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        debug!("Returning Ok(0) for request: {request:?}, peer: {peer:?}");
        self.sender
            .send((peer.clone(), request.clone()))
            .await
            .unwrap();
        if let Some(handler) = self.handler.as_ref() {
            handler(peer.clone(), request)
        } else {
            Ok(RegionResponse::new(0))
        }
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct RetryErrorDatanodeHandler;

#[async_trait::async_trait]
impl MockDatanodeHandler for RetryErrorDatanodeHandler {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        debug!("Returning retry later for request: {request:?}, peer: {peer:?}");
        Err(Error::RetryLater {
            source: BoxedError::new(
                error::UnexpectedSnafu {
                    err_msg: "retry later",
                }
                .build(),
            ),
            clean_poisons: false,
        })
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct UnexpectedErrorDatanodeHandler;

#[async_trait::async_trait]
impl MockDatanodeHandler for UnexpectedErrorDatanodeHandler {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        debug!("Returning mock error for request: {request:?}, peer: {peer:?}");
        error::UnexpectedSnafu {
            err_msg: "mock error",
        }
        .fail()
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct RequestOutdatedErrorDatanodeHandler;

#[derive(Debug, Snafu)]
#[snafu(display("A mock RequestOutdated error"))]
struct MockRequestOutdatedError;

impl StackError for MockRequestOutdatedError {
    fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}

    fn next(&self) -> Option<&dyn StackError> {
        None
    }
}

impl ErrorExt for MockRequestOutdatedError {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::RequestOutdated
    }
}

#[async_trait::async_trait]
impl MockDatanodeHandler for RequestOutdatedErrorDatanodeHandler {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        debug!("Returning mock error for request: {request:?}, peer: {peer:?}");
        Err(BoxedError::new(MockRequestOutdatedError)).context(error::ExternalSnafu)
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct NaiveDatanodeHandler;

#[async_trait::async_trait]
impl MockDatanodeHandler for NaiveDatanodeHandler {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        debug!("Returning Ok(0) for request: {request:?}, peer: {peer:?}");
        Ok(RegionResponse::new(0))
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct PartialSuccessDatanodeHandler {
    pub retryable: bool,
}

#[async_trait::async_trait]
impl MockDatanodeHandler for PartialSuccessDatanodeHandler {
    async fn handle(&self, peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
        let success = peer.id.is_multiple_of(2);
        if success {
            Ok(RegionResponse::new(0))
        } else if self.retryable {
            Err(Error::RetryLater {
                source: BoxedError::new(
                    error::UnexpectedSnafu {
                        err_msg: "retry later",
                    }
                    .build(),
                ),
                clean_poisons: false,
            })
        } else {
            error::UnexpectedSnafu {
                err_msg: "mock error",
            }
            .fail()
        }
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct AllFailureDatanodeHandler {
    pub retryable: bool,
}

#[async_trait::async_trait]
impl MockDatanodeHandler for AllFailureDatanodeHandler {
    async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
        if self.retryable {
            Err(Error::RetryLater {
                source: BoxedError::new(
                    error::UnexpectedSnafu {
                        err_msg: "retry later",
                    }
                    .build(),
                ),
                clean_poisons: false,
            })
        } else {
            error::UnexpectedSnafu {
                err_msg: "mock error",
            }
            .fail()
        }
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[derive(Clone)]
pub struct ListMetadataDatanodeHandler {
    pub region_metadatas: HashMap<RegionId, Option<RegionMetadata>>,
}

impl ListMetadataDatanodeHandler {
    pub fn new(region_metadatas: HashMap<RegionId, Option<RegionMetadata>>) -> Self {
        Self { region_metadatas }
    }
}

#[async_trait::async_trait]
impl MockDatanodeHandler for ListMetadataDatanodeHandler {
    async fn handle(&self, _peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        let Some(Body::ListMetadata(req)) = request.body else {
            unreachable!()
        };
        let mut response = RegionResponse::new(0);

        let mut output = Vec::with_capacity(req.region_ids.len());
        for region_id in req.region_ids {
            match self.region_metadatas.get(&RegionId::from_u64(region_id)) {
                Some(metadata) => {
                    output.push(metadata.clone());
                }
                None => {
                    output.push(None);
                }
            }
        }

        response.metadata = serde_json::to_vec(&output).unwrap();
        Ok(response)
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}
