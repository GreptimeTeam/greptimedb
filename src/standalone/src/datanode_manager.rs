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

use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::region::{RegionRequest, RegionResponse as RegionResponseV1, region_request};
use async_trait::async_trait;
use client::region;
use common_error::ext::BoxedError;
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::node_manager::{
    Datanode, DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef,
};
use common_meta::peer::Peer;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use snafu::ResultExt;

use crate::error::{InvalidRegionRequestSnafu, InvokeRegionServerSnafu, Result};

pub struct StandaloneDatanodeManager {
    pub region_server: RegionServer,
    pub flow_server: FlownodeRef,
}

#[async_trait]
impl DatanodeManager for StandaloneDatanodeManager {
    async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
        RegionInvoker::arc(self.region_server.clone())
    }
}

#[async_trait]
impl FlownodeManager for StandaloneDatanodeManager {
    async fn flownode(&self, _node: &Peer) -> FlownodeRef {
        self.flow_server.clone()
    }
}

/// Relative to [client::region::RegionRequester]
struct RegionInvoker {
    region_server: RegionServer,
}

impl RegionInvoker {
    fn arc(region_server: RegionServer) -> Arc<Self> {
        Arc::new(Self { region_server })
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<RegionResponseV1> {
        self.region_server
            .handle_region_request(request)
            .await
            .context(InvokeRegionServerSnafu)
    }
}

#[async_trait]
impl Datanode for RegionInvoker {
    async fn handle_request(&self, request: Arc<RegionRequest>) -> MetaResult<RegionResponse> {
        // Local execution consumes the request; standalone does not retain it for retries.
        let mut request = Arc::try_unwrap(request)
            .map_err(|_| {
                InvalidRegionRequestSnafu {
                    reason: "local region request has multiple owners",
                }
                .build()
            })
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        let request_count = match request.body.as_ref() {
            Some(region_request::Body::Inserts(requests)) => Some(requests.requests.len()),
            Some(region_request::Body::Deletes(requests)) => Some(requests.requests.len()),
            _ => None,
        };
        if request_count.is_some() {
            request
                .header
                .get_or_insert_with(Default::default)
                .query_context
                .get_or_insert_with(Default::default)
                .extensions
                .insert(
                    api::region::ACCEPT_PARTIAL_RESULT.to_string(),
                    "true".to_string(),
                );
        }
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_region_request"));
        let response = self
            .handle_inner(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        region::decode_region_response(response, request_count).map_err(Into::into)
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let region_id = request.region_id.to_string();
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!(
                "RegionInvoker::handle_query",
                region_id = region_id
            ));
        self.region_server
            .handle_read(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::Rows;
    use api::v1::region::{InsertRequest, InsertRequests};
    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use datanode::tests::{MockRegionEngine, mock_region_server};
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::CreateRequestBuilder;
    use store_api::region_request::RegionRequest as StoreRegionRequest;
    use store_api::storage::RegionId;

    use super::*;

    #[tokio::test]
    async fn test_partial_write_preserves_user_status() {
        for (code, http_status) in [
            (StatusCode::StorageUnavailable, 503),
            (StatusCode::InvalidArguments, 400),
        ] {
            let mut server = mock_region_server();
            let (engine, _receiver) = MockRegionEngine::with_mock_fn(
                MITO_ENGINE_NAME,
                Box::new(move |region_id, request| {
                    if matches!(request, StoreRegionRequest::Put(_)) {
                        Err(BoxedError::new(MockError::new(code)))
                            .context(datanode::error::HandleRegionRequestSnafu { region_id })
                    } else {
                        Ok(0)
                    }
                }),
            );
            server.register_engine(engine);
            let mut requests = Vec::new();
            for number in 0..2 {
                let region_id = RegionId::new(1, number);
                server
                    .handle_request(
                        region_id,
                        StoreRegionRequest::Create(CreateRequestBuilder::new().build()),
                    )
                    .await
                    .unwrap();
                requests.push(InsertRequest {
                    region_id: region_id.as_u64(),
                    rows: Some(Rows::default()),
                    ..Default::default()
                });
            }
            let datanode: DatanodeRef = RegionInvoker::arc(server);
            let error = datanode
                .handle(RegionRequest {
                    header: Some(Default::default()),
                    body: Some(region_request::Body::Inserts(InsertRequests { requests })),
                })
                .await
                .unwrap_err();
            assert_eq!(code, error.status_code());
            assert_eq!(RetryHint::Retryable, error.retry_hint());
            assert_eq!(
                http_status,
                servers::error::status_code_to_http_status(&error.status_code()).as_u16()
            );
            let meta_error::Error::PartialRegion { response } = error else {
                panic!("expected partial write")
            };
            assert_eq!(2, response.failures.len());
            assert!(
                response
                    .failures
                    .iter()
                    .all(|(_, error)| error.status_code() == code)
            );
        }
    }
}
