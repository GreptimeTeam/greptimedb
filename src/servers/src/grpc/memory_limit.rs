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

use std::task::{Context, Poll};

use common_memory_manager::{MemoryManager, OnExhaustedPolicy};
use futures::future::BoxFuture;
use tonic::server::NamedService;
use tower::{Layer, Service};

use crate::memory_metrics::GrpcMemoryMetrics;

/// State containing MemoryManager and policy for gRPC
#[derive(Clone)]
pub struct GrpcMemoryLimitState {
    pub manager: MemoryManager<GrpcMemoryMetrics>,
    pub policy: OnExhaustedPolicy,
}

#[derive(Clone)]
pub struct MemoryLimiterExtensionLayer {
    state: GrpcMemoryLimitState,
}

impl MemoryLimiterExtensionLayer {
    pub fn new(state: GrpcMemoryLimitState) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for MemoryLimiterExtensionLayer {
    type Service = MemoryLimiterExtensionService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MemoryLimiterExtensionService {
            inner: service,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MemoryLimiterExtensionService<S> {
    inner: S,
    state: GrpcMemoryLimitState,
}

impl<S: NamedService> NamedService for MemoryLimiterExtensionService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for MemoryLimiterExtensionService<S>
where
    S: Service<http::Request<ReqBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        req.extensions_mut().insert(self.state.clone());
        Box::pin(self.inner.call(req))
    }
}
