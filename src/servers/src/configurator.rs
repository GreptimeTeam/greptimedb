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

use axum::Router as HttpRouter;
use common_error::ext::BoxedError;
use tonic::transport::server::Router as GrpcRouter;

use crate::grpc::builder::GrpcServerBuilder;

/// A configurator that customizes or enhances an HTTP router.
#[async_trait::async_trait]
pub trait HttpConfigurator<C>: Send + Sync {
    /// Configures the given HTTP router using the provided context.
    async fn configure_http(
        &self,
        route: HttpRouter,
        ctx: C,
    ) -> std::result::Result<HttpRouter, BoxedError>;
}

pub type HttpConfiguratorRef<C> = Arc<dyn HttpConfigurator<C>>;

/// A configurator that customizes or enhances a gRPC router.
#[async_trait::async_trait]
pub trait GrpcRouterConfigurator<C>: Send + Sync {
    /// Configures the given gRPC router using the provided context.
    async fn configure_grpc_router(
        &self,
        route: GrpcRouter,
        ctx: C,
    ) -> std::result::Result<GrpcRouter, BoxedError>;
}

pub type GrpcRouterConfiguratorRef<C> = Arc<dyn GrpcRouterConfigurator<C>>;

/// A configurator that customizes or enhances a [`GrpcServerBuilder`].
#[async_trait::async_trait]
pub trait GrpcBuilderConfigurator<C>: Send + Sync {
    async fn configure(
        &self,
        builder: GrpcServerBuilder,
        ctx: C,
    ) -> std::result::Result<GrpcServerBuilder, BoxedError>;
}

pub type GrpcBuilderConfiguratorRef<C> = Arc<dyn GrpcBuilderConfigurator<C>>;
