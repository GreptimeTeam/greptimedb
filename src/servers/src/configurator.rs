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
        router: HttpRouter,
        ctx: C,
    ) -> std::result::Result<HttpRouter, BoxedError>;
}

pub type HttpConfiguratorRef<C> = Arc<dyn HttpConfigurator<C>>;

/// A list of HTTP configurators. Implement the trait to apply all configurators sequentially.
pub type HttpConfiguratorList<C> = Vec<HttpConfiguratorRef<C>>;

/// A reference to a list of HTTP configurators that can be stored in plugins.
pub type HttpConfiguratorListRef<C> = Arc<HttpConfiguratorList<C>>;

#[async_trait::async_trait]
impl<C: Clone + Send + Sync + 'static> HttpConfigurator<C> for HttpConfiguratorList<C> {
    async fn configure_http(
        &self,
        mut router: HttpRouter,
        ctx: C,
    ) -> std::result::Result<HttpRouter, BoxedError> {
        for configurator in self {
            router = configurator.configure_http(router, ctx.clone()).await?;
        }
        Ok(router)
    }
}

/// A configurator that customizes or enhances a gRPC router.
#[async_trait::async_trait]
pub trait GrpcRouterConfigurator<C>: Send + Sync {
    /// Configures the given gRPC router using the provided context.
    async fn configure_grpc_router(
        &self,
        router: GrpcRouter,
        ctx: C,
    ) -> std::result::Result<GrpcRouter, BoxedError>;
}

pub type GrpcRouterConfiguratorRef<C> = Arc<dyn GrpcRouterConfigurator<C>>;

/// A list of gRPC router configurators. Implement the trait to apply all configurators sequentially.
pub type GrpcRouterConfiguratorList<C> = Vec<GrpcRouterConfiguratorRef<C>>;

/// A reference to a list of gRPC router configurators that can be stored in plugins.
pub type GrpcRouterConfiguratorListRef<C> = Arc<GrpcRouterConfiguratorList<C>>;

#[async_trait::async_trait]
impl<C: Clone + Send + Sync + 'static> GrpcRouterConfigurator<C> for GrpcRouterConfiguratorList<C> {
    async fn configure_grpc_router(
        &self,
        mut router: GrpcRouter,
        ctx: C,
    ) -> std::result::Result<GrpcRouter, BoxedError> {
        for configurator in self {
            router = configurator
                .configure_grpc_router(router, ctx.clone())
                .await?;
        }
        Ok(router)
    }
}

/// A configurator that customizes or enhances a [`GrpcServerBuilder`].
#[async_trait::async_trait]
pub trait GrpcBuilderConfigurator<C>: Send + Sync {
    /// Configures the given gRPC server builder using the provided context.
    async fn configure(
        &self,
        builder: GrpcServerBuilder,
        ctx: C,
    ) -> std::result::Result<GrpcServerBuilder, BoxedError>;
}

pub type GrpcBuilderConfiguratorRef<C> = Arc<dyn GrpcBuilderConfigurator<C>>;

/// A list of gRPC builder configurators. Implement the trait to apply all configurators sequentially.
pub type GrpcBuilderConfiguratorList<C> = Vec<GrpcBuilderConfiguratorRef<C>>;

/// A reference to a list of gRPC builder configurators that can be stored in plugins.
pub type GrpcBuilderConfiguratorListRef<C> = Arc<GrpcBuilderConfiguratorList<C>>;

#[async_trait::async_trait]
impl<C: Clone + Send + Sync + 'static> GrpcBuilderConfigurator<C> for GrpcBuilderConfiguratorList<C> {
    async fn configure(
        &self,
        mut builder: GrpcServerBuilder,
        ctx: C,
    ) -> std::result::Result<GrpcServerBuilder, BoxedError> {
        for configurator in self {
            builder = configurator.configure(builder, ctx.clone()).await?;
        }
        Ok(builder)
    }
}
