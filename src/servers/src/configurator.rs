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
pub trait HttpConfigurator: Send + Sync {
    /// Configures the given HTTP router.
    async fn configure_http(
        &self,
        router: HttpRouter,
    ) -> std::result::Result<HttpRouter, BoxedError>;
}

pub type HttpConfiguratorRef = Arc<dyn HttpConfigurator>;

/// A configurator that customizes or enhances a gRPC router.
#[async_trait::async_trait]
pub trait GrpcRouterConfigurator: Send + Sync {
    /// Configures the given gRPC router.
    async fn configure_grpc_router(
        &self,
        router: GrpcRouter,
    ) -> std::result::Result<GrpcRouter, BoxedError>;
}

pub type GrpcRouterConfiguratorRef = Arc<dyn GrpcRouterConfigurator>;

/// A registry that holds both HTTP and gRPC router configurators.
///
/// This provides a single place to register all configurators that can be
/// stored in plugins and applied to routers during server startup.
#[derive(Default)]
pub struct ConfiguratorRegistry {
    http_configurators: Vec<HttpConfiguratorRef>,
    grpc_router_configurators: Vec<GrpcRouterConfiguratorRef>,
}

impl ConfiguratorRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an HTTP configurator.
    pub fn register_http(&mut self, configurator: HttpConfiguratorRef) {
        self.http_configurators.push(configurator);
    }

    /// Registers a gRPC router configurator.
    pub fn register_grpc_router(&mut self, configurator: GrpcRouterConfiguratorRef) {
        self.grpc_router_configurators.push(configurator);
    }

    /// Applies all registered HTTP configurators to the given router sequentially.
    pub async fn configure_http(
        &self,
        mut router: HttpRouter,
    ) -> std::result::Result<HttpRouter, BoxedError> {
        for configurator in &self.http_configurators {
            router = configurator.configure_http(router).await?;
        }
        Ok(router)
    }

    /// Applies all registered gRPC router configurators to the given router sequentially.
    pub async fn configure_grpc_router(
        &self,
        mut router: GrpcRouter,
    ) -> std::result::Result<GrpcRouter, BoxedError> {
        for configurator in &self.grpc_router_configurators {
            router = configurator.configure_grpc_router(router).await?;
        }
        Ok(router)
    }
}

pub type ConfiguratorRegistryRef = Arc<ConfiguratorRegistry>;

/// A configurator that customizes or enhances a [`GrpcServerBuilder`].
///
/// This is kept separate from the registry because it uses a different context type
/// (e.g., `GrpcConfigureContext` in flownode).
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
