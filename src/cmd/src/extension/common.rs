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
use std::result::Result;
use std::sync::Arc;

use catalog::CatalogManagerRef;
use catalog::information_schema::InformationSchemaTableFactoryRef;
use common_error::ext::BoxedError;
use common_meta::FlownodeId;
use common_meta::kv_backend::KvBackendRef;
use flow::FrontendClient;
use servers::grpc::builder::GrpcServerBuilder;

/// Provides additional information schema table factories beyond the built-in
/// ones.
///
/// These are typically provided by enterprise or optional modules, such as:
/// - `information_schema.triggers`
/// - `information_schema.alerts`
#[async_trait::async_trait]
pub trait InformationSchemaTableFactories: Send + Sync {
    async fn create_factories(
        &self,
        ctx: TableFactoryContext,
    ) -> Result<HashMap<String, InformationSchemaTableFactoryRef>, BoxedError>;
}

pub type InformationSchemaTableFactoriesRef = Arc<dyn InformationSchemaTableFactories>;

/// Context for information schema table factory providers.
pub struct TableFactoryContext {
    pub fe_client: Option<Arc<FrontendClient>>,
}

/// Allows extending the gRPC server with additional services (e.g., enterprise
/// features).
#[async_trait::async_trait]
pub trait GrpcExtension: Send + Sync {
    async fn extend_grpc_services(
        &self,
        builder: &mut GrpcServerBuilder,
        ctx: GrpcExtensionContext,
    ) -> Result<(), BoxedError>;
}

/// Context provided to gRPC service extensions during server construction.
pub struct GrpcExtensionContext {
    pub kv_backend: KvBackendRef,
    pub fe_client: Arc<FrontendClient>,
    pub flownode_id: FlownodeId,
    pub catalog_manager: CatalogManagerRef,
}

pub type GrpcExtensionRef = Arc<dyn GrpcExtension>;
