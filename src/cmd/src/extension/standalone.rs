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

use catalog::CatalogManagerRef;
use catalog::information_schema::InformationSchemaTableFactoryRef;
use common_error::ext::BoxedError;
#[cfg(feature = "enterprise")]
use common_meta::ddl_manager::TriggerDdlManagerRef;
use common_meta::kv_backend::KvBackendRef;
use flow::FrontendClient;
#[cfg(feature = "enterprise")]
use operator::statement::TriggerQuerierRef;

/// The extension point for standalone instance.
#[derive(Default)]
pub struct Extension {
    #[cfg(feature = "enterprise")]
    pub trigger_ddl_manager: Option<TriggerDdlManagerRef>,
    #[cfg(feature = "enterprise")]
    pub trigger_querier: Option<TriggerQuerierRef>,
}

/// Factory trait to create Extension instance.
#[async_trait::async_trait]
pub trait ExtensionFactory: InformationSchemaTableFactories + Send + Sync {
    async fn create(&self, ctx: ExtensionContext) -> Result<Extension, BoxedError>;
}

pub type StandaloneExtensionFactoryRef = Arc<dyn ExtensionFactory>;

pub struct ExtensionContext {
    pub kv_backend: KvBackendRef,
    pub catalog_manager: CatalogManagerRef,
    pub frontend_client: Arc<FrontendClient>,
}

/// Provides additional information schema table factories beyond the built-in
/// ones.
///
/// These are typically provided by enterprise or optional modules, such as:
/// - `information_schema.triggers`
/// - `information_schema.alerts`
///
/// It is separated from the [`ExtensionFactory`] to avoid circular dependencies,
/// since the [`CatalogManagerRef`] of [`ExtensionContext`] is dependent on the
/// [`InformationSchemaTableFactories`].
#[async_trait::async_trait]
pub trait InformationSchemaTableFactories: Send + Sync {
    async fn create_factories(
        &self,
        ctx: InfoTableFactoryContext,
    ) -> Result<HashMap<String, InformationSchemaTableFactoryRef>, BoxedError>;
}

pub type InformationSchemaTableFactoriesRef = Arc<dyn InformationSchemaTableFactories>;

/// Context for information schema table factory providers.
pub struct InfoTableFactoryContext {
    pub fe_client: Arc<FrontendClient>,
}
