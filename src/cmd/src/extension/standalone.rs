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

use crate::extension::common::{InformationSchemaTableFactories, TableFactoryContext};

#[derive(Default)]
pub struct Extension {
    #[cfg(feature = "enterprise")]
    pub trigger_ddl_manager: Option<TriggerDdlManagerRef>,
    #[cfg(feature = "enterprise")]
    pub trigger_querier: Option<TriggerQuerierRef>,
}

/// Factory trait to create Extension instances.
pub trait ExtensionFactory: InformationSchemaTableFactories + Send + Sync {
    fn create(
        &self,
        ctx: ExtensionContext,
    ) -> impl Future<Output = Result<Extension, BoxedError>> + Send;
}

pub struct ExtensionContext {
    pub kv_backend: KvBackendRef,
    pub catalog_manager: CatalogManagerRef,
    pub frontend_client: Arc<FrontendClient>,
}

pub struct DefaultExtensionFactory;

#[async_trait::async_trait]
impl InformationSchemaTableFactories for DefaultExtensionFactory {
    async fn create_factories(
        &self,
        _ctx: TableFactoryContext,
    ) -> Result<HashMap<String, InformationSchemaTableFactoryRef>, BoxedError> {
        Ok(HashMap::new())
    }
}

impl ExtensionFactory for DefaultExtensionFactory {
    async fn create(&self, _ctx: ExtensionContext) -> Result<Extension, BoxedError> {
        Ok(Extension::default())
    }
}
