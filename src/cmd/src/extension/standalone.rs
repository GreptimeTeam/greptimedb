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

#[cfg(feature = "enterprise")]
pub use ee::*;
#[cfg(feature = "enterprise")]
use operator::statement::TriggerQuerierFactoryRef;

use crate::extension::common::InformationSchemaTableFactoriesRef;

#[cfg(feature = "enterprise")]
mod ee {
    use std::sync::Arc;

    use catalog::CatalogManagerRef;
    use common_error::ext::BoxedError;
    use common_meta::ddl_manager::TriggerDdlManagerRef;
    use common_meta::kv_backend::KvBackendRef;
    use flow::FrontendClient;

    #[async_trait::async_trait]
    pub trait TriggerDdlManagerFactory: Send + Sync {
        async fn create(
            &self,
            ctx: TriggerDdlManagerRequest,
        ) -> Result<TriggerDdlManagerRef, BoxedError>;
    }

    pub type TriggerDdlManagerFactoryRef = Arc<dyn TriggerDdlManagerFactory>;

    pub struct TriggerDdlManagerRequest {
        pub kv_backend: KvBackendRef,
        pub catalog_manager: CatalogManagerRef,
        pub fe_client: Arc<FrontendClient>,
    }
}

#[derive(Default)]
pub struct Extension {
    pub info_schema_factories: Option<InformationSchemaTableFactoriesRef>,
    #[cfg(feature = "enterprise")]
    pub trigger_ddl_manager_factory: Option<TriggerDdlManagerFactoryRef>,
    #[cfg(feature = "enterprise")]
    pub trigger_querier_factory: Option<TriggerQuerierFactoryRef>,
}
