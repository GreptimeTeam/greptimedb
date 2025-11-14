#[cfg(feature = "enterprise")]
pub use ee::*;

use crate::extension::common::InformationSchemaTableFactoryProviderRef;

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
    #[cfg(feature = "enterprise")]
    pub trigger_ddl_manager_factory: Option<TriggerDdlManagerFactoryRef>,
    pub info_schema_factory_provider: Option<InformationSchemaTableFactoryProviderRef>,
}
