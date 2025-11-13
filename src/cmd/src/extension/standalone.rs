#[cfg(feature = "enterprise")]
pub use ee::*;

#[cfg(feature = "enterprise")]
use crate::extension::common::IstFactoryProviderRef;

#[cfg(feature = "enterprise")]
mod ee {
    use std::sync::Arc;

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
        pub fe_client: Arc<FrontendClient>,
    }
}

#[derive(Default)]
pub struct Extension {
    #[cfg(feature = "enterprise")]
    pub trigger_ddl_manager_factory: Option<TriggerDdlManagerFactoryRef>,
    /// Information schema table factory provider.
    #[cfg(feature = "enterprise")]
    pub ist_factory_provider: Option<IstFactoryProviderRef>,
}
