#[cfg(feature = "enterprise")]
pub use ee::*;

#[cfg(feature = "enterprise")]
mod ee {
    use std::sync::Arc;

    use common_error::ext::BoxedError;
    use common_meta::ddl_manager::TriggerDdlManagerRef;
    use common_meta::kv_backend::KvBackendRef;

    use crate::metasrv::{SelectorContext, SelectorRef};

    #[async_trait::async_trait]
    pub trait TriggerDdlManagerFactory {
        async fn create(
            &self,
            req: MakeTriggerDdlManagerRequest,
        ) -> Result<TriggerDdlManagerRef, BoxedError>;
    }

    pub type TriggerDdlManagerFactoryRef = Arc<dyn TriggerDdlManagerFactory + Send + Sync>;

    pub struct MakeTriggerDdlManagerRequest {
        pub kv_backend: KvBackendRef,
        pub selector: SelectorRef,
        pub select_ctx: SelectorContext,
    }
}

#[derive(Default)]
pub struct Extension {
    #[cfg(feature = "enterprise")]
    pub trigger_ddl_manager_factory: Option<TriggerDdlManagerFactoryRef>,
}
