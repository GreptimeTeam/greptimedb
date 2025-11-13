#[cfg(feature = "enterprise")]
pub use ee::*;

#[cfg(feature = "enterprise")]
mod ee {
    use std::collections::HashMap;
    use std::sync::Arc;

    use catalog::information_schema::InformationSchemaTableFactoryRef;
    use common_error::ext::BoxedError;
    use flow::FrontendClient;

    /// Providers a set of factories for creating information schema tables.
    ///
    /// `Ist` is an abbreviation of `Information schema table`.
    ///
    /// For example, the enterprise version will provider some information schema
    /// tables, such as `information_schema.triggers` and `information_schema.alerts`
    /// etc.
    #[async_trait::async_trait]
    pub trait IstFactoryProvider: Send + Sync {
        async fn create_factories(
            &self,
            ctx: IstContext,
        ) -> std::result::Result<HashMap<String, InformationSchemaTableFactoryRef>, BoxedError>;
    }

    pub type IstFactoryProviderRef = Arc<dyn IstFactoryProvider>;

    pub struct IstContext {
        pub fe_client: Option<Arc<FrontendClient>>,
    }
}
