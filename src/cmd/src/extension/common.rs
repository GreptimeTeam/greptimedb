#[cfg(feature = "enterprise")]
pub use ee::*;

#[cfg(feature = "enterprise")]
mod ee {
    use std::collections::HashMap;
    use std::sync::Arc;

    use catalog::information_schema::InformationSchemaTableFactoryRef;
    use flow::FrontendClient;

    /// Providers a set of factories for creating information schema tables.
    ///
    /// `Ist` is an abbreviation of `Information schema table`.
    ///
    /// For example, the enterprise version will provider some information schema
    /// tables, such as `information_schema.triggers` and `information_schema.alerts`
    /// etc.
    pub trait IstFactoryProvider: Send + Sync {
        fn create_factories(
            &self,
            ctx: IstContext,
        ) -> HashMap<String, InformationSchemaTableFactoryRef>;
    }

    pub type IstFactoryProviderRef = Arc<dyn IstFactoryProvider>;

    pub struct IstContext {
        pub fe_client: Option<Arc<FrontendClient>>,
    }
}
