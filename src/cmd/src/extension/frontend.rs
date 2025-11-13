#[cfg(feature = "enterprise")]
use crate::extension::common::IstFactoryProviderRef;

#[derive(Default)]
pub struct Extension {
    /// Information schema table factory provider.
    #[cfg(feature = "enterprise")]
    pub ist_factory_provider: Option<IstFactoryProviderRef>,
}
