#[cfg(feature = "enterprise")]
use crate::extension::common::IstFactoryProviderRef;

#[derive(Default)]
pub struct Extension {
    #[cfg(feature = "enterprise")]
    pub information_table_extension: Option<IstFactoryProviderRef>,
}
