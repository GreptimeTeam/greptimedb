use crate::extension::common::InformationSchemaTableFactoryProviderRef;

#[derive(Default)]
pub struct Extension {
    pub info_schema_factory_provider: Option<InformationSchemaTableFactoryProviderRef>,
}
