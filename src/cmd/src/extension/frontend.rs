#[cfg(feature = "enterprise")]
use catalog::information_schema::InformationSchemaTableFactoryRef;

#[derive(Default)]
pub struct Extension {
    #[cfg(feature = "enterprise")]
    pub information_table_extension: Option<InformationSchemaTableFactoryRef>,
}
