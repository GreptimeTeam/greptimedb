use std::sync::Arc;

use arrow2::datatypes::Schema as ArrowSchema;

#[derive(Debug, Clone)]
pub struct Schema {
    arrow_schema: Arc<ArrowSchema>,
}

impl Schema {
    pub fn new(arrow_schema: Arc<ArrowSchema>) -> Self {
        Self { arrow_schema }
    }
    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }
}

pub type SchemaRef = Arc<Schema>;

impl From<Arc<ArrowSchema>> for Schema {
    fn from(s: Arc<ArrowSchema>) -> Schema {
        Schema::new(s)
    }
}
