use store_api::logstore::namespace::Namespace;

#[derive(Clone, Default)]
pub struct LocalNamespace {
    pub name: String,
    pub id: u64,
}

impl Namespace for LocalNamespace {
    fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[allow(dead_code)]
impl LocalNamespace {
    fn id(&self) -> u64 {
        self.id
    }
    pub fn new(name: &str, id: u64) -> Self {
        Self {
            name: name.to_string(),
            id,
        }
    }
}
