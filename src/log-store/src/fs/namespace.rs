use store_api::logstore::namespace::{Id, Namespace};

#[derive(Clone, Debug)]
pub struct LocalNamespace {
    pub(crate) id: Id,
}

impl Default for LocalNamespace {
    fn default() -> Self {
        LocalNamespace::new(0)
    }
}

impl LocalNamespace {
    pub(crate) fn new(id: Id) -> Self {
        Self { id }
    }
}

impl Namespace for LocalNamespace {
    fn id(&self) -> Id {
        self.id
    }
}
