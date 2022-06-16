use std::sync::Arc;

use store_api::logstore::namespace::Namespace;

#[derive(Clone)]
pub struct LocalNamespace {
    inner: Arc<LocalNamespaceInner>,
}

impl Default for LocalNamespace {
    fn default() -> Self {
        LocalNamespace::new("", 0)
    }
}

struct LocalNamespaceInner {
    name: String,
    id: u64,
}

impl Namespace for LocalNamespace {
    fn name(&self) -> &str {
        self.inner.name.as_str()
    }
}

#[allow(dead_code)]
impl LocalNamespace {
    fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn new(name: &str, id: u64) -> Self {
        let inner = Arc::new(LocalNamespaceInner {
            name: name.to_string(),
            id,
        });
        Self { inner }
    }
}
