use std::sync::Arc;

pub trait Namespace: Send + Sync {
    fn name(&self) -> String;
}

pub type NamespaceRef = Arc<dyn Namespace>;
