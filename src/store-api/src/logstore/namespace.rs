pub trait Namespace: Send + Sync + Clone + std::fmt::Debug {
    fn name(&self) -> &str;
}
