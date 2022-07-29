pub trait Namespace: Send + Sync + Clone + std::fmt::Debug {
    fn new(name: &str) -> Self;

    fn name(&self) -> &str;
}
