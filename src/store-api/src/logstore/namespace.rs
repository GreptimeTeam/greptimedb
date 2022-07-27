pub trait Namespace: Send + Sync + Clone + std::fmt::Debug {
    fn new(name: &str, id: u64) -> Self;

    fn name(&self) -> &str;
}
