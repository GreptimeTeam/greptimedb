pub trait Namespace: Send + Sync + Clone {
    fn new(name: &str, id: u64) -> Self;

    fn name(&self) -> &str;
}
