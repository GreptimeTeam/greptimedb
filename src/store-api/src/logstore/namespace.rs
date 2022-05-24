pub trait Namespace: Send + Sync + Clone {
    fn name(&self) -> &str;
}
