pub type Id = u64;

pub trait Namespace: Send + Sync + Clone + std::fmt::Debug {
    fn id(&self) -> Id;
}
