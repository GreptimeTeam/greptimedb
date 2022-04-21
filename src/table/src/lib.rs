mod engine;

/// Table abstraction.
#[async_trait::async_trait]
pub trait Table: Send + Sync {}
