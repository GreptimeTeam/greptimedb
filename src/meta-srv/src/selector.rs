pub mod lease_based;

use crate::error::Result;

pub type Namespace = u64;

#[async_trait::async_trait]
pub trait Selector: Send + Sync + 'static {
    type Context;
    type Output;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output>;
}
