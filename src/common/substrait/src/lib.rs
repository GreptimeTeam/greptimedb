mod df_logical;
mod error;

use async_trait::async_trait;
use bytes::{Buf, Bytes};

pub use crate::df_logical::DFLogicalSubstraitConvertor;

#[async_trait]
pub trait SubstraitPlan {
    type Error: std::error::Error;

    type Plan;

    async fn convert_buf<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error>;

    fn convert_plan(&self, plan: Self::Plan) -> Result<Bytes, Self::Error>;
}
