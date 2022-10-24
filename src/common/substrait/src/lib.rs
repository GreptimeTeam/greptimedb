mod df_logical;
mod error;

use bytes::{Buf, Bytes};

pub use crate::df_logical::DFLogicalSubstraitConvertor;

pub trait SubstraitPlan {
    type Error: std::error::Error;

    type Plan;

    fn decode<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error>;

    fn encode(&self, plan: Self::Plan) -> Result<Bytes, Self::Error>;
}
