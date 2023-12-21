// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(let_chains)]

mod df_substrait;
pub mod error;
pub mod extension_serializer;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion::catalog::CatalogList;

pub use crate::df_substrait::DFLogicalSubstraitConvertor;

#[async_trait]
pub trait SubstraitPlan {
    type Error: std::error::Error;

    type Plan;

    async fn decode<B: Buf + Send>(
        &self,
        message: B,
        catalog_list: Arc<dyn CatalogList>,
        catalog: &str,
        schema: &str,
    ) -> Result<Self::Plan, Self::Error>;

    fn encode(&self, plan: &Self::Plan) -> Result<Bytes, Self::Error>;
}
