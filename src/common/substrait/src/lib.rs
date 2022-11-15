// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod df_logical;
pub mod error;
mod schema;
mod types;

use bytes::{Buf, Bytes};

pub use crate::df_logical::DFLogicalSubstraitConvertor;

pub trait SubstraitPlan {
    type Error: std::error::Error;

    type Plan;

    fn decode<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error>;

    fn encode(&self, plan: Self::Plan) -> Result<Bytes, Self::Error>;
}
