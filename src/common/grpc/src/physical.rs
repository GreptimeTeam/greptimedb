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

mod expr;
pub mod plan;

use std::result::Result;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

pub type ExecutionPlanRef = Arc<dyn ExecutionPlan>;

pub trait AsExcutionPlan {
    type Error: std::error::Error;

    fn try_into_physical_plan(&self) -> Result<ExecutionPlanRef, Self::Error>;

    fn try_from_physical_plan(plan: ExecutionPlanRef) -> Result<Self, Self::Error>
    where
        Self: Sized;
}
