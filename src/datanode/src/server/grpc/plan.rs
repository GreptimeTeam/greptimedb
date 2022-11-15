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

use std::sync::Arc;

use common_grpc::{AsExcutionPlan, DefaultAsPlanImpl};
use common_query::physical_plan::{PhysicalPlanAdapter, PhysicalPlanRef};
use common_query::Output;
use datatypes::schema::Schema;
use query::QueryEngineRef;
use snafu::ResultExt;

use crate::error::{ConvertSchemaSnafu, ExecutePhysicalPlanSnafu, IntoPhysicalPlanSnafu, Result};

pub struct PhysicalPlanner {
    query_engine: QueryEngineRef,
}

impl PhysicalPlanner {
    pub fn new(query_engine: QueryEngineRef) -> Self {
        Self { query_engine }
    }

    pub fn parse(bytes: Vec<u8>) -> Result<PhysicalPlanRef> {
        let physical_plan = DefaultAsPlanImpl { bytes }
            .try_into_physical_plan()
            .context(IntoPhysicalPlanSnafu)?;

        let schema: Arc<Schema> = Arc::new(
            physical_plan
                .schema()
                .try_into()
                .context(ConvertSchemaSnafu)?,
        );
        Ok(Arc::new(PhysicalPlanAdapter::new(schema, physical_plan)))
    }

    pub async fn execute(&self, plan: PhysicalPlanRef, _original_ql: Vec<u8>) -> Result<Output> {
        self.query_engine
            .execute_physical(&plan)
            .await
            .context(ExecutePhysicalPlanSnafu)
    }
}
