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

use std::collections::HashMap;
use std::fmt::Debug;

use datafusion_common::ScalarValue;
use datafusion_expr::LogicalPlan as DfLogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::Schema;
use snafu::ResultExt;

use crate::error::Result;

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    DfPlan(DfLogicalPlan),
}

impl LogicalPlan {
    /// Get the schema for this logical plan
    pub fn schema(&self) -> Result<Schema> {
        match self {
            Self::DfPlan(plan) => {
                let df_schema = plan.schema();
                df_schema
                    .clone()
                    .try_into()
                    .context(crate::error::DatatypeSnafu)
            }
        }
    }

    pub fn param_types(&self) -> Option<HashMap<String, Option<ConcreteDataType>>> {
        match self {
            Self::DfPlan(plan) => {
                // TODO(SSebo): return proper error
                let types = plan.get_parameter_types().ok()?;

                Some(
                    types
                        .into_iter()
                        .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
                        .collect(),
                )
            }
        }
    }

    pub fn with_param_values(&self, param_values: Vec<ScalarValue>) -> Option<LogicalPlan> {
        match self {
            // TODO(SSebo): return proper error
            Self::DfPlan(plan) => plan
                .clone()
                .with_param_values(param_values)
                .ok()
                .map(LogicalPlan::DfPlan),
        }
    }
}
