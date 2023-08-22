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
use std::fmt::{Debug, Display};

use common_query::prelude::ScalarValue;
use datafusion_expr::LogicalPlan as DfLogicalPlan;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::Schema;
use snafu::ResultExt;

use crate::error::{ConvertDatafusionSchemaSnafu, DataFusionSnafu, Result};

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
                    .context(ConvertDatafusionSchemaSnafu)
            }
        }
    }

    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    ///
    /// ```text
    /// Projection: employee.id
    ///    Filter: employee.state Eq Utf8(\"CO\")\
    ///       CsvScan: employee projection=Some([0, 3])
    /// ```
    pub fn display_indent(&self) -> impl Display + '_ {
        let LogicalPlan::DfPlan(plan) = self;
        plan.display_indent()
    }

    /// Walk the logical plan, find any `PlaceHolder` tokens,
    /// and return a map of their IDs and ConcreteDataTypes
    pub fn get_param_types(&self) -> Result<HashMap<String, Option<ConcreteDataType>>> {
        let LogicalPlan::DfPlan(plan) = self;
        let types = plan.get_parameter_types().context(DataFusionSnafu)?;

        Ok(types
            .into_iter()
            .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
            .collect())
    }

    /// Return a logical plan with all placeholders/params (e.g $1 $2,
    /// ...) replaced with corresponding values provided in the
    /// params_values
    pub fn replace_params_with_values(&self, values: &[ScalarValue]) -> Result<LogicalPlan> {
        let LogicalPlan::DfPlan(plan) = self;

        plan.clone()
            .replace_params_with_values(values)
            .context(DataFusionSnafu)
            .map(LogicalPlan::DfPlan)
    }
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        Self::DfPlan(plan)
    }
}
