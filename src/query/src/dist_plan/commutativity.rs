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

use std::sync::Arc;

use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};

#[allow(dead_code)]
pub enum Commutativity {
    Commutative,
    PartialCommutative,
    ConditionalCommutative(Option<Transformer>),
    TransformedCommutative(Option<Transformer>),
    NonCommutative,
    Unimplemented,
    /// For unrelated plans like DDL
    Unsupported,
}

pub struct Categorizer {}

impl Categorizer {
    pub fn check_plan(plan: &LogicalPlan) -> Commutativity {
        match plan {
            LogicalPlan::Projection(_) => Commutativity::Unimplemented,
            LogicalPlan::Filter(_) => Commutativity::Commutative,
            LogicalPlan::Window(_) => Commutativity::Unimplemented,
            LogicalPlan::Aggregate(_) => {
                // check all children exprs and uses the strictest level
                Commutativity::Unimplemented
            }
            LogicalPlan::Sort(_) => Commutativity::NonCommutative,
            LogicalPlan::Join(_) => Commutativity::NonCommutative,
            LogicalPlan::CrossJoin(_) => Commutativity::NonCommutative,
            LogicalPlan::Repartition(_) => {
                // unsupported? or non-commutative
                Commutativity::Unimplemented
            }
            LogicalPlan::Union(_) => Commutativity::Unimplemented,
            LogicalPlan::TableScan(_) => Commutativity::NonCommutative,
            LogicalPlan::EmptyRelation(_) => Commutativity::NonCommutative,
            LogicalPlan::Subquery(_) => Commutativity::Unimplemented,
            LogicalPlan::SubqueryAlias(_) => Commutativity::Unimplemented,
            LogicalPlan::Limit(_) => Commutativity::PartialCommutative,
            LogicalPlan::Extension(extension) => {
                Self::check_extension_plan(extension.node.as_ref() as _)
            }
            LogicalPlan::Distinct(_) => Commutativity::PartialCommutative,
            LogicalPlan::Unnest(_) => Commutativity::Commutative,
            LogicalPlan::Statement(_) => Commutativity::Unsupported,
            LogicalPlan::CreateExternalTable(_) => Commutativity::Unsupported,
            LogicalPlan::CreateMemoryTable(_) => Commutativity::Unsupported,
            LogicalPlan::CreateView(_) => Commutativity::Unsupported,
            LogicalPlan::CreateCatalogSchema(_) => Commutativity::Unsupported,
            LogicalPlan::CreateCatalog(_) => Commutativity::Unsupported,
            LogicalPlan::DropTable(_) => Commutativity::Unsupported,
            LogicalPlan::DropView(_) => Commutativity::Unsupported,
            LogicalPlan::Values(_) => Commutativity::Unsupported,
            LogicalPlan::Explain(_) => Commutativity::Unsupported,
            LogicalPlan::Analyze(_) => Commutativity::Unsupported,
            LogicalPlan::Prepare(_) => Commutativity::Unsupported,
            LogicalPlan::DescribeTable(_) => Commutativity::Unsupported,
            LogicalPlan::Dml(_) => Commutativity::Unsupported,
        }
    }

    pub fn check_extension_plan(_plan: &dyn UserDefinedLogicalNode) -> Commutativity {
        todo!("enumerate all the extension plans here")
    }
}

pub type Transformer = Arc<dyn Fn(&LogicalPlan) -> Option<LogicalPlan>>;

pub fn partial_commutative_transformer(plan: &LogicalPlan) -> Option<LogicalPlan> {
    Some(plan.clone())
}
