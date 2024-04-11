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

//! Transform Substrait into execution plan

use std::collections::HashMap;

use common_decimal::Decimal128;
use common_time::{Date, Timestamp};
use datafusion_substrait::variation_const::{
    DATE_32_TYPE_REF, DATE_64_TYPE_REF, DEFAULT_TYPE_REF, TIMESTAMP_MICRO_TYPE_REF,
    TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF, TIMESTAMP_SECOND_TYPE_REF,
    UNSIGNED_INTEGER_TYPE_REF,
};
use datatypes::arrow::compute::kernels::window;
use datatypes::arrow::ipc::Binary;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::value::Value;
use hydroflow::futures::future::Map;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use substrait::substrait_proto::proto::aggregate_function::AggregationInvocation;
use substrait::substrait_proto::proto::aggregate_rel::{Grouping, Measure};
use substrait::substrait_proto::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait::substrait_proto::proto::expression::literal::LiteralType;
use substrait::substrait_proto::proto::expression::reference_segment::ReferenceType::StructField;
use substrait::substrait_proto::proto::expression::{
    IfThen, Literal, MaskExpression, RexType, ScalarFunction,
};
use substrait::substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait::substrait_proto::proto::extensions::SimpleExtensionDeclaration;
use substrait::substrait_proto::proto::function_argument::ArgType;
use substrait::substrait_proto::proto::r#type::Kind;
use substrait::substrait_proto::proto::read_rel::ReadType;
use substrait::substrait_proto::proto::rel::RelType;
use substrait::substrait_proto::proto::{self, plan_rel, Expression, Plan as SubPlan, Rel};

use crate::adapter::error::{
    DatatypesSnafu, Error, EvalSnafu, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu,
    TableNotFoundSnafu,
};
use crate::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, GlobalId, MapFilterProject, SafeMfpPlan, ScalarExpr,
    UnaryFunc, UnmaterializableFunc, VariadicFunc,
};
use crate::plan::{AccumulablePlan, KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{self, ColumnType, RelationType};

/// a simple macro to generate a not implemented error
macro_rules! not_impl_err {
    ($($arg:tt)*)  => {
        NotImplementedSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

/// generate a plan error
macro_rules! plan_err {
    ($($arg:tt)*)  => {
        PlanSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

mod aggr;
mod expr;
mod literal;
mod plan;

use literal::{from_substrait_literal, from_substrait_type};

/// In Substrait, a function can be define by an u32 anchor, and the anchor can be mapped to a name
///
/// So in substrait plan, a ref to a function can be a single u32 anchor instead of a full name in string
pub struct FunctionExtensions {
    anchor_to_name: HashMap<u32, String>,
}

impl FunctionExtensions {
    /// Create a new FunctionExtensions from a list of SimpleExtensionDeclaration
    pub fn try_from_proto(extensions: &[SimpleExtensionDeclaration]) -> Result<Self, Error> {
        let mut anchor_to_name = HashMap::new();
        for e in extensions {
            match &e.mapping_type {
                Some(ext) => match ext {
                    MappingType::ExtensionFunction(ext_f) => {
                        anchor_to_name.insert(ext_f.function_anchor, ext_f.name.clone());
                    }
                    _ => not_impl_err!("Extension type not supported: {ext:?}")?,
                },
                None => not_impl_err!("Cannot parse empty extension")?,
            }
        }
        Ok(Self { anchor_to_name })
    }

    /// Get the name of a function by it's anchor
    pub fn get(&self, anchor: &u32) -> Option<&String> {
        self.anchor_to_name.get(anchor)
    }
}

/// A context that holds the information of the dataflow
pub struct DataflowContext {
    /// `id` refer to any source table in the dataflow, and `name` is the name of the table
    /// which is a `Vec<String>` in substrait
    id_to_name: HashMap<GlobalId, Vec<String>>,
    /// see `id_to_name`
    name_to_id: HashMap<Vec<String>, GlobalId>,
    /// the schema of the table
    schema: HashMap<GlobalId, RelationType>,
}

impl DataflowContext {
    /// Retrieves a GlobalId and table schema representing a table previously registered by calling the [register_table] function.
    ///
    /// Returns an error if no table has been registered with the provided names
    pub fn table(&self, name: &Vec<String>) -> Result<(GlobalId, RelationType), Error> {
        let id = self
            .name_to_id
            .get(name)
            .copied()
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })?;
        let schema = self
            .schema
            .get(&id)
            .cloned()
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })?;
        Ok((id, schema))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use prost::Message;
    use query::parser::QueryLanguageParser;
    use query::plan::LogicalPlan;
    use query::QueryEngine;
    use session::context::QueryContext;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;

    pub fn create_test_ctx() -> DataflowContext {
        let gid = GlobalId::User(0);
        let name = vec!["numbers".to_string()];
        let schema = RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)]);

        DataflowContext {
            id_to_name: HashMap::from([(gid, name.clone())]),
            name_to_id: HashMap::from([(name.clone(), gid)]),
            schema: HashMap::from([(gid, schema)]),
        }
    }

    pub fn create_test_query_engine() -> Arc<dyn QueryEngine> {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };
        catalog_list.register_table_sync(req).unwrap();
        let factory = query::QueryEngineFactory::new(catalog_list, None, None, None, false);

        let engine = factory.query_engine();

        assert_eq!("datafusion", engine.name());
        engine
    }

    pub async fn sql_to_substrait(engine: Arc<dyn QueryEngine>, sql: &str) -> proto::Plan {
        // let engine = create_test_query_engine();
        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();
        let LogicalPlan::DfPlan(plan) = plan;

        // encode then decode so to rely on the impl of conversion from logical plan to substrait plan
        let bytes = DFLogicalSubstraitConvertor {}.encode(&plan).unwrap();

        proto::Plan::decode(bytes).unwrap()
    }
}
