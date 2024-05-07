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
use std::sync::Arc;

use common_telemetry::info;
use datatypes::data_type::ConcreteDataType as CDT;
use prost::Message;
use query::parser::QueryLanguageParser;
use query::plan::LogicalPlan;
use query::QueryEngine;
use session::context::QueryContext;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::adapter::error::{
    Error, InvalidQueryPlanSnafu, InvalidQueryProstSnafu, InvalidQuerySubstraitSnafu,
    NotImplementedSnafu, TableNotFoundSnafu,
};
use crate::adapter::FlowNodeContext;
use crate::expr::GlobalId;
use crate::plan::TypedPlan;
use crate::repr::RelationType;
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
use snafu::{OptionExt, ResultExt};
use substrait::substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait::substrait_proto::proto::extensions::SimpleExtensionDeclaration;

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

/// To reuse existing code for parse sql, the sql is first parsed into a datafusion logical plan,
/// then to a substrait plan, and finally to a flow plan.
///
/// TODO(discord9): check if use empty `QueryContext` influence anything
pub async fn sql_to_flow_plan(
    ctx: &mut FlowNodeContext,
    engine: &Arc<dyn QueryEngine>,
    sql: &str,
) -> Result<TypedPlan, Error> {
    let query_ctx = ctx.query_context.clone().unwrap();
    let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx).context(InvalidQueryPlanSnafu)?;
    let plan = engine
        .planner()
        .plan(stmt, query_ctx)
        .await
        .context(InvalidQueryPlanSnafu)?;
    let LogicalPlan::DfPlan(plan) = plan;

    // encode then decode so to rely on the impl of conversion from logical plan to substrait plan
    let bytes = DFLogicalSubstraitConvertor {}
        .encode(&plan)
        .context(InvalidQuerySubstraitSnafu)?;

    let sub_plan = substrait::substrait_proto::proto::Plan::decode(bytes)
        .map_err(|inner| InvalidQueryProstSnafu { inner }.build())?;
    let flow_plan = TypedPlan::from_substrait_plan(ctx, &sub_plan)?;

    Ok(flow_plan)
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
    use substrait::substrait_proto::proto;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;
    use crate::adapter::TriMap;
    use crate::repr::ColumnType;

    pub fn create_test_ctx() -> FlowNodeContext {
        let gid = GlobalId::User(0);
        let name = vec!["numbers".to_string()];
        let schema = RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)]);
        let mut tri_map = TriMap::new();
        tri_map.insert(Some(name.clone()), Some(0), gid);
        FlowNodeContext {
            schema: HashMap::from([(gid, schema)]),
            table_repr: tri_map,
            ..Default::default()
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
