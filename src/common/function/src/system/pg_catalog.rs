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

mod pg_get_userbyid;
mod table_is_visible;
mod version;

use std::sync::Arc;

use common_query::error::Result;
use datafusion::catalog::TableFunction;
use datafusion_expr::{Signature, Volatility};
use datafusion_postgres::pg_catalog::{
    create_format_type_udf, create_pg_get_partkeydef_udf, has_privilege_udf, PgCatalogStaticTables,
};
use datatypes::arrow::datatypes::{DataType, Field};
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::scalars::{Scalar, ScalarVectorBuilder};
use datatypes::value::ListValue;
use datatypes::vectors::{ListVectorBuilder, MutableVector, StringVector, VectorRef};
use derive_more::Display;
use pg_get_userbyid::PGGetUserByIdFunction;
use table_is_visible::PGTableIsVisibleFunction;
use version::PGVersionFunction;

use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;

#[macro_export]
macro_rules! pg_catalog_func_fullname {
    ($name:literal) => {
        concat!("pg_catalog.", $name)
    };
}

const CURRENT_SCHEMA_FUNCTION_NAME: &str = "current_schema";
const CURRENT_SCHEMAS_FUNCTION_NAME: &str = "current_schemas";
const SESSION_USER_FUNCTION_NAME: &str = "session_user";

#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct CurrentSchemaFunction;

#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct CurrentSchemasFunction;

#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct SessionUserFunction;

impl Function for CurrentSchemaFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMA_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
    }

    fn eval(&self, func_ctx: &FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let db = func_ctx.query_ctx.current_schema();

        Ok(Arc::new(StringVector::from_slice(&[&db])) as _)
    }
}

impl Function for CurrentSchemasFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMAS_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "x",
            DataType::Utf8,
            false,
        ))))
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Boolean], Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let input = &columns[0];

        // Create a UTF8 array with a single value
        let mut values = vec!["public".into()];
        // include implicit schemas
        if input.get(0).as_bool().unwrap_or(false) {
            values.push("information_schema".into());
            values.push("pg_catalog".into());
            values.push("greptime_private".into());
        }
        let list_value = ListValue::new(values, ConcreteDataType::string_datatype());

        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::string_datatype(), 8);

        results.push(Some(list_value.as_scalar_ref()));

        Ok(results.to_vector())
    }
}

impl Function for SessionUserFunction {
    fn name(&self) -> &str {
        SESSION_USER_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
    }

    fn eval(&self, func_ctx: &FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let user = func_ctx.query_ctx.current_user();

        Ok(Arc::new(StringVector::from_slice(&[user.username()])) as _)
    }
}

pub(super) struct PGCatalogFunction;

impl PGCatalogFunction {
    pub fn register(registry: &FunctionRegistry) {
        let static_tables =
            Arc::new(PgCatalogStaticTables::try_new().expect("load postgres static tables"));

        registry.register_scalar(PGTableIsVisibleFunction);
        registry.register_scalar(PGGetUserByIdFunction);
        registry.register_scalar(PGVersionFunction);
        registry.register_scalar(CurrentSchemaFunction);
        registry.register_scalar(CurrentSchemasFunction);
        registry.register_scalar(SessionUserFunction);
        registry.register(create_format_type_udf());
        registry.register(create_pg_get_partkeydef_udf());
        registry.register(has_privilege_udf::create_has_privilege_udf(
            "has_table_privilege",
        ));
        registry.register(has_privilege_udf::create_has_privilege_udf(
            "pg_catalog.has_table_privilege",
        ));
        registry.register(has_privilege_udf::create_has_privilege_udf(
            "has_schema_privilege",
        ));
        registry.register(has_privilege_udf::create_has_privilege_udf(
            "pg_catalog.has_schema_privilege",
        ));
        registry.register(has_privilege_udf::create_has_privilege_udf(
            "has_any_column_privilege",
        ));
        registry.register(has_privilege_udf::create_has_privilege_udf(
            "pg_catalog.has_any_column_privilege",
        ));
        registry.register_table_function(TableFunction::new(
            "pg_get_keywords".to_string(),
            static_tables.pg_get_keywords.clone(),
        ));
    }
}
