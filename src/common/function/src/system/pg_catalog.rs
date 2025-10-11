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

mod version;

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, as_boolean_array};
use datafusion::catalog::TableFunction;
use datafusion::common::ScalarValue;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datafusion_postgres::pg_catalog::{self, PgCatalogStaticTables};
use datatypes::arrow::datatypes::{DataType, Field};
use version::PGVersionFunction;

use crate::function::{Function, find_function_context};
use crate::function_registry::FunctionRegistry;
use crate::system::define_nullary_udf;

const CURRENT_SCHEMA_FUNCTION_NAME: &str = "current_schema";
const CURRENT_SCHEMAS_FUNCTION_NAME: &str = "current_schemas";
const SESSION_USER_FUNCTION_NAME: &str = "session_user";

define_nullary_udf!(CurrentSchemaFunction);
define_nullary_udf!(CurrentSchemasFunction);
define_nullary_udf!(SessionUserFunction);

// Though "current_schema" can be aliased to "database", to not cause any breaking changes,
// we are not doing it: not until https://github.com/apache/datafusion/issues/17469 is resolved.
impl Function for CurrentSchemaFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMA_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let func_ctx = find_function_context(&args)?;
        let db = func_ctx.query_ctx.current_schema();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(db))))
    }
}

impl Function for SessionUserFunction {
    fn name(&self) -> &str {
        SESSION_USER_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let func_ctx = find_function_context(&args)?;
        let user = func_ctx.query_ctx.current_user();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
            user.username().to_string(),
        ))))
    }
}

impl Function for CurrentSchemasFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMAS_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "x",
            DataType::Utf8View,
            false,
        ))))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let input = as_boolean_array(&args[0]);

        // Create a UTF8 array with a single value
        let mut values = vec!["public"];
        // include implicit schemas
        if input.value(0) {
            values.push("information_schema");
            values.push("pg_catalog");
            values.push("greptime_private");
        }

        let list_array = SingleRowListArrayBuilder::new(Arc::new(StringArray::from(values)));

        let array: ArrayRef = Arc::new(list_array.build_list_array());

        Ok(ColumnarValue::Array(array))
    }
}

pub(super) struct PGCatalogFunction;

impl PGCatalogFunction {
    pub fn register(registry: &FunctionRegistry) {
        let static_tables =
            Arc::new(PgCatalogStaticTables::try_new().expect("load postgres static tables"));

        registry.register_scalar(PGVersionFunction::default());
        registry.register_scalar(CurrentSchemaFunction::default());
        registry.register_scalar(CurrentSchemasFunction::default());
        registry.register_scalar(SessionUserFunction::default());
        registry.register(pg_catalog::format_type::create_format_type_udf());
        registry.register(pg_catalog::create_pg_get_partkeydef_udf());
        registry.register(pg_catalog::has_privilege_udf::create_has_privilege_udf(
            "has_table_privilege",
        ));
        registry.register(pg_catalog::has_privilege_udf::create_has_privilege_udf(
            "has_schema_privilege",
        ));
        registry.register(pg_catalog::has_privilege_udf::create_has_privilege_udf(
            "has_database_privilege",
        ));
        registry.register(pg_catalog::has_privilege_udf::create_has_privilege_udf(
            "has_any_column_privilege",
        ));
        registry.register_table_function(TableFunction::new(
            "pg_get_keywords".to_string(),
            static_tables.pg_get_keywords.clone(),
        ));
        registry.register(pg_catalog::create_pg_relation_is_publishable_udf());
        registry.register(pg_catalog::create_pg_get_statisticsobjdef_columns_udf());
        registry.register(pg_catalog::create_pg_get_userbyid_udf());
        registry.register(pg_catalog::create_pg_table_is_visible());
        registry.register(pg_catalog::pg_get_expr_udf::create_pg_get_expr_udf());
        // TODO(sunng87): upgrade datafusion to add
        //registry.register(pg_catalog::create_pg_encoding_to_char_udf());
    }
}
