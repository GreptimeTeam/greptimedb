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

use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME,
};
use datafusion::arrow::array::{ArrayRef, StringArray, StringBuilder, as_boolean_array};
use datafusion::catalog::TableFunction;
use datafusion::common::ScalarValue;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use datafusion_pg_catalog::pg_catalog::{self, PgCatalogStaticTables};
use datatypes::arrow::datatypes::{DataType, Field};
use derive_more::derive::Display;

use crate::function::{Function, find_function_context};
use crate::function_registry::FunctionRegistry;
use crate::system::define_nullary_udf;

const CURRENT_SCHEMA_FUNCTION_NAME: &str = "current_schema";
const CURRENT_SCHEMAS_FUNCTION_NAME: &str = "current_schemas";
const SESSION_USER_FUNCTION_NAME: &str = "session_user";
const CURRENT_DATABASE_FUNCTION_NAME: &str = "current_database";
const OBJ_DESCRIPTION_FUNCTION_NAME: &str = "obj_description";
const COL_DESCRIPTION_FUNCTION_NAME: &str = "col_description";
const SHOBJ_DESCRIPTION_FUNCTION_NAME: &str = "shobj_description";
const PG_MY_TEMP_SCHEMA_FUNCTION_NAME: &str = "pg_my_temp_schema";

define_nullary_udf!(CurrentSchemaFunction);
define_nullary_udf!(SessionUserFunction);
define_nullary_udf!(CurrentDatabaseFunction);
define_nullary_udf!(PgMyTempSchemaFunction);

impl Function for CurrentDatabaseFunction {
    fn name(&self) -> &str {
        CURRENT_DATABASE_FUNCTION_NAME
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
        let db = func_ctx.query_ctx.current_catalog().to_string();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(db))))
    }
}

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

#[derive(Display, Debug)]
#[display("{}", self.name())]
pub(super) struct CurrentSchemasFunction {
    signature: Signature,
}

impl CurrentSchemasFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean]),
                Volatility::Stable,
            ),
        }
    }
}

impl Function for CurrentSchemasFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMAS_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
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
            values.push(INFORMATION_SCHEMA_NAME);
            values.push(PG_CATALOG_NAME);
            values.push(DEFAULT_PRIVATE_SCHEMA_NAME);
        }

        let list_array = SingleRowListArrayBuilder::new(Arc::new(StringArray::from(values)));

        let array: ArrayRef = Arc::new(list_array.build_list_array());

        Ok(ColumnarValue::Array(array))
    }
}

/// PostgreSQL obj_description - returns NULL for compatibility
#[derive(Display, Debug, Clone)]
#[display("{}", self.name())]
pub(super) struct ObjDescriptionFunction {
    signature: Signature,
}

impl ObjDescriptionFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt32]),
                ],
                Volatility::Stable,
            ),
        }
    }
}

impl Function for ObjDescriptionFunction {
    fn name(&self) -> &str {
        OBJ_DESCRIPTION_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let mut builder = StringBuilder::with_capacity(num_rows, 0);
        for _ in 0..num_rows {
            builder.append_null();
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// PostgreSQL col_description - returns NULL for compatibility
#[derive(Display, Debug, Clone)]
#[display("{}", self.name())]
pub(super) struct ColDescriptionFunction {
    signature: Signature,
}

impl ColDescriptionFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::UInt32, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt32, DataType::Int64]),
                ],
                Volatility::Stable,
            ),
        }
    }
}

impl Function for ColDescriptionFunction {
    fn name(&self) -> &str {
        COL_DESCRIPTION_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let mut builder = StringBuilder::with_capacity(num_rows, 0);
        for _ in 0..num_rows {
            builder.append_null();
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// PostgreSQL shobj_description - returns NULL for compatibility
#[derive(Display, Debug, Clone)]
#[display("{}", self.name())]
pub(super) struct ShobjDescriptionFunction {
    signature: Signature,
}

impl ShobjDescriptionFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
                ],
                Volatility::Stable,
            ),
        }
    }
}

impl Function for ShobjDescriptionFunction {
    fn name(&self) -> &str {
        SHOBJ_DESCRIPTION_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let mut builder = StringBuilder::with_capacity(num_rows, 0);
        for _ in 0..num_rows {
            builder.append_null();
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// PostgreSQL pg_my_temp_schema - returns 0 (no temp schema) for compatibility
impl Function for PgMyTempSchemaFunction {
    fn name(&self) -> &str {
        PG_MY_TEMP_SCHEMA_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::UInt32(Some(0))))
    }
}

pub(super) struct PGCatalogFunction;

impl PGCatalogFunction {
    pub fn register(registry: &FunctionRegistry) {
        let static_tables =
            Arc::new(PgCatalogStaticTables::try_new().expect("load postgres static tables"));

        registry.register_scalar(CurrentSchemaFunction::default());
        registry.register_scalar(CurrentSchemasFunction::new());
        registry.register_scalar(SessionUserFunction::default());
        registry.register_scalar(CurrentDatabaseFunction::default());
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
        registry.register(pg_catalog::create_pg_encoding_to_char_udf());
        registry.register(pg_catalog::create_pg_relation_size_udf());
        registry.register(pg_catalog::create_pg_total_relation_size_udf());
        registry.register(pg_catalog::create_pg_stat_get_numscans());
        registry.register(pg_catalog::create_pg_get_constraintdef());
        registry.register_scalar(ObjDescriptionFunction::new());
        registry.register_scalar(ColDescriptionFunction::new());
        registry.register_scalar(ShobjDescriptionFunction::new());
        registry.register_scalar(PgMyTempSchemaFunction::default());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::Array;
    use datafusion_common::ScalarValue;
    use datafusion_expr::ColumnarValue;

    use super::*;

    fn create_test_args(args: Vec<ColumnarValue>, number_rows: usize) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(Default::default()),
        }
    }

    #[test]
    fn test_obj_description_function() {
        let func = ObjDescriptionFunction::new();
        assert_eq!("obj_description", func.name());
        assert_eq!(DataType::Utf8, func.return_type(&[]).unwrap());

        let args = create_test_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("pg_class".to_string()))),
            ],
            1,
        );
        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            assert_eq!(1, arr.len());
            assert!(arr.is_null(0));
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_col_description_function() {
        let func = ColDescriptionFunction::new();
        assert_eq!("col_description", func.name());
        assert_eq!(DataType::Utf8, func.return_type(&[]).unwrap());

        let args = create_test_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            1,
        );
        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            assert_eq!(1, arr.len());
            assert!(arr.is_null(0));
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_shobj_description_function() {
        let func = ShobjDescriptionFunction::new();
        assert_eq!("shobj_description", func.name());
        assert_eq!(DataType::Utf8, func.return_type(&[]).unwrap());

        let args = create_test_args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("pg_database".to_string()))),
            ],
            1,
        );
        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(arr) = result {
            assert_eq!(1, arr.len());
            assert!(arr.is_null(0));
        } else {
            panic!("Expected Array result");
        }
    }
}
