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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME,
};
use datafusion::arrow::array::{ArrayRef, StringArray, as_boolean_array};
use datafusion::catalog::TableFunction;
use datafusion::common::ScalarValue;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion_common::DataFusionError;
use datafusion_expr::async_udf::AsyncScalarUDF;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_pg_catalog::pg_catalog::{self, PgCatalogStaticTables};
use datatypes::arrow::datatypes::{DataType, Field};
use derive_more::derive::Display;
use table::metadata::TableId;

use crate::function::{Function, FunctionContext, find_function_context};
use crate::function_factory::ScalarFunctionFactory;
use crate::function_registry::FunctionRegistry;
use crate::system::define_nullary_udf;

const CURRENT_SCHEMA_FUNCTION_NAME: &str = "current_schema";
const CURRENT_SCHEMAS_FUNCTION_NAME: &str = "current_schemas";
const SESSION_USER_FUNCTION_NAME: &str = "session_user";
const CURRENT_DATABASE_FUNCTION_NAME: &str = "current_database";
const OBJ_DESCRIPTION_FUNCTION_NAME: &str = "obj_description";
const COL_DESCRIPTION_FUNCTION_NAME: &str = "col_description";
const SHOBJ_DESCRIPTION_FUNCTION_NAME: &str = "shobj_description";
const PG_DESCRIBE_OBJECT_FUNCTION_NAME: &str = "pg_describe_object";

define_nullary_udf!(CurrentSchemaFunction);
define_nullary_udf!(SessionUserFunction);
define_nullary_udf!(CurrentDatabaseFunction);

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

/// Async UDF for obj_description - returns table comments from catalog
#[derive(Debug)]
pub(super) struct ObjDescriptionFunction {
    signature: Signature,
    func_ctx: FunctionContext,
}

impl PartialEq for ObjDescriptionFunction {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for ObjDescriptionFunction {}

impl std::hash::Hash for ObjDescriptionFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl ObjDescriptionFunction {
    fn new(signature: Signature, func_ctx: FunctionContext) -> Self {
        Self { signature, func_ctx }
    }

    fn signature_static() -> Signature {
        Signature::one_of(
            vec![
                // obj_description(oid, catalog_name)
                TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
                // obj_description(oid) - single argument form
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::UInt32]),
            ],
            Volatility::Stable,
        )
    }

    pub fn factory() -> ScalarFunctionFactory {
        let signature = Self::signature_static();
        ScalarFunctionFactory {
            name: OBJ_DESCRIPTION_FUNCTION_NAME.to_string(),
            factory: Arc::new(move |ctx: FunctionContext| {
                let udf_impl = ObjDescriptionFunction::new(signature.clone(), ctx);
                let async_udf = AsyncScalarUDF::new(Arc::new(udf_impl));
                async_udf.into_scalar_udf()
            }),
        }
    }
}

impl ScalarUDFImpl for ObjDescriptionFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        OBJ_DESCRIPTION_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(
            "obj_description is async-only".to_string(),
        ))
    }
}

#[async_trait]
impl datafusion_expr::async_udf::AsyncScalarUDFImpl for ObjDescriptionFunction {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let catalog_name = if args.args.len() > 1 {
            extract_string_scalar(&args.args[1])?
        } else {
            "pg_class".to_string()
        };

        // Only support pg_class (tables) for now
        if catalog_name != "pg_class" {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }

        let oid = extract_oid_scalar(&args.args[0])?;

        // Get catalog manager from function context
        let Some(handler) = self.func_ctx.state.procedure_service_handler.as_ref() else {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        };

        let catalog_manager = handler.catalog_manager();

        // Look up table by ID
        match catalog_manager.table_info_by_id(oid as TableId).await {
            Ok(Some(table_info)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(table_info.desc.clone())))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

/// Async UDF for col_description - returns column comments from catalog
#[derive(Debug)]
pub(super) struct ColDescriptionFunction {
    signature: Signature,
    func_ctx: FunctionContext,
}

impl PartialEq for ColDescriptionFunction {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for ColDescriptionFunction {}

impl std::hash::Hash for ColDescriptionFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl ColDescriptionFunction {
    fn new(signature: Signature, func_ctx: FunctionContext) -> Self {
        Self { signature, func_ctx }
    }

    fn signature_static() -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Int64]),
            ],
            Volatility::Stable,
        )
    }

    pub fn factory() -> ScalarFunctionFactory {
        let signature = Self::signature_static();
        ScalarFunctionFactory {
            name: COL_DESCRIPTION_FUNCTION_NAME.to_string(),
            factory: Arc::new(move |ctx: FunctionContext| {
                let udf_impl = ColDescriptionFunction::new(signature.clone(), ctx);
                let async_udf = AsyncScalarUDF::new(Arc::new(udf_impl));
                async_udf.into_scalar_udf()
            }),
        }
    }
}

impl ScalarUDFImpl for ColDescriptionFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        COL_DESCRIPTION_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(
            "col_description is async-only".to_string(),
        ))
    }
}

#[async_trait]
impl datafusion_expr::async_udf::AsyncScalarUDFImpl for ColDescriptionFunction {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Plan(
                "col_description requires 2 arguments".to_string(),
            ));
        }

        let table_oid = extract_oid_scalar(&args.args[0])?;
        let column_number = extract_i64_scalar(&args.args[1])?;

        // Column numbers are 1-based in PostgreSQL
        if column_number < 1 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }

        // Get catalog manager from function context
        let Some(handler) = self.func_ctx.state.procedure_service_handler.as_ref() else {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        };

        let catalog_manager = handler.catalog_manager();

        // Look up table by ID and get column comment
        match catalog_manager.table_info_by_id(table_oid as TableId).await {
            Ok(Some(table_info)) => {
                let column_index = (column_number - 1) as usize;
                let schema = table_info.meta.schema.column_schemas();
                if column_index < schema.len() {
                    let column_comment = schema[column_index].column_comment();
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                        column_comment.cloned(),
                    )))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

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
        _args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

/// PostgreSQL pg_class OID (for tables)
const PG_CLASS_OID: u32 = 1259;

/// Async UDF for pg_describe_object - returns human-readable description of database objects
#[derive(Debug)]
pub(super) struct PgDescribeObjectFunction {
    signature: Signature,
    func_ctx: FunctionContext,
}

impl PartialEq for PgDescribeObjectFunction {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for PgDescribeObjectFunction {}

impl std::hash::Hash for PgDescribeObjectFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl PgDescribeObjectFunction {
    fn new(signature: Signature, func_ctx: FunctionContext) -> Self {
        Self { signature, func_ctx }
    }

    fn signature_static() -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::UInt32, DataType::UInt32, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
            ],
            Volatility::Stable,
        )
    }

    pub fn factory() -> ScalarFunctionFactory {
        let signature = Self::signature_static();
        ScalarFunctionFactory {
            name: PG_DESCRIBE_OBJECT_FUNCTION_NAME.to_string(),
            factory: Arc::new(move |ctx: FunctionContext| {
                let udf_impl = PgDescribeObjectFunction::new(signature.clone(), ctx);
                let async_udf = AsyncScalarUDF::new(Arc::new(udf_impl));
                async_udf.into_scalar_udf()
            }),
        }
    }
}

impl ScalarUDFImpl for PgDescribeObjectFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        PG_DESCRIBE_OBJECT_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(
            "pg_describe_object is async-only".to_string(),
        ))
    }
}

#[async_trait]
impl datafusion_expr::async_udf::AsyncScalarUDFImpl for PgDescribeObjectFunction {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() < 3 {
            return Err(DataFusionError::Plan(
                "pg_describe_object requires 3 arguments".to_string(),
            ));
        }

        let catalog_oid = extract_oid_scalar(&args.args[0])?;
        let object_oid = extract_oid_scalar(&args.args[1])?;
        let sub_object_id = extract_i64_scalar(&args.args[2])?;

        // Only support pg_class (tables) for now
        if catalog_oid != PG_CLASS_OID {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }

        // Get catalog manager from function context
        let Some(handler) = self.func_ctx.state.procedure_service_handler.as_ref() else {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        };

        let catalog_manager = handler.catalog_manager();

        // Look up table by ID
        match catalog_manager.table_info_by_id(object_oid as TableId).await {
            Ok(Some(table_info)) => {
                // If sub_object_id > 0, it refers to a column
                if sub_object_id > 0 {
                    let column_index = (sub_object_id - 1) as usize;
                    let schema = table_info.meta.schema.column_schemas();
                    if column_index < schema.len() {
                        let desc = format!(
                            "column {} of table {}",
                            schema[column_index].name, table_info.name
                        );
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(desc))))
                    } else {
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                    }
                } else {
                    let desc = format!("table {}", table_info.name);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(desc))))
                }
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

fn extract_oid_scalar(value: &ColumnarValue) -> datafusion_common::Result<u32> {
    match value {
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v as u32),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(*v as u32),
        _ => Err(DataFusionError::Plan(
            "Expected integer OID value".to_string(),
        )),
    }
}

fn extract_string_scalar(value: &ColumnarValue) -> datafusion_common::Result<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Ok(s.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(s.clone()),
        _ => Err(DataFusionError::Plan("Expected string value".to_string())),
    }
}

fn extract_i64_scalar(value: &ColumnarValue) -> datafusion_common::Result<i64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(*v as i64),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => Ok(*v as i64),
        _ => Err(DataFusionError::Plan("Expected integer value".to_string())),
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

        // PostgreSQL description functions for connector compatibility (async UDFs)
        registry.register(ObjDescriptionFunction::factory());
        registry.register(ColDescriptionFunction::factory());
        registry.register(PgDescribeObjectFunction::factory());
        // shobj_description remains sync (always returns NULL by design)
        registry.register_scalar(ShobjDescriptionFunction::new());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::ScalarValue;
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::async_udf::AsyncScalarUDFImpl;

    use super::*;
    use crate::function::FunctionContext;

    fn create_test_args(args: Vec<ColumnarValue>) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(Default::default()),
        }
    }

    #[test]
    fn test_obj_description_factory() {
        let factory = ObjDescriptionFunction::factory();
        assert_eq!("obj_description", factory.name());

        // Test that factory produces a UDF
        let udf = factory.provide(FunctionContext::default());
        assert_eq!("obj_description", udf.name());
        assert_eq!(DataType::Utf8, udf.return_type(&[]).unwrap());
    }

    #[tokio::test]
    async fn test_obj_description_async() {
        let ctx = FunctionContext::default();
        let func = ObjDescriptionFunction::new(ObjDescriptionFunction::signature_static(), ctx);

        // Test with non-pg_class catalog - should return NULL
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("pg_namespace".to_string()))),
        ]);
        let result = func.invoke_async_with_args(args).await.unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }

        // Test with pg_class catalog - should return NULL (no handler in default context)
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("pg_class".to_string()))),
        ]);
        let result = func.invoke_async_with_args(args).await.unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }
    }

    #[test]
    fn test_col_description_factory() {
        let factory = ColDescriptionFunction::factory();
        assert_eq!("col_description", factory.name());

        let udf = factory.provide(FunctionContext::default());
        assert_eq!("col_description", udf.name());
        assert_eq!(DataType::Utf8, udf.return_type(&[]).unwrap());
    }

    #[tokio::test]
    async fn test_col_description_async() {
        let ctx = FunctionContext::default();
        let func = ColDescriptionFunction::new(ColDescriptionFunction::signature_static(), ctx);

        // Test with invalid column number (0) - should return NULL
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
        ]);
        let result = func.invoke_async_with_args(args).await.unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }

        // Test with valid column number - should return NULL (no handler in default context)
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ]);
        let result = func.invoke_async_with_args(args).await.unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }
    }

    #[test]
    fn test_shobj_description_function() {
        let func = ShobjDescriptionFunction::new();
        assert_eq!("shobj_description", func.name());
        assert_eq!(DataType::Utf8, func.return_type(&[]).unwrap());

        // Should always return NULL
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("pg_database".to_string()))),
        ]);
        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }
    }

    #[test]
    fn test_pg_describe_object_factory() {
        let factory = PgDescribeObjectFunction::factory();
        assert_eq!("pg_describe_object", factory.name());

        let udf = factory.provide(FunctionContext::default());
        assert_eq!("pg_describe_object", udf.name());
        assert_eq!(DataType::Utf8, udf.return_type(&[]).unwrap());
    }

    #[tokio::test]
    async fn test_pg_describe_object_async() {
        let ctx = FunctionContext::default();
        let func =
            PgDescribeObjectFunction::new(PgDescribeObjectFunction::signature_static(), ctx);

        // Test with non-pg_class catalog OID - should return NULL
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1247))), // pg_type OID
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
        ]);
        let result = func.invoke_async_with_args(args).await.unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }

        // Test with pg_class catalog OID - should return NULL (no handler in default context)
        let args = create_test_args(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1259))), // pg_class OID
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
        ]);
        let result = func.invoke_async_with_args(args).await.unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Utf8(v)) = result {
            assert!(v.is_none());
        } else {
            panic!("Expected Scalar Utf8 result");
        }
    }

    #[test]
    fn test_extract_helpers() {
        // Test extract_oid_scalar
        let v = ColumnarValue::Scalar(ScalarValue::UInt32(Some(123)));
        assert_eq!(123, extract_oid_scalar(&v).unwrap());

        let v = ColumnarValue::Scalar(ScalarValue::Int64(Some(456)));
        assert_eq!(456, extract_oid_scalar(&v).unwrap());

        // Test extract_string_scalar
        let v = ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string())));
        assert_eq!("test", extract_string_scalar(&v).unwrap());

        // Test extract_i64_scalar
        let v = ColumnarValue::Scalar(ScalarValue::Int64(Some(789)));
        assert_eq!(789, extract_i64_scalar(&v).unwrap());

        let v = ColumnarValue::Scalar(ScalarValue::Int32(Some(100)));
        assert_eq!(100, extract_i64_scalar(&v).unwrap());
    }
}
