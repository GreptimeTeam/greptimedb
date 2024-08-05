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

//! Python script engine
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_function::function::Function;
use common_function::function_registry::FUNCTION_REGISTRY;
use common_query::error::{PyUdfSnafu, UdfTempRecordBatchSnafu};
use common_query::prelude::Signature;
use common_query::{Output, OutputData};
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::{ExternalSnafu, Result as RecordBatchResult};
use common_recordbatch::{
    OrderOption, RecordBatch, RecordBatchStream, RecordBatches, SendableRecordBatchStream,
};
use datafusion_expr::Volatility;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::VectorRef;
use futures::Stream;
use query::parser::{QueryLanguageParser, QueryStatement};
use query::QueryEngineRef;
use snafu::{ensure, ResultExt};
use sql::statements::statement::Statement;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::python::error::{self, DatabaseQuerySnafu, PyRuntimeSnafu, Result, TokioJoinSnafu};
use crate::python::ffi_types::copr::{exec_parsed, parse, AnnotationInfo, CoprocessorRef};
use crate::python::utils::spawn_blocking_script;
const PY_ENGINE: &str = "python";

#[derive(Debug)]
pub struct PyUDF {
    copr: CoprocessorRef,
}

impl std::fmt::Display for PyUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})->",
            &self.copr.name,
            self.copr
                .deco_args
                .arg_names
                .as_ref()
                .unwrap_or(&vec![])
                .join(",")
        )
    }
}

impl PyUDF {
    fn from_copr(copr: CoprocessorRef) -> Arc<Self> {
        Arc::new(Self { copr })
    }

    /// Register to `FUNCTION_REGISTRY`
    fn register_as_udf(zelf: Arc<Self>) {
        FUNCTION_REGISTRY.register(zelf)
    }

    fn register_to_query_engine(zelf: Arc<Self>, engine: QueryEngineRef) {
        engine.register_function(zelf)
    }

    /// Fake a schema, should only be used with dynamically eval a Python Udf
    fn fake_schema(&self, columns: &[VectorRef]) -> SchemaRef {
        // try to give schema right names in args so script can run as UDF without modify
        // because when running as PyUDF, the incoming columns should have matching names to make sense
        // for Coprocessor
        let args = self.copr.deco_args.arg_names.clone();
        let try_get_name = |i: usize| {
            if let Some(arg_name) = args.as_ref().and_then(|args| args.get(i)) {
                arg_name.clone()
            } else {
                format!("name_{i}")
            }
        };
        let col_sch: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnSchema::new(try_get_name(i), col.data_type(), true))
            .collect();
        let schema = datatypes::schema::Schema::new(col_sch);
        Arc::new(schema)
    }
}

impl Function for PyUDF {
    fn name(&self) -> &str {
        &self.copr.name
    }

    fn return_type(
        &self,
        _input_types: &[datatypes::prelude::ConcreteDataType],
    ) -> common_query::error::Result<datatypes::prelude::ConcreteDataType> {
        // TODO(discord9): use correct return annotation if exist
        match self.copr.return_types.first() {
            Some(Some(AnnotationInfo {
                datatype: Some(ty), ..
            })) => Ok(ty.clone()),
            _ => PyUdfSnafu {
                msg: "Can't found return type for python UDF {self}",
            }
            .fail(),
        }
    }

    fn signature(&self) -> common_query::prelude::Signature {
        if self.copr.arg_types.is_empty() {
            return Signature::any(0, Volatility::Volatile);
        }

        // try our best to get a type signature
        let mut arg_types = Vec::with_capacity(self.copr.arg_types.len());
        let mut know_all_types = true;
        for ty in self.copr.arg_types.iter() {
            match ty {
                Some(AnnotationInfo {
                    datatype: Some(ty), ..
                }) => arg_types.push(ty.clone()),
                _ => {
                    know_all_types = false;
                    break;
                }
            }
        }

        // The Volatility should be volatile, the return value from evaluation may be changed.
        if know_all_types {
            Signature::variadic(arg_types, Volatility::Volatile)
        } else {
            Signature::any(self.copr.arg_types.len(), Volatility::Volatile)
        }
    }

    fn eval(
        &self,
        func_ctx: common_function::function::FunctionContext,
        columns: &[datatypes::vectors::VectorRef],
    ) -> common_query::error::Result<datatypes::vectors::VectorRef> {
        // FIXME(discord9): exec_parsed require a RecordBatch(basically a Vector+Schema), where schema can't pop out from nowhere, right?
        let schema = self.fake_schema(columns);
        let columns = columns.to_vec();
        let rb = Some(RecordBatch::new(schema, columns).context(UdfTempRecordBatchSnafu)?);

        let res = exec_parsed(
            &self.copr,
            &rb,
            &HashMap::new(),
            &EvalContext {
                query_ctx: func_ctx.query_ctx.clone(),
            },
        )
        .map_err(BoxedError::new)
        .context(common_query::error::ExecuteSnafu)?;

        let len = res.columns().len();
        if len == 0 {
            return PyUdfSnafu {
                msg: "Python UDF should return exactly one column, found zero column".to_string(),
            }
            .fail();
        } // if more than one columns, just return first one

        // TODO(discord9): more error handling
        let res0 = res.column(0);
        Ok(res0.clone())
    }
}

pub struct PyScript {
    query_engine: QueryEngineRef,
    pub(crate) copr: CoprocessorRef,
}

impl PyScript {
    pub fn from_script(script: &str, query_engine: QueryEngineRef) -> Result<Self> {
        let copr = Arc::new(parse::parse_and_compile_copr(
            script,
            Some(query_engine.clone()),
        )?);

        Ok(PyScript { copr, query_engine })
    }
    /// Register Current Script as UDF, register name is same as script name
    /// FIXME(discord9): possible inject attack?
    pub async fn register_udf(&self) {
        let udf = PyUDF::from_copr(self.copr.clone());
        PyUDF::register_as_udf(udf.clone());
        PyUDF::register_to_query_engine(udf, self.query_engine.clone());
    }
}

pub struct CoprStream {
    stream: SendableRecordBatchStream,
    copr: CoprocessorRef,
    ret_schema: SchemaRef,
    params: HashMap<String, String>,
    eval_ctx: EvalContext,
}

impl CoprStream {
    fn try_new(
        stream: SendableRecordBatchStream,
        copr: CoprocessorRef,
        params: HashMap<String, String>,
        eval_ctx: EvalContext,
    ) -> Result<Self> {
        let mut schema = vec![];
        for (ty, name) in copr.return_types.iter().zip(&copr.deco_args.ret_names) {
            let ty = ty.clone().ok_or_else(|| {
                PyRuntimeSnafu {
                    msg: "return type not annotated, can't generate schema",
                }
                .build()
            })?;
            let is_nullable = ty.is_nullable;
            let ty = ty.datatype.ok_or_else(|| {
                PyRuntimeSnafu {
                    msg: "return type not annotated, can't generate schema",
                }
                .build()
            })?;
            let col_schema = ColumnSchema::new(name, ty, is_nullable);
            schema.push(col_schema);
        }
        let ret_schema = Arc::new(Schema::new(schema));
        Ok(Self {
            stream,
            copr,
            ret_schema,
            params,
            eval_ctx,
        })
    }
}

impl RecordBatchStream for CoprStream {
    fn schema(&self) -> SchemaRef {
        // FIXME(discord9): use copr returns for schema
        self.ret_schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for CoprStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(recordbatch))) => {
                let batch =
                    exec_parsed(&self.copr, &Some(recordbatch), &self.params, &self.eval_ctx)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(other) => Poll::Ready(other),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[async_trait]
impl Script for PyScript {
    type Error = error::Error;

    fn engine_name(&self) -> &str {
        PY_ENGINE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self, params: HashMap<String, String>, ctx: EvalContext) -> Result<Output> {
        if let Some(sql) = &self.copr.deco_args.sql {
            let stmt = QueryLanguageParser::parse_sql(sql, &ctx.query_ctx).unwrap();
            ensure!(
                matches!(stmt, QueryStatement::Sql(Statement::Query { .. })),
                error::UnsupportedSqlSnafu { sql }
            );
            let plan = self
                .query_engine
                .planner()
                .plan(stmt, ctx.query_ctx.clone())
                .await
                .context(DatabaseQuerySnafu)?;
            let res = self
                .query_engine
                .execute(plan, ctx.query_ctx.clone())
                .await
                .context(DatabaseQuerySnafu)?;
            let copr = self.copr.clone();
            match res.data {
                OutputData::Stream(stream) => Ok(Output::new_with_stream(Box::pin(
                    CoprStream::try_new(stream, copr, params, ctx)?,
                ))),
                _ => unreachable!(),
            }
        } else {
            let copr = self.copr.clone();
            let params = params.clone();
            let batch = spawn_blocking_script(move || exec_parsed(&copr, &None, &params, &ctx))
                .await
                .context(TokioJoinSnafu)??;
            let batches = RecordBatches::try_new(batch.schema.clone(), vec![batch]).unwrap();
            Ok(Output::new_with_record_batches(batches))
        }
    }
}

pub struct PyEngine {
    query_engine: QueryEngineRef,
}

impl PyEngine {
    pub fn new(query_engine: QueryEngineRef) -> Self {
        Self { query_engine }
    }
}

#[async_trait]
impl ScriptEngine for PyEngine {
    type Error = error::Error;
    type Script = PyScript;

    fn name(&self) -> &str {
        PY_ENGINE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn compile(&self, script: &str, _ctx: CompileContext) -> Result<PyScript> {
        let copr = Arc::new(parse::parse_and_compile_copr(
            script,
            Some(self.query_engine.clone()),
        )?);

        Ok(PyScript {
            copr,
            query_engine: self.query_engine.clone(),
        })
    }
}

#[cfg(test)]
pub(crate) use tests::sample_script_engine;

#[cfg(test)]
mod tests {
    use catalog::memory::MemoryCatalogManager;
    use common_catalog::consts::NUMBERS_TABLE_ID;
    use common_recordbatch::util;
    use datatypes::prelude::ScalarVector;
    use datatypes::value::Value;
    use datatypes::vectors::{Float64Vector, Int64Vector};
    use query::QueryEngineFactory;
    use table::table::numbers::NumbersTable;

    use super::*;

    pub(crate) fn sample_script_engine() -> PyEngine {
        let catalog_manager =
            MemoryCatalogManager::new_with_table(NumbersTable::table(NUMBERS_TABLE_ID));
        let query_engine =
            QueryEngineFactory::new(catalog_manager, None, None, None, None, false).query_engine();

        PyEngine::new(query_engine.clone())
    }

    #[tokio::test]
    async fn test_sql_in_py() {
        let script_engine = sample_script_engine();

        let script = r#"
import greptime as gt

@copr(args=["number"], returns = ["number"], sql = "select * from numbers")
def test(number) -> vector[u32]:
    from greptime import query
    return query().sql("select * from numbers")[0]
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script
            .execute(HashMap::default(), EvalContext::default())
            .await
            .unwrap();
        let res = common_recordbatch::util::collect_batches(match output.data {
            OutputData::Stream(s) => s,
            _ => unreachable!(),
        })
        .await
        .unwrap();
        let rb = res.iter().next().expect("One and only one recordbatch");
        assert_eq!(rb.column(0).len(), 100);
    }

    #[tokio::test]
    async fn test_user_params_in_py() {
        let script_engine = sample_script_engine();

        let script = r#"
@copr(returns = ["number"])
def test(**params) -> vector[i64]:
    return int(params['a']) + int(params['b'])
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let params = HashMap::from([
            ("a".to_string(), "30".to_string()),
            ("b".to_string(), "12".to_string()),
        ]);
        let output = script
            .execute(params, EvalContext::default())
            .await
            .unwrap();
        let res = match output.data {
            OutputData::RecordBatches(s) => s,
            data => unreachable!("data: {data:?}"),
        };
        let rb = res.iter().next().expect("One and only one recordbatch");
        assert_eq!(rb.column(0).len(), 1);
        let result = rb.column(0).get(0);
        assert!(matches!(result, Value::Int64(42)));
    }

    #[tokio::test]
    async fn test_data_frame_in_py() {
        let script_engine = sample_script_engine();

        let script = r#"
from greptime import col

@copr(args=["number"], returns = ["number"], sql = "select * from numbers")
def test(number) -> vector[u32]:
    from greptime import PyDataFrame
    return PyDataFrame.from_sql("select * from numbers").filter(col("number")==col("number")).collect()[0][0]
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script
            .execute(HashMap::new(), EvalContext::default())
            .await
            .unwrap();
        let res = common_recordbatch::util::collect_batches(match output.data {
            OutputData::Stream(s) => s,
            data => unreachable!("data: {data:?}"),
        })
        .await
        .unwrap();
        let rb = res.iter().next().expect("One and only one recordbatch");
        assert_eq!(rb.column(0).len(), 100);
    }

    #[tokio::test]
    async fn test_compile_execute() {
        let script_engine = sample_script_engine();

        // To avoid divide by zero, the script divides `add(a, b)` by `g.sqrt(c + 1)` instead of `g.sqrt(c)`
        let script = r#"
import greptime as g
def add(a, b):
    return a + b;

@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c) -> vector[f64]:
    return add(a, b) / g.sqrt(c + 1)
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script
            .execute(HashMap::new(), EvalContext::default())
            .await
            .unwrap();
        match output.data {
            OutputData::Stream(stream) => {
                let numbers = util::collect(stream).await.unwrap();

                assert_eq!(1, numbers.len());
                let number = &numbers[0];
                assert_eq!(number.num_columns(), 1);
                assert_eq!("r", number.schema.column_schemas()[0].name);

                assert_eq!(1, number.num_columns());
                assert_eq!(100, number.column(0).len());
                let rows = number
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Vector>()
                    .unwrap();
                assert_eq!(0f64, rows.get_data(0).unwrap());
                assert_eq!((99f64 + 99f64) / 100f64.sqrt(), rows.get_data(99).unwrap())
            }
            _ => unreachable!(),
        }

        // test list comprehension
        let script = r#"
import greptime as gt

@copr(args=["number"], returns = ["r"], sql="select number from numbers limit 100")
def test(a) -> vector[i64]:
    return gt.vector([x for x in a if x % 2 == 0])
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script
            .execute(HashMap::new(), EvalContext::default())
            .await
            .unwrap();
        match output.data {
            OutputData::Stream(stream) => {
                let numbers = util::collect(stream).await.unwrap();

                assert_eq!(1, numbers.len());
                let number = &numbers[0];
                assert_eq!(number.num_columns(), 1);
                assert_eq!("r", number.schema.column_schemas()[0].name);

                assert_eq!(1, number.num_columns());
                assert_eq!(50, number.column(0).len());
                let rows = number
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Vector>()
                    .unwrap();
                assert_eq!(0, rows.get_data(0).unwrap());
                assert_eq!(98, rows.get_data(49).unwrap())
            }
            _ => unreachable!(),
        }
    }
}
