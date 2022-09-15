pub mod compile;
pub mod parse;

use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow;
use datatypes::arrow::array::{Array, ArrayRef};
use datatypes::arrow::compute::cast::CastOptions;
use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datatypes::schema::Schema;
use datatypes::vectors::Helper;
use datatypes::vectors::{BooleanVector, Vector, VectorRef};
use rustpython_bytecode::CodeObject;
use rustpython_vm as vm;
use rustpython_vm::{class::PyClassImpl, AsObject};
#[cfg(test)]
use serde::Deserialize;
use snafu::{OptionExt, ResultExt};
use vm::builtins::{PyBaseExceptionRef, PyTuple};
use vm::scope::Scope;
use vm::{Interpreter, PyObjectRef, VirtualMachine};

use crate::python::builtins::greptime_builtin;
use crate::python::coprocessor::parse::DecoratorArgs;
use crate::python::error::{
    ensure, ret_other_error_with, ArrowSnafu, OtherSnafu, Result, TypeCastSnafu,
};
use crate::python::utils::{format_py_error, py_vec_obj_to_array};
use crate::python::{utils::is_instance, PyVector};

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationInfo {
    /// if None, use types infered by PyVector
    pub datatype: Option<DataType>,
    pub is_nullable: bool,
}

pub type CoprocessorRef = Arc<Coprocessor>;

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone)]
pub struct Coprocessor {
    pub name: String,
    pub deco_args: DecoratorArgs,
    /// get from python function args' annotation, first is type, second is is_nullable
    pub arg_types: Vec<Option<AnnotationInfo>>,
    /// get from python function returns' annotation, first is type, second is is_nullable
    pub return_types: Vec<Option<AnnotationInfo>>,
    /// store its corresponding script, also skip serde when in `cfg(test)` to reduce work in compare
    #[cfg_attr(test, serde(skip))]
    pub script: String,
    // We must use option here, because we use `serde` to deserialize coprocessor
    // from ron file and `Deserialize` requires Coprocessor implementing `Default` trait,
    // but CodeObject doesn't.
    #[cfg_attr(test, serde(skip))]
    pub code_obj: Option<CodeObject>,
}

impl PartialEq for Coprocessor {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.deco_args == other.deco_args
            && self.arg_types == other.arg_types
            && self.return_types == other.return_types
            && self.script == other.script
    }
}

impl Eq for Coprocessor {}

impl Coprocessor {
    /// generate [`Schema`] according to return names, types,
    /// if no annotation
    /// the datatypes of the actual columns is used directly
    fn gen_schema(&self, cols: &[ArrayRef]) -> Result<Arc<ArrowSchema>> {
        let names = &self.deco_args.ret_names;
        let anno = &self.return_types;
        ensure!(
            cols.len() == names.len() && names.len() == anno.len(),
            OtherSnafu {
                reason: format!(
                    "Unmatched length for cols({}), names({}) and anno({})",
                    cols.len(),
                    names.len(),
                    anno.len()
                )
            }
        );
        Ok(Arc::new(ArrowSchema::from(
            names
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    let real_ty = cols[idx].data_type().to_owned();
                    let AnnotationInfo {
                        datatype: ty,
                        is_nullable,
                    } = anno[idx].to_owned().unwrap_or_else(||
                    // default to be not nullable and use DataType infered by PyVector itself
                    AnnotationInfo{
                        datatype: Some(real_ty.to_owned()),
                        is_nullable: false
                    });
                    Field::new(
                        name,
                        // if type is like `_` or `_ | None`
                        ty.unwrap_or(real_ty),
                        is_nullable,
                    )
                })
                .collect::<Vec<Field>>(),
        )))
    }

    /// check if real types and annotation types(if have) is the same, if not try cast columns to annotated type
    fn check_and_cast_type(&self, cols: &mut [ArrayRef]) -> Result<()> {
        let return_types = &self.return_types;
        // allow ignore Return Type Annotation
        if return_types.is_empty() {
            return Ok(());
        }
        ensure!(
            cols.len() == return_types.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect {}, found {}",
                    return_types.len(),
                    cols.len()
                )
            }
        );
        for (col, anno) in cols.iter_mut().zip(return_types) {
            if let Some(AnnotationInfo {
                datatype: Some(datatype),
                is_nullable: _,
            }) = anno
            {
                let real_ty = col.data_type();
                let anno_ty = datatype;
                if real_ty != anno_ty {
                    {
                        // This`CastOption` allow for overflowly cast and int to float loosely cast etc..,
                        // check its doc for more information
                        *col = arrow::compute::cast::cast(
                            col.as_ref(),
                            anno_ty,
                            CastOptions {
                                wrapped: true,
                                partial: true,
                            },
                        )
                        .context(ArrowSnafu)?
                        .into();
                    }
                }
            }
        }
        Ok(())
    }
}

/// cast a `dyn Array` of type unsigned/int/float into a `dyn Vector`
fn try_into_vector<T: datatypes::types::Primitive>(arg: Arc<dyn Array>) -> Result<Arc<dyn Vector>> {
    // wrap try_into_vector in here to convert `datatypes::error::Error` to `python::error::Error`
    Helper::try_into_vector(arg).context(TypeCastSnafu)
}

/// convert a `Vec<ArrayRef>` into a `Vec<PyVector>` only when they are of supported types
/// PyVector now only support unsigned&int8/16/32/64, float32/64 and bool when doing meanful arithmetics operation
fn try_into_py_vector(fetch_args: Vec<ArrayRef>) -> Result<Vec<PyVector>> {
    let mut args: Vec<PyVector> = Vec::with_capacity(fetch_args.len());
    for (idx, arg) in fetch_args.into_iter().enumerate() {
        let v: VectorRef = match arg.data_type() {
            DataType::Float32 => try_into_vector::<f32>(arg)?,
            DataType::Float64 => try_into_vector::<f64>(arg)?,
            DataType::UInt8 => try_into_vector::<u8>(arg)?,
            DataType::UInt16 => try_into_vector::<u16>(arg)?,
            DataType::UInt32 => try_into_vector::<u32>(arg)?,
            DataType::UInt64 => try_into_vector::<u64>(arg)?,
            DataType::Int8 => try_into_vector::<i8>(arg)?,
            DataType::Int16 => try_into_vector::<i16>(arg)?,
            DataType::Int32 => try_into_vector::<i32>(arg)?,
            DataType::Int64 => try_into_vector::<i64>(arg)?,
            DataType::Boolean => {
                let v: VectorRef =
                    Arc::new(BooleanVector::try_from_arrow_array(arg).context(TypeCastSnafu)?);
                v
            }
            _ => {
                return ret_other_error_with(format!(
                    "Unsupport data type at column {idx}: {:?} for coprocessor",
                    arg.data_type()
                ))
                .fail()
            }
        };
        args.push(PyVector::from(v));
    }
    Ok(args)
}

/// convert a tuple of `PyVector` or one `PyVector`(wrapped in a Python Object Ref[`PyObjectRef`])
/// to a `Vec<ArrayRef>`
/// by default, a constant(int/float/bool) gives the a constant array of same length with input args
fn try_into_columns(
    obj: &PyObjectRef,
    vm: &VirtualMachine,
    col_len: usize,
) -> Result<Vec<ArrayRef>> {
    if is_instance::<PyTuple>(obj, vm) {
        let tuple = obj.payload::<PyTuple>().with_context(|| {
            ret_other_error_with(format!("can't cast obj {:?} to PyTuple)", obj))
        })?;
        let cols = tuple
            .iter()
            .map(|obj| py_vec_obj_to_array(obj, vm, col_len))
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(cols)
    } else {
        let col = py_vec_obj_to_array(obj, vm, col_len)?;
        Ok(vec![col])
    }
}

/// select columns according to `fetch_names` from `rb`
/// and cast them into a Vec of PyVector
fn select_from_rb(rb: &DfRecordBatch, fetch_names: &[String]) -> Result<Vec<PyVector>> {
    let field_map: HashMap<&String, usize> = rb
        .schema()
        .fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (&field.name, idx))
        .collect();
    let fetch_idx: Vec<usize> = fetch_names
        .iter()
        .map(|field| {
            field_map.get(field).copied().context(OtherSnafu {
                reason: format!("Can't found field name {field}"),
            })
        })
        .collect::<Result<Vec<usize>>>()?;
    let fetch_args: Vec<Arc<dyn Array>> = fetch_idx
        .into_iter()
        .map(|idx| rb.column(idx).clone())
        .collect();
    try_into_py_vector(fetch_args)
}

/// match between arguments' real type and annotation types
/// if type anno is vector[_] then use real type
fn check_args_anno_real_type(
    args: &[PyVector],
    copr: &Coprocessor,
    rb: &DfRecordBatch,
) -> Result<()> {
    for (idx, arg) in args.iter().enumerate() {
        let anno_ty = copr.arg_types[idx].to_owned();
        let real_ty = arg.to_arrow_array().data_type().to_owned();
        let is_nullable: bool = rb.schema().fields[idx].is_nullable;
        ensure!(
            anno_ty
                .to_owned()
                .map(|v| v.datatype == None // like a vector[_]
                    || v.datatype == Some(real_ty.to_owned()) && v.is_nullable == is_nullable)
                .unwrap_or(true),
            OtherSnafu {
                reason: format!(
                    "column {}'s Type annotation is {:?}, but actual type is {:?}",
                    copr.deco_args.arg_names[idx], anno_ty, real_ty
                )
            }
        )
    }
    Ok(())
}

/// set arguments with given name and values in python scopes
fn set_items_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    arg_names: &[String],
    args: Vec<PyVector>,
) -> Result<()> {
    let _ = arg_names
        .iter()
        .zip(args)
        .map(|(name, vector)| {
            scope
                .locals
                .as_object()
                .set_item(name, vm.new_pyobj(vector), vm)
        })
        .collect::<StdResult<Vec<()>, PyBaseExceptionRef>>()
        .map_err(|e| format_py_error(e, vm))?;
    Ok(())
}

/// The coprocessor function accept a python script and a Record Batch:
/// ## What it does
/// 1. it take a python script and a [`DfRecordBatch`], extract columns and annotation info according to `args` given in decorator in python script
/// 2. execute python code and return a vector or a tuple of vector,
/// 3. the returning vector(s) is assembled into a new [`DfRecordBatch`] according to `returns` in python decorator and return to caller
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
/// use arrow::array::PrimitiveArray;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use common_function::scalars::python::exec_coprocessor;
/// let python_source = r#"
/// @copr(args=["cpu", "mem"], returns=["perf", "what"])
/// def a(cpu, mem):
///     return cpu + mem, cpu - mem
/// "#;
/// let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
/// let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
/// let schema = Arc::new(Schema::from(vec![
///  Field::new("cpu", DataType::Float32, false),
///  Field::new("mem", DataType::Float64, false),
/// ]));
/// let rb =
/// DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
/// let ret = exec_coprocessor(python_source, &rb).unwrap();
/// assert_eq!(ret.column(0).len(), 4);
/// ```
///
/// # Type Annotation
/// you can use type annotations in args and returns to designate types, so coprocessor will check for corrsponding types.
///
/// Currently support types are `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64` and `f16`, `f32`, `f64`
///
/// use `f64 | None` to mark if returning column is nullable like in [`DfRecordBatch`]'s schema's [`Field`]'s is_nullable
///
/// you can also use single underscore `_` to let coprocessor infer what type it is, so `_` and `_ | None` are both valid in type annotation.
/// Note: using `_` means not nullable column, using `_ | None` means nullable column
///
/// a example (of python script) given below:
/// ```python
/// @copr(args=["cpu", "mem"], returns=["perf", "minus", "mul", "div"])
/// def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[_ | None]):
///     return cpu + mem, cpu - mem, cpu * mem, cpu / mem
/// ```
///
/// # Return Constant columns
/// You can return constant in python code like `return 1, 1.0, True`
/// which create a constant array(with same value)(currently support int, float and bool) as column on return
#[cfg(test)]
pub fn exec_coprocessor(script: &str, rb: &DfRecordBatch) -> Result<RecordBatch> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. also check for exist of `args` in `rb`, if not found, return error
    // TODO(discord9): cache the result of parse_copr
    let copr = parse::parse_and_compile_copr(script)?;
    exec_parsed(&copr, rb)
}

pub(crate) fn exec_with_cached_vm(
    copr: &Coprocessor,
    rb: &DfRecordBatch,
    args: Vec<PyVector>,
    vm: &Interpreter,
) -> Result<RecordBatch> {
    vm.enter(|vm| -> Result<RecordBatch> {
        PyVector::make_class(&vm.ctx);
        // set arguments with given name and values
        let scope = vm.new_scope_with_builtins();
        set_items_in_scope(&scope, vm, &copr.deco_args.arg_names, args)?;

        // It's safe to unwrap code_object, it's already compiled before.
        let code_obj = vm.ctx.new_code(copr.code_obj.clone().unwrap());
        let ret = vm
            .run_code_obj(code_obj, scope)
            .map_err(|e| format_py_error(e, vm))?;

        // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
        let col_len = rb.num_rows();
        let mut cols: Vec<ArrayRef> = try_into_columns(&ret, vm, col_len)?;
        ensure!(
            cols.len() == copr.deco_args.ret_names.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect {}, found {}",
                    copr.deco_args.ret_names.len(),
                    cols.len()
                )
            }
        );

        // if cols and schema's data types is not match, try coerce it to given type(if annotated)(if error occur, return relevant error with question mark)
        copr.check_and_cast_type(&mut cols)?;
        // 6. return a assembled DfRecordBatch
        let schema = copr.gen_schema(&cols)?;
        let res_rb = DfRecordBatch::try_new(schema.clone(), cols).context(ArrowSnafu)?;
        Ok(RecordBatch {
            schema: Arc::new(Schema::try_from(schema).context(TypeCastSnafu)?),
            df_recordbatch: res_rb,
        })
    })
}

/// init interpreter with type PyVector and Module: greptime
pub(crate) fn init_interpreter() -> Interpreter {
    vm::Interpreter::with_init(Default::default(), |vm| {
        PyVector::make_class(&vm.ctx);
        vm.add_native_module("greptime", Box::new(greptime_builtin::make_module));
    })
}

/// using a parsed `Coprocessor` struct as input to execute python code
pub(crate) fn exec_parsed(copr: &Coprocessor, rb: &DfRecordBatch) -> Result<RecordBatch> {
    // 3. get args from `rb`, and cast them into PyVector
    let args: Vec<PyVector> = select_from_rb(rb, &copr.deco_args.arg_names)?;
    check_args_anno_real_type(&args, copr, rb)?;
    let interpreter = init_interpreter();
    // 4. then set args in scope and compile then run `CodeObject` which already append a new `Call` node
    exec_with_cached_vm(copr, rb, args, &interpreter)
}

/// execute script just like [`exec_coprocessor`] do,
/// but instead of return a internal [`Error`] type,
/// return a friendly String format of error
///
/// use `ln_offset` and `filename` to offset line number and mark file name in error prompt
#[cfg(test)]
#[allow(dead_code)]
pub fn exec_copr_print(
    script: &str,
    rb: &DfRecordBatch,
    ln_offset: usize,
    filename: &str,
) -> StdResult<RecordBatch, String> {
    let res = exec_coprocessor(script, rb);
    res.map_err(|e| {
        crate::python::error::pretty_print_error_in_src(script, &e, ln_offset, filename)
    })
}

#[cfg(test)]
mod tests {
    use crate::python::coprocessor::parse::parse_and_compile_copr;

    #[test]
    fn test_parse_copr() {
        let script = r#"
def add(a, b):
    return a + b

@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c):
    import greptime as g
    return add(a, b) / g.sqrt(c)
"#;

        let copr = parse_and_compile_copr(script).unwrap();
        assert_eq!(copr.name, "test");
        let deco_args = copr.deco_args.clone();
        assert_eq!(
            deco_args.sql.unwrap(),
            "select number as a,number as b,number as c from numbers limit 100"
        );
        assert_eq!(deco_args.ret_names, vec!["r"]);
        assert_eq!(deco_args.arg_names, vec!["a", "b", "c"]);
        assert_eq!(copr.arg_types, vec![None, None, None]);
        assert_eq!(copr.return_types, vec![None]);
        assert_eq!(copr.script, script);
        assert!(copr.code_obj.is_some());
    }
}
