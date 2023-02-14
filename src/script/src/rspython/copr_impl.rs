use std::cell::RefCell;
use std::collections::HashSet;
use std::result::Result as StdResult;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use common_telemetry::info;
use datatypes::vectors::VectorRef;
use rustpython_vm::builtins::{PyBaseExceptionRef, PyTuple};
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::convert::ToPyObject;
use rustpython_vm::scope::Scope;
use rustpython_vm::{vm, AsObject, Interpreter, PyObjectRef, VirtualMachine};
use snafu::{OptionExt, ResultExt};

use crate::ffi_types::copr::PyQueryEngine;
use crate::ffi_types::{check_args_anno_real_type, select_from_rb, Coprocessor, PyVector};
use crate::python::error::{
    ensure, ret_other_error_with, ArrowSnafu, NewRecordBatchSnafu, OtherSnafu, Result,
    TypeCastSnafu,
};
use crate::python::utils::{format_py_error, is_instance, py_vec_obj_to_array};

thread_local!(static INTERPRETER: RefCell<Option<Arc<Interpreter>>> = RefCell::new(None));

/// Using `RustPython` to run a parsed `Coprocessor` struct as input to execute python code
pub(crate) fn rspy_exec_parsed(copr: &Coprocessor, rb: &RecordBatch) -> Result<RecordBatch> {
    // 3. get args from `rb`, and cast them into PyVector
    let args: Vec<PyVector> = select_from_rb(rb, &copr.deco_args.arg_names)?;
    check_args_anno_real_type(&args, copr, rb)?;
    let interpreter = init_interpreter();
    // 4. then set args in scope and compile then run `CodeObject` which already append a new `Call` node
    exec_with_cached_vm(copr, rb, args, &interpreter)
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

fn set_query_engine_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    query_engine: PyQueryEngine,
) -> Result<()> {
    scope
        .locals
        .as_object()
        .set_item("query", query_engine.to_pyobject(vm), vm)
        .map_err(|e| format_py_error(e, vm))
}

pub(crate) fn exec_with_cached_vm(
    copr: &Coprocessor,
    rb: &RecordBatch,
    args: Vec<PyVector>,
    vm: &Arc<Interpreter>,
) -> Result<RecordBatch> {
    vm.enter(|vm| -> Result<RecordBatch> {
        PyVector::make_class(&vm.ctx);
        // set arguments with given name and values
        let scope = vm.new_scope_with_builtins();
        set_items_in_scope(&scope, vm, &copr.deco_args.arg_names, args)?;
        // set_dataframe_in_scope(&scope, vm, "dataframe", rb)?;

        if let Some(engine) = &copr.query_engine {
            let query_engine = PyQueryEngine::from_weakref(engine.clone());

            // put a object named with query of class PyQueryEngine in scope
            PyQueryEngine::make_class(&vm.ctx);
            set_query_engine_in_scope(&scope, vm, query_engine)?;
        }

        // It's safe to unwrap code_object, it's already compiled before.
        let code_obj = vm.ctx.new_code(copr.code_obj.clone().unwrap());
        let ret = vm
            .run_code_obj(code_obj, scope)
            .map_err(|e| format_py_error(e, vm))?;

        // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
        let col_len = rb.num_rows();
        let mut cols = try_into_columns(&ret, vm, col_len)?;
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
        RecordBatch::new(schema, cols).context(NewRecordBatchSnafu)
    })
}

/// convert a tuple of `PyVector` or one `PyVector`(wrapped in a Python Object Ref[`PyObjectRef`])
/// to a `Vec<ArrayRef>`
/// by default, a constant(int/float/bool) gives the a constant array of same length with input args
fn try_into_columns(
    obj: &PyObjectRef,
    vm: &VirtualMachine,
    col_len: usize,
) -> Result<Vec<VectorRef>> {
    if is_instance::<PyTuple>(obj, vm) {
        let tuple = obj
            .payload::<PyTuple>()
            .with_context(|| ret_other_error_with(format!("can't cast obj {obj:?} to PyTuple)")))?;
        let cols = tuple
            .iter()
            .map(|obj| py_vec_obj_to_array(obj, vm, col_len))
            .collect::<Result<Vec<VectorRef>>>()?;
        Ok(cols)
    } else {
        let col = py_vec_obj_to_array(obj, vm, col_len)?;
        Ok(vec![col])
    }
}

/// init interpreter with type PyVector and Module: greptime
pub(crate) fn init_interpreter() -> Arc<Interpreter> {
    INTERPRETER.with(|i| {
        i.borrow_mut()
            .get_or_insert_with(|| {
                // we limit stdlib imports for safety reason, i.e `fcntl` is not allowed here
                let native_module_allow_list = HashSet::from([
                    "array", "cmath", "gc", "hashlib", "_json", "_random", "math",
                ]);
                // TODO(discord9): edge cases, can't use "..Default::default" because Settings is `#[non_exhaustive]`
                // so more in here: https://internals.rust-lang.org/t/allow-constructing-non-exhaustive-structs-using-default-default/13868
                let mut settings = vm::Settings::default();
                // disable SIG_INT handler so our own binary can take ctrl_c handler
                settings.no_sig_int = true;
                let interpreter = Arc::new(vm::Interpreter::with_init(settings, |vm| {
                    // not using full stdlib to prevent security issue, instead filter out a few simple util module
                    vm.add_native_modules(
                        rustpython_stdlib::get_module_inits()
                            .into_iter()
                            .filter(|(k, _)| native_module_allow_list.contains(k.as_ref())),
                    );

                    // We are freezing the stdlib to include the standard library inside the binary.
                    // so according to this issue:
                    // https://github.com/RustPython/RustPython/issues/4292
                    // add this line for stdlib, so rustpython can found stdlib's python part in bytecode format
                    vm.add_frozen(rustpython_pylib::frozen_stdlib());
                    // add our own custom datatype and module
                    PyVector::make_class(&vm.ctx);
                    // TODO(discord9): refactor
                    // vm.add_native_module("greptime", Box::new(greptime_builtin::make_module));

                    // data_frame::PyDataFrame::make_class(&vm.ctx);
                    // data_frame::PyExpr::make_class(&vm.ctx);
                    // vm.add_native_module("data_frame", Box::new(data_frame::make_module));
                }));
                info!("Initialized Python interpreter.");
                interpreter
            })
            .clone()
    })
}
