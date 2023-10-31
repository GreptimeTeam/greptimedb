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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::result::Result as StdResult;

use common_recordbatch::RecordBatch;
use common_telemetry::info;
use datatypes::vectors::VectorRef;
use rustpython_vm::builtins::{PyBaseExceptionRef, PyDict, PyStr, PyTuple};
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::convert::ToPyObject;
use rustpython_vm::scope::Scope;
use rustpython_vm::{vm, AsObject, Interpreter, PyObjectRef, PyPayload, VirtualMachine};
use snafu::{OptionExt, ResultExt};

use crate::python::error::{ensure, ret_other_error_with, NewRecordBatchSnafu, OtherSnafu, Result};
use crate::python::ffi_types::copr::PyQueryEngine;
use crate::python::ffi_types::py_recordbatch::PyRecordBatch;
use crate::python::ffi_types::{check_args_anno_real_type, select_from_rb, Coprocessor, PyVector};
use crate::python::metric;
use crate::python::rspython::builtins::init_greptime_builtins;
use crate::python::rspython::dataframe_impl::data_frame::set_dataframe_in_scope;
use crate::python::rspython::dataframe_impl::init_data_frame;
use crate::python::rspython::utils::{format_py_error, is_instance, py_obj_to_vec};

thread_local!(static INTERPRETER: RefCell<Option<Rc<Interpreter>>> = RefCell::new(None));

/// Using `RustPython` to run a parsed `Coprocessor` struct as input to execute python code
pub(crate) fn rspy_exec_parsed(
    copr: &Coprocessor,
    rb: &Option<RecordBatch>,
    params: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let _t = metric::METRIC_RSPY_EXEC_TOTAL_ELAPSED.start_timer();
    // 3. get args from `rb`, and cast them into PyVector
    let args: Vec<PyVector> = if let Some(rb) = rb {
        let arg_names = copr.deco_args.arg_names.clone().unwrap_or_default();
        let args = select_from_rb(rb, &arg_names)?;
        check_args_anno_real_type(&arg_names, &args, copr, rb)?;
        args
    } else {
        vec![]
    };
    let interpreter = init_interpreter();
    // 4. then set args in scope and compile then run `CodeObject` which already append a new `Call` node
    exec_with_cached_vm(copr, rb, args, params, &interpreter)
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
    name: &str,
    query_engine: PyQueryEngine,
) -> Result<()> {
    scope
        .locals
        .as_object()
        .set_item(name, query_engine.to_pyobject(vm), vm)
        .map_err(|e| format_py_error(e, vm))
}

pub(crate) fn exec_with_cached_vm(
    copr: &Coprocessor,
    rb: &Option<RecordBatch>,
    args: Vec<PyVector>,
    params: &HashMap<String, String>,
    vm: &Rc<Interpreter>,
) -> Result<RecordBatch> {
    vm.enter(|vm| -> Result<RecordBatch> {
        let _t = metric::METRIC_RSPY_EXEC_ELAPSED.start_timer();

        // set arguments with given name and values
        let scope = vm.new_scope_with_builtins();
        if let Some(rb) = rb {
            set_dataframe_in_scope(&scope, vm, "__dataframe__", rb)?;
        }

        if let Some(arg_names) = &copr.deco_args.arg_names {
            assert_eq!(arg_names.len(), args.len());
            set_items_in_scope(&scope, vm, arg_names, args)?;
        }

        if let Some(engine) = &copr.query_engine {
            let query_engine = PyQueryEngine::from_weakref(engine.clone());

            // put a object named with query of class PyQueryEngine in scope
            set_query_engine_in_scope(&scope, vm, "__query__", query_engine)?;
        }

        if let Some(kwarg) = &copr.kwarg {
            let dict = PyDict::new_ref(&vm.ctx);
            for (k, v) in params {
                dict.set_item(k, PyStr::from(v.clone()).into_pyobject(vm), vm)
                    .map_err(|e| format_py_error(e, vm))?;
            }
            scope
                .locals
                .as_object()
                .set_item(kwarg, vm.new_pyobj(dict), vm)
                .map_err(|e| format_py_error(e, vm))?;
        }

        // It's safe to unwrap code_object, it's already compiled before.
        let code_obj = vm.ctx.new_code(copr.code_obj.clone().unwrap());
        let ret = vm
            .run_code_obj(code_obj, scope)
            .map_err(|e| format_py_error(e, vm))?;

        // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
        let col_len = rb.as_ref().map(|rb| rb.num_rows()).unwrap_or(1);
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
/// to a `Vec<VectorRef>`
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
            .map(|obj| py_obj_to_vec(obj, vm, col_len))
            .collect::<Result<Vec<VectorRef>>>()?;
        Ok(cols)
    } else {
        let col = py_obj_to_vec(obj, vm, col_len)?;
        Ok(vec![col])
    }
}

/// init interpreter with type PyVector and Module: greptime
pub(crate) fn init_interpreter() -> Rc<Interpreter> {
    let _t = metric::METRIC_RSPY_INIT_ELAPSED.start_timer();
    INTERPRETER.with(|i| {
        i.borrow_mut()
            .get_or_insert_with(|| {
                // we limit stdlib imports for safety reason, i.e `fcntl` is not allowed here
                let native_module_allow_list = HashSet::from([
                    "array", "cmath", "gc", "hashlib", "_json", "_random", "math",
                ]);
                // edge cases, can't use "..Default::default" because Settings is `#[non_exhaustive]`
                // so more in here: https://internals.rust-lang.org/t/allow-constructing-non-exhaustive-structs-using-default-default/13868
                let mut settings = vm::Settings::default();
                // disable SIG_INT handler so our own binary can take ctrl_c handler
                settings.no_sig_int = true;
                let interpreter = Rc::new(vm::Interpreter::with_init(settings, |vm| {
                    // not using full stdlib to prevent security issue, instead filter out a few simple util module
                    vm.add_native_modules(
                        rustpython_stdlib::get_module_inits()
                            .filter(|(k, _)| native_module_allow_list.contains(k.as_ref())),
                    );

                    // We are freezing the stdlib to include the standard library inside the binary.
                    // so according to this issue:
                    // https://github.com/RustPython/RustPython/issues/4292
                    // add this line for stdlib, so rustpython can found stdlib's python part in bytecode format
                    vm.add_frozen(rustpython_pylib::FROZEN_STDLIB);
                    // add our own custom datatype and module
                    let _ = PyVector::make_class(&vm.ctx);
                    let _ = PyQueryEngine::make_class(&vm.ctx);
                    let _ = PyRecordBatch::make_class(&vm.ctx);
                    init_greptime_builtins("greptime", vm);
                    init_data_frame("data_frame", vm);
                }));
                info!("Initialized Python interpreter.");
                interpreter
            })
            .clone()
    })
}
