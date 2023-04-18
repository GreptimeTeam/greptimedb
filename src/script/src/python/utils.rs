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

use futures::Future;
use once_cell::sync::OnceCell;
use rustpython_vm::builtins::PyBaseExceptionRef;
use rustpython_vm::VirtualMachine;
use tokio::runtime::Runtime;

use crate::python::error;

pub fn format_py_error(excep: PyBaseExceptionRef, vm: &VirtualMachine) -> error::Error {
    let mut msg = String::new();
    if let Err(e) = vm.write_exception(&mut msg, &excep) {
        return error::PyRuntimeSnafu {
            msg: format!("Failed to write exception msg, err: {e}"),
        }
        .build();
    }
    error::PyRuntimeSnafu { msg }.build()
}
static LOCAL_RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
fn get_local_runtime() -> std::thread::Result<&'static Runtime> {
    let rt = LOCAL_RUNTIME
        .get_or_try_init(|| tokio::runtime::Runtime::new().map_err(|e| Box::new(e) as _))?;
    Ok(rt)
}
/// a terrible hack to call async from sync by:
/// TODO(discord9): find a better way
/// 1. spawn a new thread
/// 2. create a new runtime in new thread and call `block_on` on it
pub fn block_on_async<T, F>(f: F) -> std::thread::Result<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = get_local_runtime()?;

    std::thread::spawn(move || rt.block_on(f)).join()
}
