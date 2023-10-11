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

//! Common traits and structures for the procedure framework.

#![feature(assert_matches)]

pub mod error;
pub mod local;
pub mod options;
mod procedure;
pub mod store;
pub mod watcher;

pub use crate::error::{Error, Result};
pub use crate::procedure::{
    BoxedProcedure, Context, ContextProvider, LockKey, Procedure, ProcedureId, ProcedureManager,
    ProcedureManagerRef, ProcedureState, ProcedureWithId, Status,
};
pub use crate::watcher::Watcher;
