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

mod build;
mod database;
mod procedure_state;
mod timezone;
mod version;

use std::sync::Arc;

use build::BuildFunction;
use database::DatabaseFunction;
use procedure_state::ProcedureStateFunction;
use timezone::TimezoneFunction;
use version::VersionFunction;

use crate::function_registry::FunctionRegistry;

pub(crate) struct SystemFunction;

impl SystemFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(BuildFunction));
        registry.register(Arc::new(VersionFunction));
        registry.register(Arc::new(DatabaseFunction));
        registry.register(Arc::new(TimezoneFunction));
        registry.register(Arc::new(ProcedureStateFunction));
    }
}
