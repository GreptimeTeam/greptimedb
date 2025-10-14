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

#![feature(assert_matches)]

pub mod arrow_array;
pub mod data_type;
pub mod duration;
pub mod error;
pub mod interval;
pub mod json;
pub mod macros;
pub mod prelude;
pub mod scalars;
pub mod schema;
pub mod serialize;
pub mod time;
pub mod timestamp;
pub mod type_id;
pub mod types;
pub mod value;
pub mod vectors;

pub use arrow::{self, compute};
pub use error::{Error, Result};
