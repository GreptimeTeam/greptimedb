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

//! Storage related APIs

#![feature(let_chains)]
#![feature(iterator_try_collect)]

pub mod codec;
pub mod data_source;
pub mod logstore;
pub mod manifest;
pub mod metadata;
pub mod metric_engine_consts;
mod metrics;
pub mod mito_engine_options;
pub mod path_utils;
pub mod region_engine;
pub mod region_request;
pub mod storage;
