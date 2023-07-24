// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(btree_drain_filter)]

pub mod error;
pub mod heartbeat;
pub mod helper;
pub mod ident;
pub mod instruction;
pub mod key;
pub mod kv_backend;
pub mod metrics;
pub mod peer;
pub mod rpc;
pub mod table_name;
pub mod util;

pub type ClusterId = u64;
pub type DatanodeId = u64;

pub use instruction::RegionIdent;
