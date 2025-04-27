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
#![feature(btree_extract_if)]
#![feature(let_chains)]

pub mod cache;
pub mod cache_invalidator;
pub mod cluster;
pub mod datanode;
pub mod ddl;
pub mod ddl_manager;
pub mod distributed_time_constants;
pub mod error;
pub mod flow_name;
pub mod heartbeat;
pub mod instruction;
pub mod key;
pub mod kv_backend;
pub mod leadership_notifier;
pub mod lock_key;
pub mod metrics;
pub mod node_expiry_listener;
pub mod node_manager;
pub mod peer;
pub mod poison_key;
pub mod range_stream;
pub mod region_keeper;
pub mod region_registry;
pub mod rpc;
pub mod sequence;
pub mod state_store;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;
pub mod util;
pub mod wal_options_allocator;

// The id of the datanode.
pub type DatanodeId = u64;
// The id of the flownode.
pub type FlownodeId = u64;

/// Schema options.
pub type SchemaOptions = key::schema_name::SchemaNameValue;

pub use instruction::RegionIdent;
