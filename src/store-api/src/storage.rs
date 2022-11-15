// Copyright 2022 Greptime Team
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

//! Storage APIs.

mod chunk;
pub mod consts;
mod descriptors;
mod engine;
mod metadata;
mod region;
mod requests;
mod responses;
mod snapshot;
mod types;

pub use datatypes::data_type::ConcreteDataType;
pub use datatypes::schema::{
    ColumnDefaultConstraint, ColumnSchema, Schema, SchemaBuilder, SchemaRef,
};

pub use self::chunk::{Chunk, ChunkReader};
pub use self::descriptors::*;
pub use self::engine::{CreateOptions, EngineContext, OpenOptions, StorageEngine};
pub use self::metadata::RegionMeta;
pub use self::region::{Region, WriteContext};
pub use self::requests::{
    AddColumn, AlterOperation, AlterRequest, GetRequest, PutOperation, ScanRequest, WriteRequest,
};
pub use self::responses::{GetResponse, ScanResponse, WriteResponse};
pub use self::snapshot::{ReadContext, Snapshot};
pub use self::types::{OpType, SequenceNumber};
