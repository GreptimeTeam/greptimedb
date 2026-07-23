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

use datatypes::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;

mod schema;
mod stream;

pub(crate) use schema::align_schema_by_nested_paths;
pub(crate) use stream::NestedSchemaAligner;

use crate::error::Result;

pub(crate) type ProjectedRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;
