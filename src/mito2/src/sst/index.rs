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

pub mod applier;
mod codec;
pub mod creator;
mod object_store;

const INDEX_BLOB_TYPE: &str = "greptime-inverted-index-v1";

// TODO(zhongzc): how to determine this value?
const MIN_MEMORY_USAGE_THRESHOLD: usize = 8192;

const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;
