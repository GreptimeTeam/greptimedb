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

use std::collections::VecDeque;

use crate::error;
use crate::memtable::bulk::row_group_reader::MemtableRowGroupReaderBuilder;
use crate::read::Batch;

pub struct BulkPartIter {
    row_groups_to_read: VecDeque<usize>,
    builder: MemtableRowGroupReaderBuilder,
}

impl BulkPartIter {
    pub(crate) fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        todo!()
    }
}
