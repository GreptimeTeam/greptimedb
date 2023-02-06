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

use crate::error::Result;
use crate::sst::FileHandle;

#[allow(unused)]
pub(crate) struct CompactionTask {
    inputs: Vec<CompactionInput>,
}

#[allow(unused)]
impl CompactionTask {
    // TODO(hl): Actual SST compaction tasks
    pub async fn run(self) -> Result<()> {
        Ok(())
    }
}

#[allow(unused)]
pub(crate) struct CompactionInput {
    input_level: u8,
    output_level: u8,
    file: FileHandle,
}
