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
use datatypes::arrow::util::pretty;

pub trait RecordBatchExt {
    fn pretty_print(&self) -> String;
}

impl RecordBatchExt for RecordBatch {
    fn pretty_print(&self) -> String {
        match pretty::pretty_format_batches(std::slice::from_ref(self)) {
            Ok(s) => s.to_string(),
            Err(e) => format!("unable to pretty print {self:?}: {e}"),
        }
    }
}
