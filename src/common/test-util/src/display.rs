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

use std::fmt;
use std::fmt::Formatter;

use client::error::Result;
use common_error::ext::ErrorExt;
use common_query::Output;
use common_recordbatch::util::collect_batches;

pub struct ResultDisplayer(pub Result<Output>);

impl fmt::Display for ResultDisplayer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Ok(result) => match result {
                Output::AffectedRows(rows) => {
                    write!(f, "Affected Rows: {rows}")
                }
                Output::RecordBatches(recordbatches) => {
                    let pretty = recordbatches.pretty_print().unwrap();
                    write!(f, "{pretty}")
                }
                Output::Stream(_) => unimplemented!(),
            },
            Err(e) => {
                let status_code = e.status_code();
                let root_cause = e.output_msg();
                write!(
                    f,
                    "Error: {}({status_code}), {root_cause}",
                    status_code as u32
                )
            }
        }
    }
}

impl ResultDisplayer {
    pub async fn display(self) -> String {
        match self.0 {
            Ok(Output::Stream(s)) => collect_batches(s).await.unwrap().pretty_print().unwrap(),
            _ => self.to_string(),
        }
    }
}
