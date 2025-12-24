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

pub mod columnar_value;
pub mod error;
pub mod logical_plan;
pub mod prelude;
pub mod request;
pub mod stream;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;

use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use api::greptime_proto::v1::AddColumnLocation as Location;
use api::greptime_proto::v1::add_column_location::LocationType;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use sqlparser_derive::{Visit, VisitMut};

/// new Output struct with output data(previously Output) and output meta
#[derive(Debug)]
pub struct Output {
    pub data: OutputData,
    pub meta: OutputMeta,
}

/// Original Output struct
/// carrying result data to response/client/user interface
pub enum OutputData {
    AffectedRows(OutputRows),
    RecordBatches(RecordBatches),
    Stream(SendableRecordBatchStream),
}

impl OutputData {
    /// Consume the data to pretty printed string.
    pub async fn pretty_print(self) -> String {
        match self {
            OutputData::AffectedRows(x) => {
                format!("Affected Rows: {x}")
            }
            OutputData::RecordBatches(x) => x.pretty_print().unwrap_or_else(|e| e.to_string()),
            OutputData::Stream(x) => common_recordbatch::util::collect_batches(x)
                .await
                .and_then(|x| x.pretty_print())
                .unwrap_or_else(|e| e.to_string()),
        }
    }
}

/// OutputMeta stores meta information produced/generated during the execution
#[derive(Debug, Default)]
pub struct OutputMeta {
    /// May exist for query output. One can retrieve execution metrics from this plan.
    pub plan: Option<Arc<dyn ExecutionPlan>>,
    pub cost: OutputCost,
}

impl Output {
    pub fn new_with_affected_rows(affected_rows: OutputRows) -> Self {
        Self {
            data: OutputData::AffectedRows(affected_rows),
            meta: Default::default(),
        }
    }

    pub fn new_with_record_batches(recordbatches: RecordBatches) -> Self {
        Self {
            data: OutputData::RecordBatches(recordbatches),
            meta: Default::default(),
        }
    }

    pub fn new_with_stream(stream: SendableRecordBatchStream) -> Self {
        Self {
            data: OutputData::Stream(stream),
            meta: Default::default(),
        }
    }

    pub fn new(data: OutputData, meta: OutputMeta) -> Self {
        Self { data, meta }
    }

    pub fn extract_rows_and_cost(&self) -> (OutputRows, OutputCost) {
        match self.data {
            OutputData::AffectedRows(rows) => (rows, self.meta.cost),
            _ => (0, self.meta.cost),
        }
    }
}

impl Debug for OutputData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputData::AffectedRows(rows) => write!(f, "OutputData::AffectedRows({rows})"),
            OutputData::RecordBatches(recordbatches) => {
                write!(f, "OutputData::RecordBatches({recordbatches:?})")
            }
            OutputData::Stream(s) => {
                write!(f, "OutputData::Stream(<{}>)", s.name())
            }
        }
    }
}

impl OutputMeta {
    pub fn new(plan: Option<Arc<dyn ExecutionPlan>>, cost: usize) -> Self {
        Self { plan, cost }
    }

    pub fn new_with_plan(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan: Some(plan),
            cost: 0,
        }
    }

    pub fn new_with_cost(cost: usize) -> Self {
        Self { plan: None, cost }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
pub enum AddColumnLocation {
    First,
    After { column_name: String },
}

impl Display for AddColumnLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AddColumnLocation::First => write!(f, r#"FIRST"#),
            AddColumnLocation::After { column_name } => {
                write!(f, r#"AFTER {column_name}"#)
            }
        }
    }
}

impl From<&AddColumnLocation> for Location {
    fn from(value: &AddColumnLocation) -> Self {
        match value {
            AddColumnLocation::First => Location {
                location_type: LocationType::First.into(),
                after_column_name: String::default(),
            },
            AddColumnLocation::After { column_name } => Location {
                location_type: LocationType::After.into(),
                after_column_name: column_name.clone(),
            },
        }
    }
}

pub type OutputRows = usize;
pub type OutputCost = usize;
