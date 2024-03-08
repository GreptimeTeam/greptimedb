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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use api::greptime_proto::v1::add_column_location::LocationType;
use api::greptime_proto::v1::AddColumnLocation as Location;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

pub mod columnar_value;
pub mod error;
mod function;
pub mod logical_plan;
pub mod physical_plan;
pub mod prelude;
mod signature;
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug)]
pub struct Output {
    pub data: OutputData,
    pub meta: OutputMeta,
}

// sql output
pub enum OutputData {
    AffectedRows(usize),
    RecordBatches(RecordBatches),
    Stream(SendableRecordBatchStream),
}

#[derive(Debug, Default)]
pub struct OutputMeta {
    pub plan: Option<Arc<dyn PhysicalPlan>>,
    pub cost: usize,
}

impl Output {
    pub fn new_with_affectedrows(affected_rows: usize) -> Self {
        Self {
            data: OutputData::AffectedRows(affected_rows),
            meta: Default::default(),
        }
    }

    pub fn new_with_recordbatches(recordbatches: RecordBatches) -> Self {
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
}

impl Debug for OutputData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputData::AffectedRows(rows) => write!(f, "OutputData::AffectedRows({rows})"),
            OutputData::RecordBatches(recordbatches) => {
                write!(f, "OutputData::RecordBatches({recordbatches:?})")
            }
            OutputData::Stream(_) => {
                write!(f, "OutputData::Stream(<stream>)")
            }
        }
    }
}

impl OutputMeta {
    pub fn new(plan: Option<Arc<dyn PhysicalPlan>>, cost: usize) -> Self {
        Self { plan, cost }
    }

    pub fn new_with_plan(plan: Arc<dyn PhysicalPlan>) -> Self {
        Self {
            plan: Some(plan),
            cost: 0,
        }
    }

    pub fn new_with_cost(cost: usize) -> Self {
        Self { plan: None, cost }
    }
}

pub use datafusion::physical_plan::ExecutionPlan as DfPhysicalPlan;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
pub enum AddColumnLocation {
    First,
    After { column_name: String },
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
                after_column_name: column_name.to_string(),
            },
        }
    }
}
