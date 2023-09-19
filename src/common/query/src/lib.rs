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

use api::greptime_proto::v1::add_column_location::LocationType;
use api::greptime_proto::v1::AddColumnLocation as Location;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use serde::{Deserialize, Serialize};

pub mod columnar_value;
pub mod error;
mod function;
pub mod logical_plan;
pub mod physical_plan;
pub mod prelude;
mod signature;
use sqlparser_derive::{Visit, VisitMut};

// sql output
pub enum Output {
    AffectedRows(usize),
    RecordBatches(RecordBatches),
    Stream(SendableRecordBatchStream),
}

impl Debug for Output {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Output::AffectedRows(rows) => write!(f, "Output::AffectedRows({rows})"),
            Output::RecordBatches(recordbatches) => {
                write!(f, "Output::RecordBatches({recordbatches:?})")
            }
            Output::Stream(_) => write!(f, "Output::Stream(<stream>)"),
        }
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
                after_column_name: "".to_string(),
            },
            AddColumnLocation::After { column_name } => Location {
                location_type: LocationType::After.into(),
                after_column_name: column_name.to_string(),
            },
        }
    }
}
