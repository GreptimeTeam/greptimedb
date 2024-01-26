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

use api::helper::{pb_value_to_value_ref, value_to_grpc_value};
use api::v1::Row as ProtoRow;
use datatypes::value::Value;
use itertools::Itertools;

use super::error::NoProtoTypeSnafu;
use crate::adapter::error::Result;
use crate::repr::Row as FlowRow;

/// Convert Protobuf-Row to Row used by GrepFlow
pub(crate) fn row_proto_to_flow(row: ProtoRow) -> FlowRow {
    FlowRow::pack(
        row.values
            .iter()
            .map(|pb_val| -> Value { pb_value_to_value_ref(pb_val, &None).into() }),
    )
}

pub(crate) fn row_flow_to_proto(row: FlowRow) -> ProtoRow {
    let values = row
        .unpack()
        .into_iter()
        .map(|val| value_to_grpc_value(val.clone()))
        .collect_vec();
    ProtoRow { values }
}
