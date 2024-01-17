use api::helper::{pb_value_to_value_ref, to_proto_value};
use api::v1::Row as ProtoRow;
use datatypes::value::Value;
use itertools::Itertools;

use crate::repr::Row as FlowRow;
/// Convert Protobuf-Row to Row used by GrepFlow
pub(crate) fn row_proto2flow(row: ProtoRow) -> FlowRow {
    FlowRow::pack(
        row.values
            .iter()
            .map(|pb_val| -> Value { pb_value_to_value_ref(pb_val, &None).into() }),
    )
}

pub(crate) fn row_flow2proto(row: FlowRow) -> ProtoRow {
    let values = row
        .unpack()
        .into_iter()
        .map(|val| to_proto_value(val).unwrap())
        .collect_vec();
    ProtoRow { values }
}
