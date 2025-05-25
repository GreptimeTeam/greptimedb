use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_grpc::FlightData;
use common_recordbatch::DfRecordBatch;

/// Encodes record batch to a Schema message and a RecordBatch message.
pub fn encode_to_flight_data(rb: DfRecordBatch) -> (FlightData, FlightData) {
    let mut encoder = FlightEncoder::default();
    (
        encoder.encode(FlightMessage::Schema(rb.schema())),
        encoder.encode(FlightMessage::RecordBatch(rb)),
    )
}
