use datafusion::error::DataFusionError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unexpected empty grpc physical plan type: {}", name))]
    EmptyGrpcPhysicalPlan { name: String },

    #[snafu(display("Unexpected empty grpc physical expr: {}", name))]
    EmptyGrpcExpr { name: String },

    #[snafu(display("Unsupported datafusion physical expr: {}", name))]
    UnsupportedDfExpr { name: String },

    #[snafu(display("Missing required field in protobuf: {}", field))]
    MissingField { field: String },

    #[snafu(display("Failed to new datafusion projection exec: {}", source))]
    NewProjection { source: DataFusionError },

    #[snafu(display("Unsupported df execution plan: {}", name))]
    UnsupportedDf { name: String },
}
