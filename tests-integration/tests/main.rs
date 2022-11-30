#[macro_use]
mod grpc;
#[macro_use]
mod http;

grpc_tests!(File, S3);
http_tests!(File, S3);
