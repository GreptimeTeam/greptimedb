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

use bytes::Bytes;
use prost::Message;
use snafu::ResultExt;

use crate::error;
use crate::prom_remote_write::try_decompress;

// Wire-compatible mirror of Prometheus prompb/io/prometheus/write/v2/types.proto.
// Replace this with the generated type once it is exposed by our proto crate.
pub(crate) fn decode_remote_write_v2_request(
    is_zstd: bool,
    body: Bytes,
) -> crate::error::Result<Request> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();

    // Match the v1 decoder's VictoriaMetrics fallback: some clients may send a
    // mismatched content-encoding header, so try the other compression on failure.
    let buf = if let Ok(buf) = try_decompress(is_zstd, &body[..]) {
        buf
    } else {
        try_decompress(!is_zstd, &body[..])?
    };

    Request::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Request {
    #[prost(string, repeated, tag = "4")]
    pub symbols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "5")]
    pub timeseries: ::prost::alloc::vec::Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct TimeSeries {
    #[prost(uint32, repeated, tag = "1")]
    pub labels_refs: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag = "2")]
    pub samples: ::prost::alloc::vec::Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub histograms: ::prost::alloc::vec::Vec<Histogram>,
    #[prost(message, repeated, tag = "4")]
    pub exemplars: ::prost::alloc::vec::Vec<Exemplar>,
    #[prost(message, optional, tag = "5")]
    pub metadata: ::core::option::Option<Metadata>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Exemplar {
    #[prost(uint32, repeated, tag = "1")]
    pub labels_refs: ::prost::alloc::vec::Vec<u32>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
    #[prost(int64, tag = "3")]
    pub start_timestamp: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Metadata {
    #[prost(enumeration = "metadata::MetricType", tag = "1")]
    pub r#type: i32,
    #[prost(uint32, tag = "3")]
    pub help_ref: u32,
    #[prost(uint32, tag = "4")]
    pub unit_ref: u32,
}

pub(crate) mod metadata {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub(crate) enum MetricType {
        Unspecified = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        Gaugehistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Histogram {
    #[prost(oneof = "histogram::Count", tags = "1, 2")]
    pub count: ::core::option::Option<histogram::Count>,
    #[prost(double, tag = "3")]
    pub sum: f64,
    #[prost(sint32, tag = "4")]
    pub schema: i32,
    #[prost(double, tag = "5")]
    pub zero_threshold: f64,
    #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
    pub zero_count: ::core::option::Option<histogram::ZeroCount>,
    #[prost(message, repeated, tag = "8")]
    pub negative_spans: ::prost::alloc::vec::Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "9")]
    pub negative_deltas: ::prost::alloc::vec::Vec<i64>,
    #[prost(double, repeated, tag = "10")]
    pub negative_counts: ::prost::alloc::vec::Vec<f64>,
    #[prost(message, repeated, tag = "11")]
    pub positive_spans: ::prost::alloc::vec::Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "12")]
    pub positive_deltas: ::prost::alloc::vec::Vec<i64>,
    #[prost(double, repeated, tag = "13")]
    pub positive_counts: ::prost::alloc::vec::Vec<f64>,
    #[prost(enumeration = "histogram::ResetHint", tag = "14")]
    pub reset_hint: i32,
    #[prost(int64, tag = "15")]
    pub timestamp: i64,
    #[prost(double, repeated, tag = "16")]
    pub custom_values: ::prost::alloc::vec::Vec<f64>,
    #[prost(int64, tag = "17")]
    pub start_timestamp: i64,
}

pub(crate) mod histogram {
    #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
    pub(crate) enum Count {
        #[prost(uint64, tag = "1")]
        CountInt(u64),
        #[prost(double, tag = "2")]
        CountFloat(f64),
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
    pub(crate) enum ZeroCount {
        #[prost(uint64, tag = "6")]
        ZeroCountInt(u64),
        #[prost(double, tag = "7")]
        ZeroCountFloat(f64),
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub(crate) enum ResetHint {
        Unspecified = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct BucketSpan {
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    #[prost(uint32, tag = "2")]
    pub length: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_remote_write_v2_request() {
        let request = Request {
            symbols: vec![
                "".to_string(),
                "__name__".to_string(),
                "http_requests_total".to_string(),
            ],
            timeseries: vec![TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![Sample {
                    value: 42.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: Some(Metadata {
                    r#type: metadata::MetricType::Counter as i32,
                    help_ref: 0,
                    unit_ref: 0,
                }),
            }],
        };
        let body =
            Bytes::from(crate::prom_store::snappy_compress(&request.encode_to_vec()).unwrap());

        let decoded = decode_remote_write_v2_request(false, body).unwrap();

        assert_eq!(decoded.symbols, request.symbols);
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].labels_refs, vec![1, 2]);
        assert_eq!(decoded.timeseries[0].samples.len(), 1);
        assert_eq!(decoded.timeseries[0].samples[0].value, 42.0);
        assert_eq!(decoded.timeseries[0].metadata.as_ref().unwrap().r#type, 1);
    }
}
