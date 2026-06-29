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

use api::greptime_proto::io::prometheus::write::v2::Metadata;
use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
use bytes::Bytes;
use servers::prom_remote_write::v2::test_util as remote_write_v2;

#[test]
fn test_decode_remote_write_v2_native_histogram_dump() {
    const BODY: &[u8] = include_bytes!(
        "testdata/prom_remote_write/remote_write_v2_native_hist_1782358162264510000.raw.snappy"
    );
    const TEXT: &str = include_str!(
        "testdata/prom_remote_write/remote_write_v2_native_hist_1782358162264510000.txt"
    );

    let decoded = remote_write_v2::decode_request(false, Bytes::from_static(BODY)).unwrap();

    assert_text_dump_shape(TEXT, 1, 0, 1, 0, 1);
    assert!(TEXT.contains("received_from_a_http_request_duration_seconds"));
    assert!(TEXT.contains("positive_spans: <"));
    assert_eq!(
        decoded.symbols,
        vec![
            "",
            "__name__",
            "received_from_a_http_request_duration_seconds",
            "handler",
            "/api/v1/write",
            "instance",
            "localhost:9090",
            "job",
            "prometheus",
            "source_prometheus",
            "A",
        ]
    );
    assert_eq!(decoded.timeseries.len(), 1);

    let series = &decoded.timeseries[0];
    assert_eq!(series.labels_refs, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert!(series.samples.is_empty());
    assert!(series.exemplars.is_empty());
    assert_eq!(series.metadata, Some(Metadata::default()));
    assert_eq!(series.histograms.len(), 1);

    let histogram = &series.histograms[0];
    assert_eq!(histogram.count, Some(Count::CountInt(24)));
    assert_eq!(histogram.sum, 0.205418879);
    assert_eq!(histogram.schema, 3);
    assert_eq!(histogram.zero_threshold, 2.938735877055719e-39);
    assert_eq!(histogram.zero_count, Some(ZeroCount::ZeroCountInt(0)));
    assert_eq!(histogram.positive_spans.len(), 3);
    assert_eq!(histogram.positive_spans[0].offset, -63);
    assert_eq!(histogram.positive_spans[0].length, 1);
    assert_eq!(histogram.positive_spans[1].offset, 1);
    assert_eq!(histogram.positive_spans[1].length, 14);
    assert_eq!(histogram.positive_spans[2].offset, 5);
    assert_eq!(histogram.positive_spans[2].length, 1);
    assert_eq!(
        histogram.positive_deltas,
        vec![2, -1, 3, -3, 3, -3, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0]
    );
    assert_eq!(histogram.timestamp, 1782358160412);
}

fn assert_text_dump_shape(
    text: &str,
    timeseries: usize,
    samples: usize,
    histograms: usize,
    exemplars: usize,
    metadata: usize,
) {
    assert_eq!(count_textproto_block(text, "timeseries: <"), timeseries);
    assert_eq!(count_textproto_block(text, "samples: <"), samples);
    assert_eq!(count_textproto_block(text, "histograms: <"), histograms);
    assert_eq!(count_textproto_block(text, "exemplars: <"), exemplars);
    assert_eq!(count_textproto_block(text, "metadata: <"), metadata);
}

fn count_textproto_block(text: &str, block: &str) -> usize {
    text.lines().filter(|line| line.trim() == block).count()
}
