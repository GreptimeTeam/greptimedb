use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pipeline::{parse, Content, GreptimeTransformer, Pipeline, Value as PipelineValue};
use serde_json::Deserializer;

fn processor(input: &str) -> impl IntoIterator<Item = greptime_proto::v1::Rows> + '_ {
    let input_values = Deserializer::from_str(input).into_iter::<serde_json::Value>();

    let pipeline_yaml = r#"
---
description: Pipeline for Akamai DataStream2 Log

processors:
  - urlencoding:
      fields:
        - breadcrumbs
        - UA
        - referer
        - queryStr
      method: decode
      ignore_missing: true
  - gsub:
      field: reqTimeSec
      pattern: "\\."
      replacement: ""
  - epoch:
      field: reqTimeSec
      resolution: millisecond
      ignore_missing: true
  - regex:
      field: breadcrumbs
      patterns:
        - "(?<parent>\\[[^\\[]*c=c[^\\]]*\\])"
        - "(?<edge>\\[[^\\[]*c=g[^\\]]*\\])"
        - "(?<origin>\\[[^\\[]*c=o[^\\]]*\\])"
        - "(?<peer>\\[[^\\[]*c=p[^\\]]*\\])"
        - "(?<cloud_wrapper>\\[[^\\[]*c=w[^\\]]*\\])"
      ignore_missing: true
  - regex:
      fields:
        - breadcrumbs_parent
        - breadcrumbs_edge
        - breadcrumbs_origin
        - breadcrumbs_peer
        - breadcrumbs_cloud_wrapper
      ignore_missing: true
      patterns:
        - "a=(?<ip>[^,\\]]+)"
        - "b=(?<request_id>[^,\\]]+)"
        - "k=(?<request_end_time>[^,\\]]+)"
        - "l=(?<turn_around_time>[^,\\]]+)"
        - "m=(?<dns_lookup_time>[^,\\]]+)"
        - "n=(?<geo>[^,\\]]+)"
        - "o=(?<asn>[^,\\]]+)"
  - regex:
      field: queryStr, cmcd
      patterns:
        - "(?i)CMCD=//(?<version>[\\d\\.]+)@V/(?<data>.+$)"
      ignore_missing: true
  - cmcd:
      field: cmcd_data, cmcd
      ignore_missing: true

transform:
  - fields:
      - breadcrumbs
      - referer
      - queryStr, query_str
      - customField, custom_field
      - reqId, req_id
      - city
      - state
      - country
      - securityRules, security_rules
      - ewUsageInfo, ew_usage_info
      - ewExecutionInfo, ew_execution_info
      - errorCode, error_code
      - xForwardedFor, x_forwarded_for
      - range
      - accLang, acc_lang
      - reqMethod, req_method
      - reqHost, req_host
      - proto
      - cliIP, cli_ip
      - rspContentType, rsp_content_type
      - tlsVersion, tls_version
    type: string
  - fields:
      - version
      - cacheStatus, cache_status
      - lastByte, last_byte
    type: uint8
  - fields:
      - streamId, stream_id
      - billingRegion, billing_region
      - transferTimeMSec, transfer_time_msec
      - turnAroundTimeMSec, turn_around_time_msec
      - reqEndTimeMSec, req_end_time_msec
      - maxAgeSec, max_age_sec
      - reqPort, req_port
      - statusCode, status_code
      - cp
      - dnsLookupTimeMSec, dns_lookup_time_msec
      - tlsOverheadTimeMSec, tls_overhead_time_msec
    type: uint32
    on_failure: ignore
  - fields:
      - bytes
      - rspContentLen, rsp_content_len
      - objSize, obj_size
      - uncompressedSize, uncompressed_size
      - overheadBytes, overhead_bytes
      - totalBytes, total_bytes
    type: uint64
    on_failure: ignore
  - fields:
      - UA, user_agent
      - cookie
      - reqPath, req_path
    type: string
    # index: fulltext
  - field: reqTimeSec, req_time_sec
    # epoch time is special, the resolution MUST BE specified
    type: epoch, ms
    index: timestamp

  # the following is from cmcd
  - fields:
      - cmcd_version
      - cmcd_cid, cmcd_content_id
      - cmcd_nor, cmcd_next_object_requests
      - cmcd_nrr, cmcd_next_range_request
      - cmcd_ot, cmcd_object_type
      - cmcd_sf, cmcd_streaming_format
      - cmcd_sid, cmcd_session_id
      - cmcd_st, cmcd_stream_type
      - cmcd_v
    type: string
  - fields:
      - cmcd_br, cmcd_encoded_bitrate
      - cmcd_bl, cmcd_buffer_length
      - cmcd_d, cmcd_object_duration
      - cmcd_dl, cmcd_deadline
      - cmcd_mtp, cmcd_measured_throughput
      - cmcd_rtp, cmcd_requested_max_throughput
      - cmcd_tb, cmcd_top_bitrate
    type: uint64
  - fields:
      - cmcd_pr, cmcd_playback_rate
    type: float64
  - fields:
      - cmcd_bs, cmcd_buffer_starvation
      - cmcd_su, cmcd_startup
    type: boolean

  # the following is from breadcrumbs
  - fields:
      - breadcrumbs_parent_ip
      - breadcrumbs_parent_request_id
      - breadcrumbs_parent_geo
      - breadcrumbs_edge_ip
      - breadcrumbs_edge_request_id
      - breadcrumbs_edge_geo
      - breadcrumbs_origin_ip
      - breadcrumbs_origin_request_id
      - breadcrumbs_origin_geo
      - breadcrumbs_peer_ip
      - breadcrumbs_peer_request_id
      - breadcrumbs_peer_geo
      - breadcrumbs_cloud_wrapper_ip
      - breadcrumbs_cloud_wrapper_request_id
      - breadcrumbs_cloud_wrapper_geo
    type: string
  - fields:
      - breadcrumbs_parent_request_end_time
      - breadcrumbs_parent_turn_around_time
      - breadcrumbs_parent_dns_lookup_time
      - breadcrumbs_parent_asn
      - breadcrumbs_edge_request_end_time
      - breadcrumbs_edge_turn_around_time
      - breadcrumbs_edge_dns_lookup_time
      - breadcrumbs_edge_asn
      - breadcrumbs_origin_request_end_time
      - breadcrumbs_origin_turn_around_time
      - breadcrumbs_origin_dns_lookup_time
      - breadcrumbs_origin_asn
      - breadcrumbs_peer_request_end_time
      - breadcrumbs_peer_turn_around_time
      - breadcrumbs_peer_dns_lookup_time
      - breadcrumbs_peer_asn
      - breadcrumbs_cloud_wrapper_request_end_time
      - breadcrumbs_cloud_wrapper_turn_around_time
      - breadcrumbs_cloud_wrapper_dns_lookup_time
      - breadcrumbs_cloud_wrapper_asn
    type: uint32
"#;

    let pipeline: Pipeline<GreptimeTransformer> =
        parse(&Content::Yaml(pipeline_yaml.into())).unwrap();
    let pipeline_data = input_values
        .into_iter()
        .map(|v| PipelineValue::try_from(v.unwrap()).unwrap());
    let transformed_data = pipeline_data
        .into_iter()
        .map(move |v| pipeline.exec(v).unwrap())
        .collect::<Vec<_>>();
    transformed_data
}

fn criterion_benchmark(c: &mut Criterion) {
    let input_value_str = include_str!("./data.log");
    let mut group = c.benchmark_group("pipeline");
    group.sample_size(10);
    group.bench_function("processor 20", |b| {
        b.iter(|| processor(black_box(input_value_str)))
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
