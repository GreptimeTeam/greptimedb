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

use bytes::{Buf, BufMut, Bytes};
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric,
};
use prost::encoding::{DecodeContext, WireType, decode_varint, message};
use prost::{DecodeError, Message};

/// Decode scalar metrics with bounded attribute capacity hints. Prost still
/// handles field validation, unknown fields, oneofs and recursive messages.
pub fn decode_metrics(bytes: Bytes) -> Result<ExportMetricsServiceRequest, DecodeError> {
    RequestDecoder::decode(bytes).map(|request| request.0)
}

macro_rules! decoder {
    ($name:ident, $inner:ty, |$this:ident, $tag:ident, $wire:ident, $buf:ident, $ctx:ident| $merge:block) => {
        #[derive(Debug, Default)]
        struct $name($inner);

        impl Message for $name {
            fn encode_raw(&self, buf: &mut impl BufMut) {
                self.0.encode_raw(buf);
            }

            fn merge_field(
                &mut self,
                $tag: u32,
                $wire: WireType,
                $buf: &mut impl Buf,
                $ctx: DecodeContext,
            ) -> Result<(), DecodeError> {
                let $this = self;
                $merge
            }

            fn encoded_len(&self) -> usize {
                self.0.encoded_len()
            }

            fn clear(&mut self) {
                self.0.clear();
            }
        }
    };
}

decoder!(
    RequestDecoder,
    ExportMetricsServiceRequest,
    |this, tag, wire, buf, ctx| {
        if tag == 1 {
            let mut resource = ResourceDecoder::default();
            message::merge(wire, &mut resource, buf, ctx)?;
            this.0.resource_metrics.push(resource.0);
            Ok(())
        } else {
            this.0.merge_field(tag, wire, buf, ctx)
        }
    }
);

decoder!(
    ResourceDecoder,
    ResourceMetrics,
    |this, tag, wire, buf, ctx| {
        if tag == 2 {
            let mut scope = ScopeDecoder::default();
            message::merge(wire, &mut scope, buf, ctx)?;
            this.0.scope_metrics.push(scope.0);
            Ok(())
        } else {
            this.0.merge_field(tag, wire, buf, ctx)
        }
    }
);

decoder!(ScopeDecoder, ScopeMetrics, |this, tag, wire, buf, ctx| {
    if tag == 2 {
        let mut metric = MetricDecoder::default();
        message::merge(wire, &mut metric, buf, ctx)?;
        this.0.metrics.push(metric.0);
        Ok(())
    } else {
        this.0.merge_field(tag, wire, buf, ctx)
    }
});

decoder!(MetricDecoder, Metric, |this, tag, wire, buf, ctx| {
    match tag {
        5 => {
            let mut gauge = GaugeDecoder(match this.0.data.take() {
                Some(metric::Data::Gauge(gauge)) => gauge,
                _ => Gauge::default(),
            });
            message::merge(wire, &mut gauge, buf, ctx)?;
            this.0.data = Some(metric::Data::Gauge(gauge.0));
            Ok(())
        }
        7 => {
            let mut sum = SumDecoder(match this.0.data.take() {
                Some(metric::Data::Sum(sum)) => sum,
                _ => Sum::default(),
            });
            message::merge(wire, &mut sum, buf, ctx)?;
            this.0.data = Some(metric::Data::Sum(sum.0));
            Ok(())
        }
        _ => this.0.merge_field(tag, wire, buf, ctx),
    }
});

decoder!(GaugeDecoder, Gauge, |this, tag, wire, buf, ctx| {
    if tag == 1 {
        this.0
            .data_points
            .push(decode_number_point(wire, buf, ctx)?);
        Ok(())
    } else {
        this.0.merge_field(tag, wire, buf, ctx)
    }
});

decoder!(SumDecoder, Sum, |this, tag, wire, buf, ctx| {
    if tag == 1 {
        this.0
            .data_points
            .push(decode_number_point(wire, buf, ctx)?);
        Ok(())
    } else {
        this.0.merge_field(tag, wire, buf, ctx)
    }
});

fn decode_number_point(
    wire: WireType,
    buf: &mut impl Buf,
    ctx: DecodeContext,
) -> Result<NumberDataPoint, DecodeError> {
    // Large, label-rich samples otherwise repeatedly grow a Vec while decoding.
    // The hint is capped independently of the untrusted length; small points
    // retain prost's normal allocation behavior.
    let capacity = match decode_varint(&mut buf.chunk()) {
        Ok(len) if len >= 256 => 32,
        _ => 0,
    };
    let mut point = NumberDataPoint {
        attributes: Vec::with_capacity(capacity),
        ..Default::default()
    };
    message::merge(wire, &mut point, buf, ctx)?;
    Ok(point)
}
