use core::fmt;
use std::fmt::{Debug, Formatter};

use api::prom_store::remote::Sample;
use bytes::{Buf, Bytes};
use object_pool::Pool;
use prost::encoding::message::merge;
use prost::encoding::{decode_key, decode_varint, DecodeContext, WireType};
use prost::DecodeError;

// pub type Label = greptime_proto::prometheus::remote::Label;
pub type Label = PromLabel;

pub struct PromLabel {
    pub name: Bytes,
    pub value: Bytes,
}

impl PromLabel {
    pub fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
    {
        const STRUCT_NAME: &'static str = "PromLabel";
        match tag {
            1u32 => {
                let value = &mut self.name;
                prost::encoding::bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "name");
                    error
                })
            }
            2u32 => {
                let value = &mut self.value;
                prost::encoding::bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "value");
                    error
                })
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    pub fn clear(&mut self) {
        self.name.clear();
        self.value.clear();
    }
}

impl Default for PromLabel {
    fn default() -> Self {
        PromLabel {
            name: Default::default(),
            value: Default::default(),
        }
    }
}

impl Debug for PromLabel {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("PromLabel");
        let builder = {
            let wrapper = {
                #[allow(non_snake_case)]
                fn ScalarWrapper<T>(v: T) -> T {
                    v
                }
                ScalarWrapper(&self.name)
            };
            builder.field("name", &wrapper)
        };
        let builder = {
            let wrapper = {
                #[allow(non_snake_case)]
                fn ScalarWrapper<T>(v: T) -> T {
                    v
                }
                ScalarWrapper(&self.value)
            };
            builder.field("value", &wrapper)
        };
        builder.finish()
    }
}

pub struct PromTimeSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
}

impl PromTimeSeries {
    pub fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
    {
        match tag {
            1u32 => {
                let mut label = Label::default();

                let len = decode_varint(buf)?;
                let remaining = buf.remaining();
                if len > remaining as u64 {
                    return Err(DecodeError::new("buffer underflow"));
                }

                let limit = remaining - len as usize;
                while buf.remaining() > limit {
                    let (tag, wire_type) = decode_key(buf)?;
                    label.merge_field(tag, wire_type, buf, ctx.clone())?;
                }
                if buf.remaining() != limit {
                    return Err(DecodeError::new("delimited length exceeded"));
                }
                self.labels.push(label);
                Ok(())
            }
            2u32 => {
                let mut sample = Sample::default();
                merge(WireType::LengthDelimited, &mut sample, buf, ctx)?;
                self.samples.push(sample);
                Ok(())
            }
            3u32 => prost::encoding::skip_field(wire_type, tag, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    pub fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
    }
}

impl Default for PromTimeSeries {
    fn default() -> Self {
        PromTimeSeries {
            labels: Default::default(),
            samples: Default::default(),
        }
    }
}

impl Debug for PromTimeSeries {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("PromTimeSeries");
        let builder = {
            let wrapper = &self.labels;
            builder.field("labels", &wrapper)
        };
        let builder = {
            let wrapper = &self.samples;
            builder.field("samples", &wrapper)
        };
        builder.finish()
    }
}

pub struct PromWriteRequest {
    pub timeseries: Vec<PromTimeSeries>,
    timeseries_pool: Pool<PromTimeSeries>,
}

impl Debug for PromWriteRequest {
    fn fmt(&self, _f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl PromWriteRequest {
    pub fn merge<B>(&mut self, mut buf: B) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        let ctx = DecodeContext::default();
        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            assert_eq!(WireType::LengthDelimited, wire_type);

            match tag {
                1u32 => {
                    let (_, mut series) = self
                        .timeseries_pool
                        .pull(|| PromTimeSeries::default())
                        .detach();

                    // rewrite merge loop
                    let len = decode_varint(&mut buf)?;
                    let remaining = buf.remaining();
                    if len > remaining as u64 {
                        return Err(DecodeError::new("buffer underflow"));
                    }

                    let limit = remaining - len as usize;
                    while buf.remaining() > limit {
                        let (tag, wire_type) = decode_key(&mut buf)?;
                        series.merge_field(tag, wire_type, &mut buf, ctx.clone())?;
                    }
                    if buf.remaining() != limit {
                        return Err(DecodeError::new("delimited length exceeded"));
                    }

                    self.timeseries.push(series);
                }
                3u32 => {
                    // we can ignore metadata now.
                    prost::encoding::skip_field(wire_type, tag, &mut buf, ctx.clone())?;
                }
                _ => prost::encoding::skip_field(wire_type, tag, &mut buf, ctx.clone())?,
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        for mut ts in self.timeseries.drain(..) {
            ts.clear();
            self.timeseries_pool.attach(ts);
        }
    }
}

impl Default for PromWriteRequest {
    fn default() -> Self {
        PromWriteRequest {
            timeseries: Vec::with_capacity(10000),
            timeseries_pool: Pool::new(10000, || PromTimeSeries::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use crate::proto::PromWriteRequest;

    #[test]
    fn test_decode_write_request() {
        let data = base64::engine::general_purpose::STANDARD
            .decode(std::fs::read("/tmp/prom-data/1709380539690445357").unwrap())
            .unwrap();

        let buf = data.as_slice();
        let mut request = PromWriteRequest::default();
        request.clear();
        request.merge(buf).unwrap();
        assert_eq!(10000, request.timeseries.len());

        for ts in &request.timeseries {
            assert_eq!(1, ts.samples.len());
        }
    }
}
