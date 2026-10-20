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

use std::borrow::Borrow;
use std::marker::PhantomData;

use api::v1::region::{RegionRequest, RegionResponse, region_request};
use prost::Message;
use tonic::Status;
use tonic::codec::{Codec, EncodeBuf, Encoder};
use tonic_prost::ProstDecoder;

/// Encodes owned or shared region requests using the existing protobuf wire format.
pub(crate) struct RegionCodec<R>(PhantomData<R>);

impl<R> Default for RegionCodec<R> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<R> Codec for RegionCodec<R>
where
    R: Borrow<RegionRequest> + Send + 'static,
{
    type Encode = R;
    type Decode = RegionResponse;
    type Encoder = RegionEncoder<R>;
    type Decoder = ProstDecoder<RegionResponse>;

    fn encoder(&mut self) -> Self::Encoder {
        RegionEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder::default()
    }
}

/// Borrows the protobuf payload while consuming its owned or shared container.
pub(crate) struct RegionEncoder<R>(PhantomData<R>);

impl<R: Borrow<RegionRequest>> Encoder for RegionEncoder<R> {
    type Item = R;
    type Error = Status;

    fn encode(&mut self, item: R, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        let request = item.borrow();
        if matches!(
            request.body,
            Some(region_request::Body::Inserts(_) | region_request::Body::Deletes(_))
        ) {
            // Only the header is copied; retries keep borrowing the shared rows.
            let mut header = request.header.clone().unwrap_or_default();
            api::region::set_accept_partial_result(&mut header);
            RegionRequest {
                header: Some(header),
                body: None,
            }
            .encode(&mut *buf)
            .map_err(|error| Status::internal(error.to_string()))?;
            if let Some(body) = &request.body {
                body.encode(buf);
            }
            Ok(())
        } else {
            request
                .encode(buf)
                .map_err(|error| Status::internal(error.to_string()))
        }
    }
}
