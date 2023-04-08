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

// modified from https://github.com/tokio-rs/tokio/blob/03912b9cf7141a5e27612a0b283d266637d2088f/tokio-util/src/io/sync_bridge.rs

use std::io::Read;

use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug)]
pub struct SyncIoBridge<T> {
    src: T,
}

impl<T: AsyncRead + Unpin> Read for SyncIoBridge<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let src = &mut self.src;
        futures::executor::block_on(AsyncReadExt::read(src, buf))
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let src = &mut self.src;
        futures::executor::block_on(src.read_to_end(buf))
    }

    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        let src = &mut self.src;
        futures::executor::block_on(src.read_to_string(buf))
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let src = &mut self.src;
        // The AsyncRead trait returns the count, synchronous doesn't.
        let _n = futures::executor::block_on(src.read_exact(buf))?;
        Ok(())
    }
}

impl<T: Unpin> SyncIoBridge<T> {
    pub fn new(src: T) -> Self {
        SyncIoBridge { src }
    }
}
