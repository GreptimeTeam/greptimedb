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

use std::io::Write;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, BytesMut};

#[derive(Clone, Default)]
pub struct SharedBuffer {
    pub buffer: Arc<Mutex<BytesMut>>,
}

impl SharedBuffer {
    pub fn with_capacity(size: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(BytesMut::with_capacity(size))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        let mut buffer = self.buffer.lock().unwrap();
        buffer.put_slice(buf);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // This flush implementation is intentionally left to blank.
        // The actual flush is in `BufferedWriter::try_flush`
        Ok(())
    }
}
