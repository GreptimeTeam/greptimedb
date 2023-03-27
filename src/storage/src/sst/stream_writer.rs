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

use bytes::{BufMut, BytesMut};
use common_base::buffer::BufferMut;
use crossbeam::channel::Sender;
use object_store::Writer;

use crate::error;

struct StreamWriter {
    buf: BytesMut,
    tx: Option<Sender<BytesMut>>,
    finish: Option<tokio::sync::oneshot::Receiver<()>>,
    threshold: usize,
}

impl std::io::Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write_from_slice(buf).unwrap();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        while self.buf.len() >= self.threshold {
            let full = self.buf.split_to(self.threshold);
            self.tx.as_ref().unwrap().send(full).unwrap();
        }
        Ok(())
    }
}

impl StreamWriter {
    pub fn new(mut writer: Writer, threshold: usize) -> Self {
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<()>();

        let (tx, rx) = crossbeam::channel::unbounded();
        common_runtime::spawn_write(async move {
            while let Ok(buf) = rx.recv().unwrap() {
                writer.append(buf).await.unwrap();
            }
            finished_tx.send(()).unwrap();
        });

        Self {
            buf: BytesMut::with_capacity(threshold),
            tx: Some(tx),
            finish: Some(finished_rx),
            threshold,
        }
    }

    pub async fn close(mut self) -> error::Result<()> {
        // make sure existing data are written in chunks with size of threshold.
        self.flush().unwrap();
        if let Some(v) = self.finish.take() {
            let all = self.buf.split();
            let tx = self.tx.take().unwrap();
            let len = all.len();
            tx.send(all).unwrap();
            println!("Close, remaining: {}", len);
            drop(tx);
            v.await.unwrap();
        } else {
            panic!("Cannot close twice")
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::ObjectStore;

    use super::*;

    #[tokio::test]
    async fn test_append() {
        let mut builder = Fs::default();
        builder.root("/Users/lei/test_data");
        let store = ObjectStore::new(builder).unwrap().finish();
        let mut writer = store.writer("abcd").await.unwrap();

        let mut stream_writer = StreamWriter::new(writer, 4);
        stream_writer.write("ab".as_bytes()).unwrap();
        stream_writer.flush().unwrap();
        println!("write ab");
        stream_writer.write("cd".as_bytes()).unwrap();
        stream_writer.flush().unwrap();
        println!("write cd");
        stream_writer.write("efg".as_bytes()).unwrap();
        stream_writer.close().await.unwrap();
    }
}
