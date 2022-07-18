use std::io::Read;

use arrow_format::{self, ipc::planus::ReadAsRoot};
use datatypes::arrow::{
    datatypes::Schema,
    error::{ArrowError, Result},
    io::ipc::{
        read::{read_dictionary, read_record_batch, Dictionaries, StreamMetadata, StreamState},
        IpcSchema,
    },
};

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

pub struct ArrowStreamReader<R: Read> {
    reader: R,
    metadata: StreamMetadata,
    dictionaries: Dictionaries,
    finished: bool,
    data_buffer: Vec<u8>,
    message_buffer: Vec<u8>,
}

impl<R: Read> ArrowStreamReader<R> {
    pub fn new(reader: R, metadata: StreamMetadata) -> Self {
        Self {
            reader,
            metadata,
            dictionaries: Default::default(),
            finished: false,
            data_buffer: vec![],
            message_buffer: vec![],
        }
    }

    /// Return the schema of the stream
    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Check if the stream is exactly finished
    pub fn check_exactly_finished(&mut self) -> Result<bool> {
        if self.is_finished() {
            return Ok(false);
        }

        let _ = self.maybe_next(&[])?;

        Ok(self.is_finished())
    }

    pub fn maybe_next(&mut self, null_mask: &[u8]) -> Result<Option<StreamState>> {
        if self.finished {
            return Ok(None);
        }

        let batch = if null_mask.is_empty() {
            read_next(
                &mut self.reader,
                &self.metadata,
                &mut self.dictionaries,
                &mut self.message_buffer,
                &mut self.data_buffer,
            )?
        } else {
            read_next(
                &mut self.reader,
                &valid_metadata(&self.metadata, null_mask),
                &mut self.dictionaries,
                &mut self.message_buffer,
                &mut self.data_buffer,
            )?
        };

        if batch.is_none() {
            self.finished = true;
        }

        Ok(batch)
    }
}

fn valid_metadata(metadata: &StreamMetadata, null_mask: &[u8]) -> StreamMetadata {
    let null_mask = bit_vec::BitVec::from_bytes(null_mask);

    let schema = Schema::from(
        metadata
            .schema
            .fields
            .iter()
            .zip(&null_mask)
            .filter(|(_, mask)| !*mask)
            .map(|(field, _)| field.clone())
            .collect::<Vec<_>>(),
    )
    .with_metadata(metadata.schema.metadata.clone());

    let ipc_schema = IpcSchema {
        fields: metadata
            .ipc_schema
            .fields
            .iter()
            .zip(&null_mask)
            .filter(|(_, mask)| !*mask)
            .map(|(ipc_field, _)| ipc_field.clone())
            .collect::<Vec<_>>(),
        is_little_endian: metadata.ipc_schema.is_little_endian,
    };

    StreamMetadata {
        schema,
        version: metadata.version,
        ipc_schema,
    }
}

fn read_next<R: Read>(
    reader: &mut R,
    metadata: &StreamMetadata,
    dictionaries: &mut Dictionaries,
    message_buffer: &mut Vec<u8>,
    data_buffer: &mut Vec<u8>,
) -> Result<Option<StreamState>> {
    // determine metadata length
    let mut meta_length: [u8; 4] = [0; 4];

    match reader.read_exact(&mut meta_length) {
        Ok(()) => (),
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(Some(StreamState::Waiting))
            } else {
                Err(ArrowError::from(e))
            };
        }
    }

    let meta_length = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_length == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_length)?;
        }
        i32::from_le_bytes(meta_length) as usize
    };

    if meta_length == 0 {
        // the stream has ended, mark the reader as finished
        return Ok(None);
    }

    message_buffer.clear();
    message_buffer.resize(meta_length, 0);
    reader.read_exact(message_buffer)?;

    let message = arrow_format::ipc::MessageRef::read_as_root(message_buffer).map_err(|err| {
        ArrowError::OutOfSpec(format!("Unable to get root as message: {:?}", err))
    })?;
    let header = message.header()?.ok_or_else(|| {
        ArrowError::OutOfSpec(
            "IPC: unable to fetch the message header. The file or stream is corrupted.".to_string(),
        )
    })?;

    match header {
        arrow_format::ipc::MessageHeaderRef::Schema(_) => {
            Err(ArrowError::OutOfSpec("A stream ".to_string()))
        }
        arrow_format::ipc::MessageHeaderRef::RecordBatch(batch) => {
            // read the block that makes up the record batch into a buffer
            data_buffer.clear();
            data_buffer.resize(message.body_length()? as usize, 0);
            reader.read_exact(data_buffer)?;

            let mut reader = std::io::Cursor::new(data_buffer);

            read_record_batch(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                None,
                dictionaries,
                metadata.version,
                &mut reader,
                0,
            )
            .map(|x| Some(StreamState::Some(x)))
        }
        arrow_format::ipc::MessageHeaderRef::DictionaryBatch(batch) => {
            // read the block that makes up the dictionary batch into a buffer
            let mut buf = vec![0; message.body_length()? as usize];
            reader.read_exact(&mut buf)?;

            let mut dict_reader = std::io::Cursor::new(buf);

            read_dictionary(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                dictionaries,
                &mut dict_reader,
                0,
            )?;

            // read the next message until we encounter a RecordBatch message
            read_next(reader, metadata, dictionaries, message_buffer, data_buffer)
        }
        t => Err(ArrowError::OutOfSpec(format!(
            "Reading types other than record batches not yet supported, unable to read {:?} ",
            t
        ))),
    }
}
