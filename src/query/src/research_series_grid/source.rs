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

//! Read-only research format shared with the isolated Go series-layout experiment.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

use datafusion_common::{DataFusionError, Result};
use serde::Deserialize;

use crate::research_series_grid::decoded_block::{BlockCache, DecodedBlock};
use crate::research_series_grid::vm_codec;

const BLOCK_SAMPLES: usize = 8192;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct SnapshotIndex {
    pub version: u32,
    #[serde(rename = "DatasetID")]
    pub dataset_id: String,
    pub samples: u64,
    pub series: Vec<Series>,
    #[serde(skip)]
    cache: Option<Arc<Mutex<BlockCache>>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Series {
    pub labels: Vec<Label>,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Label {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Block {
    offset: u64,
    length: usize,
    count: usize,
    min: i64,
    max: i64,
    #[serde(rename = "CRC")]
    crc: u32,
}

fn invalid(reason: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(format!("Research series snapshot: {}", reason.into()))
}

impl SnapshotIndex {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let mut index: Self = serde_json::from_reader(File::open(path.join("index.json"))?)
            .map_err(|e| invalid(e.to_string()))?;
        if !matches!(index.version, 1 | 2) || index.dataset_id.is_empty() {
            return Err(invalid("expected identified format-1 or format-2 snapshot"));
        }
        let size = std::fs::metadata(path.join("blocks.bin"))?.len();
        let mut samples = 0_u64;
        for series in &index.series {
            if !series
                .labels
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
            {
                return Err(invalid("labels must be unique and sorted"));
            }
            let mut previous = None;
            for block in &series.blocks {
                if block.count == 0
                    || block.count > BLOCK_SAMPLES
                    || block.length == 0
                    || block.length
                        > if index.version == 1 {
                            snap::raw::max_compress_len(BLOCK_SAMPLES * 16)
                        } else {
                            22 + 20 * BLOCK_SAMPLES
                        }
                    || block
                        .offset
                        .checked_add(block.length as u64)
                        .is_none_or(|end| end > size)
                    || block.min > block.max
                    || previous.is_some_and(|max| block.min <= max)
                {
                    return Err(invalid("invalid block descriptor"));
                }
                previous = Some(block.max);
                samples = samples
                    .checked_add(block.count as u64)
                    .ok_or_else(|| invalid("sample count overflow"))?;
            }
        }
        if samples != index.samples {
            return Err(invalid("sample count mismatch"));
        }
        let capacity = match std::env::var("GT_RESEARCH_DECODED_CACHE_BYTES") {
            Ok(value) => value
                .parse::<usize>()
                .map_err(|_| invalid("invalid decoded cache capacity"))?,
            Err(std::env::VarError::NotPresent) => 0,
            Err(error) => return Err(invalid(error.to_string())),
        };
        if capacity > 0 {
            index.cache = Some(Arc::new(Mutex::new(BlockCache::new(capacity))));
        }
        Ok(index)
    }
}

/// One reader owns its file cursor and scratch buffers; it must not be shared by workers.
pub(crate) struct SeriesReader {
    file: File,
    encoded: Vec<u8>,
    raw: Vec<u8>,
    vm_decoder: Option<vm_codec::Decoder>,
    cache: Option<Arc<Mutex<BlockCache>>>,
    pub read_bytes: usize,
    pub decoded_samples: usize,
}

impl SeriesReader {
    pub(crate) fn open(path: &Path, version: u32) -> Result<Self> {
        let vm_decoder = match version {
            1 => None,
            2 => Some(vm_codec::Decoder::new().map_err(invalid)?),
            _ => return Err(invalid("unsupported snapshot version")),
        };
        Ok(Self {
            file: File::open(path.join("blocks.bin"))?,
            encoded: Vec::new(),
            raw: Vec::new(),
            vm_decoder,
            cache: None,
            read_bytes: 0,
            decoded_samples: 0,
        })
    }

    pub(crate) fn for_snapshot(path: &Path, source: &SnapshotIndex) -> Result<Self> {
        let mut reader = Self::open(path, source.version)?;
        reader.cache = source.cache.clone();
        Ok(reader)
    }

    pub(crate) fn read(
        &mut self,
        series: &Series,
        start: i64,
        end: i64,
    ) -> Result<(Vec<i64>, Vec<f64>)> {
        if self.cache.is_none() {
            return self.read_uncached(series, start, end);
        }
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for block in series
            .blocks
            .iter()
            .filter(|block| block.max >= start && block.min <= end)
        {
            let decoded = self.decode_block(block)?;
            for (timestamp, bits) in decoded.samples(start, end) {
                timestamps.push(timestamp);
                values.push(f64::from_bits(bits));
            }
        }
        Ok((timestamps, values))
    }

    pub(crate) fn visit_samples(
        &mut self,
        series: &Series,
        start: i64,
        end: i64,
        mut visit: impl FnMut(i64, f64) -> Result<()>,
    ) -> Result<()> {
        for block in series
            .blocks
            .iter()
            .filter(|block| block.max >= start && block.min <= end)
        {
            let decoded = self.decode_block(block)?;
            for (timestamp, bits) in decoded.samples(start, end) {
                visit(timestamp, f64::from_bits(bits))?;
            }
        }
        Ok(())
    }

    fn decode_block(&mut self, block: &Block) -> Result<Arc<DecodedBlock>> {
        let key = [
            block.offset,
            block.length as u64,
            block.count as u64,
            block.min as u64,
            block.max as u64,
            u64::from(block.crc),
        ];
        if let Some(cache) = &self.cache
            && let Some(decoded) = cache
                .lock()
                .map_err(|_| invalid("decoded cache mutex poisoned"))?
                .get(key)
        {
            return Ok(decoded);
        }
        // Reuse the complete checksum, timestamp-order and descriptor validation.
        let single = Series {
            labels: Vec::new(),
            blocks: vec![block.clone()],
        };
        let (times, samples) = self.read_uncached(&single, block.min, block.max)?;
        let decoded = Arc::new(
            DecodedBlock::new(times, samples.into_iter().map(f64::to_bits).collect())
                .map_err(invalid)?,
        );
        if let Some(cache) = &self.cache {
            cache
                .lock()
                .map_err(|_| invalid("decoded cache mutex poisoned"))?
                .insert(key, Arc::clone(&decoded));
        }
        Ok(decoded)
    }

    fn read_uncached(
        &mut self,
        series: &Series,
        start: i64,
        end: i64,
    ) -> Result<(Vec<i64>, Vec<f64>)> {
        let first = series.blocks.partition_point(|block| block.max < start);
        let capacity = series.blocks[first..]
            .iter()
            .take_while(|block| block.min <= end)
            .try_fold(0_usize, |sum, block| sum.checked_add(block.count))
            .ok_or_else(|| invalid("series sample count overflow"))?;
        let mut timestamps = Vec::with_capacity(capacity);
        let mut values = Vec::with_capacity(capacity);
        for block in series.blocks[first..]
            .iter()
            .take_while(|block| block.min <= end)
        {
            self.encoded.resize(block.length, 0);
            self.file.seek(SeekFrom::Start(block.offset))?;
            self.file.read_exact(&mut self.encoded)?;
            self.read_bytes += block.length;
            if let Some(decoder) = &mut self.vm_decoder {
                decoder
                    .decode(&self.encoded, block.count)
                    .map_err(invalid)?;
                // Format 2 retains the original interleaved-sample checksum.
                // Keep its verification cost in this codec-only comparison.
                self.raw.resize(block.count * 16, 0);
                for ((sample, timestamp), bits) in self
                    .raw
                    .chunks_exact_mut(16)
                    .zip(&decoder.timestamps)
                    .zip(&decoder.value_bits)
                {
                    sample[..8].copy_from_slice(&timestamp.to_le_bytes());
                    sample[8..].copy_from_slice(&bits.to_le_bytes());
                }
                if crc32fast::hash(&self.raw) != block.crc {
                    return Err(invalid("block checksum mismatch"));
                }
                self.decoded_samples += block.count;
                append_samples(
                    decoder
                        .timestamps
                        .iter()
                        .copied()
                        .zip(decoder.value_bits.iter().map(|bits| *bits as u64)),
                    block,
                    start,
                    end,
                    &mut timestamps,
                    &mut values,
                )?;
                continue;
            }
            let size =
                snap::raw::decompress_len(&self.encoded).map_err(|e| invalid(e.to_string()))?;
            if size != block.count * 16 {
                return Err(invalid("decoded size mismatch"));
            }
            self.raw.resize(size, 0);
            snap::raw::Decoder::new()
                .decompress(&self.encoded, &mut self.raw)
                .map_err(|e| invalid(e.to_string()))?;
            if crc32fast::hash(&self.raw) != block.crc {
                return Err(invalid("block checksum mismatch"));
            }
            self.decoded_samples += block.count;
            let samples = self.raw.chunks_exact(16).map(|sample| {
                let mut bytes = [0; 8];
                bytes.copy_from_slice(&sample[..8]);
                let timestamp = i64::from_le_bytes(bytes);
                bytes.copy_from_slice(&sample[8..]);
                (timestamp, u64::from_le_bytes(bytes))
            });
            append_samples(samples, block, start, end, &mut timestamps, &mut values)?;
        }
        Ok((timestamps, values))
    }
}

fn append_samples(
    samples: impl Iterator<Item = (i64, u64)>,
    block: &Block,
    start: i64,
    end: i64,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> Result<()> {
    let mut previous = None;
    for (i, (timestamp, bits)) in samples.enumerate() {
        if previous.is_some_and(|prev| timestamp <= prev)
            || (i == 0 && timestamp != block.min)
            || (i + 1 == block.count && timestamp != block.max)
        {
            return Err(invalid("invalid decoded timestamps"));
        }
        previous = Some(timestamp);
        if timestamp >= start && timestamp <= end {
            timestamps.push(timestamp);
            values.push(f64::from_bits(bits));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vm_blocks_preserve_filtering_and_reject_checksum_mismatch() {
        let dir = std::env::temp_dir().join(format!("research-vm-series-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let mut encoded = vec![2, 3];
        encoded.extend_from_slice(&10_i64.to_le_bytes());
        encoded.extend_from_slice(&(-0.0_f64).to_bits().to_le_bytes());
        encoded.extend_from_slice(&1_u32.to_le_bytes());
        encoded.push(2);
        let mut raw = Vec::new();
        for timestamp in [10_i64, 11, 12] {
            raw.extend_from_slice(&timestamp.to_le_bytes());
            raw.extend_from_slice(&(-0.0_f64).to_bits().to_le_bytes());
        }
        std::fs::write(dir.join("blocks.bin"), &encoded).unwrap();
        let mut series = Series {
            labels: Vec::new(),
            blocks: vec![Block {
                offset: 0,
                length: encoded.len(),
                count: 3,
                min: 10,
                max: 12,
                crc: crc32fast::hash(&raw),
            }],
        };
        let mut reader = SeriesReader::open(&dir, 2).unwrap();
        let (times, values) = reader.read(&series, 11, 11).unwrap();
        assert_eq!(times, [11]);
        assert_eq!(values[0].to_bits(), (-0.0_f64).to_bits());
        series.blocks[0].crc ^= 1;
        assert!(reader.read(&series, 10, 12).is_err());
        drop(reader);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preserves_bits_and_rejects_corruption() {
        let dir = std::env::temp_dir().join(format!("research-series-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.as_path();
        let bits = [(-0.0_f64).to_bits(), 0x7ff0000000000002, 1];
        let mut raw = Vec::new();
        for (timestamp, value) in [-10_i64, 0, 10].into_iter().zip(bits) {
            raw.extend_from_slice(&timestamp.to_le_bytes());
            raw.extend_from_slice(&value.to_le_bytes());
        }
        let encoded = snap::raw::Encoder::new().compress_vec(&raw).unwrap();
        let index = serde_json::json!({
            "Version": 1, "DatasetID": "fixture", "Samples": 3,
            "Series": [{"Labels": [{"name": "__name__", "value": "metric"}],
                "Blocks": [{"Offset": 0, "Length": encoded.len(), "Count": 3,
                    "Min": -10, "Max": 10, "CRC": crc32fast::hash(&raw)}]}]
        });
        std::fs::write(path.join("index.json"), serde_json::to_vec(&index).unwrap()).unwrap();
        std::fs::write(path.join("blocks.bin"), &encoded).unwrap();
        let snapshot = SnapshotIndex::open(path).unwrap();
        let mut reader = SeriesReader::open(path, snapshot.version).unwrap();
        let (times, values) = reader.read(&snapshot.series[0], -10, 10).unwrap();
        assert_eq!(times, [-10, 0, 10]);
        assert_eq!(values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(), bits);
        let mut cached = SeriesReader::open(path, snapshot.version).unwrap();
        cached.cache = Some(Arc::new(Mutex::new(BlockCache::new(4096))));
        cached.read(&snapshot.series[0], -10, 10).unwrap();
        assert_eq!(cached.read_bytes, encoded.len());
        let (selected_times, selected_values) = cached.read(&snapshot.series[0], 0, 0).unwrap();
        assert_eq!(selected_times, [0]);
        assert_eq!(selected_values[0].to_bits(), bits[1]);
        assert_eq!(cached.read_bytes, encoded.len());
        for input in [&mut reader, &mut cached] {
            let mut visited = Vec::new();
            input
                .visit_samples(&snapshot.series[0], -10, 0, |t, v| {
                    visited.push((t, v.to_bits()));
                    Ok(())
                })
                .unwrap();
            assert_eq!(visited, [(-10, bits[0]), (0, bits[1])]);
            let error = input
                .visit_samples(&snapshot.series[0], -10, 10, |_, _| {
                    Err(invalid("visitor stopped"))
                })
                .unwrap_err();
            assert!(error.to_string().contains("visitor stopped"));
        }
        assert_eq!(cached.read_bytes, encoded.len());
        let mut changed = snapshot.series[0].blocks[0].clone();
        changed.crc ^= 1;
        assert!(
            cached
                .read(
                    &Series {
                        labels: Vec::new(),
                        blocks: vec![changed]
                    },
                    -10,
                    10
                )
                .is_err()
        );
        let (times, values) = reader.read(&snapshot.series[0], 0, 0).unwrap();
        assert_eq!(times, [0]);
        assert_eq!(values[0].to_bits(), bits[1]);
        assert!(
            reader
                .read(&snapshot.series[0], 11, 12)
                .unwrap()
                .0
                .is_empty()
        );
        std::fs::write(path.join("blocks.bin"), vec![0; encoded.len()]).unwrap();
        assert!(reader.read(&snapshot.series[0], -10, 10).is_err());
        assert!(
            reader
                .visit_samples(&snapshot.series[0], -10, 10, |_, _| Ok(()))
                .is_err()
        );
        std::fs::write(path.join("blocks.bin"), []).unwrap();
        assert!(SnapshotIndex::open(path).is_err());
        drop(reader);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[ignore = "Reads the full external research dataset"]
    fn verify_imported_snapshot() {
        let path = std::env::var("GT_RESEARCH_SERIES_SNAPSHOT").expect("snapshot path");
        let path = Path::new(&path);
        let index = SnapshotIndex::open(path).unwrap();
        let mut reader = SeriesReader::open(path, index.version).unwrap();
        let mut count = 0_u64;
        for series in &index.series {
            let (timestamps, values) = reader.read(series, i64::MIN, i64::MAX).unwrap();
            assert_eq!(timestamps.len(), values.len());
            assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
            count += timestamps.len() as u64;
        }
        assert_eq!(count, index.samples);
    }
}
