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

//! Streams logical column metadata into the target region during repartition.

use std::collections::HashMap;

use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use futures_util::{Stream, TryStreamExt, future, pin_mut, stream};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metadata_region::MetadataRegion;
use crate::utils;

// The row limit bounds per-row protobuf overhead. The byte limit counts key/value payload.
const COPY_BATCH_ROWS: usize = 8 * 1024;
const COPY_BATCH_BYTES: ReadableSize = ReadableSize::mb(4);

impl MetadataRegion {
    /// Updates logical region metadata so that any entries previously referencing
    /// `source_region_id` are modified to reference the data region of `physical_region_id`.
    ///
    /// This method should be called after copying files from `source_region_id`
    /// into the target region. It scans the metadata for the target physical
    /// region, finds logical regions with the same region number as the source,
    /// and reinserts region and column entries updated to use the target's
    /// region number.
    pub async fn transform_logical_region_metadata(
        &self,
        physical_region_id: RegionId,
        source_region_id: RegionId,
    ) -> Result<()> {
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let target_by_source = self
            .logical_regions(data_region_id)
            .await?
            .into_iter()
            .filter(|r| r.region_number() == source_region_id.region_number())
            .map(|id| {
                (
                    id.as_u64().to_string(),
                    RegionId::new(id.table_id(), data_region_id.region_number()),
                )
            })
            .collect::<HashMap<_, _>>();
        if target_by_source.is_empty() {
            info!(
                "No logical regions found from source region {}, physical region id: {}",
                source_region_id, physical_region_id,
            );
            return Ok(());
        }

        // The copied SSTs may contain entries for other regions or columns without a
        // live region marker. Only copy columns belonging to the selected markers.
        let mut decoded_name = Vec::new();
        let columns =
            self.scan_entries(metadata_region_id)
                .await?
                .try_filter_map(|(key, value)| {
                    future::ready(
                        remap_column_key(&key, &target_by_source, &mut decoded_name)
                            .map(|key| key.map(|key| (key, value))),
                    )
                });
        self.copy_entries(metadata_region_id, columns).await?;

        // Publish region markers only after all column batches have been written.
        let markers = target_by_source
            .values()
            .map(|id| Ok((Self::concat_region_key(*id), String::new())));
        self.copy_entries(metadata_region_id, stream::iter(markers))
            .await?;
        info!(
            "Transformed {} logical regions metadata to physical region {}, source region: {}",
            target_by_source.len(),
            data_region_id,
            source_region_id
        );
        Ok(())
    }

    /// Copies entries into WAL-backed puts, waiting for each write before reading further.
    /// An entry larger than the payload budget is written on its own.
    async fn copy_entries(
        &self,
        target: RegionId,
        entries: impl Stream<Item = Result<(String, String)>>,
    ) -> Result<()> {
        pin_mut!(entries);
        let mut batch = Vec::new();
        let mut bytes = 0;
        while let Some((key, value)) = entries.try_next().await? {
            let size = key.len() + value.len();
            if !batch.is_empty()
                && (batch.len() == COPY_BATCH_ROWS
                    || bytes + size > COPY_BATCH_BYTES.as_bytes() as usize)
            {
                self.put_entries(target, std::mem::take(&mut batch)).await?;
                bytes = 0;
            }
            bytes += size;
            batch.push((key, value));
        }
        if !batch.is_empty() {
            self.put_entries(target, batch).await?;
        }
        Ok(())
    }
}

/// Retains the encoded column name and validates it with one reusable scratch buffer.
fn remap_column_key(
    key: &str,
    target_by_source: &HashMap<String, RegionId>,
    decoded_name: &mut Vec<u8>,
) -> Result<Option<String>> {
    let Some((source, encoded)) = MetadataRegion::split_column_key(key) else {
        return Ok(None);
    };
    let Some(&target) = target_by_source.get(source) else {
        return Ok(None);
    };
    *decoded_name =
        MetadataRegion::decode_column_name(source, encoded, std::mem::take(decoded_name))?
            .into_bytes();
    Ok(Some(MetadataRegion::encoded_column_key(target, encoded)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::test_util::TestEnv;
    use crate::utils::to_metadata_region_id;

    #[tokio::test]
    async fn test_copy_entries_retries_after_partial_write() {
        let env = TestEnv::new().await;
        let region = env.default_physical_region_id();
        env.create_physical_region(region, "/copy", vec![]).await;
        let metadata = env.metadata_region();
        let region = to_metadata_region_id(region);

        // The oversized first entry is flushed alone. The second is still buffered
        // when the input fails, so a retry must be able to replay the first entry.
        let entries = [
            (
                "large".to_string(),
                "x".repeat(COPY_BATCH_BYTES.as_bytes() as usize + 1),
            ),
            ("tail".to_string(), "value".to_string()),
        ];
        let failure = crate::error::InvalidRequestSnafu {
            region_id: region,
            reason: "injected read error",
        }
        .build();
        let input = entries.iter().cloned().map(Ok).chain([Err(failure)]);
        assert!(
            metadata
                .copy_entries(region, stream::iter(input))
                .await
                .is_err()
        );
        assert_eq!(
            metadata.get(region, "large").await.unwrap(),
            Some(entries[0].1.clone())
        );
        assert_eq!(metadata.get(region, "tail").await.unwrap(), None);

        metadata
            .copy_entries(region, stream::iter(entries.into_iter().map(Ok)))
            .await
            .unwrap();
        assert_eq!(
            metadata.get(region, "tail").await.unwrap().as_deref(),
            Some("value")
        );
    }

    #[tokio::test]
    async fn test_invalid_source_column_leaves_target_marker_unpublished() {
        let env = TestEnv::new().await;
        let target = env.default_physical_region_id();
        env.create_physical_region(target, "/copy", vec![]).await;
        let metadata = env.metadata_region();
        let region = to_metadata_region_id(target);
        let source = RegionId::new(target.table_id(), 0);
        let logical = RegionId::new(10, source.region_number());
        let target_logical = RegionId::new(logical.table_id(), target.region_number());

        // /w decodes to 0xff, which is not a UTF-8 column name.
        metadata
            .put_entries(
                region,
                vec![
                    (MetadataRegion::concat_region_key(logical), String::new()),
                    (
                        MetadataRegion::encoded_column_key(logical, "/w"),
                        "value".to_string(),
                    ),
                ],
            )
            .await
            .unwrap();

        let err = metadata
            .transform_logical_region_metadata(target, source)
            .await
            .unwrap_err();
        assert_eq!(
            metadata
                .get(region, &MetadataRegion::concat_region_key(target_logical))
                .await
                .unwrap(),
            None
        );
        assert!(
            matches!(err, crate::error::Error::InvalidRequest { region_id, .. } if region_id == logical),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_transform_only_marked_source_regions() {
        let env = TestEnv::new().await;
        let target = env.default_physical_region_id();
        env.create_physical_region(target, "/copy", vec![]).await;
        let metadata = env.metadata_region();
        let region = to_metadata_region_id(target);
        let source = RegionId::new(target.table_id(), 0);
        let id = |table| RegionId::new(table, source.region_number());
        let target_id = |table| RegionId::new(table, target.region_number());
        let marker = MetadataRegion::concat_region_key;
        let column = MetadataRegion::concat_column_key;
        let entries = vec![
            (marker(id(10)), String::new()),
            (column(id(10), "标签"), "old-format value".to_string()),
            (marker(id(11)), String::new()),
            (column(id(12), "orphan"), "value".to_string()),
            (marker(RegionId::new(13, 3)), String::new()),
        ];
        metadata.put_entries(region, entries.clone()).await.unwrap();
        for _ in 0..2 {
            metadata
                .transform_logical_region_metadata(target, source)
                .await
                .unwrap();
        }
        let expected = entries
            .into_iter()
            .chain([
                (marker(target_id(10)), String::new()),
                (
                    column(target_id(10), "标签"),
                    "old-format value".to_string(),
                ),
                (marker(target_id(11)), String::new()),
            ])
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            *metadata.load_all(region).await.unwrap().key_values,
            expected
        );
    }
}
