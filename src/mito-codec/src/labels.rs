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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use common_telemetry::debug;
use datatypes::series::{Label, MetricName};
use datatypes::value::Value;
use snafu::OptionExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use store_api::region_engine::RawLabelSource;
use store_api::storage::{
    ColumnId,
    consts::{ReservedColumnId, is_internal_column},
};

use crate::error::{
    InvalidDensePrimaryKeySnafu, InvalidSparsePrimaryKeySnafu, Result, UnsupportedOperationSnafu,
};
use crate::row_converter::dense::DensePrimaryKeyCodec;
use crate::row_converter::sparse::{SparseOffsetsCache, SparsePrimaryKeyView};

/// Selects labels materialized from a primary key.
///
/// `All` produces a complete metric name and label set. `Only` produces a projected
/// label vector, including `__name__` only when explicitly requested. Unexplained sparse
/// column IDs are errors with `All`; `Only` skips them after checking their framing.
#[derive(Clone, Debug, Default)]
pub enum LabelProjection {
    #[default]
    All,
    Only(Arc<[Arc<str>]>),
}

/// Decoded labels: either a complete metric name or a possibly partial label vector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedLabels {
    Complete(MetricName),
    Projected(Vec<Label>),
}

enum Layout {
    Sparse(SparseOffsetsCache),
    Dense {
        codec: DensePrimaryKeyCodec,
        offsets: Vec<usize>,
    },
}

struct BoundLabel {
    id: ColumnId,
    name: Arc<str>,
    position: usize,
}

/// Decodes labels on demand using the metadata snapshot bound to a raw-label source.
pub struct PrimaryKeyLabelDecoder {
    metric_name: Arc<str>,
    physical_metadata: RegionMetadataRef,
    complete: bool,
    include_name: bool,
    labels: Vec<BoundLabel>,
    value_scratch: Vec<u8>,
    layout: Layout,
}

impl PrimaryKeyLabelDecoder {
    /// Binds a decoder to a primary-key source snapshot and projection.
    ///
    /// Explicit sources already contain complete labels and are rejected. Dense decoding
    /// requires the full key layout described by the bound snapshot; compatibility with
    /// older key layouts must be normalized upstream.
    pub fn new(source: &RawLabelSource, projection: LabelProjection) -> Result<Self> {
        let RawLabelSource::PrimaryKey {
            metric_name,
            physical_metadata,
            logical_tags,
        } = source
        else {
            return UnsupportedOperationSnafu {
                err_msg: "explicit raw labels are already complete",
            }
            .fail();
        };
        let metadata = physical_metadata.as_ref();
        let mut labels = Vec::with_capacity(logical_tags.len());
        let mut names = HashSet::new();
        let mut ids = HashSet::new();
        for tag in logical_tags.iter() {
            let column = metadata.column_by_id(tag.column_id);
            if !names.insert(tag.name.clone())
                || !ids.insert(tag.column_id)
                || tag.name.as_ref() == "__name__"
                || is_internal_column(&tag.name)
                || is_metric_engine_internal_column(&tag.name)
                || ReservedColumnId::is_reserved(tag.column_id)
                || !column.is_some_and(|column| {
                    column.semantic_type == SemanticType::Tag
                        && column.column_schema.name.as_str() == tag.name.as_ref()
                        && metadata.primary_key.contains(&tag.column_id)
                })
            {
                return InvalidSparsePrimaryKeySnafu {
                    reason: format!("invalid logical tag binding {}", tag.name),
                }
                .fail();
            }
            let position = metadata.primary_key_index(tag.column_id).context(
                InvalidSparsePrimaryKeySnafu {
                    reason: format!("logical tag {} is not a primary-key column", tag.name),
                },
            )?;
            labels.push(BoundLabel {
                id: tag.column_id,
                name: tag.name.clone(),
                position,
            });
        }
        labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let complete = matches!(&projection, LabelProjection::All);
        let include_name = match &projection {
            LabelProjection::All => false,
            LabelProjection::Only(requested) => {
                requested.iter().any(|name| name.as_ref() == "__name__")
            }
        };
        if let LabelProjection::Only(requested) = &projection {
            let requested: HashSet<_> = requested.iter().map(AsRef::as_ref).collect();
            labels.retain(|label| requested.contains(label.name.as_ref()));
        }
        let layout = match metadata.primary_key_encoding {
            PrimaryKeyEncoding::Sparse => Layout::Sparse(SparseOffsetsCache::new()),
            PrimaryKeyEncoding::Dense => Layout::Dense {
                codec: DensePrimaryKeyCodec::new(metadata),
                offsets: Vec::new(),
            },
        };
        let encoding = match metadata.primary_key_encoding {
            PrimaryKeyEncoding::Sparse => "sparse",
            PrimaryKeyEncoding::Dense => "dense",
        };
        let projection_mode = if complete { "all" } else { "only" };
        debug!(
            encoding,
            projection_mode,
            logical_tag_count = logical_tags.len(),
            selected_tag_count = labels.len(),
            "Constructed primary key label decoder"
        );
        Ok(Self {
            metric_name: metric_name.clone(),
            physical_metadata: physical_metadata.clone(),
            complete,
            include_name,
            labels,
            value_scratch: Vec::new(),
            layout,
        })
    }

    /// Decodes labels without modifying or consuming `read_key`.
    ///
    /// `All` returns a complete [`MetricName`]; `Only` returns a partial vector. Dense
    /// keys are fully checked against the exact snapshot layout even when decoding only
    /// selected fields. Sparse keys receive a full structural audit before values are read.
    pub fn decode(&mut self, read_key: &[u8]) -> Result<DecodedLabels> {
        let mut values = Vec::with_capacity(self.labels.len() + usize::from(self.include_name));
        let Self {
            layout,
            labels,
            physical_metadata,
            complete,
            value_scratch,
            ..
        } = self;
        match layout {
            Layout::Sparse(cache) => {
                let mut view = SparsePrimaryKeyView::new(read_key, cache)?;
                view.visit_label_ids(|id| {
                    if *complete
                        && !physical_metadata.column_by_id(id).is_some_and(|column| {
                            column.semantic_type == SemanticType::Tag
                                && physical_metadata.primary_key.contains(&id)
                        })
                    {
                        return InvalidSparsePrimaryKeySnafu {
                            reason: format!("unrecognized sparse tag id {id}"),
                        }
                        .fail();
                    }
                    Ok(())
                })?;
                for label in labels.iter() {
                    if let Some(value) = view.label(label.id, value_scratch)? {
                        values.push(Label {
                            name: label.name.clone(),
                            value: Arc::from(value),
                        });
                    }
                }
            }
            Layout::Dense { codec, offsets } => {
                offsets.clear();
                let mut total = 0;
                for pos in 0..codec.num_fields() {
                    let encoded = codec.encoded_value_at(read_key, pos, offsets)?;
                    total += encoded.len();
                }
                if total != read_key.len() {
                    return InvalidDensePrimaryKeySnafu {
                        reason: "trailing bytes after dense primary key",
                    }
                    .fail();
                }
                for label in labels.iter() {
                    let value = codec.decode_value_at(read_key, label.position, offsets)?;
                    if !matches!(value, Value::Null) {
                        values.push(Label {
                            name: label.name.clone(),
                            value: Arc::from(value.to_string()),
                        });
                    }
                }
            }
        }
        if *complete {
            Ok(DecodedLabels::Complete(
                MetricName::try_new(Some(self.metric_name.clone()), values).map_err(|error| {
                    InvalidSparsePrimaryKeySnafu {
                        reason: error.to_string(),
                    }
                    .build()
                })?,
            ))
        } else {
            if self.include_name {
                values.push(Label {
                    name: Arc::from("__name__"),
                    value: self.metric_name.clone(),
                });
                values.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            }
            Ok(DecodedLabels::Projected(values))
        }
    }
}

#[cfg(test)]
mod tests;
