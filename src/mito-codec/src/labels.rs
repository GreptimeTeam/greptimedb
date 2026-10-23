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
use datatypes::data_type::ConcreteDataType;
use datatypes::series::frame::{LabelDecodeEncoding, LabelDecodeSpec};
use datatypes::series::{Label, SeriesLabels};
use datatypes::value::Value;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use store_api::region_engine::RawLabelSource;
use store_api::storage::{
    ColumnId,
    consts::{ReservedColumnId, is_internal_column},
};

use crate::error::{
    InvalidDensePrimaryKeySnafu, InvalidLabelDecodeDescriptionSnafu, InvalidSparsePrimaryKeySnafu,
    Result, UnsupportedOperationSnafu,
};
use crate::row_converter::SortField;
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
    Complete(SeriesLabels),
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

/// One ordered key column of the minimal decode layout.
struct KeyColumn {
    id: ColumnId,
    name: String,
    data_type: ConcreteDataType,
}

/// Minimal primary-key layout labels can be decoded from.
///
/// It holds only what decoding reads: the metric name, the primary-key encoding, and the ordered
/// primary-key columns with their physical types. A scan-bound [`RawLabelSource::PrimaryKey`] and a
/// frame schema's [`LabelDecodeSpec`] both reduce to this view, so both bind the same decoder.
/// Every key column is a `Tag` column, which is checked when a decoder is bound, so the layout
/// carries no per-column role.
struct KeyLayout {
    metric_name: Arc<str>,
    encoding: PrimaryKeyEncoding,
    columns: Vec<KeyColumn>,
}

impl KeyLayout {
    /// Finds a listed primary-key column.
    fn column(&self, id: ColumnId) -> Option<&KeyColumn> {
        self.columns.iter().find(|column| column.id == id)
    }

    /// Returns the position of a listed primary-key column.
    fn position(&self, id: ColumnId) -> Option<usize> {
        self.columns.iter().position(|column| column.id == id)
    }
}

/// Decodes labels on demand using the minimal primary-key layout bound to a raw-label source.
pub struct PrimaryKeyLabelDecoder {
    key: KeyLayout,
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
    ///
    /// Every primary-key column must be a `Tag` column; [`RegionMetadata::validate`] enforces it, so
    /// this only rejects a snapshot built without validation.
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
        if let Some(column) = metadata
            .primary_key_columns()
            .find(|column| column.semantic_type != SemanticType::Tag)
        {
            return InvalidSparsePrimaryKeySnafu {
                reason: format!(
                    "primary-key column {} is not a tag column",
                    column.column_schema.name
                ),
            }
            .fail();
        }
        let key = KeyLayout {
            metric_name: metric_name.clone(),
            encoding: metadata.primary_key_encoding,
            columns: metadata
                .primary_key_columns()
                .map(|column| KeyColumn {
                    id: column.column_id,
                    name: column.column_schema.name.clone(),
                    data_type: column.column_schema.data_type.clone(),
                })
                .collect(),
        };
        Self::bind(
            key,
            logical_tags
                .iter()
                .map(|tag| (tag.column_id, tag.name.as_ref())),
            projection,
        )
    }

    /// Binds a decoder to a frame schema's label decode description and projection.
    ///
    /// This is the decoding path for a consumer that received only the raw frame schema: the
    /// description carries the metric name, the primary-key encoding, the ordered key columns with
    /// their physical types, and the logical tags. The description is validated first, so a version
    /// or structure this revision cannot honour is an error instead of a best-effort decode.
    ///
    /// The description lists exactly the encoded key fields, and region metadata guarantees each
    /// of them is a tag column, so no per-column role is carried or re-checked; an array position
    /// is an encoding position. The logical tags are still checked here, one by one, exactly as
    /// for a typed binding, which is the only place the decoder inspects them (a listed column no
    /// tag selects is not examined).
    pub fn from_decode_spec(spec: &LabelDecodeSpec, projection: LabelProjection) -> Result<Self> {
        spec.validate()
            .context(InvalidLabelDecodeDescriptionSnafu)?;
        let key = KeyLayout {
            metric_name: Arc::from(spec.metric_name.as_str()),
            encoding: match spec.encoding {
                LabelDecodeEncoding::Dense => PrimaryKeyEncoding::Dense,
                LabelDecodeEncoding::Sparse => PrimaryKeyEncoding::Sparse,
            },
            columns: spec
                .primary_key
                .iter()
                .map(|column| KeyColumn {
                    id: column.column_id,
                    name: column.name.clone(),
                    data_type: column.physical_type.clone(),
                })
                .collect(),
        };
        Self::bind(
            key,
            spec.logical_tags
                .iter()
                .map(|tag| (tag.column_id, tag.name.as_str())),
            projection,
        )
    }

    /// Binds the ordered logical tags of `key` to one projection and one decode layout.
    fn bind<'a, I>(key: KeyLayout, tags: I, projection: LabelProjection) -> Result<Self>
    where
        I: IntoIterator<Item = (ColumnId, &'a str)>,
    {
        let mut labels = Vec::new();
        let mut names = HashSet::new();
        let mut ids = HashSet::new();
        let mut logical_tag_count = 0;
        for (column_id, name) in tags {
            logical_tag_count += 1;
            if !names.insert(name)
                || !ids.insert(column_id)
                || name == "__name__"
                || is_internal_column(name)
                || is_metric_engine_internal_column(name)
                || ReservedColumnId::is_reserved(column_id)
                || !key
                    .column(column_id)
                    .is_some_and(|column| column.name == name)
            {
                return InvalidSparsePrimaryKeySnafu {
                    reason: format!("invalid logical tag binding {name}"),
                }
                .fail();
            }
            let position = key
                .position(column_id)
                .context(InvalidSparsePrimaryKeySnafu {
                    reason: format!("logical tag {name} is not a primary-key column"),
                })?;
            labels.push(BoundLabel {
                id: column_id,
                name: Arc::from(name),
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
        let layout = match key.encoding {
            PrimaryKeyEncoding::Sparse => Layout::Sparse(SparseOffsetsCache::new()),
            PrimaryKeyEncoding::Dense => Layout::Dense {
                codec: DensePrimaryKeyCodec::with_fields(
                    key.columns
                        .iter()
                        .map(|column| (column.id, SortField::new(column.data_type.clone())))
                        .collect(),
                ),
                offsets: Vec::new(),
            },
        };
        let encoding = match key.encoding {
            PrimaryKeyEncoding::Sparse => "sparse",
            PrimaryKeyEncoding::Dense => "dense",
        };
        let projection_mode = if complete { "all" } else { "only" };
        debug!(
            encoding,
            projection_mode,
            logical_tag_count,
            selected_tag_count = labels.len(),
            "Constructed primary key label decoder"
        );
        Ok(Self {
            key,
            complete,
            include_name,
            labels,
            value_scratch: Vec::new(),
            layout,
        })
    }

    /// Decodes labels without modifying or consuming `read_key`.
    ///
    /// `All` returns a complete [`SeriesLabels`]; `Only` returns a partial vector. Dense
    /// keys are fully checked against the exact snapshot layout even when decoding only
    /// selected fields. Sparse keys receive a full structural audit before values are read.
    pub fn decode(&mut self, read_key: &[u8]) -> Result<DecodedLabels> {
        let mut values = Vec::with_capacity(self.labels.len() + usize::from(self.include_name));
        let Self {
            layout,
            key,
            labels,
            complete,
            value_scratch,
            ..
        } = self;
        match layout {
            Layout::Sparse(cache) => {
                let mut view = SparsePrimaryKeyView::new(read_key, cache)?;
                view.visit_label_ids(|id| {
                    if *complete && key.column(id).is_none() {
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
                SeriesLabels::try_new(Some(key.metric_name.clone()), values).map_err(|error| {
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
                    value: self.key.metric_name.clone(),
                });
                values.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            }
            Ok(DecodedLabels::Projected(values))
        }
    }
}

#[cfg(test)]
mod tests;
