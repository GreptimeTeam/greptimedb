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

use common_telemetry::debug;
use datatypes::data_type::ConcreteDataType;
use datatypes::series::frame::{LabelDecodeEncoding, LabelDecodeSpec};
use datatypes::series::{Label, SeriesLabels};
use datatypes::value::Value;
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use store_api::storage::{
    ColumnId,
    consts::{ReservedColumnId, is_internal_column},
};

use crate::error::{
    InvalidDensePrimaryKeySnafu, InvalidLabelDecodeDescriptionSnafu, InvalidSparsePrimaryKeySnafu,
    Result,
};
use crate::row_converter::SortField;
use crate::row_converter::dense::DensePrimaryKeyCodec;
use crate::row_converter::sparse::{SparseOffsetsCache, SparsePrimaryKeyView};

enum Layout {
    Sparse(SparseOffsetsCache),
    Dense {
        codec: DensePrimaryKeyCodec,
        offsets: Vec<usize>,
    },
}

fn invalid_binding_error(encoding: PrimaryKeyEncoding, reason: String) -> crate::error::Error {
    match encoding {
        PrimaryKeyEncoding::Dense => InvalidDensePrimaryKeySnafu { reason }.build(),
        PrimaryKeyEncoding::Sparse => InvalidSparsePrimaryKeySnafu { reason }.build(),
    }
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
/// primary-key columns with their physical types. A frame schema's [`LabelDecodeSpec`] reduces
/// to this view, so decoding binds directly to the described layout.
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
    labels: Vec<BoundLabel>,
    value_scratch: Vec<u8>,
    layout: Layout,
}

impl PrimaryKeyLabelDecoder {
    /// Binds a decoder to a frame schema's label decode description.
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
    pub fn from_decode_spec(spec: &LabelDecodeSpec) -> Result<Self> {
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
        )
    }

    /// Binds the ordered logical tags of `key` to one decode layout.
    fn bind<'a, I>(key: KeyLayout, tags: I) -> Result<Self>
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
                return Err(invalid_binding_error(
                    key.encoding,
                    format!("invalid logical tag binding {name}"),
                ));
            }
            let position = key.position(column_id).ok_or_else(|| {
                invalid_binding_error(
                    key.encoding,
                    format!("logical tag {name} is not a primary-key column"),
                )
            })?;
            labels.push(BoundLabel {
                id: column_id,
                name: Arc::from(name),
                position,
            });
        }
        labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));
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
        debug!(
            encoding,
            logical_tag_count,
            selected_tag_count = labels.len(),
            "Constructed primary key label decoder"
        );
        Ok(Self {
            key,
            labels,
            value_scratch: Vec::new(),
            layout,
        })
    }

    /// Decodes labels without modifying or consuming `read_key`.
    ///
    /// Returns complete [`SeriesLabels`]. Dense keys are fully checked against the exact
    /// described layout. Sparse keys receive a full structural audit before values are read.
    pub fn decode(&mut self, read_key: &[u8]) -> Result<SeriesLabels> {
        let mut values = Vec::with_capacity(self.labels.len());
        let Self {
            layout,
            key,
            labels,
            value_scratch,
            ..
        } = self;
        match layout {
            Layout::Sparse(cache) => {
                let mut view = SparsePrimaryKeyView::new(read_key, cache)?;
                view.visit_label_ids(|id| {
                    if key.column(id).is_none() {
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
        SeriesLabels::try_new(Some(key.metric_name.clone()), values).map_err(|error| {
            InvalidSparsePrimaryKeySnafu {
                reason: error.to_string(),
            }
            .build()
        })
    }
}

#[cfg(test)]
mod tests;
