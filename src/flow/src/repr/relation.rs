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

use std::collections::{BTreeMap, HashMap};

use datafusion_common::DFSchema;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{DatafusionSnafu, InternalSnafu, InvalidQuerySnafu, Result, UnexpectedSnafu};
use crate::expr::{MapFilterProject, SafeMfpPlan, ScalarExpr};

/// a set of column indices that are "keys" for the collection.
#[derive(Default, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Key {
    /// indicate whose column form key
    pub column_indices: Vec<usize>,
}

impl Key {
    /// create a new Key
    pub fn new() -> Self {
        Default::default()
    }

    /// create a new Key from a vector of column indices
    pub fn from(mut column_indices: Vec<usize>) -> Self {
        column_indices.sort_unstable();
        Self { column_indices }
    }

    /// Add a column to Key
    pub fn add_col(&mut self, col: usize) {
        self.column_indices.push(col);
    }

    /// Add columns to Key
    pub fn add_cols<I>(&mut self, cols: I)
    where
        I: IntoIterator<Item = usize>,
    {
        self.column_indices.extend(cols);
    }

    /// Remove a column from Key
    pub fn remove_col(&mut self, col: usize) {
        self.column_indices.retain(|&r| r != col);
    }

    /// get all columns in Key
    pub fn get(&self) -> &Vec<usize> {
        &self.column_indices
    }

    /// True if Key is empty
    pub fn is_empty(&self) -> bool {
        self.column_indices.is_empty()
    }

    /// True if all columns in self are also in other
    pub fn subset_of(&self, other: &Key) -> bool {
        self.column_indices
            .iter()
            .all(|c| other.column_indices.contains(c))
    }
}

/// The type of a relation.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct RelationType {
    /// The type for each column, in order.
    pub column_types: Vec<ColumnType>,
    /// Sets of indices that are "keys" for the collection.
    ///
    /// Each element in this list is a set of column indices, each with the
    /// property that the collection contains at most one record with each
    /// distinct set of values for each column. Alternately, for a specific set
    /// of values assigned to the these columns there is at most one record.
    ///
    /// A collection can contain multiple sets of keys, although it is common to
    /// have either zero or one sets of key indices.
    pub keys: Vec<Key>,
    /// optionally indicate the column that is TIME INDEX
    pub time_index: Option<usize>,
    /// mark all the columns that are added automatically by flow, but are not present in original sql
    pub auto_columns: Vec<usize>,
}

impl RelationType {
    pub fn with_autos(mut self, auto_cols: &[usize]) -> Self {
        self.auto_columns = auto_cols.to_vec();
        self
    }

    /// Trying to apply a mpf on current types, will return a new RelationType
    /// with the new types, will also try to preserve keys&time index information
    /// if the old key&time index columns are preserve in given mfp
    ///
    /// i.e. old column of size 3, with a mfp's
    ///
    /// project = `[2, 1]`,
    ///
    /// the old key = `[1]`, old time index = `[2]`,
    ///
    /// then new key=`[1]`, new time index=`[0]`
    ///
    /// note that this function will remove empty keys like key=`[]` will be removed
    pub fn apply_mfp(&self, mfp: &SafeMfpPlan) -> Result<Self> {
        let mfp = &mfp.mfp;
        let mut all_types = self.column_types.clone();
        for expr in &mfp.expressions {
            let expr_typ = expr.typ(&self.column_types)?;
            all_types.push(expr_typ);
        }
        let all_types = all_types;
        let mfp_out_types = mfp
            .projection
            .iter()
            .map(|i| {
                all_types.get(*i).cloned().with_context(|| UnexpectedSnafu {
                    reason: format!(
                        "MFP index out of bound, len is {}, but the index is {}",
                        all_types.len(),
                        *i
                    ),
                })
            })
            .try_collect()?;

        let old_to_new_col = mfp.get_old_to_new_mapping();

        // since it's just a mfp, we also try to preserve keys&time index information, if they survive mfp transform
        let keys = self
            .keys
            .iter()
            .filter_map(|key| {
                key.column_indices
                    .iter()
                    .map(|old| old_to_new_col.get(old).cloned())
                    .collect::<Option<Vec<_>>>()
                    // remove empty keys
                    .and_then(|v| if v.is_empty() { None } else { Some(v) })
                    .map(Key::from)
            })
            .collect_vec();

        let time_index = self
            .time_index
            .and_then(|old| old_to_new_col.get(&old).cloned());
        let auto_columns = self
            .auto_columns
            .iter()
            .filter_map(|old| old_to_new_col.get(old).cloned())
            .collect_vec();
        Ok(Self {
            column_types: mfp_out_types,
            keys,
            time_index,
            auto_columns,
        })
    }
    /// Constructs a `RelationType` representing the relation with no columns and
    /// no keys.
    pub fn empty() -> Self {
        RelationType::new(vec![])
    }

    /// Constructs a new `RelationType` from specified column types.
    ///
    /// The `RelationType` will have no keys.
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        RelationType {
            column_types,
            keys: Vec::new(),
            time_index: None,
            auto_columns: vec![],
        }
    }

    /// Adds a new key for the relation. Also sorts the key indices.
    ///
    /// will ignore empty key
    pub fn with_key(mut self, mut indices: Vec<usize>) -> Self {
        if indices.is_empty() {
            return self;
        }
        indices.sort_unstable();
        let key = Key::from(indices);
        if !self.keys.contains(&key) {
            self.keys.push(key);
        }
        self
    }

    /// Adds new keys for the relation. Also sorts the key indices.
    ///
    /// will ignore empty keys
    pub fn with_keys(mut self, keys: Vec<Vec<usize>>) -> Self {
        for key in keys {
            self = self.with_key(key)
        }
        self
    }

    /// will also remove time index from keys if it's in keys
    pub fn with_time_index(mut self, time_index: Option<usize>) -> Self {
        self.time_index = time_index;
        for key in &mut self.keys {
            key.remove_col(time_index.unwrap_or(usize::MAX));
        }
        self
    }

    /// Computes the number of columns in the relation.
    pub fn arity(&self) -> usize {
        self.column_types.len()
    }

    /// Gets the index of the columns used when creating a default index.
    pub fn default_key(&self) -> Vec<usize> {
        if let Some(key) = self.keys.first() {
            if key.is_empty() {
                (0..self.column_types.len()).collect()
            } else {
                key.get().clone()
            }
        } else {
            (0..self.column_types.len()).collect()
        }
    }

    /// True if any collection described by `self` could safely be described by `other`.
    ///
    /// In practice this means checking that the scalar types match exactly, and that the
    /// nullability of `self` is at least as strict as `other`, and that all keys of `other`
    /// contain some key of `self` (as a set of key columns is less strict than any subset).
    pub fn subtypes(&self, other: &RelationType) -> bool {
        if self.column_types.len() != other.column_types.len() {
            return false;
        }

        for (col1, col2) in self.column_types.iter().zip(other.column_types.iter()) {
            if col1.nullable && !col2.nullable {
                return false;
            }
            if col1.scalar_type != col2.scalar_type {
                return false;
            }
        }

        let all_keys = other
            .keys
            .iter()
            .all(|key1| self.keys.iter().any(|key2| key1.subset_of(key2)));
        if !all_keys {
            return false;
        }

        true
    }

    /// Return relation describe with column names
    pub fn into_named(self, names: Vec<Option<ColumnName>>) -> RelationDesc {
        RelationDesc { typ: self, names }
    }

    /// Return relation describe without column names
    pub fn into_unnamed(self) -> RelationDesc {
        RelationDesc {
            names: vec![None; self.column_types.len()],
            typ: self,
        }
    }
}

/// The type of a `Value`
///
/// [`ColumnType`] bundles information about the scalar type of a datum (e.g.,
/// Int32 or String) with its nullability.
///
/// To construct a column type, either initialize the struct directly, or
/// use the [`ScalarType::nullable`] method.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct ColumnType {
    /// The underlying scalar type (e.g., Int32 or String) of this column.
    pub scalar_type: ConcreteDataType,
    /// Whether this datum can be null.
    #[serde(default = "return_true")]
    pub nullable: bool,
}

impl ColumnType {
    /// Constructs a new `ColumnType` from a scalar type and a nullability flag.
    pub fn new(scalar_type: ConcreteDataType, nullable: bool) -> Self {
        ColumnType {
            scalar_type,
            nullable,
        }
    }

    /// Constructs a new `ColumnType` from a scalar type, with nullability set to
    /// ***true***
    pub fn new_nullable(scalar_type: ConcreteDataType) -> Self {
        ColumnType {
            scalar_type,
            nullable: true,
        }
    }

    /// Returns the scalar type of this column.
    pub fn scalar_type(&self) -> &ConcreteDataType {
        &self.scalar_type
    }

    /// Returns true if this column can be null.
    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

/// This method exists solely for the purpose of making ColumnType nullable by
/// default in unit tests. The default value of a bool is false, and the only
/// way to make an object take on any other value by default is to pass it a
/// function that returns the desired default value. See
/// <https://github.com/serde-rs/serde/issues/1030>
#[inline(always)]
fn return_true() -> bool {
    true
}

/// A description of the shape of a relation.
///
/// It bundles a [`RelationType`] with the name of each column in the relation.
/// Individual column names are optional.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct RelationDesc {
    pub typ: RelationType,
    pub names: Vec<Option<ColumnName>>,
}

impl RelationDesc {
    pub fn len(&self) -> Result<usize> {
        ensure!(
            self.typ.column_types.len() == self.names.len(),
            InternalSnafu {
                reason: "Expect typ and names field to be of same length"
            }
        );
        Ok(self.names.len())
    }

    pub fn to_df_schema(&self) -> Result<DFSchema> {
        let fields: Vec<_> = self
            .iter()
            .enumerate()
            .map(|(i, (name, typ))| {
                let name = name.clone().unwrap_or(format!("Col_{i}"));
                let nullable = typ.nullable;
                let data_type = typ.scalar_type.clone().as_arrow_type();
                arrow_schema::Field::new(name, data_type, nullable)
            })
            .collect();
        let arrow_schema = arrow_schema::Schema::new(fields);

        DFSchema::try_from(arrow_schema.clone()).context({
            DatafusionSnafu {
                context: format!("Error when converting to DFSchema: {:?}", arrow_schema),
            }
        })
    }

    /// apply mfp, and also project col names for the projected columns
    pub fn apply_mfp(&self, mfp: &SafeMfpPlan) -> Result<Self> {
        // TODO(discord9): find a way to deduce name at best effect
        let names = {
            let mfp = &mfp.mfp;
            let mut names = self.names.clone();
            for expr in &mfp.expressions {
                if let ScalarExpr::Column(i) = expr {
                    names.push(self.names.get(*i).cloned().flatten());
                } else {
                    names.push(None);
                }
            }
            mfp.projection
                .iter()
                .map(|i| names.get(*i).cloned().flatten())
                .collect_vec()
        };
        Ok(Self {
            typ: self.typ.apply_mfp(mfp)?,
            names,
        })
    }
}

impl RelationDesc {
    /// Constructs a new `RelationDesc` that represents the empty relation
    /// with no columns and no keys.
    pub fn empty() -> Self {
        RelationDesc {
            typ: RelationType::empty(),
            names: vec![],
        }
    }

    /// Constructs a new `RelationDesc` from a `RelationType` and an iterator
    /// over column names.
    ///
    pub fn try_new<I, N>(typ: RelationType, names: I) -> Result<Self>
    where
        I: IntoIterator<Item = N>,
        N: Into<Option<ColumnName>>,
    {
        let names: Vec<_> = names.into_iter().map(|name| name.into()).collect();
        ensure!(
            typ.arity() == names.len(),
            InvalidQuerySnafu {
                reason: format!(
                    "Length mismatch between RelationType {:?} and column names {:?}",
                    typ.column_types, names
                )
            }
        );
        Ok(RelationDesc { typ, names })
    }

    /// Constructs a new `RelationDesc` from a `RelationType` and an iterator
    /// over column names.
    ///
    /// # Panics
    ///
    /// Panics if the arity of the `RelationType` is not equal to the number of
    /// items in `names`.
    pub fn new_unchecked<I, N>(typ: RelationType, names: I) -> Self
    where
        I: IntoIterator<Item = N>,
        N: Into<Option<ColumnName>>,
    {
        let names: Vec<_> = names.into_iter().map(|name| name.into()).collect();
        assert_eq!(typ.arity(), names.len());
        RelationDesc { typ, names }
    }

    pub fn from_names_and_types<I, T, N>(iter: I) -> Self
    where
        I: IntoIterator<Item = (N, T)>,
        T: Into<ColumnType>,
        N: Into<Option<ColumnName>>,
    {
        let (names, types): (Vec<_>, Vec<_>) = iter.into_iter().unzip();
        let types = types.into_iter().map(Into::into).collect();
        let typ = RelationType::new(types);
        Self::new_unchecked(typ, names)
    }
    /// Concatenates a `RelationDesc` onto the end of this `RelationDesc`.
    pub fn concat(mut self, other: Self) -> Self {
        let self_len = self.typ.column_types.len();
        self.names.extend(other.names);
        self.typ.column_types.extend(other.typ.column_types);
        for k in other.typ.keys {
            let k = k
                .column_indices
                .into_iter()
                .map(|idx| idx + self_len)
                .collect();
            self = self.with_key(k);
        }
        self
    }

    /// Appends a column with the specified name and type.
    pub fn with_column<N>(mut self, name: N, column_type: ColumnType) -> Self
    where
        N: Into<Option<ColumnName>>,
    {
        self.typ.column_types.push(column_type);
        self.names.push(name.into());
        self
    }

    /// Adds a new key for the relation.
    pub fn with_key(mut self, indices: Vec<usize>) -> Self {
        self.typ = self.typ.with_key(indices);
        self
    }

    /// Drops all existing keys.
    pub fn without_keys(mut self) -> Self {
        self.typ.keys.clear();
        self
    }

    /// Builds a new relation description with the column names replaced with
    /// new names.
    ///
    pub fn try_with_names<I, N>(self, names: I) -> Result<Self>
    where
        I: IntoIterator<Item = N>,
        N: Into<Option<ColumnName>>,
    {
        Self::try_new(self.typ, names)
    }

    /// Computes the number of columns in the relation.
    pub fn arity(&self) -> usize {
        self.typ.arity()
    }

    /// Returns the relation type underlying this relation description.
    pub fn typ(&self) -> &RelationType {
        &self.typ
    }

    /// Returns an iterator over the columns in this relation.
    pub fn iter(&self) -> impl Iterator<Item = (&Option<ColumnName>, &ColumnType)> {
        self.iter_names().zip(self.iter_types())
    }

    /// Returns an iterator over the types of the columns in this relation.
    pub fn iter_types(&self) -> impl Iterator<Item = &ColumnType> {
        self.typ.column_types.iter()
    }

    /// Returns an iterator over the names of the columns in this relation.
    pub fn iter_names(&self) -> impl Iterator<Item = &Option<ColumnName>> {
        self.names.iter()
    }

    /// Finds a column by name.
    ///
    /// Returns the index and type of the column named `name`. If no column with
    /// the specified name exists, returns `None`. If multiple columns have the
    /// specified name, the leftmost column is returned.
    pub fn get_by_name(&self, name: &ColumnName) -> Option<(usize, &ColumnType)> {
        self.iter_names()
            .position(|n| n.as_ref() == Some(name))
            .map(|i| (i, &self.typ.column_types[i]))
    }

    /// Gets the name of the `i`th column.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
    pub fn get_name(&self, i: usize) -> &Option<ColumnName> {
        &self.names[i]
    }

    /// Mutably gets the name of the `i`th column.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
    pub fn get_name_mut(&mut self, i: usize) -> &mut Option<ColumnName> {
        &mut self.names[i]
    }

    /// Gets the name of the `i`th column if that column name is unambiguous.
    ///
    /// If at least one other column has the same name as the `i`th column,
    /// returns `None`. If the `i`th column has no name, returns `None`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a valid column index.
    pub fn get_unambiguous_name(&self, i: usize) -> Option<&ColumnName> {
        let name = &self.names[i];
        if self.iter_names().filter(|n| *n == name).count() == 1 {
            name.as_ref()
        } else {
            None
        }
    }
}

/// The name of a column in a [`RelationDesc`].
pub type ColumnName = String;
