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

//! Native histogram PromQL helpers.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use common_query::native_histogram::*;
use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Float64Builder, Int64Array,
    StringBuilder, StructArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, create_udaf, create_udf};

use crate::functions::extract_array;
use crate::range_array::{RangeArray, unpack};

fn extract_histogram_array(value: &ColumnarValue, func_name: &str) -> DfResult<ArrayRef> {
    let array = extract_array(value)?;
    if array.data_type() != &native_histogram_arrow_type() {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: expected native histogram struct, found {}",
            array.data_type()
        )));
    }
    Ok(array)
}

fn read_scalar_f64_arg(
    value: &ColumnarValue,
    row: usize,
    len: usize,
    func_name: &str,
) -> DfResult<f64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(value.unwrap_or(f64::NAN)),
        ColumnarValue::Array(array) => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{func_name}: expected Float64 argument, found {}",
                        array.data_type()
                    ))
                })?;
            if array.len() != len {
                return Err(DataFusionError::Execution(format!(
                    "{func_name}: Float64 argument length mismatch: {} vs {len}",
                    array.len()
                )));
            }
            Ok(if array.is_null(row) {
                f64::NAN
            } else {
                array.value(row)
            })
        }
        other => Err(DataFusionError::Execution(format!(
            "{func_name}: expected Float64 argument, found {}",
            other.data_type()
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AnnotationReturn {
    FloatNull,
    BooleanFalse,
}

impl AnnotationReturn {
    fn data_type(self) -> DataType {
        match self {
            Self::FloatNull => DataType::Float64,
            Self::BooleanFalse => DataType::Boolean,
        }
    }

    fn scalar_value(self) -> ScalarValue {
        match self {
            Self::FloatNull => ScalarValue::Float64(None),
            Self::BooleanFalse => ScalarValue::Boolean(Some(false)),
        }
    }
}

#[derive(Debug, Clone)]
struct NativeHistogramAnnotationUdf {
    name: &'static str,
    signature: Signature,
    return_kind: AnnotationReturn,
    message: String,
    collector: Option<PromqlAnnotationCollector>,
}

impl NativeHistogramAnnotationUdf {
    fn new(
        name: &'static str,
        return_kind: AnnotationReturn,
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> Self {
        Self {
            name,
            signature: Signature::variadic_any(Volatility::Volatile),
            return_kind,
            message,
            collector,
        }
    }
}

impl PartialEq for NativeHistogramAnnotationUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.return_kind == other.return_kind
            && self.message == other.message
    }
}

impl Eq for NativeHistogramAnnotationUdf {}

impl Hash for NativeHistogramAnnotationUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.return_kind.hash(state);
        self.message.hash(state);
    }
}

impl ScalarUDFImpl for NativeHistogramAnnotationUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.return_kind.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if let Some(collector) = args
            .config_options
            .extensions
            .get::<PromqlAnnotationCollector>()
            .cloned()
            .or_else(|| self.collector.clone())
        {
            collector.record_info(self.message.clone());
        }
        Ok(ColumnarValue::Scalar(self.return_kind.scalar_value()))
    }
}

pub struct NativeHistogramDrop;

impl NativeHistogramDrop {
    const fn float_null_name() -> &'static str {
        "prom_native_histogram_drop_float"
    }

    const fn bool_false_name() -> &'static str {
        "prom_native_histogram_drop_bool"
    }

    pub fn float_null_udf(
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> ScalarUDF {
        ScalarUDF::new_from_impl(NativeHistogramAnnotationUdf::new(
            Self::float_null_name(),
            AnnotationReturn::FloatNull,
            message,
            collector,
        ))
    }

    pub fn bool_false_udf(
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> ScalarUDF {
        ScalarUDF::new_from_impl(NativeHistogramAnnotationUdf::new(
            Self::bool_false_name(),
            AnnotationReturn::BooleanFalse,
            message,
            collector,
        ))
    }
}

// Synthetic drop UDFs only cover info-level invalid PromQL combinations.
// Warning annotations stay in the histogram UDFs that actually drop samples.
fn record_info(collector: &Option<PromqlAnnotationCollector>, message: impl Into<String>) {
    if let Some(collector) = collector {
        collector.record_info(message);
    }
}

fn record_warning(collector: &Option<PromqlAnnotationCollector>, message: impl Into<String>) {
    if let Some(collector) = collector {
        collector.record_warning(message);
    }
}

fn record_custom_reconciliation(
    collector: &Option<PromqlAnnotationCollector>,
    name: &'static str,
    lhs: &NativeHistogram,
    rhs: &NativeHistogram,
) {
    if lhs.needs_custom_reconciliation(rhs) {
        record_info(
            collector,
            format!("{name}: reconciled native histograms with different custom buckets"),
        );
    }
}

fn record_counter_reset_contradiction(
    collector: &Option<PromqlAnnotationCollector>,
    name: &'static str,
    lhs: &NativeHistogram,
    rhs: &NativeHistogram,
) {
    if lhs.counter_reset_hints_contradict(rhs) {
        record_warning(
            collector,
            format!("{name}: native histogram counter reset hints contradict"),
        );
    }
}

fn scalar_histogram_udf(
    name: &'static str,
    extra_input_types: Vec<DataType>,
    calc: fn(&NativeHistogram, &[ColumnarValue], usize, usize, &'static str) -> DfResult<f64>,
) -> ScalarUDF {
    let mut input_types = vec![native_histogram_arrow_type()];
    input_types.extend(extra_input_types);
    create_udf(
        name,
        input_types,
        DataType::Float64,
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            if input.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "{name} requires a native histogram argument"
                )));
            }
            let histograms = extract_histogram_array(&input[0], name)?;
            let histograms = histograms
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let mut result = Float64Builder::with_capacity(histograms.len());
            for row in 0..histograms.len() {
                match read_histogram(histograms, row)? {
                    Some(histogram) => {
                        result.append_value(calc(&histogram, input, row, histograms.len(), name)?)
                    }
                    None => result.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }) as _,
    )
}

fn histogram_pair_udf(
    name: &'static str,
    op: fn(&NativeHistogram, &NativeHistogram) -> Option<NativeHistogram>,
) -> ScalarUDF {
    histogram_pair_udf_with_collector(name, op, None)
}

fn histogram_pair_udf_with_collector(
    name: &'static str,
    op: fn(&NativeHistogram, &NativeHistogram) -> Option<NativeHistogram>,
    collector: Option<PromqlAnnotationCollector>,
) -> ScalarUDF {
    create_udf(
        name,
        vec![native_histogram_arrow_type(), native_histogram_arrow_type()],
        native_histogram_arrow_type(),
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            let lhs = extract_histogram_array(&input[0], name)?;
            let rhs = extract_histogram_array(&input[1], name)?;
            if lhs.len() != rhs.len() {
                return Err(DataFusionError::Execution(format!(
                    "{name}: native histogram argument length mismatch: {} vs {}",
                    lhs.len(),
                    rhs.len()
                )));
            }

            let lhs = lhs
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let rhs = rhs
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let mut result = Vec::with_capacity(lhs.len());
            for row in 0..lhs.len() {
                result.push(
                    match (read_histogram(lhs, row)?, read_histogram(rhs, row)?) {
                        (Some(lhs), Some(rhs)) => {
                            record_custom_reconciliation(&collector, name, &lhs, &rhs);
                            let result = op(&lhs, &rhs);
                            if result.is_some() && name == NativeHistogramAdd::name() {
                                record_counter_reset_contradiction(&collector, name, &lhs, &rhs);
                            }
                            if result.is_none() {
                                record_warning(
                                    &collector,
                                    format!(
                                    "{name}: dropped native histogram sample with incompatible schemas"
                                    ),
                                );
                            }
                            result
                        }
                        _ => None,
                    },
                );
            }
            Ok(ColumnarValue::Array(build_histogram_array(&result)))
        }) as _,
    )
}

fn histogram_transform_udf(
    name: &'static str,
    op: fn(NativeHistogram) -> NativeHistogram,
) -> ScalarUDF {
    create_udf(
        name,
        vec![native_histogram_arrow_type()],
        native_histogram_arrow_type(),
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            let histograms = extract_histogram_array(&input[0], name)?;
            let histograms = histograms
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let mut result = Vec::with_capacity(histograms.len());
            for row in 0..histograms.len() {
                result.push(read_histogram(histograms, row)?.map(op));
            }
            Ok(ColumnarValue::Array(build_histogram_array(&result)))
        }) as _,
    )
}

fn histogram_string_udf(name: &'static str) -> ScalarUDF {
    create_udf(
        name,
        vec![native_histogram_arrow_type()],
        DataType::Utf8,
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            let histograms = extract_histogram_array(&input[0], name)?;
            let histograms = histograms
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let mut result = StringBuilder::with_capacity(histograms.len(), histograms.len() * 32);
            for row in 0..histograms.len() {
                match read_histogram(histograms, row)? {
                    Some(histogram) => result.append_value(histogram.promql_string()),
                    None => result.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }) as _,
    )
}

fn histogram_scalar_udf(
    name: &'static str,
    input_types: Vec<DataType>,
    histogram_index: usize,
    scalar_index: usize,
    op: fn(NativeHistogram, f64) -> Option<NativeHistogram>,
) -> ScalarUDF {
    create_udf(
        name,
        input_types,
        native_histogram_arrow_type(),
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            let histograms = extract_histogram_array(&input[histogram_index], name)?;
            let histograms = histograms
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let mut result = Vec::with_capacity(histograms.len());
            for row in 0..histograms.len() {
                result.push(match read_histogram(histograms, row)? {
                    Some(histogram) => {
                        let scalar =
                            read_scalar_f64_arg(&input[scalar_index], row, histograms.len(), name)?;
                        op(histogram, scalar)
                    }
                    None => None,
                });
            }
            Ok(ColumnarValue::Array(build_histogram_array(&result)))
        }) as _,
    )
}

fn histogram_compare_udf(
    name: &'static str,
    op: fn(&NativeHistogram, &NativeHistogram) -> bool,
) -> ScalarUDF {
    create_udf(
        name,
        vec![native_histogram_arrow_type(), native_histogram_arrow_type()],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            let lhs = extract_histogram_array(&input[0], name)?;
            let rhs = extract_histogram_array(&input[1], name)?;
            if lhs.len() != rhs.len() {
                return Err(DataFusionError::Execution(format!(
                    "{name}: native histogram argument length mismatch: {} vs {}",
                    lhs.len(),
                    rhs.len()
                )));
            }

            let lhs = lhs
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let rhs = rhs
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("validated native histogram struct");
            let mut result = Vec::with_capacity(lhs.len());
            for row in 0..lhs.len() {
                result.push(
                    match (read_histogram(lhs, row)?, read_histogram(rhs, row)?) {
                        (Some(lhs), Some(rhs)) => Some(op(&lhs, &rhs)),
                        _ => None,
                    },
                );
            }
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result))))
        }) as _,
    )
}

pub struct NativeHistogramAdd;

impl NativeHistogramAdd {
    pub const fn name() -> &'static str {
        "prom_native_histogram_add"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_pair_udf(Self::name(), NativeHistogram::add)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        histogram_pair_udf_with_collector(Self::name(), NativeHistogram::add, collector)
    }
}

pub struct NativeHistogramSub;

impl NativeHistogramSub {
    pub const fn name() -> &'static str {
        "prom_native_histogram_sub"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_pair_udf(Self::name(), NativeHistogram::sub)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        histogram_pair_udf_with_collector(Self::name(), NativeHistogram::sub, collector)
    }
}

pub struct NativeHistogramMulScalar;

impl NativeHistogramMulScalar {
    pub const fn name() -> &'static str {
        "prom_native_histogram_mul_scalar"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_scalar_udf(
            Self::name(),
            vec![native_histogram_arrow_type(), DataType::Float64],
            0,
            1,
            |histogram, scalar| Some(histogram.scale(scalar)),
        )
    }
}

pub struct NativeHistogramScalarMul;

impl NativeHistogramScalarMul {
    pub const fn name() -> &'static str {
        "prom_native_histogram_scalar_mul"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_scalar_udf(
            Self::name(),
            vec![DataType::Float64, native_histogram_arrow_type()],
            1,
            0,
            |histogram, scalar| Some(histogram.scale(scalar)),
        )
    }
}

pub struct NativeHistogramDivScalar;

impl NativeHistogramDivScalar {
    pub const fn name() -> &'static str {
        "prom_native_histogram_div_scalar"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_scalar_udf(
            Self::name(),
            vec![native_histogram_arrow_type(), DataType::Float64],
            0,
            1,
            |histogram, scalar| Some(histogram.divide_by(scalar)),
        )
    }
}

pub struct NativeHistogramNeg;

impl NativeHistogramNeg {
    pub const fn name() -> &'static str {
        "prom_native_histogram_neg"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_transform_udf(Self::name(), NativeHistogram::negated)
    }
}

pub struct NativeHistogramEq;

impl NativeHistogramEq {
    pub const fn name() -> &'static str {
        "prom_native_histogram_eq"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_compare_udf(Self::name(), NativeHistogram::promql_eq)
    }
}

pub struct NativeHistogramNotEq;

impl NativeHistogramNotEq {
    pub const fn name() -> &'static str {
        "prom_native_histogram_not_eq"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_compare_udf(Self::name(), |lhs, rhs| !lhs.promql_eq(rhs))
    }
}

pub struct NativeHistogramCount;

impl NativeHistogramCount {
    pub const fn name() -> &'static str {
        "prom_native_histogram_count"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(Self::name(), vec![], |histogram, _, _, _, _| {
            Ok(histogram.count)
        })
    }
}

pub struct NativeHistogramSum;

impl NativeHistogramSum {
    pub const fn name() -> &'static str {
        "prom_native_histogram_sum"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(Self::name(), vec![], |histogram, _, _, _, _| {
            Ok(histogram.sum)
        })
    }
}

pub struct NativeHistogramAvg;

impl NativeHistogramAvg {
    pub const fn name() -> &'static str {
        "prom_native_histogram_avg"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(Self::name(), vec![], |histogram, _, _, _, _| {
            Ok(histogram.sum / histogram.count)
        })
    }
}

pub struct NativeHistogramStddev;

impl NativeHistogramStddev {
    pub const fn name() -> &'static str {
        "prom_native_histogram_stddev"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(Self::name(), vec![], |histogram, _, _, _, _| {
            Ok(histogram.estimated_stddev())
        })
    }
}

pub struct NativeHistogramStdvar;

impl NativeHistogramStdvar {
    pub const fn name() -> &'static str {
        "prom_native_histogram_stdvar"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(Self::name(), vec![], |histogram, _, _, _, _| {
            Ok(histogram.estimated_stdvar())
        })
    }
}

pub struct NativeHistogramToString;

impl NativeHistogramToString {
    pub const fn name() -> &'static str {
        "prom_native_histogram_to_string"
    }

    pub fn scalar_udf() -> ScalarUDF {
        histogram_string_udf(Self::name())
    }
}

pub struct NativeHistogramQuantile;

impl NativeHistogramQuantile {
    pub const fn name() -> &'static str {
        "prom_native_histogram_quantile"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(
            Self::name(),
            vec![DataType::Float64],
            |histogram, input, row, len, name| {
                Ok(histogram.quantile(read_scalar_f64_arg(&input[1], row, len, name)?))
            },
        )
    }
}

pub struct NativeHistogramFraction;

impl NativeHistogramFraction {
    pub const fn name() -> &'static str {
        "prom_native_histogram_fraction"
    }

    pub fn scalar_udf() -> ScalarUDF {
        scalar_histogram_udf(
            Self::name(),
            vec![DataType::Float64, DataType::Float64],
            |histogram, input, row, len, name| {
                let lower = read_scalar_f64_arg(&input[1], row, len, name)?;
                let upper = read_scalar_f64_arg(&input[2], row, len, name)?;
                Ok(histogram.fraction(lower, upper))
            },
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum NativeHistogramAggregateKind {
    Sum,
    Avg,
}

impl NativeHistogramAggregateKind {
    const fn name(self) -> &'static str {
        match self {
            Self::Sum => NativeHistogramAggSum::name(),
            Self::Avg => NativeHistogramAggAvg::name(),
        }
    }

    const fn needs_count(self) -> bool {
        matches!(self, Self::Avg)
    }
}

pub struct NativeHistogramAggSum;

impl NativeHistogramAggSum {
    pub const fn name() -> &'static str {
        "prom_native_histogram_agg_sum"
    }

    pub fn aggregate_udf() -> AggregateUDF {
        native_histogram_aggregate_udf(NativeHistogramAggregateKind::Sum, None)
    }

    pub fn aggregate_udf_with_collector(
        collector: Option<PromqlAnnotationCollector>,
    ) -> AggregateUDF {
        native_histogram_aggregate_udf(NativeHistogramAggregateKind::Sum, collector)
    }
}

pub struct NativeHistogramAggAvg;

impl NativeHistogramAggAvg {
    pub const fn name() -> &'static str {
        "prom_native_histogram_agg_avg"
    }

    pub fn aggregate_udf() -> AggregateUDF {
        native_histogram_aggregate_udf(NativeHistogramAggregateKind::Avg, None)
    }

    pub fn aggregate_udf_with_collector(
        collector: Option<PromqlAnnotationCollector>,
    ) -> AggregateUDF {
        native_histogram_aggregate_udf(NativeHistogramAggregateKind::Avg, collector)
    }
}

#[derive(Debug)]
struct NativeHistogramAggregateAccumulator {
    kind: NativeHistogramAggregateKind,
    value: Option<NativeHistogram>,
    count: u64,
    dropped_incompatible: bool,
    collector: Option<PromqlAnnotationCollector>,
}

impl NativeHistogramAggregateAccumulator {
    fn new(
        kind: NativeHistogramAggregateKind,
        collector: Option<PromqlAnnotationCollector>,
    ) -> Self {
        Self {
            kind,
            value: None,
            count: 0,
            dropped_incompatible: false,
            collector,
        }
    }

    fn from_args(
        kind: NativeHistogramAggregateKind,
        collector: Option<PromqlAnnotationCollector>,
        _args: AccumulatorArgs,
    ) -> DfResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(Self::new(kind, collector)))
    }

    fn push_histogram(&mut self, histogram: NativeHistogram, count: u64) {
        if self.dropped_incompatible {
            return;
        }

        let value = match self.value.take() {
            Some(value) => {
                record_custom_reconciliation(&self.collector, self.kind.name(), &value, &histogram);
                record_counter_reset_contradiction(
                    &self.collector,
                    self.kind.name(),
                    &value,
                    &histogram,
                );
                match value.add(&histogram) {
                    Some(value) => Some(value),
                    None => {
                        self.record_incompatible();
                        None
                    }
                }
            }
            None => Some(histogram),
        };
        if !self.dropped_incompatible {
            self.value = value;
            self.count += count;
        }
    }

    fn mark_incompatible(&mut self) {
        self.value = None;
        self.count = 0;
        self.dropped_incompatible = true;
    }

    fn record_incompatible(&mut self) {
        self.mark_incompatible();
        record_warning(
            &self.collector,
            format!(
                "{}: dropped native histogram aggregate with incompatible schemas",
                self.kind.name()
            ),
        );
    }
}

fn range_sum_histograms(
    samples: Vec<NativeHistogram>,
    name: &'static str,
    collector: &Option<PromqlAnnotationCollector>,
) -> Option<NativeHistogram> {
    let mut value = None;
    for histogram in samples {
        value = match value {
            Some(value) => {
                record_custom_reconciliation(collector, name, &value, &histogram);
                record_counter_reset_contradiction(collector, name, &value, &histogram);
                match value.add(&histogram) {
                    Some(value) => Some(value),
                    None => {
                        record_warning(
                            collector,
                            format!(
                                "{name}: dropped native histogram range with incompatible schemas"
                            ),
                        );
                        return None;
                    }
                }
            }
            None => Some(histogram),
        };
    }
    value
}

#[derive(Debug, Clone, Copy)]
enum NativeHistogramRangeHistogramKind {
    Sum,
    Avg,
    Last,
}

#[derive(Debug, Clone, Copy)]
enum NativeHistogramRangeFloatKind {
    Absent,
    Count,
    Present,
    Changes,
    Resets,
}

fn collect_window_histograms(
    histograms: &StructArray,
    offset: usize,
    length: usize,
) -> DfResult<Option<Vec<NativeHistogram>>> {
    let mut samples = Vec::with_capacity(length);
    for row in offset..offset + length {
        let Some(histogram) = read_histogram(histograms, row)? else {
            return Ok(None);
        };
        samples.push(histogram);
    }
    Ok(Some(samples))
}

fn native_histogram_range_histogram(
    input: &[ColumnarValue],
    kind: NativeHistogramRangeHistogramKind,
    func_name: &'static str,
    collector: Option<PromqlAnnotationCollector>,
) -> DfResult<ColumnarValue> {
    if input.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "{func_name} function should have 2 inputs"
        )));
    }

    let ts_range = extract_range_dict(
        &input[0],
        func_name,
        "timestamp range vector",
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let value_range = extract_range_dict(
        &input[1],
        func_name,
        "value range vector",
        &native_histogram_arrow_type(),
    )?;
    if ts_range.keys().values() != value_range.keys().values() {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: timestamp and value ranges should have the same window layout"
        )));
    }

    let histograms = value_range
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("validated native histogram range");
    let mut result = Vec::with_capacity(value_range.keys().len());
    for key in value_range.keys().values() {
        let (offset, length) = unpack(*key);
        let offset = offset as usize;
        let length = length as usize;
        if length == 0 {
            result.push(None);
            continue;
        }
        let Some(samples) = collect_window_histograms(histograms, offset, length)? else {
            result.push(None);
            continue;
        };
        let histogram = match kind {
            NativeHistogramRangeHistogramKind::Sum => {
                range_sum_histograms(samples, func_name, &collector)
                    .map(NativeHistogram::into_gauge)
            }
            NativeHistogramRangeHistogramKind::Avg => {
                let count = samples.len();
                range_sum_histograms(samples, func_name, &collector)
                    .map(|histogram| histogram.divide_by(count as f64).into_gauge())
            }
            NativeHistogramRangeHistogramKind::Last => {
                samples.last().cloned().map(NativeHistogram::into_gauge)
            }
        };
        result.push(histogram);
    }

    Ok(ColumnarValue::Array(build_histogram_array(&result)))
}

fn native_histogram_range_float(
    input: &[ColumnarValue],
    kind: NativeHistogramRangeFloatKind,
    func_name: &'static str,
) -> DfResult<ColumnarValue> {
    if input.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "{func_name} function should have 2 inputs"
        )));
    }

    let ts_range = extract_range_dict(
        &input[0],
        func_name,
        "timestamp range vector",
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let value_range = extract_range_dict(
        &input[1],
        func_name,
        "value range vector",
        &native_histogram_arrow_type(),
    )?;
    if ts_range.keys().values() != value_range.keys().values() {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: timestamp and value ranges should have the same window layout"
        )));
    }

    let timestamps = ts_range
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("validated timestamp range")
        .values();
    let histograms = value_range
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("validated native histogram range");
    let mut result = Float64Builder::with_capacity(value_range.keys().len());
    for key in value_range.keys().values() {
        let (offset, length) = unpack(*key);
        let offset = offset as usize;
        let length = length as usize;
        if length == 0 {
            match kind {
                NativeHistogramRangeFloatKind::Absent => result.append_value(1.0),
                _ => result.append_null(),
            }
            continue;
        }
        let Some(samples) = collect_window_histograms(histograms, offset, length)? else {
            result.append_null();
            continue;
        };
        let value = match kind {
            NativeHistogramRangeFloatKind::Absent => {
                result.append_null();
                continue;
            }
            NativeHistogramRangeFloatKind::Count => length as f64,
            NativeHistogramRangeFloatKind::Present => 1.0,
            NativeHistogramRangeFloatKind::Changes => samples
                .windows(2)
                .filter(|pair| !pair[0].promql_eq(&pair[1]))
                .count() as f64,
            NativeHistogramRangeFloatKind::Resets => samples
                .windows(2)
                .zip(timestamps[offset..offset + length].windows(2))
                .filter(|(pair, ts_pair)| {
                    pair[1].detect_counter_reset(&pair[0], ts_pair[0], ts_pair[1])
                })
                .count() as f64,
        };
        result.append_value(value);
    }

    Ok(ColumnarValue::Array(Arc::new(result.finish())))
}

fn create_native_range_histogram_udf(
    name: &'static str,
    kind: NativeHistogramRangeHistogramKind,
    collector: Option<PromqlAnnotationCollector>,
) -> ScalarUDF {
    create_udf(
        name,
        vec![
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            RangeArray::convert_data_type(native_histogram_arrow_type()),
        ],
        native_histogram_arrow_type(),
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            native_histogram_range_histogram(input, kind, name, collector.clone())
        }) as _,
    )
}

fn create_native_range_float_udf(
    name: &'static str,
    kind: NativeHistogramRangeFloatKind,
) -> ScalarUDF {
    create_udf(
        name,
        vec![
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            RangeArray::convert_data_type(native_histogram_arrow_type()),
        ],
        DataType::Float64,
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| native_histogram_range_float(input, kind, name))
            as _,
    )
}

pub struct NativeHistogramSumOverTime;
pub struct NativeHistogramAvgOverTime;
pub struct NativeHistogramAbsentOverTime;
pub struct NativeHistogramCountOverTime;
pub struct NativeHistogramLastOverTime;
pub struct NativeHistogramPresentOverTime;
pub struct NativeHistogramChanges;
pub struct NativeHistogramResets;

impl NativeHistogramSumOverTime {
    pub const fn name() -> &'static str {
        "prom_native_histogram_sum_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_range_histogram_udf(
            Self::name(),
            NativeHistogramRangeHistogramKind::Sum,
            collector,
        )
    }
}

impl NativeHistogramAvgOverTime {
    pub const fn name() -> &'static str {
        "prom_native_histogram_avg_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_range_histogram_udf(
            Self::name(),
            NativeHistogramRangeHistogramKind::Avg,
            collector,
        )
    }
}

impl NativeHistogramAbsentOverTime {
    pub const fn name() -> &'static str {
        "prom_native_histogram_absent_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_native_range_float_udf(Self::name(), NativeHistogramRangeFloatKind::Absent)
    }
}

impl NativeHistogramCountOverTime {
    pub const fn name() -> &'static str {
        "prom_native_histogram_count_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_native_range_float_udf(Self::name(), NativeHistogramRangeFloatKind::Count)
    }
}

impl NativeHistogramLastOverTime {
    pub const fn name() -> &'static str {
        "prom_native_histogram_last_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_native_range_histogram_udf(
            Self::name(),
            NativeHistogramRangeHistogramKind::Last,
            None,
        )
    }
}

impl NativeHistogramPresentOverTime {
    pub const fn name() -> &'static str {
        "prom_native_histogram_present_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_native_range_float_udf(Self::name(), NativeHistogramRangeFloatKind::Present)
    }
}

impl NativeHistogramChanges {
    pub const fn name() -> &'static str {
        "prom_native_histogram_changes"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_native_range_float_udf(Self::name(), NativeHistogramRangeFloatKind::Changes)
    }
}

impl NativeHistogramResets {
    pub const fn name() -> &'static str {
        "prom_native_histogram_resets"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_native_range_float_udf(Self::name(), NativeHistogramRangeFloatKind::Resets)
    }
}

fn native_histogram_scalar(histogram: Option<NativeHistogram>) -> ScalarValue {
    let array = build_histogram_array(&[histogram]);
    let histogram = array
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("native histogram array is a StructArray")
        .clone();
    ScalarValue::Struct(Arc::new(histogram))
}

fn native_histogram_aggregate_udf(
    kind: NativeHistogramAggregateKind,
    collector: Option<PromqlAnnotationCollector>,
) -> AggregateUDF {
    let state_types = if kind.needs_count() {
        vec![
            native_histogram_arrow_type(),
            DataType::UInt64,
            DataType::Boolean,
        ]
    } else {
        vec![native_histogram_arrow_type(), DataType::Boolean]
    };

    create_udaf(
        kind.name(),
        vec![native_histogram_arrow_type()],
        Arc::new(native_histogram_arrow_type()),
        Volatility::Volatile,
        Arc::new(move |args| {
            NativeHistogramAggregateAccumulator::from_args(kind, collector.clone(), args)
        }),
        Arc::new(state_types),
    )
}

impl DfAccumulator for NativeHistogramAggregateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let histograms = values
            .first()
            .and_then(|array| array.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expected native histogram struct input",
                    self.kind.name()
                ))
            })?;

        for row in 0..histograms.len() {
            let Some(histogram) = read_histogram(histograms, row)? else {
                continue;
            };
            self.push_histogram(histogram, 1);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let histogram = match (self.kind, self.dropped_incompatible, self.value.clone()) {
            (_, true, _) => None,
            (_, false, None) => None,
            (NativeHistogramAggregateKind::Sum, false, value) => value,
            (NativeHistogramAggregateKind::Avg, false, Some(value)) if self.count > 0 => {
                Some(value.divide_by(self.count as f64))
            }
            (NativeHistogramAggregateKind::Avg, _, _) => None,
        };

        Ok(native_histogram_scalar(histogram))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let mut state = vec![native_histogram_scalar(self.value.clone())];
        if self.kind.needs_count() {
            state.push(ScalarValue::UInt64(Some(self.count)));
        }
        state.push(ScalarValue::Boolean(Some(self.dropped_incompatible)));
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        if states.is_empty() {
            return Ok(());
        }

        let histograms = states[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expected native histogram struct state",
                    self.kind.name()
                ))
            })?;
        let counts = if self.kind.needs_count() {
            Some(
                states
                    .get(1)
                    .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{}: expected UInt64 count state",
                            self.kind.name()
                        ))
                    })?,
            )
        } else {
            None
        };
        let dropped_index = if self.kind.needs_count() { 2 } else { 1 };
        let dropped = states
            .get(dropped_index)
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expected Boolean dropped state",
                    self.kind.name()
                ))
            })?;

        for row in 0..histograms.len() {
            if dropped.value(row) {
                self.mark_incompatible();
                continue;
            }
            let Some(histogram) = read_histogram(histograms, row)? else {
                continue;
            };
            let count = counts.map(|counts| counts.value(row)).unwrap_or(1);
            self.push_histogram(histogram, count);
        }

        Ok(())
    }
}

fn histogram_delta(
    samples: &[NativeHistogram],
    timestamps: &[i64],
    is_counter: bool,
) -> Option<NativeHistogram> {
    if samples.len() < 2 || samples.len() != timestamps.len() {
        return None;
    }

    if !is_counter {
        return samples
            .last()?
            .sub(samples.first()?)
            .map(NativeHistogram::into_gauge);
    }

    let mut result = samples.first()?.zero_like();
    let mut segment_start = samples.first()?;
    for (sample_pair, ts_pair) in samples.windows(2).zip(timestamps.windows(2)) {
        let previous_ts = ts_pair[0];
        let current_ts = ts_pair[1];
        let previous = &sample_pair[0];
        let current = &sample_pair[1];
        if current.detect_counter_reset(previous, previous_ts, current_ts) {
            result = result.add(&previous.sub(segment_start)?)?;
            result = result.add(current)?;
            segment_start = current;
        }
    }
    Some(
        result
            .add(&samples.last()?.sub(segment_start)?)?
            .into_gauge(),
    )
}

fn idelta_value(
    samples: &[NativeHistogram],
    is_rate: bool,
    previous_ts: i64,
    current_ts: i64,
    sampled_interval_secs: f64,
) -> Option<NativeHistogram> {
    if samples.len() < 2 {
        return None;
    }
    let previous = &samples[samples.len() - 2];
    let current = samples.last()?;
    let result = if is_rate && current.detect_counter_reset(previous, previous_ts, current_ts) {
        current.clone()
    } else {
        current.sub(previous)?
    };
    Some(
        if is_rate {
            result.scale(1.0 / sampled_interval_secs)
        } else {
            result
        }
        .into_gauge(),
    )
}

fn extract_range_dict(
    columnar_value: &ColumnarValue,
    func_name: &str,
    arg_name: &str,
    expected_value_type: &DataType,
) -> DfResult<DictionaryArray<Int64Type>> {
    let array = extract_array(columnar_value)?;
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect {arg_name} as DictionaryArray<Int64>, found {}",
                array.data_type()
            ))
        })?
        .clone();

    if &dict.value_type() != expected_value_type {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: expect {arg_name} values of type {expected_value_type}, found {}",
            dict.value_type()
        )));
    }

    RangeArray::try_new(dict.clone()).map_err(DataFusionError::from)?;
    Ok(dict)
}

fn native_extrapolated_rate<const IS_COUNTER: bool, const IS_RATE: bool>(
    input: &[ColumnarValue],
    range_length: i64,
    func_name: &'static str,
    collector: Option<PromqlAnnotationCollector>,
) -> DfResult<ColumnarValue> {
    if input.len() != 4 {
        return Err(DataFusionError::Plan(format!(
            "{func_name} function should have 4 inputs"
        )));
    }

    let ts_dict = extract_range_dict(
        &input[0],
        func_name,
        "timestamp range vector",
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let value_dict = extract_range_dict(
        &input[1],
        func_name,
        "value range vector",
        &native_histogram_arrow_type(),
    )?;
    let eval_ts = extract_array(&input[2])?;
    let eval_ts = eval_ts
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect evaluation timestamp vector as Timestamp(Millisecond), found {}",
                eval_ts.data_type()
            ))
        })?;

    let keys = ts_dict.keys().values();
    if value_dict.keys().values() != keys || eval_ts.len() != keys.len() {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: timestamp, value, and evaluation ranges should have the same layout"
        )));
    }

    let all_timestamps = ts_dict
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("validated timestamp range")
        .values();
    let all_histograms = value_dict
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("validated native histogram range");
    let range_length_secs = range_length as f64 / 1000.0;
    let mut result = Vec::with_capacity(keys.len());

    for index in 0..keys.len() {
        let (raw_offset, raw_length) = unpack(keys[index]);
        let offset = raw_offset as usize;
        let length = raw_length as usize;
        if length < 2 {
            result.push(None);
            continue;
        }

        let mut samples = Vec::with_capacity(length);
        let mut has_null = false;
        for row in offset..offset + length {
            let Some(histogram) = read_histogram(all_histograms, row)? else {
                has_null = true;
                break;
            };
            samples.push(histogram);
        }
        if has_null {
            result.push(None);
            continue;
        }

        let timestamps = &all_timestamps[offset..offset + length];
        for pair in samples.windows(2) {
            record_custom_reconciliation(&collector, func_name, &pair[0], &pair[1]);
        }
        let Some(mut histogram) = histogram_delta(&samples, timestamps, IS_COUNTER) else {
            record_warning(
                &collector,
                format!("{func_name}: dropped native histogram range with incompatible schemas"),
            );
            result.push(None);
            continue;
        };

        let first_ts = all_timestamps[offset];
        let last_ts = all_timestamps[offset + length - 1];
        let range_end = eval_ts.value(index);
        let range_start = range_end - range_length;
        let sampled_interval_ms = (last_ts - first_ts) as f64;
        if sampled_interval_ms <= 0.0 {
            result.push(None);
            continue;
        }
        let average_interval_ms = sampled_interval_ms / (length - 1) as f64;
        let mut duration_to_start_ms = (first_ts - range_start) as f64;
        let duration_to_end_ms = (range_end - last_ts) as f64;

        if IS_COUNTER && histogram.count > 0.0 && samples[0].count >= 0.0 {
            let duration_to_zero = sampled_interval_ms * (samples[0].count / histogram.count);
            if duration_to_zero < duration_to_start_ms {
                duration_to_start_ms = duration_to_zero;
            }
        }

        let extrapolation_threshold = average_interval_ms * 1.1;
        let mut extrapolated_interval_ms = sampled_interval_ms;
        if duration_to_start_ms < extrapolation_threshold {
            extrapolated_interval_ms += duration_to_start_ms;
        } else {
            extrapolated_interval_ms += average_interval_ms / 2.0;
        }
        if duration_to_end_ms < extrapolation_threshold {
            extrapolated_interval_ms += duration_to_end_ms;
        } else {
            extrapolated_interval_ms += average_interval_ms / 2.0;
        }

        let mut factor = extrapolated_interval_ms / sampled_interval_ms;
        if IS_RATE {
            factor /= range_length_secs;
        }
        histogram = histogram.scale(factor).into_gauge();
        result.push(Some(histogram));
    }

    Ok(ColumnarValue::Array(build_histogram_array(&result)))
}

fn create_native_extrapolated_udf<const IS_COUNTER: bool, const IS_RATE: bool>(
    name: &'static str,
    collector: Option<PromqlAnnotationCollector>,
) -> ScalarUDF {
    let input_types = vec![
        RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
        RangeArray::convert_data_type(native_histogram_arrow_type()),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Int64,
    ];
    create_udf(
        name,
        input_types,
        native_histogram_arrow_type(),
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            let range_length = extract_array(&input[3])?;
            let range_length = range_length
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{name}: expect Int64 as range length type, found {}",
                        range_length.data_type()
                    ))
                })?;
            if range_length.is_empty() || range_length.is_null(0) {
                return Err(DataFusionError::Execution(format!(
                    "{name}: range length must contain a non-null Int64 value"
                )));
            }
            native_extrapolated_rate::<IS_COUNTER, IS_RATE>(
                input,
                range_length.value(0),
                name,
                collector.clone(),
            )
        }) as _,
    )
}

pub struct NativeHistogramDelta;
pub struct NativeHistogramRate;
pub struct NativeHistogramIncrease;

impl NativeHistogramDelta {
    pub const fn name() -> &'static str {
        "prom_native_histogram_delta"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_extrapolated_udf::<false, false>(Self::name(), collector)
    }
}

impl NativeHistogramRate {
    pub const fn name() -> &'static str {
        "prom_native_histogram_rate"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_extrapolated_udf::<true, true>(Self::name(), collector)
    }
}

impl NativeHistogramIncrease {
    pub const fn name() -> &'static str {
        "prom_native_histogram_increase"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_extrapolated_udf::<true, false>(Self::name(), collector)
    }
}

fn native_idelta<const IS_RATE: bool>(
    input: &[ColumnarValue],
    func_name: &'static str,
    collector: Option<PromqlAnnotationCollector>,
) -> DfResult<ColumnarValue> {
    if input.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "{func_name} function should have 2 inputs"
        )));
    }

    let ts_range = extract_range_dict(
        &input[0],
        func_name,
        "timestamp range vector",
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let value_range = extract_range_dict(
        &input[1],
        func_name,
        "value range vector",
        &native_histogram_arrow_type(),
    )?;

    if ts_range.keys().values() != value_range.keys().values() {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: timestamp and value ranges should have the same window layout"
        )));
    }

    let ts_values = ts_range
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("validated timestamp range")
        .values();
    let histograms = value_range
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("validated native histogram range");
    let mut result = Vec::with_capacity(ts_range.keys().len());

    for key in ts_range.keys().values() {
        let (offset, length) = unpack(*key);
        let offset = offset as usize;
        let length = length as usize;
        if length < 2 {
            result.push(None);
            continue;
        }

        let mut samples = Vec::with_capacity(2);
        let mut has_null = false;
        for row in offset + length - 2..offset + length {
            let Some(histogram) = read_histogram(histograms, row)? else {
                has_null = true;
                break;
            };
            samples.push(histogram);
        }
        if has_null {
            result.push(None);
            continue;
        }

        let sampled_interval_secs =
            (ts_values[offset + length - 1] - ts_values[offset + length - 2]) as f64 / 1000.0;
        if sampled_interval_secs <= 0.0 {
            result.push(None);
            continue;
        }
        record_custom_reconciliation(&collector, func_name, &samples[0], &samples[1]);
        let value = idelta_value(
            &samples,
            IS_RATE,
            ts_values[offset + length - 2],
            ts_values[offset + length - 1],
            sampled_interval_secs,
        );
        if value.is_none() {
            record_warning(
                &collector,
                format!("{func_name}: dropped native histogram range with incompatible schemas"),
            );
        }
        result.push(value);
    }

    Ok(ColumnarValue::Array(build_histogram_array(&result)))
}

fn create_native_idelta_udf<const IS_RATE: bool>(
    name: &'static str,
    collector: Option<PromqlAnnotationCollector>,
) -> ScalarUDF {
    create_udf(
        name,
        vec![
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            RangeArray::convert_data_type(native_histogram_arrow_type()),
        ],
        native_histogram_arrow_type(),
        Volatility::Volatile,
        Arc::new(move |input: &[ColumnarValue]| {
            native_idelta::<IS_RATE>(input, name, collector.clone())
        }) as _,
    )
}

pub struct NativeHistogramIDelta;
pub struct NativeHistogramIRate;

impl NativeHistogramIDelta {
    pub const fn name() -> &'static str {
        "prom_native_histogram_idelta"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_idelta_udf::<false>(Self::name(), collector)
    }
}

impl NativeHistogramIRate {
    pub const fn name() -> &'static str {
        "prom_native_histogram_irate"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        create_native_idelta_udf::<true>(Self::name(), collector)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;

    fn sample_histogram(count: f64, sum: f64, positive_buckets: Vec<f64>) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            sum,
            reset_hint: 0,
            start_timestamp: None,
            custom_values: Vec::new(),
            positive_spans: vec![Span {
                offset: 0,
                length: positive_buckets.len() as u32,
            }],
            negative_spans: Vec::new(),
            count,
            zero_count: 0.0,
            positive_buckets,
            negative_buckets: Vec::new(),
        }
    }

    fn run_scalar_udf(udf: ScalarUDF, input: Vec<ColumnarValue>) -> f64 {
        let result = run_udf(udf, input, DataType::Float64);
        extract_array(&result)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    fn run_udf(udf: ScalarUDF, input: Vec<ColumnarValue>, return_type: DataType) -> ColumnarValue {
        let arg_fields = input
            .iter()
            .enumerate()
            .map(|(idx, input)| Arc::new(Field::new(format!("arg_{idx}"), input.data_type(), true)))
            .collect();
        let args = ScalarFunctionArgs {
            args: input,
            arg_fields,
            number_rows: 1,
            return_field: Arc::new(Field::new("result", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        udf.invoke_with_args(args).unwrap()
    }

    fn run_histogram_udf(udf: ScalarUDF, input: Vec<ColumnarValue>) -> NativeHistogram {
        let result = run_udf(udf, input, native_histogram_arrow_type());
        let array = extract_array(&result)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        read_histogram(&array, 0).unwrap().unwrap()
    }

    fn run_histogram_range_udf(
        udf: ScalarUDF,
        histograms: Vec<NativeHistogram>,
    ) -> NativeHistogram {
        let values = histograms.into_iter().map(Some).collect::<Vec<_>>();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            (0..values.len()).map(|idx| Some((idx as i64 + 1) * 1000)),
        ));
        let histograms = build_histogram_array(&values);
        let range = [(0, values.len() as u32)];
        let ts_range = RangeArray::from_ranges(timestamps, range).unwrap();
        let value_range = RangeArray::from_ranges(histograms, range).unwrap();

        run_histogram_udf(
            udf,
            vec![
                ColumnarValue::Array(Arc::new(ts_range.into_dict())),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
            ],
        )
    }

    #[test]
    fn count_sum_and_avg_read_struct() {
        let histograms = vec![Some(sample_histogram(6.0, 10.0, vec![2.0, 4.0]))];
        let array = build_histogram_array(&histograms);
        let input = vec![ColumnarValue::Array(array)];

        let count = run_scalar_udf(NativeHistogramCount::scalar_udf(), input.clone());
        assert_eq!(count, 6.0);

        let sum = run_scalar_udf(NativeHistogramSum::scalar_udf(), input.clone());
        assert_eq!(sum, 10.0);

        let avg = run_scalar_udf(NativeHistogramAvg::scalar_udf(), input);
        assert_eq!(avg, 10.0 / 6.0);
    }

    #[test]
    fn quantile_uses_bucket_bounds() {
        let histogram = sample_histogram(6.0, 10.0, vec![2.0, 4.0]);
        assert_eq!(histogram.quantile(0.0), 0.5);
        assert!(histogram.quantile(0.5) > 1.0);
        assert!(histogram.quantile(0.5) < 2.0);
    }

    #[test]
    fn comparison_uses_promql_histogram_equality() {
        let mut left = sample_histogram(1.0, 1.0, vec![1.0, 0.0]);
        left.reset_hint = COUNTER_RESET_HINT;
        left.start_timestamp = Some(1000);
        let mut right = sample_histogram(1.0, 1.0, vec![1.0]);
        right.reset_hint = NOT_COUNTER_RESET_HINT;
        right.start_timestamp = Some(2000);

        let result = run_udf(
            NativeHistogramEq::scalar_udf(),
            vec![
                ColumnarValue::Array(build_histogram_array(&[Some(left)])),
                ColumnarValue::Array(build_histogram_array(&[Some(right)])),
            ],
            DataType::Boolean,
        );
        let values = extract_array(&result).unwrap();
        let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(values.value(0));
    }

    #[test]
    fn unary_minus_returns_gauge_histogram() {
        let result = run_histogram_udf(
            NativeHistogramNeg::scalar_udf(),
            vec![ColumnarValue::Array(build_histogram_array(&[Some(
                sample_histogram(2.0, 3.0, vec![2.0]),
            )]))],
        );

        assert_eq!(result.reset_hint, GAUGE_RESET_HINT);
        assert_eq!(result.count, -2.0);
        assert_eq!(result.sum, -3.0);
        assert_eq!(result.positive_buckets, vec![-2.0]);
    }

    #[test]
    fn histogram_over_time_functions_return_gauge_histograms() {
        let mut first = sample_histogram(1.0, 1.0, vec![1.0]);
        first.reset_hint = COUNTER_RESET_HINT;
        let mut second = sample_histogram(2.0, 2.0, vec![2.0]);
        second.reset_hint = COUNTER_RESET_HINT;

        let result = run_histogram_range_udf(
            NativeHistogramSumOverTime::scalar_udf(),
            vec![first.clone(), second.clone()],
        );
        assert_eq!(result.reset_hint, GAUGE_RESET_HINT);
        assert_eq!(result.count, 3.0);
        assert_eq!(result.sum, 3.0);
        assert_eq!(result.positive_buckets, vec![3.0]);

        let result = run_histogram_range_udf(
            NativeHistogramAvgOverTime::scalar_udf(),
            vec![first.clone(), second.clone()],
        );
        assert_eq!(result.reset_hint, GAUGE_RESET_HINT);
        assert_eq!(result.count, 1.5);
        assert_eq!(result.sum, 1.5);
        assert_eq!(result.positive_buckets, vec![1.5]);

        let result = run_histogram_range_udf(
            NativeHistogramLastOverTime::scalar_udf(),
            vec![first, second],
        );
        assert_eq!(result.reset_hint, GAUGE_RESET_HINT);
        assert_eq!(result.count, 2.0);
        assert_eq!(result.sum, 2.0);
        assert_eq!(result.positive_buckets, vec![2.0]);
    }

    #[test]
    fn absent_over_time_handles_histogram_ranges() {
        let values = vec![Some(sample_histogram(1.0, 1.0, vec![1.0]))];
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([Some(1000)]));
        let histograms = build_histogram_array(&values);
        let ranges = [(0, 1), (0, 0)];
        let ts_range = RangeArray::from_ranges(timestamps, ranges).unwrap();
        let value_range = RangeArray::from_ranges(histograms, ranges).unwrap();

        let result = run_udf(
            NativeHistogramAbsentOverTime::scalar_udf(),
            vec![
                ColumnarValue::Array(Arc::new(ts_range.into_dict())),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
            ],
            DataType::Float64,
        );
        let result = extract_array(&result).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!(result.is_null(0));
        assert_eq!(result.value(1), 1.0);
    }

    #[test]
    fn delta_requires_exact_layout() {
        let first = sample_histogram(2.0, 3.0, vec![1.0, 1.0]);
        let last = sample_histogram(5.0, 8.0, vec![2.0, 3.0]);
        let delta = histogram_delta(&[first, last], &[0, 1], false).unwrap();
        assert_eq!(delta.count, 3.0);
        assert_eq!(delta.sum, 5.0);
        assert_eq!(delta.reset_hint, GAUGE_RESET_HINT);
        assert_eq!(delta.positive_buckets, vec![1.0, 2.0]);
    }

    #[test]
    fn reset_hint_shortcuts_detection() {
        let previous = sample_histogram(6.0, 10.0, vec![2.0, 4.0]);

        let mut current = sample_histogram(7.0, 12.0, vec![3.0, 4.0]);
        current.reset_hint = COUNTER_RESET_HINT;
        assert!(current.detect_reset(&previous));

        let mut current = sample_histogram(5.0, 8.0, vec![1.0, 4.0]);
        current.reset_hint = NOT_COUNTER_RESET_HINT;
        assert!(!current.detect_reset(&previous));
    }

    #[test]
    fn start_timestamp_detects_counter_reset() {
        let first = sample_histogram(6.0, 10.0, vec![2.0, 4.0]);
        let mut last = sample_histogram(7.0, 12.0, vec![3.0, 4.0]);
        last.start_timestamp = Some(1500);

        let delta = histogram_delta(&[first.clone(), last.clone()], &[1000, 2000], true).unwrap();
        assert_eq!(delta.count, 7.0);
        assert_eq!(delta.sum, 12.0);

        let idelta = idelta_value(&[first, last], true, 1000, 2000, 1.0).unwrap();
        assert_eq!(idelta.count, 7.0);
        assert_eq!(idelta.sum, 12.0);
    }
}
