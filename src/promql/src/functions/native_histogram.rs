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

use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;

use common_query::native_histogram::*;
use common_query::prometheus::format_prometheus_float;
use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Float64Builder, Int64Array, StringBuilder,
    StructArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, create_udaf, create_udf};

use crate::functions::{
    AvgOverTime, Deriv, DoubleExponentialSmoothing, IDelta, Increase, LastOverTime, MaxOverTime,
    MinOverTime, PredictLinear, QuantileOverTime, Rate, StddevOverTime, StdvarOverTime,
    SumOverTime, extract_array, extract_range_dict,
};
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
    BooleanTrue,
    BooleanFalse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AnnotationLevel {
    Info,
    Warning,
}

impl AnnotationReturn {
    fn data_type(self) -> DataType {
        match self {
            Self::FloatNull => DataType::Float64,
            Self::BooleanTrue | Self::BooleanFalse => DataType::Boolean,
        }
    }

    fn scalar_value(self) -> ScalarValue {
        match self {
            Self::FloatNull => ScalarValue::Float64(None),
            Self::BooleanTrue => ScalarValue::Boolean(Some(true)),
            Self::BooleanFalse => ScalarValue::Boolean(Some(false)),
        }
    }
}

#[derive(Debug, Clone)]
struct NativeHistogramAnnotationUdf {
    name: &'static str,
    signature: Signature,
    return_kind: AnnotationReturn,
    level: AnnotationLevel,
    message: String,
    collector: Option<PromqlAnnotationCollector>,
}

impl NativeHistogramAnnotationUdf {
    fn new(
        name: &'static str,
        return_kind: AnnotationReturn,
        level: AnnotationLevel,
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> Self {
        Self {
            name,
            signature: Signature::variadic_any(Volatility::Volatile),
            return_kind,
            level,
            message,
            collector,
        }
    }
}

impl PartialEq for NativeHistogramAnnotationUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.return_kind == other.return_kind
            && self.level == other.level
            && self.message == other.message
    }
}

impl Eq for NativeHistogramAnnotationUdf {}

impl Hash for NativeHistogramAnnotationUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.return_kind.hash(state);
        self.level.hash(state);
        self.message.hash(state);
    }
}

impl ScalarUDFImpl for NativeHistogramAnnotationUdf {
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
        let has_dropped_sample = !args.args.is_empty()
            && (0..args.number_rows).any(|row| {
                args.args.iter().all(|arg| match arg {
                    ColumnarValue::Array(array) => array.is_valid(row),
                    ColumnarValue::Scalar(value) => !value.is_null(),
                })
            });
        if has_dropped_sample
            && let Some(collector) = args
                .config_options
                .extensions
                .get::<PromqlAnnotationCollector>()
                .cloned()
                .or_else(|| self.collector.clone())
        {
            match self.level {
                AnnotationLevel::Info => collector.record_info(self.message.clone()),
                AnnotationLevel::Warning => collector.record_warning(self.message.clone()),
            }
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

    const fn bool_true_name() -> &'static str {
        "prom_native_histogram_keep_bool"
    }

    pub fn float_null_udf(
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> ScalarUDF {
        ScalarUDF::new_from_impl(NativeHistogramAnnotationUdf::new(
            Self::float_null_name(),
            AnnotationReturn::FloatNull,
            AnnotationLevel::Info,
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
            AnnotationLevel::Info,
            message,
            collector,
        ))
    }

    pub fn bool_true_udf(
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> ScalarUDF {
        ScalarUDF::new_from_impl(NativeHistogramAnnotationUdf::new(
            Self::bool_true_name(),
            AnnotationReturn::BooleanTrue,
            AnnotationLevel::Info,
            message,
            collector,
        ))
    }

    pub fn warning_bool_false_udf(
        message: String,
        collector: Option<PromqlAnnotationCollector>,
    ) -> ScalarUDF {
        ScalarUDF::new_from_impl(NativeHistogramAnnotationUdf::new(
            Self::bool_false_name(),
            AnnotationReturn::BooleanFalse,
            AnnotationLevel::Warning,
            message,
            collector,
        ))
    }
}

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
        record_counter_reset_contradiction_warning(collector, name);
    }
}

fn record_counter_reset_contradiction_warning(
    collector: &Option<PromqlAnnotationCollector>,
    name: &'static str,
) {
    record_warning(
        collector,
        format!("{name}: native histogram counter reset hints contradict"),
    );
}

fn scalar_histogram_udf<F>(
    name: &'static str,
    extra_input_types: Vec<DataType>,
    calc: F,
) -> ScalarUDF
where
    F: Fn(&NativeHistogram, &[ColumnarValue], usize, usize, &'static str) -> DfResult<f64>
        + Send
        + Sync
        + 'static,
{
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
                            record_counter_reset_contradiction(&collector, name, &lhs, &rhs);
                            let result = op(&lhs, &rhs);
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

/// Formats float samples as PromQL label values.
pub struct PromqlFloatToString;

impl PromqlFloatToString {
    pub const fn name() -> &'static str {
        "prom_float_to_string"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            vec![DataType::Float64],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(|input: &[ColumnarValue]| {
                let values = extract_array(&input[0])?;
                let values = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("validated Float64 input");
                let mut result = StringBuilder::new();
                for value in values.iter() {
                    match value {
                        Some(value) => result.append_value(format_prometheus_float(value)),
                        None => result.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(result.finish())))
            }),
        )
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
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        scalar_histogram_udf(
            Self::name(),
            vec![DataType::Float64],
            move |histogram, input, row, len, name| {
                let q = read_scalar_f64_arg(&input[1], row, len, name)?;
                let (value, info) = histogram.quantile_with_info(q);
                if let Some(info) = info {
                    let message = match info {
                        NativeHistogramQuantileInfo::NaNSkew => {
                            "input to histogram_quantile has NaN observations, result is skewed higher"
                        }
                        NativeHistogramQuantileInfo::NaNResult => {
                            "input to histogram_quantile has NaN observations, result is NaN"
                        }
                    };
                    record_info(&collector, message);
                }
                Ok(value)
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
        Self::scalar_udf_with_collector(None)
    }

    pub fn scalar_udf_with_collector(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        scalar_histogram_udf(
            Self::name(),
            vec![DataType::Float64, DataType::Float64],
            move |histogram, input, row, len, name| {
                let lower = read_scalar_f64_arg(&input[1], row, len, name)?;
                let upper = read_scalar_f64_arg(&input[2], row, len, name)?;
                let (value, excluded_nans) = histogram.fraction_with_info(lower, upper);
                if excluded_nans {
                    record_info(
                        &collector,
                        "input to histogram_fraction has NaN observations, which are excluded from all fractions",
                    );
                }
                Ok(value)
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
    counter_reset_seen: bool,
    not_counter_reset_seen: bool,
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
            counter_reset_seen: false,
            not_counter_reset_seen: false,
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

    fn observe_reset_hints(&mut self, counter_reset_seen: bool, not_counter_reset_seen: bool) {
        self.counter_reset_seen |= counter_reset_seen;
        self.not_counter_reset_seen |= not_counter_reset_seen;
        if self.counter_reset_seen && self.not_counter_reset_seen {
            record_counter_reset_contradiction_warning(&self.collector, self.kind.name());
        }
    }

    fn push_histogram(&mut self, histogram: NativeHistogram, count: u64) -> DfResult<()> {
        if self.kind.needs_count() && count == 0 {
            return Ok(());
        }

        self.observe_reset_hints(
            histogram.reset_hint == COUNTER_RESET_HINT,
            histogram.reset_hint == NOT_COUNTER_RESET_HINT,
        );
        if self.dropped_incompatible {
            return Ok(());
        }
        let combined_count = if self.kind.needs_count() {
            self.count.checked_add(count).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: native histogram sample count overflow",
                    self.kind.name()
                ))
            })?
        } else {
            self.count
        };
        let value = match self.value.take() {
            Some(value) => {
                record_custom_reconciliation(&self.collector, self.kind.name(), &value, &histogram);
                let combined = match self.kind {
                    NativeHistogramAggregateKind::Sum => value.add(&histogram),
                    NativeHistogramAggregateKind::Avg => {
                        weighted_histogram_mean(value, self.count, histogram, count, combined_count)
                    }
                };
                match combined {
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
            self.count = combined_count;
        }
        Ok(())
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

fn weighted_histogram_mean(
    left: NativeHistogram,
    left_count: u64,
    right: NativeHistogram,
    right_count: u64,
    total_count: u64,
) -> Option<NativeHistogram> {
    let total_count = total_count as f64;
    left.scale(left_count as f64 / total_count)
        .add(&right.scale(right_count as f64 / total_count))
}

fn range_fold_histograms(
    samples: Vec<NativeHistogram>,
    kind: NativeHistogramAggregateKind,
    name: &'static str,
    collector: &Option<PromqlAnnotationCollector>,
) -> Option<NativeHistogram> {
    if samples
        .iter()
        .any(|histogram| histogram.reset_hint == COUNTER_RESET_HINT)
        && samples
            .iter()
            .any(|histogram| histogram.reset_hint == NOT_COUNTER_RESET_HINT)
    {
        record_counter_reset_contradiction_warning(collector, name);
    }

    let mut value = None;
    let mut count = 0u64;
    for histogram in samples {
        value = match value {
            Some(value) => {
                record_custom_reconciliation(collector, name, &value, &histogram);
                let next_count = count.checked_add(1)?;
                let combined = match kind {
                    NativeHistogramAggregateKind::Sum => value.add(&histogram),
                    NativeHistogramAggregateKind::Avg => {
                        weighted_histogram_mean(value, count, histogram, 1, next_count)
                    }
                };
                match combined {
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
        count = count.checked_add(1)?;
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
        if matches!(kind, NativeHistogramRangeHistogramKind::Last) {
            let histogram = if (offset..offset + length).any(|row| histograms.is_null(row)) {
                None
            } else {
                read_histogram(histograms, offset + length - 1)?
            };
            result.push(histogram);
            continue;
        }
        let Some(samples) = collect_window_histograms(histograms, offset, length)? else {
            result.push(None);
            continue;
        };
        let histogram = match kind {
            NativeHistogramRangeHistogramKind::Sum => range_fold_histograms(
                samples,
                NativeHistogramAggregateKind::Sum,
                func_name,
                &collector,
            ),
            NativeHistogramRangeHistogramKind::Avg => range_fold_histograms(
                samples,
                NativeHistogramAggregateKind::Avg,
                func_name,
                &collector,
            ),
            NativeHistogramRangeHistogramKind::Last => samples.last().cloned(),
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
        if matches!(kind, NativeHistogramRangeFloatKind::Absent) {
            result.append_null();
            continue;
        }
        if matches!(
            kind,
            NativeHistogramRangeFloatKind::Count | NativeHistogramRangeFloatKind::Present
        ) {
            if (offset..offset + length).any(|row| histograms.is_null(row)) {
                result.append_null();
            } else if matches!(kind, NativeHistogramRangeFloatKind::Count) {
                result.append_value(length as f64);
            } else {
                result.append_value(1.0);
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
                    (pair[0].reset_hint == GAUGE_RESET_HINT)
                        != (pair[1].reset_hint == GAUGE_RESET_HINT)
                        || pair[1].detect_counter_reset(&pair[0], ts_pair[0], ts_pair[1])
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

/// Coordinated float/native-histogram range evaluation.
///
/// The function name is passed as the first scalar argument so these two UDF names are enough for
/// distributed plan decoding. The remaining leading arguments are timestamp, float, and histogram
/// ranges, followed by the ordinary function arguments.
pub struct MixedRange;

impl MixedRange {
    const fn float_name() -> &'static str {
        "prom_mixed_range_float"
    }

    const fn histogram_name() -> &'static str {
        "prom_mixed_range_histogram"
    }

    pub fn float_udf(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        ScalarUDF::new_from_impl(MixedRangeUdf::new(MixedRangeOutput::Float, collector))
    }

    pub fn histogram_udf(collector: Option<PromqlAnnotationCollector>) -> ScalarUDF {
        ScalarUDF::new_from_impl(MixedRangeUdf::new(MixedRangeOutput::Histogram, collector))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MixedRangeOutput {
    Float,
    Histogram,
}

impl MixedRangeOutput {
    fn name(self) -> &'static str {
        match self {
            Self::Float => MixedRange::float_name(),
            Self::Histogram => MixedRange::histogram_name(),
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Float => DataType::Float64,
            Self::Histogram => native_histogram_arrow_type(),
        }
    }
}

#[derive(Debug, Clone)]
struct MixedRangeUdf {
    output: MixedRangeOutput,
    signature: Signature,
    collector: Option<PromqlAnnotationCollector>,
}

impl MixedRangeUdf {
    fn new(output: MixedRangeOutput, collector: Option<PromqlAnnotationCollector>) -> Self {
        Self {
            output,
            signature: Signature::variadic_any(Volatility::Volatile),
            collector,
        }
    }
}

impl PartialEq for MixedRangeUdf {
    fn eq(&self, other: &Self) -> bool {
        self.output == other.output
    }
}

impl Eq for MixedRangeUdf {}

impl Hash for MixedRangeUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.output.hash(state);
    }
}

impl ScalarUDFImpl for MixedRangeUdf {
    fn name(&self) -> &str {
        self.output.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.output.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let collector = args
            .config_options
            .extensions
            .get::<PromqlAnnotationCollector>()
            .cloned()
            .or_else(|| self.collector.clone());
        mixed_range(&args, self.output, collector)
    }
}

#[derive(Debug, Clone, Copy)]
enum MixedRangeFunction {
    Rate,
    Increase,
    Delta,
    IDelta,
    IRate,
    Changes,
    Resets,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    SumOverTime,
    CountOverTime,
    LastOverTime,
    AbsentOverTime,
    PresentOverTime,
    StddevOverTime,
    StdvarOverTime,
    QuantileOverTime,
    Deriv,
    PredictLinear,
    DoubleExponentialSmoothing,
    HoltWinters,
}

impl MixedRangeFunction {
    fn parse(value: &ColumnarValue) -> DfResult<Self> {
        let name = match value {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(name)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(name)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(name))) => name.as_str(),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "mixed range function name must be a non-null string scalar, found {}",
                    other.data_type()
                )));
            }
        };

        match name {
            "rate" => Ok(Self::Rate),
            "increase" => Ok(Self::Increase),
            "delta" => Ok(Self::Delta),
            "idelta" => Ok(Self::IDelta),
            "irate" => Ok(Self::IRate),
            "changes" => Ok(Self::Changes),
            "resets" => Ok(Self::Resets),
            "avg_over_time" => Ok(Self::AvgOverTime),
            "min_over_time" => Ok(Self::MinOverTime),
            "max_over_time" => Ok(Self::MaxOverTime),
            "sum_over_time" => Ok(Self::SumOverTime),
            "count_over_time" => Ok(Self::CountOverTime),
            "last_over_time" => Ok(Self::LastOverTime),
            "absent_over_time" => Ok(Self::AbsentOverTime),
            "present_over_time" => Ok(Self::PresentOverTime),
            "stddev_over_time" => Ok(Self::StddevOverTime),
            "stdvar_over_time" => Ok(Self::StdvarOverTime),
            "quantile_over_time" => Ok(Self::QuantileOverTime),
            "deriv" => Ok(Self::Deriv),
            "predict_linear" => Ok(Self::PredictLinear),
            "double_exponential_smoothing" => Ok(Self::DoubleExponentialSmoothing),
            "holt_winters" => Ok(Self::HoltWinters),
            _ => Err(DataFusionError::Execution(format!(
                "unsupported mixed range function: {name}"
            ))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Rate => "rate",
            Self::Increase => "increase",
            Self::Delta => "delta",
            Self::IDelta => "idelta",
            Self::IRate => "irate",
            Self::Changes => "changes",
            Self::Resets => "resets",
            Self::AvgOverTime => "avg_over_time",
            Self::MinOverTime => "min_over_time",
            Self::MaxOverTime => "max_over_time",
            Self::SumOverTime => "sum_over_time",
            Self::CountOverTime => "count_over_time",
            Self::LastOverTime => "last_over_time",
            Self::AbsentOverTime => "absent_over_time",
            Self::PresentOverTime => "present_over_time",
            Self::StddevOverTime => "stddev_over_time",
            Self::StdvarOverTime => "stdvar_over_time",
            Self::QuantileOverTime => "quantile_over_time",
            Self::Deriv => "deriv",
            Self::PredictLinear => "predict_linear",
            Self::DoubleExponentialSmoothing => "double_exponential_smoothing",
            Self::HoltWinters => "holt_winters",
        }
    }

    fn policy(self) -> MixedRangePolicy {
        match self {
            Self::Rate | Self::Increase | Self::Delta | Self::AvgOverTime | Self::SumOverTime => {
                MixedRangePolicy::DropMixed
            }
            Self::IDelta | Self::IRate => MixedRangePolicy::LastTwo,
            Self::LastOverTime => MixedRangePolicy::Last,
            Self::Changes
            | Self::Resets
            | Self::CountOverTime
            | Self::AbsentOverTime
            | Self::PresentOverTime => MixedRangePolicy::Combined,
            Self::MinOverTime
            | Self::MaxOverTime
            | Self::StddevOverTime
            | Self::StdvarOverTime
            | Self::QuantileOverTime
            | Self::Deriv
            | Self::PredictLinear
            | Self::DoubleExponentialSmoothing
            | Self::HoltWinters => MixedRangePolicy::FloatOnly,
        }
    }

    fn float_udf(self) -> Option<ScalarUDF> {
        match self {
            Self::Rate => Some(Rate::scalar_udf()),
            Self::Increase => Some(Increase::scalar_udf()),
            Self::Delta => Some(crate::functions::Delta::scalar_udf()),
            Self::IDelta => Some(IDelta::<false>::scalar_udf()),
            Self::IRate => Some(IDelta::<true>::scalar_udf()),
            Self::AvgOverTime => Some(AvgOverTime::scalar_udf()),
            Self::MinOverTime => Some(MinOverTime::scalar_udf()),
            Self::MaxOverTime => Some(MaxOverTime::scalar_udf()),
            Self::SumOverTime => Some(SumOverTime::scalar_udf()),
            Self::LastOverTime => Some(LastOverTime::scalar_udf()),
            Self::StddevOverTime => Some(StddevOverTime::scalar_udf()),
            Self::StdvarOverTime => Some(StdvarOverTime::scalar_udf()),
            Self::QuantileOverTime => Some(QuantileOverTime::scalar_udf()),
            Self::Deriv => Some(Deriv::scalar_udf()),
            Self::PredictLinear => Some(PredictLinear::scalar_udf()),
            Self::DoubleExponentialSmoothing | Self::HoltWinters => {
                Some(DoubleExponentialSmoothing::scalar_udf())
            }
            Self::Changes
            | Self::Resets
            | Self::CountOverTime
            | Self::AbsentOverTime
            | Self::PresentOverTime => None,
        }
    }

    fn histogram_udf(self, collector: Option<PromqlAnnotationCollector>) -> Option<ScalarUDF> {
        match self {
            Self::Rate => Some(NativeHistogramRate::scalar_udf_with_collector(collector)),
            Self::Increase => Some(NativeHistogramIncrease::scalar_udf_with_collector(
                collector,
            )),
            Self::Delta => Some(NativeHistogramDelta::scalar_udf_with_collector(collector)),
            Self::IDelta => Some(NativeHistogramIDelta::scalar_udf_with_collector(collector)),
            Self::IRate => Some(NativeHistogramIRate::scalar_udf_with_collector(collector)),
            Self::AvgOverTime => Some(NativeHistogramAvgOverTime::scalar_udf_with_collector(
                collector,
            )),
            Self::SumOverTime => Some(NativeHistogramSumOverTime::scalar_udf_with_collector(
                collector,
            )),
            Self::LastOverTime => Some(NativeHistogramLastOverTime::scalar_udf()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum MixedRangePolicy {
    DropMixed,
    LastTwo,
    Last,
    Combined,
    FloatOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleLane {
    Float,
    Histogram,
}

fn mixed_range(
    args: &ScalarFunctionArgs,
    output: MixedRangeOutput,
    collector: Option<PromqlAnnotationCollector>,
) -> DfResult<ColumnarValue> {
    if args.args.len() < 4 {
        return Err(DataFusionError::Plan(format!(
            "{} function should have at least 4 inputs",
            output.name()
        )));
    }
    let function = MixedRangeFunction::parse(&args.args[0])?;
    let name = function.name();
    let ts_range = extract_range_dict(
        &args.args[1],
        name,
        "timestamp range vector",
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let float_range = extract_range_dict(
        &args.args[2],
        name,
        "float range vector",
        &DataType::Float64,
    )?;
    let histogram_range = extract_range_dict(
        &args.args[3],
        name,
        "native histogram range vector",
        &native_histogram_arrow_type(),
    )?;
    let keys = ts_range.keys().values();
    if float_range.keys().values() != keys || histogram_range.keys().values() != keys {
        return Err(DataFusionError::Execution(format!(
            "{name}: timestamp, float, and native histogram ranges should have the same layout"
        )));
    }
    if args.number_rows != keys.len() {
        return Err(DataFusionError::Execution(format!(
            "{name}: range inputs have {} windows but the batch has {} rows",
            keys.len(),
            args.number_rows
        )));
    }

    let timestamps = ts_range
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("validated timestamp range");
    let floats = float_range
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated float range");
    let histograms = histogram_range
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("validated native histogram range");
    if floats.len() != timestamps.len() || histograms.len() != timestamps.len() {
        return Err(DataFusionError::Execution(format!(
            "{name}: timestamp, float, and native histogram values should be row-aligned"
        )));
    }

    let bounds = checked_window_bounds(keys, timestamps.len(), name)?;
    let float_valid = (0..floats.len())
        .map(|row| floats.is_valid(row))
        .collect::<Vec<_>>();
    let histogram_valid = (0..histograms.len())
        .map(|row| histograms.is_valid(row))
        .collect::<Vec<_>>();
    let float_prefix = validity_prefix(&float_valid, name)?;
    let histogram_prefix = validity_prefix(&histogram_valid, name)?;

    if matches!(function.policy(), MixedRangePolicy::Combined) {
        if output != MixedRangeOutput::Float {
            return Err(DataFusionError::Execution(format!(
                "{name} does not return native histograms"
            )));
        }
        return combined_range_float(
            function,
            &bounds,
            timestamps,
            floats,
            histograms,
            &float_valid,
            &histogram_valid,
            &float_prefix,
            &histogram_prefix,
        );
    }

    let selections = select_mixed_range_lanes(
        function,
        &bounds,
        timestamps,
        &float_valid,
        &histogram_valid,
        &float_prefix,
        &histogram_prefix,
        &collector,
    );
    let (lane, values, valid, prefix) = match output {
        MixedRangeOutput::Float => (
            SampleLane::Float,
            floats as &dyn Array,
            float_valid.as_slice(),
            float_prefix.as_slice(),
        ),
        MixedRangeOutput::Histogram => (
            SampleLane::Histogram,
            histograms as &dyn Array,
            histogram_valid.as_slice(),
            histogram_prefix.as_slice(),
        ),
    };
    let input = compact_lane_input(
        timestamps,
        values,
        valid,
        prefix,
        &bounds,
        &selections,
        lane,
        name,
    )?;

    let udf = match output {
        MixedRangeOutput::Float => function.float_udf(),
        MixedRangeOutput::Histogram => function.histogram_udf(collector),
    }
    .ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{name} does not return {} values",
            match output {
                MixedRangeOutput::Float => "float",
                MixedRangeOutput::Histogram => "native histogram",
            }
        ))
    })?;
    let mut input = input;
    input.extend_from_slice(&args.args[4..]);
    invoke_range_udf(udf, input, args, bounds.len())
}

/// Decodes packed range keys into validated half-open `(offset, end)` bounds.
fn checked_window_bounds(
    keys: &[i64],
    value_len: usize,
    name: &str,
) -> DfResult<Vec<(usize, usize)>> {
    keys.iter()
        .map(|key| {
            let (offset, length) = unpack(*key);
            let offset = offset as usize;
            let end = offset
                .checked_add(length as usize)
                .filter(|end| *end <= value_len)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{name}: invalid range ({offset}, {length}) for {value_len} values"
                    ))
                })?;
            Ok((offset, end))
        })
        .collect()
}

/// Builds prefix counts where `prefix[i]` is the number of valid samples in `valid[..i]`.
fn validity_prefix(valid: &[bool], name: &str) -> DfResult<Vec<usize>> {
    let capacity = valid.len().checked_add(1).ok_or_else(|| {
        DataFusionError::Execution(format!("{name}: sample validity length overflow"))
    })?;
    let mut prefix = Vec::with_capacity(capacity);
    prefix.push(0usize);
    for is_valid in valid {
        prefix.push(
            prefix
                .last()
                .copied()
                .unwrap()
                .checked_add(usize::from(*is_valid))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{name}: sample count overflow"))
                })?,
        );
    }
    Ok(prefix)
}

/// Returns the number of valid samples in the half-open window `[offset, end)`.
fn valid_count(prefix: &[usize], offset: usize, end: usize) -> usize {
    prefix[end]
        .checked_sub(prefix[offset])
        .expect("validity prefix is monotonic")
}

/// Selects the sample lane for each window and records policy-required annotations.
#[allow(clippy::too_many_arguments)]
fn select_mixed_range_lanes(
    function: MixedRangeFunction,
    bounds: &[(usize, usize)],
    timestamps: &TimestampMillisecondArray,
    float_valid: &[bool],
    histogram_valid: &[bool],
    float_prefix: &[usize],
    histogram_prefix: &[usize],
    collector: &Option<PromqlAnnotationCollector>,
) -> Vec<Option<SampleLane>> {
    bounds
        .iter()
        .map(|(offset, end)| {
            let float_count = valid_count(float_prefix, *offset, *end);
            let histogram_count = valid_count(histogram_prefix, *offset, *end);
            match function.policy() {
                MixedRangePolicy::DropMixed => match (float_count > 0, histogram_count > 0) {
                    (true, true) => {
                        record_warning(
                            collector,
                            format!(
                                "{}: encountered a mix of float and native histogram samples",
                                function.name()
                            ),
                        );
                        None
                    }
                    (true, false) => Some(SampleLane::Float),
                    (false, true) => Some(SampleLane::Histogram),
                    (false, false) => None,
                },
                MixedRangePolicy::FloatOnly => {
                    if float_count > 0 && histogram_count > 0 {
                        record_info(
                            collector,
                            format!(
                                "{}: ignored native histogram samples",
                                function.name()
                            ),
                        );
                    }
                    (float_count > 0).then_some(SampleLane::Float)
                }
                MixedRangePolicy::Last => (*offset..*end).rev().find_map(|row| {
                    if histogram_valid[row] {
                        Some(SampleLane::Histogram)
                    } else if float_valid[row] {
                        Some(SampleLane::Float)
                    } else {
                        None
                    }
                }),
                MixedRangePolicy::LastTwo => {
                    let mut last_two = Vec::with_capacity(2);
                    for row in (*offset..*end).rev() {
                        // Prometheus keeps a float as the newest sample if both lanes have the
                        // same timestamp, while the histogram becomes the preceding sample.
                        if float_valid[row] {
                            last_two.push((SampleLane::Float, timestamps.value(row)));
                        }
                        if last_two.len() < 2 && histogram_valid[row] {
                            last_two.push((SampleLane::Histogram, timestamps.value(row)));
                        }
                        if last_two.len() == 2 {
                            break;
                        }
                    }
                    if last_two.len() < 2 || last_two[0].1 == last_two[1].1 {
                        None
                    } else if last_two[0].0 == last_two[1].0 {
                        Some(last_two[0].0)
                    } else {
                        record_warning(
                            collector,
                            format!(
                                "{}: encountered a mix of float and native histogram samples in the last two points",
                                function.name()
                            ),
                        );
                        None
                    }
                }
                MixedRangePolicy::Combined => unreachable!(),
            }
        })
        .collect()
}

/// Filters null placeholders from one sample lane and remaps its selected windows.
/// Windows assigned to the other lane become empty.
#[allow(clippy::too_many_arguments)]
fn compact_lane_input(
    timestamps: &TimestampMillisecondArray,
    values: &dyn Array,
    valid: &[bool],
    prefix: &[usize],
    bounds: &[(usize, usize)],
    selections: &[Option<SampleLane>],
    lane: SampleLane,
    name: &str,
) -> DfResult<Vec<ColumnarValue>> {
    let mask = BooleanArray::from(valid.to_vec());
    let filtered_timestamps = filter(timestamps, &mask)?;
    let filtered_values = filter(values, &mask)?;
    let ranges = bounds
        .iter()
        .zip(selections)
        .map(|((offset, end), selected)| {
            let compact_offset = prefix[*offset];
            let compact_length = if *selected == Some(lane) {
                valid_count(prefix, *offset, *end)
            } else {
                0
            };
            Ok((
                u32::try_from(compact_offset).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "{name}: compacted range offset exceeds u32"
                    ))
                })?,
                u32::try_from(compact_length).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "{name}: compacted range length exceeds u32"
                    ))
                })?,
            ))
        })
        .collect::<DfResult<Vec<_>>>()?;
    let timestamp_range = RangeArray::from_ranges(filtered_timestamps, ranges.clone())
        .map_err(DataFusionError::from)?;
    let value_range =
        RangeArray::from_ranges(filtered_values, ranges).map_err(DataFusionError::from)?;
    Ok(vec![
        ColumnarValue::Array(Arc::new(timestamp_range.into_dict())),
        ColumnarValue::Array(Arc::new(value_range.into_dict())),
    ])
}

fn invoke_range_udf(
    udf: ScalarUDF,
    input: Vec<ColumnarValue>,
    outer_args: &ScalarFunctionArgs,
    number_rows: usize,
) -> DfResult<ColumnarValue> {
    let arg_fields = input
        .iter()
        .enumerate()
        .map(|(index, value)| Arc::new(Field::new(format!("arg_{index}"), value.data_type(), true)))
        .collect();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: input,
        arg_fields,
        number_rows,
        return_field: outer_args.return_field.clone(),
        config_options: outer_args.config_options.clone(),
    })
}

enum MixedSample {
    Float(f64),
    Histogram(NativeHistogram),
}

#[allow(clippy::too_many_arguments)]
fn combined_range_float(
    function: MixedRangeFunction,
    bounds: &[(usize, usize)],
    timestamps: &TimestampMillisecondArray,
    floats: &Float64Array,
    histograms: &StructArray,
    float_valid: &[bool],
    histogram_valid: &[bool],
    float_prefix: &[usize],
    histogram_prefix: &[usize],
) -> DfResult<ColumnarValue> {
    let mut result = Float64Builder::with_capacity(bounds.len());
    for (offset, end) in bounds {
        let sample_count = valid_count(float_prefix, *offset, *end)
            .checked_add(valid_count(histogram_prefix, *offset, *end))
            .ok_or_else(|| {
                DataFusionError::Execution(format!("{}: sample count overflow", function.name()))
            })?;
        match function {
            MixedRangeFunction::CountOverTime => {
                if sample_count == 0 {
                    result.append_null();
                } else {
                    result.append_value(sample_count as f64);
                }
            }
            MixedRangeFunction::AbsentOverTime => {
                if sample_count == 0 {
                    result.append_value(1.0);
                } else {
                    result.append_null();
                }
            }
            MixedRangeFunction::PresentOverTime => {
                if sample_count == 0 {
                    result.append_null();
                } else {
                    result.append_value(1.0);
                }
            }
            MixedRangeFunction::Changes | MixedRangeFunction::Resets => {
                if sample_count == 0 {
                    result.append_null();
                    continue;
                }
                let mut count = 0usize;
                let mut previous = None;
                for row in *offset..*end {
                    if float_valid[row] {
                        count += mixed_transition(
                            function,
                            &mut previous,
                            timestamps.value(row),
                            MixedSample::Float(floats.value(row)),
                        );
                    }
                    if histogram_valid[row] {
                        let histogram = read_histogram(histograms, row)?
                            .expect("validated native histogram sample");
                        count += mixed_transition(
                            function,
                            &mut previous,
                            timestamps.value(row),
                            MixedSample::Histogram(histogram),
                        );
                    }
                }
                result.append_value(count as f64);
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} does not support combined range evaluation",
                    function.name()
                )));
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(result.finish())))
}

fn mixed_transition(
    function: MixedRangeFunction,
    previous: &mut Option<(i64, MixedSample)>,
    timestamp: i64,
    current: MixedSample,
) -> usize {
    let changed = previous.as_ref().is_some_and(|(previous_ts, previous)| {
        match (function, previous, &current) {
            (
                MixedRangeFunction::Changes,
                MixedSample::Float(previous),
                MixedSample::Float(current),
            ) => current != previous && !(current.is_nan() && previous.is_nan()),
            (
                MixedRangeFunction::Changes,
                MixedSample::Histogram(previous),
                MixedSample::Histogram(current),
            ) => !current.promql_eq(previous),
            (MixedRangeFunction::Changes, _, _) => true,
            (
                MixedRangeFunction::Resets,
                MixedSample::Float(previous),
                MixedSample::Float(current),
            ) => current < previous,
            (
                MixedRangeFunction::Resets,
                MixedSample::Histogram(previous),
                MixedSample::Histogram(current),
            ) => {
                (previous.reset_hint == GAUGE_RESET_HINT)
                    != (current.reset_hint == GAUGE_RESET_HINT)
                    || current.detect_counter_reset(previous, *previous_ts, timestamp)
            }
            (MixedRangeFunction::Resets, _, _) => true,
            _ => unreachable!(),
        }
    });
    *previous = Some((timestamp, current));
    usize::from(changed)
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
            DataType::Boolean,
            DataType::Boolean,
        ]
    } else {
        vec![
            native_histogram_arrow_type(),
            DataType::Boolean,
            DataType::Boolean,
            DataType::Boolean,
        ]
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
            self.push_histogram(histogram, 1)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let histogram = match (self.kind, self.dropped_incompatible, self.value.clone()) {
            (_, true, _) => None,
            (_, false, None) => None,
            (NativeHistogramAggregateKind::Sum, false, value) => value,
            (NativeHistogramAggregateKind::Avg, false, Some(value)) if self.count > 0 => {
                Some(value)
            }
            (NativeHistogramAggregateKind::Avg, _, _) => None,
        };

        Ok(native_histogram_scalar(histogram))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.value.as_ref().map_or(0, |histogram| {
                histogram.custom_values.capacity() * size_of::<f64>()
                    + histogram.positive_spans.capacity() * size_of::<Span>()
                    + histogram.negative_spans.capacity() * size_of::<Span>()
                    + histogram.positive_buckets.capacity() * size_of::<f64>()
                    + histogram.negative_buckets.capacity() * size_of::<f64>()
            })
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let mut state = vec![native_histogram_scalar(self.value.clone())];
        if self.kind.needs_count() {
            state.push(ScalarValue::UInt64(Some(self.count)));
        }
        state.push(ScalarValue::Boolean(Some(self.dropped_incompatible)));
        state.push(ScalarValue::Boolean(Some(self.counter_reset_seen)));
        state.push(ScalarValue::Boolean(Some(self.not_counter_reset_seen)));
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
        let counter_reset_seen = states
            .get(dropped_index + 1)
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expected Boolean counter reset state",
                    self.kind.name()
                ))
            })?;
        let not_counter_reset_seen = states
            .get(dropped_index + 2)
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expected Boolean not-counter-reset state",
                    self.kind.name()
                ))
            })?;

        for row in 0..histograms.len() {
            self.observe_reset_hints(
                counter_reset_seen.value(row),
                not_counter_reset_seen.value(row),
            );
            if dropped.value(row) {
                self.mark_incompatible();
            }
            if self.dropped_incompatible {
                continue;
            }
            let Some(histogram) = read_histogram(histograms, row)? else {
                continue;
            };
            let count = counts.map(|counts| counts.value(row)).unwrap_or(1);
            self.push_histogram(histogram, count)?;
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

    let first_reset = samples[1].detect_counter_reset(&samples[0], timestamps[0], timestamps[1]);
    let (initial, reset_scan_start) = if first_reset {
        // The first sample is irrelevant after an immediate reset. Adopt the
        // second sample's layout so an incompatible pre-reset layout is ignored.
        (samples[1].zero_like(), 2)
    } else {
        (samples[0].clone(), 1)
    };
    let mut result = samples.last()?.sub(&initial)?;
    for index in reset_scan_start..samples.len() {
        if samples[index].detect_counter_reset(
            &samples[index - 1],
            timestamps[index - 1],
            timestamps[index],
        ) {
            result = result.add(&samples[index - 1])?;
        }
    }
    Some(result.into_gauge())
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
        if length == 0 {
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

        let first_ts = all_timestamps[offset];
        let last_ts = all_timestamps[offset + length - 1];
        let range_end = eval_ts.value(index);
        let range_start = range_end - range_length;
        let synthetic_zero_timestamp = IS_COUNTER
            .then_some(samples[0].start_timestamp)
            .flatten()
            .filter(|start| *start != 0 && range_start < *start && *start < first_ts);
        let synthetic_zero_start = synthetic_zero_timestamp.is_some();
        if length < 2 && !synthetic_zero_start {
            result.push(None);
            continue;
        }

        let wrong_flavor = if IS_COUNTER {
            samples
                .iter()
                .any(|histogram| histogram.reset_hint == GAUGE_RESET_HINT)
        } else {
            samples[0].reset_hint != GAUGE_RESET_HINT
                || samples[samples.len() - 1].reset_hint != GAUGE_RESET_HINT
        };
        if wrong_flavor {
            let expected = if IS_COUNTER { "counter" } else { "gauge" };
            record_warning(
                &collector,
                format!("{func_name}: native histogram input should be a {expected} histogram"),
            );
        }

        let timestamps = &all_timestamps[offset..offset + length];
        for pair in samples.windows(2) {
            record_custom_reconciliation(&collector, func_name, &pair[0], &pair[1]);
        }
        let mut histogram = if length == 1 {
            samples[0].clone()
        } else if let Some(histogram) = histogram_delta(&samples, timestamps, IS_COUNTER) {
            histogram
        } else {
            record_warning(
                &collector,
                format!("{func_name}: dropped native histogram range with incompatible schemas"),
            );
            result.push(None);
            continue;
        };
        if synthetic_zero_start && length > 1 {
            let Some(with_synthetic_zero) = histogram.add(&samples[0]) else {
                record_warning(
                    &collector,
                    format!(
                        "{func_name}: dropped native histogram range with incompatible schemas"
                    ),
                );
                result.push(None);
                continue;
            };
            histogram = with_synthetic_zero;
        }

        let real_sampled_interval_ms = (last_ts - first_ts) as f64;
        let sampled_interval_ms = synthetic_zero_timestamp
            .map(|start| (last_ts - start) as f64)
            .unwrap_or(real_sampled_interval_ms);
        if sampled_interval_ms <= 0.0 {
            result.push(None);
            continue;
        }
        let average_interval_ms = if length > 1 {
            real_sampled_interval_ms / (length - 1) as f64
        } else {
            0.0
        };
        let mut duration_to_start_ms = if synthetic_zero_start {
            0.0
        } else {
            (first_ts - range_start) as f64
        };
        let duration_to_end_ms = (range_end - last_ts) as f64;

        if IS_COUNTER && !synthetic_zero_start && histogram.count > 0.0 && samples[0].count >= 0.0 {
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

        let wrong_flavor = samples.iter().any(|histogram| {
            if IS_RATE {
                histogram.reset_hint == GAUGE_RESET_HINT
            } else {
                histogram.reset_hint != GAUGE_RESET_HINT
            }
        });
        if wrong_flavor {
            let expected = if IS_RATE { "counter" } else { "gauge" };
            record_warning(
                &collector,
                format!("{func_name}: native histogram input should be a {expected} histogram"),
            );
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
            reset_hint: UNKNOWN_COUNTER_RESET_HINT,
            start_timestamp: None,
            custom_values: Vec::new(),
            positive_spans: vec![Span {
                offset: 0,
                length: positive_buckets.len() as i32,
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

    fn evaluated_histogram(
        accumulator: &mut NativeHistogramAggregateAccumulator,
    ) -> NativeHistogram {
        let ScalarValue::Struct(array) = accumulator.evaluate().unwrap() else {
            panic!("native histogram accumulator returned a non-struct value");
        };
        read_histogram(&array, 0).unwrap().unwrap()
    }

    fn histogram_range_input(values: Vec<Option<NativeHistogram>>) -> Vec<ColumnarValue> {
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            (0..values.len()).map(|idx| Some((idx as i64 + 1) * 1000)),
        ));
        let histograms = build_histogram_array(&values);
        let range = [(0, values.len() as u32)];
        let ts_range = RangeArray::from_ranges(timestamps, range).unwrap();
        let value_range = RangeArray::from_ranges(histograms, range).unwrap();

        vec![
            ColumnarValue::Array(Arc::new(ts_range.into_dict())),
            ColumnarValue::Array(Arc::new(value_range.into_dict())),
        ]
    }

    fn mixed_range_input(
        name: &str,
        floats: Vec<Option<f64>>,
        histograms: Vec<Option<NativeHistogram>>,
    ) -> Vec<ColumnarValue> {
        assert_eq!(floats.len(), histograms.len());
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            (0..floats.len()).map(|idx| Some((idx as i64 + 1) * 1000)),
        ));
        let floats = Arc::new(Float64Array::from(floats));
        let histograms = build_histogram_array(&histograms);
        let range = [(0, u32::try_from(floats.len()).unwrap())];
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(name.to_string()))),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(timestamps, range)
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(floats, range).unwrap().into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(histograms, range)
                    .unwrap()
                    .into_dict(),
            )),
        ]
    }

    fn mixed_float_result(udf: ScalarUDF, input: Vec<ColumnarValue>) -> Option<f64> {
        let result = run_udf(udf, input, DataType::Float64);
        let result = extract_array(&result).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        result.is_valid(0).then(|| result.value(0))
    }

    fn mixed_histogram_result(
        udf: ScalarUDF,
        input: Vec<ColumnarValue>,
    ) -> Option<NativeHistogram> {
        let result = run_udf(udf, input, native_histogram_arrow_type());
        let result = extract_array(&result).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        read_histogram(result, 0).unwrap()
    }

    fn run_histogram_range_udf(
        udf: ScalarUDF,
        histograms: Vec<NativeHistogram>,
    ) -> NativeHistogram {
        run_histogram_udf(
            udf,
            histogram_range_input(histograms.into_iter().map(Some).collect()),
        )
    }

    fn run_float_range_udf(
        udf: ScalarUDF,
        histograms: Vec<Option<NativeHistogram>>,
    ) -> Option<f64> {
        let result = run_udf(udf, histogram_range_input(histograms), DataType::Float64);
        let result = extract_array(&result).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        (!result.is_null(0)).then(|| result.value(0))
    }

    fn run_extrapolated_histogram_udf(
        udf: ScalarUDF,
        histograms: Vec<NativeHistogram>,
    ) -> NativeHistogram {
        let range_length = histograms.len() as i64 * 1000;
        let timestamps = (0..histograms.len())
            .map(|idx| (idx as i64 + 1) * 1000)
            .collect();
        extrapolated_histogram_result(udf, timestamps, histograms, range_length, range_length)
            .unwrap()
    }

    fn extrapolated_histogram_result(
        udf: ScalarUDF,
        timestamps: Vec<i64>,
        histograms: Vec<NativeHistogram>,
        range_end: i64,
        range_length: i64,
    ) -> Option<NativeHistogram> {
        assert_eq!(timestamps.len(), histograms.len());
        let range = [(0, u32::try_from(histograms.len()).unwrap())];
        let timestamps = Arc::new(TimestampMillisecondArray::from(timestamps));
        let histograms =
            build_histogram_array(&histograms.into_iter().map(Some).collect::<Vec<_>>());
        let mut input = vec![
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(timestamps, range)
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(histograms, range)
                    .unwrap()
                    .into_dict(),
            )),
        ];
        input.push(ColumnarValue::Array(Arc::new(
            TimestampMillisecondArray::from(vec![range_end]),
        )));
        input.push(ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            range_length,
        ]))));
        let result = run_udf(udf, input, native_histogram_arrow_type());
        let result = extract_array(&result).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        read_histogram(result, 0).unwrap()
    }

    fn collected_warnings(collector: &PromqlAnnotationCollector) -> Vec<String> {
        let mut warnings = Vec::new();
        collector.append_to(&mut warnings, &mut Vec::new());
        warnings
    }

    fn collected_infos(collector: &PromqlAnnotationCollector) -> Vec<String> {
        let mut infos = Vec::new();
        collector.append_to(&mut Vec::new(), &mut infos);
        infos
    }

    #[test]
    fn quantile_and_fraction_report_nan_observations() {
        let histogram = sample_histogram(10.0, f64::NAN, vec![8.0]);
        let histogram_arg =
            || ColumnarValue::Array(build_histogram_array(&[Some(histogram.clone())]));

        let quantile_collector = PromqlAnnotationCollector::default();
        let skewed = run_scalar_udf(
            NativeHistogramQuantile::scalar_udf_with_collector(Some(quantile_collector.clone())),
            vec![
                histogram_arg(),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))),
            ],
        );
        assert!(skewed.is_finite());
        let nan = run_scalar_udf(
            NativeHistogramQuantile::scalar_udf_with_collector(Some(quantile_collector.clone())),
            vec![
                histogram_arg(),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(0.9))),
            ],
        );
        assert!(nan.is_nan());
        let infos = collected_infos(&quantile_collector);
        assert!(
            infos
                .iter()
                .any(|info| info.ends_with("result is skewed higher"))
        );
        assert!(infos.iter().any(|info| info.ends_with("result is NaN")));

        let fraction_collector = PromqlAnnotationCollector::default();
        assert_eq!(
            run_scalar_udf(
                NativeHistogramFraction::scalar_udf_with_collector(Some(
                    fraction_collector.clone(),
                )),
                vec![
                    histogram_arg(),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NEG_INFINITY))),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::INFINITY))),
                ],
            ),
            0.8
        );
        assert_eq!(
            collected_infos(&fraction_collector),
            vec![
                "input to histogram_fraction has NaN observations, which are excluded from all fractions"
                    .to_string()
            ]
        );
    }

    #[test]
    fn mixed_ranges_follow_prometheus_sample_type_semantics() {
        let first = sample_histogram(1.0, 1.0, vec![1.0]);
        let second = sample_histogram(3.0, 3.0, vec![3.0]);
        let collector = PromqlAnnotationCollector::default();

        let mut rate = mixed_range_input(
            "rate",
            vec![Some(1.0), None, Some(3.0)],
            vec![None, Some(first.clone()), None],
        );
        rate.push(ColumnarValue::Array(Arc::new(
            TimestampMillisecondArray::from(vec![3000]),
        )));
        rate.push(ColumnarValue::Array(Arc::new(Int64Array::from(vec![3000]))));
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(Some(collector.clone())), rate.clone()),
            None
        );
        assert_eq!(
            mixed_histogram_result(MixedRange::histogram_udf(Some(collector.clone())), rate),
            None
        );
        assert!(
            collected_warnings(&collector)
                .iter()
                .any(|warning| warning.contains("mix of float and native histogram"))
        );

        let pure_rate = |floats, histograms| {
            let mut input = mixed_range_input("rate", floats, histograms);
            input.push(ColumnarValue::Array(Arc::new(
                TimestampMillisecondArray::from(vec![3000]),
            )));
            input.push(ColumnarValue::Array(Arc::new(Int64Array::from(vec![3000]))));
            input
        };
        let pure_float = pure_rate(
            vec![Some(1.0), Some(2.0), Some(3.0)],
            vec![None, None, None],
        );
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(None), pure_float.clone()),
            Some(1.0)
        );
        assert_eq!(
            mixed_histogram_result(MixedRange::histogram_udf(None), pure_float),
            None
        );
        let pure_histogram = pure_rate(
            vec![None, None, None],
            vec![
                Some(sample_histogram(1.0, 1.0, vec![1.0])),
                Some(sample_histogram(2.0, 2.0, vec![2.0])),
                Some(sample_histogram(3.0, 3.0, vec![3.0])),
            ],
        );
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(None), pure_histogram.clone()),
            None
        );
        assert_eq!(
            mixed_histogram_result(MixedRange::histogram_udf(None), pure_histogram)
                .unwrap()
                .count,
            1.0
        );

        let idelta = mixed_range_input(
            "idelta",
            vec![Some(10.0), None, None],
            vec![None, Some(first.clone()), Some(second.clone())],
        );
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(None), idelta.clone()),
            None
        );
        assert_eq!(
            mixed_histogram_result(MixedRange::histogram_udf(None), idelta)
                .unwrap()
                .count,
            2.0
        );

        let alternating = || {
            mixed_range_input(
                "changes",
                vec![Some(1.0), None, None, Some(1.0)],
                vec![None, Some(first.clone()), Some(first.clone()), None],
            )
        };
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(None), alternating()),
            Some(2.0)
        );
        let mut resets = alternating();
        resets[0] = ColumnarValue::Scalar(ScalarValue::Utf8(Some("resets".to_string())));
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(None), resets),
            Some(2.0)
        );

        for (name, expected) in [
            ("count_over_time", Some(4.0)),
            ("present_over_time", Some(1.0)),
            ("absent_over_time", None),
        ] {
            let mut input = alternating();
            input[0] = ColumnarValue::Scalar(ScalarValue::Utf8(Some(name.to_string())));
            assert_eq!(
                mixed_float_result(MixedRange::float_udf(None), input),
                expected,
                "{name}"
            );
        }

        let last = mixed_range_input(
            "last_over_time",
            vec![None, Some(4.0)],
            vec![Some(first.clone()), None],
        );
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(None), last.clone()),
            Some(4.0)
        );
        assert_eq!(
            mixed_histogram_result(MixedRange::histogram_udf(None), last),
            None
        );

        let collector = PromqlAnnotationCollector::default();
        let histogram_only_min =
            mixed_range_input("min_over_time", vec![None], vec![Some(first.clone())]);
        assert_eq!(
            mixed_float_result(
                MixedRange::float_udf(Some(collector.clone())),
                histogram_only_min,
            ),
            None
        );
        assert!(collected_infos(&collector).is_empty());

        let min = mixed_range_input(
            "min_over_time",
            vec![Some(3.0), None, Some(1.0)],
            vec![None, Some(first), None],
        );
        assert_eq!(
            mixed_float_result(MixedRange::float_udf(Some(collector.clone())), min),
            Some(1.0)
        );
        assert!(
            collected_infos(&collector)
                .iter()
                .any(|info| info.contains("ignored native histogram"))
        );
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
    fn comparison_observes_explicit_sparse_zero_buckets() {
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
        assert!(!values.value(0));
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
    fn histogram_over_time_functions_preserve_reset_hints() {
        let mut first = sample_histogram(1.0, 1.0, vec![1.0]);
        first.reset_hint = COUNTER_RESET_HINT;
        let mut second = sample_histogram(2.0, 2.0, vec![2.0]);
        second.reset_hint = COUNTER_RESET_HINT;

        let result = run_histogram_range_udf(
            NativeHistogramSumOverTime::scalar_udf(),
            vec![first.clone(), second.clone()],
        );
        assert_eq!(result.reset_hint, COUNTER_RESET_HINT);
        assert_eq!(result.count, 3.0);
        assert_eq!(result.sum, 3.0);
        assert_eq!(result.positive_buckets, vec![3.0]);

        let result = run_histogram_range_udf(
            NativeHistogramAvgOverTime::scalar_udf(),
            vec![first.clone(), second.clone()],
        );
        assert_eq!(result.reset_hint, COUNTER_RESET_HINT);
        assert_eq!(result.count, 1.5);
        assert_eq!(result.sum, 1.5);
        assert_eq!(result.positive_buckets, vec![1.5]);

        let result = run_histogram_range_udf(
            NativeHistogramLastOverTime::scalar_udf(),
            vec![first, second],
        );
        assert_eq!(result.reset_hint, COUNTER_RESET_HINT);
        assert_eq!(result.count, 2.0);
        assert_eq!(result.sum, 2.0);
        assert_eq!(result.positive_buckets, vec![2.0]);
    }

    #[test]
    fn histogram_averages_avoid_sum_overflow() {
        let large = sample_histogram(1.0e308, 1.0e308, vec![1.0e308]);

        let range_average = run_histogram_range_udf(
            NativeHistogramAvgOverTime::scalar_udf(),
            vec![large.clone(), large.clone()],
        );
        assert_eq!(range_average.count, 1.0e308);
        assert_eq!(range_average.sum, 1.0e308);
        assert_eq!(range_average.positive_buckets, vec![1.0e308]);

        let mut aggregate =
            NativeHistogramAggregateAccumulator::new(NativeHistogramAggregateKind::Avg, None);
        aggregate.push_histogram(large.clone(), 1).unwrap();
        aggregate.push_histogram(large, 1).unwrap();
        let aggregate_average = evaluated_histogram(&mut aggregate);
        assert_eq!(aggregate_average.count, 1.0e308);
        assert_eq!(aggregate_average.sum, 1.0e308);
        assert_eq!(aggregate_average.positive_buckets, vec![1.0e308]);
    }

    #[test]
    fn histogram_average_partial_states_are_weighted() {
        let mut first =
            NativeHistogramAggregateAccumulator::new(NativeHistogramAggregateKind::Avg, None);
        first
            .push_histogram(sample_histogram(1.0, 1.0, vec![1.0]), 1)
            .unwrap();
        first
            .push_histogram(sample_histogram(3.0, 3.0, vec![3.0]), 1)
            .unwrap();
        let first_state = first
            .state()
            .unwrap()
            .into_iter()
            .map(|value| value.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();

        let mut second =
            NativeHistogramAggregateAccumulator::new(NativeHistogramAggregateKind::Avg, None);
        second
            .push_histogram(sample_histogram(8.0, 8.0, vec![8.0]), 1)
            .unwrap();
        let second_state = second
            .state()
            .unwrap()
            .into_iter()
            .map(|value| value.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();

        let mut merged =
            NativeHistogramAggregateAccumulator::new(NativeHistogramAggregateKind::Avg, None);
        merged.merge_batch(&first_state).unwrap();
        merged.merge_batch(&second_state).unwrap();
        let average = evaluated_histogram(&mut merged);
        assert_eq!(average.count, 4.0);
        assert_eq!(average.sum, 4.0);
        assert_eq!(average.positive_buckets, vec![4.0]);
    }

    #[test]
    fn histogram_average_rejects_sample_count_overflow() {
        let mut aggregate =
            NativeHistogramAggregateAccumulator::new(NativeHistogramAggregateKind::Avg, None);
        aggregate.value = Some(sample_histogram(1.0, 1.0, vec![1.0]));
        aggregate.count = u64::MAX;

        let error = aggregate
            .push_histogram(sample_histogram(1.0, 1.0, vec![1.0]), 1)
            .unwrap_err();
        assert!(error.to_string().contains("sample count overflow"));
    }

    #[test]
    fn presence_only_range_functions_preserve_null_semantics() {
        let first = sample_histogram(1.0, 1.0, vec![1.0]);
        let second = sample_histogram(2.0, 2.0, vec![2.0]);
        assert_eq!(
            run_float_range_udf(
                NativeHistogramCountOverTime::scalar_udf(),
                vec![Some(first.clone()), Some(second.clone())],
            ),
            Some(2.0)
        );
        assert_eq!(
            run_float_range_udf(
                NativeHistogramPresentOverTime::scalar_udf(),
                vec![Some(first.clone()), Some(second.clone())],
            ),
            Some(1.0)
        );
        assert_eq!(
            run_float_range_udf(
                NativeHistogramCountOverTime::scalar_udf(),
                vec![None, Some(second.clone())],
            ),
            None
        );

        let result = run_udf(
            NativeHistogramLastOverTime::scalar_udf(),
            histogram_range_input(vec![None, Some(second)]),
            native_histogram_arrow_type(),
        );
        let result = extract_array(&result).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn resets_counts_histogram_flavor_transitions() {
        let mut counter = sample_histogram(1.0, 1.0, vec![1.0]);
        counter.reset_hint = NOT_COUNTER_RESET_HINT;
        let mut gauge = sample_histogram(2.0, 2.0, vec![2.0]);
        gauge.reset_hint = GAUGE_RESET_HINT;
        assert_eq!(
            run_float_range_udf(
                NativeHistogramResets::scalar_udf(),
                vec![Some(counter), Some(gauge)],
            ),
            Some(1.0)
        );

        let mut gauge = sample_histogram(1.0, 1.0, vec![1.0]);
        gauge.reset_hint = GAUGE_RESET_HINT;
        let mut counter = sample_histogram(2.0, 2.0, vec![2.0]);
        counter.reset_hint = NOT_COUNTER_RESET_HINT;
        assert_eq!(
            run_float_range_udf(
                NativeHistogramResets::scalar_udf(),
                vec![Some(gauge), Some(counter)],
            ),
            Some(1.0)
        );
    }

    #[test]
    fn wrong_flavor_functions_record_warnings() {
        let mut gauge_first = sample_histogram(1.0, 1.0, vec![1.0]);
        gauge_first.reset_hint = GAUGE_RESET_HINT;
        let mut gauge_last = sample_histogram(2.0, 2.0, vec![2.0]);
        gauge_last.reset_hint = GAUGE_RESET_HINT;
        let mut counter_first = sample_histogram(1.0, 1.0, vec![1.0]);
        counter_first.reset_hint = NOT_COUNTER_RESET_HINT;
        let mut counter_last = sample_histogram(2.0, 2.0, vec![2.0]);
        counter_last.reset_hint = NOT_COUNTER_RESET_HINT;
        let collector = PromqlAnnotationCollector::default();

        run_extrapolated_histogram_udf(
            NativeHistogramRate::scalar_udf_with_collector(Some(collector.clone())),
            vec![gauge_first.clone(), gauge_last.clone()],
        );
        run_extrapolated_histogram_udf(
            NativeHistogramDelta::scalar_udf_with_collector(Some(collector.clone())),
            vec![counter_first.clone(), counter_last.clone()],
        );
        run_histogram_range_udf(
            NativeHistogramIRate::scalar_udf_with_collector(Some(collector.clone())),
            vec![gauge_first, gauge_last],
        );
        run_histogram_range_udf(
            NativeHistogramIDelta::scalar_udf_with_collector(Some(collector.clone())),
            vec![counter_first, counter_last],
        );

        let warnings = collected_warnings(&collector);
        for expected in [
            format!(
                "{}: native histogram input should be a counter histogram",
                NativeHistogramRate::name()
            ),
            format!(
                "{}: native histogram input should be a gauge histogram",
                NativeHistogramDelta::name()
            ),
            format!(
                "{}: native histogram input should be a counter histogram",
                NativeHistogramIRate::name()
            ),
            format!(
                "{}: native histogram input should be a gauge histogram",
                NativeHistogramIDelta::name()
            ),
        ] {
            assert!(warnings.contains(&expected), "missing warning: {expected}");
        }
    }

    #[test]
    fn subtraction_records_reset_hint_contradictions_for_incompatible_histograms() {
        for incompatible in [false, true] {
            let mut left = sample_histogram(2.0, 2.0, vec![2.0]);
            left.reset_hint = COUNTER_RESET_HINT;
            let mut right = sample_histogram(1.0, 1.0, vec![1.0]);
            right.reset_hint = NOT_COUNTER_RESET_HINT;
            if incompatible {
                right.schema = CUSTOM_BUCKETS_SCHEMA;
                right.custom_values = vec![1.0];
            }
            let collector = PromqlAnnotationCollector::default();

            run_udf(
                NativeHistogramSub::scalar_udf_with_collector(Some(collector.clone())),
                vec![
                    ColumnarValue::Array(build_histogram_array(&[Some(left)])),
                    ColumnarValue::Array(build_histogram_array(&[Some(right)])),
                ],
                native_histogram_arrow_type(),
            );

            assert!(collected_warnings(&collector).contains(&format!(
                "{}: native histogram counter reset hints contradict",
                NativeHistogramSub::name()
            )));
        }
    }

    #[test]
    fn counter_reset_hint_history_survives_folds_and_state_merges() {
        let mut reset = sample_histogram(1.0, 1.0, vec![1.0]);
        reset.reset_hint = COUNTER_RESET_HINT;
        let mut unknown = sample_histogram(2.0, 2.0, vec![2.0]);
        unknown.reset_hint = UNKNOWN_COUNTER_RESET_HINT;
        let mut not_reset = sample_histogram(3.0, 3.0, vec![3.0]);
        not_reset.reset_hint = NOT_COUNTER_RESET_HINT;

        let range_collector = PromqlAnnotationCollector::default();
        assert!(
            range_fold_histograms(
                vec![reset.clone(), unknown.clone(), not_reset.clone()],
                NativeHistogramAggregateKind::Sum,
                NativeHistogramSumOverTime::name(),
                &Some(range_collector.clone()),
            )
            .is_some()
        );
        assert!(collected_warnings(&range_collector).contains(&format!(
            "{}: native histogram counter reset hints contradict",
            NativeHistogramSumOverTime::name()
        )));

        for kind in [
            NativeHistogramAggregateKind::Sum,
            NativeHistogramAggregateKind::Avg,
        ] {
            let mut first_partial = NativeHistogramAggregateAccumulator::new(kind, None);
            first_partial.push_histogram(reset.clone(), 1).unwrap();
            first_partial.push_histogram(unknown.clone(), 1).unwrap();
            let first_states = first_partial
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();

            let mut second_partial = NativeHistogramAggregateAccumulator::new(kind, None);
            second_partial.push_histogram(not_reset.clone(), 1).unwrap();
            let second_states = second_partial
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();

            let collector = PromqlAnnotationCollector::default();
            let mut merged =
                NativeHistogramAggregateAccumulator::new(kind, Some(collector.clone()));
            merged.merge_batch(&first_states).unwrap();
            merged.merge_batch(&second_states).unwrap();
            assert!(collected_warnings(&collector).contains(&format!(
                "{}: native histogram counter reset hints contradict",
                kind.name()
            )));
        }
    }

    #[test]
    fn incompatible_aggregates_do_not_hide_reset_hint_contradictions() {
        let mut reset = sample_histogram(1.0, 1.0, vec![1.0]);
        reset.reset_hint = COUNTER_RESET_HINT;
        let mut incompatible_reset = sample_histogram(2.0, 2.0, vec![2.0]);
        incompatible_reset.schema = CUSTOM_BUCKETS_SCHEMA;
        incompatible_reset.custom_values = vec![1.0];
        incompatible_reset.reset_hint = COUNTER_RESET_HINT;
        let mut not_reset = sample_histogram(3.0, 3.0, vec![3.0]);
        not_reset.reset_hint = NOT_COUNTER_RESET_HINT;

        for kind in [
            NativeHistogramAggregateKind::Sum,
            NativeHistogramAggregateKind::Avg,
        ] {
            for histograms in [
                [reset.clone(), incompatible_reset.clone(), not_reset.clone()],
                [reset.clone(), not_reset.clone(), incompatible_reset.clone()],
            ] {
                let collector = PromqlAnnotationCollector::default();
                let mut aggregate =
                    NativeHistogramAggregateAccumulator::new(kind, Some(collector.clone()));
                for histogram in histograms {
                    aggregate.push_histogram(histogram, 1).unwrap();
                }

                assert!(aggregate.dropped_incompatible);
                let warnings = collected_warnings(&collector);
                assert!(warnings.contains(&format!(
                    "{}: dropped native histogram aggregate with incompatible schemas",
                    kind.name()
                )));
                assert!(warnings.contains(&format!(
                    "{}: native histogram counter reset hints contradict",
                    kind.name()
                )));
            }

            let mut dropped_partial = NativeHistogramAggregateAccumulator::new(kind, None);
            dropped_partial.push_histogram(reset.clone(), 1).unwrap();
            dropped_partial
                .push_histogram(incompatible_reset.clone(), 1)
                .unwrap();
            let dropped_states = dropped_partial
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();

            let mut opposing_partial = NativeHistogramAggregateAccumulator::new(kind, None);
            opposing_partial
                .push_histogram(not_reset.clone(), 1)
                .unwrap();
            let opposing_states = opposing_partial
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();

            for (first, second) in [
                (&dropped_states, &opposing_states),
                (&opposing_states, &dropped_states),
            ] {
                let collector = PromqlAnnotationCollector::default();
                let mut merged =
                    NativeHistogramAggregateAccumulator::new(kind, Some(collector.clone()));
                merged.merge_batch(first).unwrap();
                merged.merge_batch(second).unwrap();

                assert!(merged.dropped_incompatible);
                assert!(collected_warnings(&collector).contains(&format!(
                    "{}: native histogram counter reset hints contradict",
                    kind.name()
                )));
            }
        }

        for (kind, name) in [
            (
                NativeHistogramAggregateKind::Sum,
                NativeHistogramSumOverTime::name(),
            ),
            (
                NativeHistogramAggregateKind::Avg,
                NativeHistogramAvgOverTime::name(),
            ),
        ] {
            let collector = PromqlAnnotationCollector::default();
            assert!(
                range_fold_histograms(
                    vec![reset.clone(), incompatible_reset.clone(), not_reset.clone()],
                    kind,
                    name,
                    &Some(collector.clone()),
                )
                .is_none()
            );
            let warnings = collected_warnings(&collector);
            assert!(warnings.contains(&format!(
                "{name}: dropped native histogram range with incompatible schemas"
            )));
            assert!(warnings.contains(&format!(
                "{name}: native histogram counter reset hints contradict"
            )));
        }
    }

    #[test]
    fn histogram_aggregate_accumulator_accounts_for_heap_allocations() {
        let histogram = sample_histogram(3.0, 3.0, vec![1.0, 2.0]);
        let heap_size = histogram.custom_values.capacity() * size_of::<f64>()
            + histogram.positive_spans.capacity() * size_of::<Span>()
            + histogram.negative_spans.capacity() * size_of::<Span>()
            + histogram.positive_buckets.capacity() * size_of::<f64>()
            + histogram.negative_buckets.capacity() * size_of::<f64>();
        let mut accumulator =
            NativeHistogramAggregateAccumulator::new(NativeHistogramAggregateKind::Sum, None);
        let empty_size = accumulator.size();

        accumulator.push_histogram(histogram, 1).unwrap();

        assert!(heap_size > 0);
        assert_eq!(accumulator.size(), empty_size + heap_size);
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

    #[test]
    fn extrapolated_rate_uses_start_timestamp_synthetic_zero() {
        let mut single = sample_histogram(1.0, 1.0, vec![1.0]);
        single.start_timestamp = Some(1_000);

        let rate = extrapolated_histogram_result(
            NativeHistogramRate::scalar_udf(),
            vec![2_000],
            vec![single.clone()],
            3_000,
            3_000,
        )
        .unwrap();
        assert_eq!(rate.count, 1.0 / 3.0);
        assert_eq!(rate.sum, 1.0 / 3.0);

        let increase = extrapolated_histogram_result(
            NativeHistogramIncrease::scalar_udf(),
            vec![2_000],
            vec![single],
            3_000,
            3_000,
        )
        .unwrap();
        assert_eq!(increase.count, 1.0);
        assert_eq!(increase.sum, 1.0);

        let mut first = sample_histogram(2.0, 2.0, vec![2.0]);
        first.start_timestamp = Some(1_000);
        let last = sample_histogram(4.0, 4.0, vec![4.0]);
        let increase = extrapolated_histogram_result(
            NativeHistogramIncrease::scalar_udf(),
            vec![2_000, 3_000],
            vec![first, last],
            3_000,
            3_000,
        )
        .unwrap();
        assert_eq!(increase.count, 4.0);
        assert_eq!(increase.sum, 4.0);
    }

    #[test]
    fn extrapolated_rate_requires_strictly_in_range_start_timestamp() {
        for (start_timestamp, range_end, range_length) in [
            (0, 3_000, 3_000),
            (500, 3_000, 2_000),
            (1_000, 3_000, 2_000),
            (2_000, 3_000, 3_000),
            (2_500, 3_000, 3_000),
        ] {
            let mut sample = sample_histogram(1.0, 1.0, vec![1.0]);
            sample.start_timestamp = Some(start_timestamp);
            assert!(
                extrapolated_histogram_result(
                    NativeHistogramRate::scalar_udf(),
                    vec![2_000],
                    vec![sample],
                    range_end,
                    range_length,
                )
                .is_none(),
                "start_timestamp={start_timestamp}"
            );
        }

        let mut gauge = sample_histogram(1.0, 1.0, vec![1.0]);
        gauge.start_timestamp = Some(1_000);
        gauge.reset_hint = GAUGE_RESET_HINT;
        assert!(
            extrapolated_histogram_result(
                NativeHistogramDelta::scalar_udf(),
                vec![2_000],
                vec![gauge],
                3_000,
                3_000,
            )
            .is_none()
        );
    }

    #[test]
    fn first_reset_ignores_incompatible_pre_reset_layout() {
        let first = sample_histogram(10.0, 10.0, vec![10.0]);
        let second = NativeHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            sum: 2.0,
            reset_hint: COUNTER_RESET_HINT,
            start_timestamp: None,
            custom_values: vec![1.0],
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: Vec::new(),
            count: 2.0,
            zero_count: 0.0,
            positive_buckets: vec![2.0],
            negative_buckets: Vec::new(),
        };

        let delta = histogram_delta(&[first, second], &[1_000, 2_000], true).unwrap();
        assert_eq!(delta.schema, CUSTOM_BUCKETS_SCHEMA);
        assert_eq!(delta.count, 2.0);
        assert_eq!(delta.sum, 2.0);
        assert_eq!(delta.positive_buckets, vec![2.0]);
    }

    #[test]
    fn counter_delta_handles_reset_segment_boundaries() {
        let first = sample_histogram(5.0, 5.0, vec![5.0]);
        let second = sample_histogram(7.0, 7.0, vec![7.0]);
        let mut reset = sample_histogram(2.0, 2.0, vec![2.0]);
        reset.reset_hint = COUNTER_RESET_HINT;
        let last = sample_histogram(4.0, 4.0, vec![4.0]);

        let delta = histogram_delta(
            &[first, second, reset, last],
            &[1_000, 2_000, 3_000, 4_000],
            true,
        )
        .unwrap();
        assert_eq!(delta.count, 6.0);
        assert_eq!(delta.sum, 6.0);
        assert_eq!(delta.positive_buckets, vec![6.0]);
    }
}
