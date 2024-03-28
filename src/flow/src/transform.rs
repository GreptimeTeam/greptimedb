//! Transform Substrait into execution plan

use std::collections::HashMap;

use common_decimal::Decimal128;
use common_time::{Date, Timestamp};
use datafusion_substrait::variation_const::{
    DEFAULT_TYPE_REF, TIMESTAMP_MICRO_TYPE_REF, TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF,
    TIMESTAMP_SECOND_TYPE_REF, UNSIGNED_INTEGER_TYPE_REF,
};
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::value::Value;
use snafu::OptionExt;
use substrait_proto::proto::expression::literal::LiteralType;
use substrait_proto::proto::expression::{Literal, RexType};
use substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait_proto::proto::r#type::Kind;
use substrait_proto::proto::rel::RelType;
use substrait_proto::proto::{plan_rel, Expression, Plan as SubPlan, Rel};

use crate::adapter::error::{Error, NotImplementedSnafu, PlanSnafu, TableNotFoundSnafu};
use crate::expr::{GlobalId, ScalarExpr};
use crate::plan::Plan;

macro_rules! not_impl_err {
    ($($arg:tt)*)  => {
        NotImplementedSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

macro_rules! plan_err {
    ($($arg:tt)*)  => {
        PlanSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

pub struct DataflowContext {
    id_to_name: HashMap<GlobalId, Vec<String>>,
    name_to_id: HashMap<Vec<String>, GlobalId>,
}

impl DataflowContext {
    pub fn new() -> Self {
        Self {
            id_to_name: HashMap::new(),
            name_to_id: HashMap::new(),
        }
    }
    pub fn register_table(&mut self, id: GlobalId, name: Vec<String>) {
        self.id_to_name.insert(id, name.clone());
        self.name_to_id.insert(name, id);
        todo!("Table provider etc.")
    }

    /// Retrieves a GlobalId representing a table previously registered by calling the [register_table] function.
    ///
    /// Returns an error if no table has been registered with the provided names
    pub fn table(&self, name: &Vec<String>) -> Result<GlobalId, Error> {
        self.name_to_id
            .get(name)
            .copied()
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })
    }
}

pub fn from_substrait_plan(ctx: &mut DataflowContext, plan: &SubPlan) -> Result<Plan, Error> {
    // Register function extension
    let function_extension = plan
        .extensions
        .iter()
        .map(|e| match &e.mapping_type {
            Some(ext) => match ext {
                MappingType::ExtensionFunction(ext_f) => Ok((ext_f.function_anchor, &ext_f.name)),
                _ => not_impl_err!("Extension type not supported: {ext:?}"),
            },
            None => not_impl_err!("Cannot parse empty extension"),
        })
        .collect::<Result<HashMap<_, _>, Error>>()?;
    // Parse relations
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => {
                        Ok(from_substrait_rel(ctx, rel, &function_extension)?)
                    },
                    plan_rel::RelType::Root(root) => {
                        Ok(from_substrait_rel(ctx, root.input.as_ref().unwrap(), &function_extension)?)
                    }
                },
                None => plan_err!("Cannot parse plan relation: None")
            }
        },
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )
    }
}

pub fn from_substrait_rel(
    ctx: &mut DataflowContext,
    rel: &Rel,
    extensions: &HashMap<u32, &String>,
) -> Result<Plan, Error> {
    todo!()
}

pub fn from_substrait_rex(
    e: &Expression,
    extensions: &HashMap<u32, &String>,
) -> Result<ScalarExpr, Error> {
    match &e.rex_type {
        Some(RexType::Literal(lit)) => {
            todo!()
        }
        _ => not_impl_err!("unsupported rex_type"),
    }
}

/// Convert a Substrait literal into a Value and its ConcreteDataType (So that we can know type even if the value is null)
pub fn from_substrait_literal(lit: &Literal) -> Result<(Value, CDT), Error> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => (Value::from(*b), CDT::boolean_datatype()),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n as i8), CDT::int8_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u8), CDT::uint8_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n as i16), CDT::int16_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u16), CDT::uint16_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n), CDT::int32_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u32), CDT::uint32_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n), CDT::int64_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u64), CDT::uint64_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::Fp32(f)) => (Value::from(*f), CDT::float32_datatype()),
        Some(LiteralType::Fp64(f)) => (Value::from(*f), CDT::float64_datatype()),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_REF => (
                Value::from(Timestamp::new_second(*t)),
                CDT::timestamp_second_datatype(),
            ),
            TIMESTAMP_MILLI_TYPE_REF => (
                Value::from(Timestamp::new_millisecond(*t)),
                CDT::timestamp_millisecond_datatype(),
            ),
            TIMESTAMP_MICRO_TYPE_REF => (
                Value::from(Timestamp::new_microsecond(*t)),
                CDT::timestamp_microsecond_datatype(),
            ),
            TIMESTAMP_NANO_TYPE_REF => (
                Value::from(Timestamp::new_nanosecond(*t)),
                CDT::timestamp_nanosecond_datatype(),
            ),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::Date(d)) => (Value::from(Date::new(*d)), CDT::date_datatype()),
        Some(LiteralType::String(s)) => (Value::from(s.clone()), CDT::string_datatype()),
        Some(LiteralType::Binary(b)) | Some(LiteralType::FixedBinary(b)) => {
            (Value::from(b.clone()), CDT::binary_datatype())
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d.value.clone().try_into().map_err(|e| {
                PlanSnafu {
                    reason: "Failed to parse decimal value".to_string(),
                }
                .build()
            })?;
            let p: u8 = d.precision.try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal precision: {e}"),
                }
                .build()
            })?;
            let s: i8 = d.scale.try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal scale: {e}"),
                }
                .build()
            })?;
            let value = i128::from_le_bytes(value);
            (
                Value::from(Decimal128::new(value, p, s)),
                CDT::decimal128_datatype(p, s),
            )
        }
        Some(LiteralType::Null(ntype)) => (Value::Null, from_substrait_null(ntype)?),
        _ => not_impl_err!("unsupported literal_type")?,
    };
    Ok(scalar_value)
}

fn from_substrait_null(null_type: &substrait_proto::proto::Type) -> Result<CDT, Error> {
    if let Some(kind) = &null_type.kind {
        match kind {
            Kind::Bool(_) => Ok(CDT::boolean_datatype()),
            Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int8_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint8_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int16_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint16_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int32_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint32_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int64_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint64_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Fp32(_) => Ok(CDT::float32_datatype()),
            Kind::Fp64(_) => Ok(CDT::float64_datatype()),
            Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => Ok(CDT::timestamp_second_datatype()),
                TIMESTAMP_MILLI_TYPE_REF => Ok(CDT::timestamp_millisecond_datatype()),
                TIMESTAMP_MICRO_TYPE_REF => Ok(CDT::timestamp_microsecond_datatype()),
                TIMESTAMP_NANO_TYPE_REF => Ok(CDT::timestamp_nanosecond_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(CDT::date_datatype()),
                DATE_64_TYPE_REF => Ok(CDT::date_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Binary(binary) => Ok(CDT::binary_datatype()),
            Kind::String(string) => Ok(CDT::string_datatype()),
            Kind::Decimal(d) => Ok(CDT::decimal128_datatype(d.precision as u8, d.scale as i8)),
            _ => not_impl_err!("Unsupported Substrait type: {kind:?}"),
        }
    } else {
        not_impl_err!("Null type without kind is not supported")
    }
}
