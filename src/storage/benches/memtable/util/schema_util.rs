use datatypes::type_id::LogicalTypeId;

pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);
