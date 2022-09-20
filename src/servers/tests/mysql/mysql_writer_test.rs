use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use servers::mysql::writer::create_mysql_column_def;

use crate::mysql::{all_datatype_testing_data, TestingData};

#[test]
fn test_create_mysql_column_def() {
    let TestingData {
        column_schemas,
        mysql_columns_def,
        ..
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::try_new(column_schemas.clone()).unwrap());
    let columns_def = create_mysql_column_def(&schema).unwrap();
    assert_eq!(column_schemas.len(), columns_def.len());

    for (i, column_def) in columns_def.iter().enumerate() {
        let column_schema = &column_schemas[i];
        assert_eq!(column_schema.name, column_def.column);
        let expected_coltype = mysql_columns_def[i];
        assert_eq!(column_def.coltype, expected_coltype);
    }

    let column_schemas = vec![ColumnSchema::new(
        "lists",
        ConcreteDataType::list_datatype(ConcreteDataType::string_datatype()),
        true,
    )];
    let schema = Arc::new(Schema::try_new(column_schemas).unwrap());
    assert!(create_mysql_column_def(&schema).is_err());
}
