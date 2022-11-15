pub mod error;
mod insert;
pub use insert::{
    build_alter_table_request, build_create_expr_from_insertion, column_to_vector,
    find_new_columns, insert_batches, insertion_expr_to_request,
};
