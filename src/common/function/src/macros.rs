/// Ensure current function is invokded under `greptime` catalog.
#[macro_export]
macro_rules! ensure_greptime {
    ($func_ctx: expr) => {{
        use common_catalog::consts::DEFAULT_CATALOG_NAME;
        snafu::ensure!(
            $func_ctx.query_ctx.current_catalog() == DEFAULT_CATALOG_NAME,
            common_query::error::PermissionDeniedSnafu {
                err_msg: format!("current catalog is not {DEFAULT_CATALOG_NAME}")
            }
        );
    }};
}
