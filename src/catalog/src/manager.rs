use std::any::Any;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use common_telemetry::info;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::BinaryVector;
use futures_util::StreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::TableEngine;

use super::error::Result;
use crate::catalog::{CatalogList, CatalogProvider, CatalogProviderRef};
use crate::consts::SYSTEM_CATALOG_NAME;
use crate::error::{CatalogNotFoundSnafu, SystemCatalogSnafu, SystemCatalogTypeMismatchSnafu};
use crate::memory::MemoryCatalogProvider;
use crate::system::{decode_system_catalog, Entry, SystemCatalogTable};

/// A `CatalogManager` consists of a system catalog and a bunch of user catalogs.
// TODO(hl): Replace current `memory::new_memory_catalog_list()` with CatalogManager
#[allow(dead_code)]
pub struct CatalogManager {
    system: Arc<SystemCatalogTable>,
    catalogs: Arc<dyn CatalogList>,
    engine: Arc<dyn TableEngine>,
}

impl CatalogList for CatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.register_catalog(name, catalog)
    }

    fn register_catalog_if_absent(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<CatalogProviderRef> {
        todo!()
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut res = self.catalogs.catalog_names();
        res.push(SYSTEM_CATALOG_NAME.to_string());
        res
    }

    fn catalog(&self, _name: &str) -> Option<Arc<dyn CatalogProvider>> {
        todo!()
    }
}

#[allow(dead_code)]
impl CatalogManager {
    pub fn new() -> Self {
        todo!()
    }

    /// Scan all entries from system catalog table
    pub async fn init(&mut self) -> Result<()> {
        self.init_system_catalog();
        let mut system_records = self.system.records().await?;

        while let Some(maybe_records) = system_records.next().await {
            match maybe_records {
                Ok(records) => {
                    self.handle_system_catalog_entries(records)?;
                }
                Err(_e) => {
                    panic!("Error while restore system catalog table during init");
                }
            }
        }

        Ok(())
    }

    fn init_system_catalog(&mut self) {
        todo!("Init system catalog table")
    }

    fn handle_system_catalog_entries(&self, records: RecordBatch) -> Result<()> {
        ensure!(
            records.df_recordbatch.columns().len() >= 2,
            SystemCatalogSnafu {
                msg: format!(
                    "Length mismatch: {}",
                    records.df_recordbatch.columns().len()
                )
            }
        );

        let key = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[0])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[1].data_type().clone(),
            })?;

        let value = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[1])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[1].data_type().clone(),
            })?;

        for (k, v) in key.iter_data().zip(value.iter_data()) {
            let entry = decode_system_catalog(k, v)?;

            match entry {
                Entry::Catalog(c) => {
                    self.catalogs.register_catalog_if_absent(
                        c.catalog_name.clone(),
                        Arc::new(MemoryCatalogProvider::new()),
                    );
                    info!("Register catalog: {}", c.catalog_name);
                }
                Entry::Schema(s) => {
                    let _catalog =
                        self.catalogs
                            .catalog(&s.catalog_name)
                            .context(CatalogNotFoundSnafu {
                                name: &s.catalog_name,
                            })?;
                    todo!("register schema to catalog")
                }
                Entry::Table(_t) => {
                    todo!("register table to schema")
                }
            }
            todo!()
        }

        Ok(())
    }
}
