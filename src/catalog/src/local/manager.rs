use std::any::Any;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use common_telemetry::{debug, info};
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BinaryVector, UInt8Vector};
use futures_util::lock::Mutex;
use futures_util::StreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::TableId;
use table::requests::OpenTableRequest;
use table::table::numbers::NumbersTable;
use table::TableRef;

use crate::consts::{
    INFORMATION_SCHEMA_NAME, MIN_USER_TABLE_ID, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_NAME,
};
use crate::error::Result;
use crate::error::{
    CatalogNotFoundSnafu, CreateTableSnafu, IllegalManagerStateSnafu, OpenTableSnafu,
    ReadSystemCatalogSnafu, SchemaNotFoundSnafu, SystemCatalogSnafu,
    SystemCatalogTypeMismatchSnafu, TableExistsSnafu, TableNotFoundSnafu,
};
use crate::local::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use crate::system::{
    decode_system_catalog, Entry, SystemCatalogTable, TableEntry, ENTRY_TYPE_INDEX, KEY_INDEX,
    VALUE_INDEX,
};
use crate::tables::SystemCatalog;
use crate::{
    format_full_table_name, CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef,
    RegisterSystemTableRequest, RegisterTableRequest, SchemaProvider, DEFAULT_CATALOG_NAME,
    DEFAULT_SCHEMA_NAME,
};

/// A `CatalogManager` consists of a system catalog and a bunch of user catalogs.
pub struct LocalCatalogManager {
    system: Arc<SystemCatalog>,
    catalogs: Arc<MemoryCatalogList>,
    engine: TableEngineRef,
    next_table_id: AtomicU32,
    init_lock: Mutex<bool>,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl LocalCatalogManager {
    /// Create a new [CatalogManager] with given user catalogs and table engine
    pub async fn try_new(engine: TableEngineRef) -> Result<Self> {
        let table = SystemCatalogTable::new(engine.clone()).await?;
        let memory_catalog_list = crate::local::memory::new_memory_catalog_list()?;
        let system_catalog = Arc::new(SystemCatalog::new(
            table,
            memory_catalog_list.clone(),
            engine.clone(),
        ));
        Ok(Self {
            system: system_catalog,
            catalogs: memory_catalog_list,
            engine,
            next_table_id: AtomicU32::new(MIN_USER_TABLE_ID),
            init_lock: Mutex::new(false),
            system_table_requests: Mutex::new(Vec::default()),
        })
    }

    /// Scan all entries from system catalog table
    pub async fn init(&self) -> Result<()> {
        self.init_system_catalog()?;
        let mut system_records = self.system.information_schema.system.records().await?;
        let mut max_table_id = 0;
        while let Some(records) = system_records
            .next()
            .await
            .transpose()
            .context(ReadSystemCatalogSnafu)?
        {
            let table_id = self.handle_system_catalog_entries(records).await?;
            max_table_id = max_table_id.max(table_id);
        }
        info!(
            "All system catalog entries processed, max table id: {}",
            max_table_id
        );
        self.next_table_id
            .store((max_table_id + 1).max(MIN_USER_TABLE_ID), Ordering::Relaxed);
        *self.init_lock.lock().await = true;

        // Processing system table hooks
        let mut sys_table_requests = self.system_table_requests.lock().await;
        for req in sys_table_requests.drain(..) {
            let catalog_name = &req.create_table_request.catalog_name;
            let schema_name = &req.create_table_request.schema_name;
            let table_name = &req.create_table_request.table_name;
            let table_id = req.create_table_request.id;

            let table = if let Some(table) =
                self.table(catalog_name.as_deref(), schema_name.as_deref(), table_name)?
            {
                table
            } else {
                let table = self
                    .engine
                    .create_table(&EngineContext::default(), req.create_table_request.clone())
                    .await
                    .with_context(|_| CreateTableSnafu {
                        table_info: format!(
                            "{}.{}.{}, id: {}",
                            catalog_name.as_deref().unwrap_or(DEFAULT_CATALOG_NAME),
                            schema_name.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME),
                            table_name,
                            table_id,
                        ),
                    })?;
                self.register_table(RegisterTableRequest {
                    catalog: catalog_name.clone(),
                    schema: schema_name.clone(),
                    table_name: table_name.clone(),
                    table_id,
                    table: table.clone(),
                })
                .await?;

                info!("Created and registered system table: {}", table_name);

                table
            };

            if let Some(hook) = req.open_hook {
                (hook)(table)?;
            }
        }

        Ok(())
    }

    fn init_system_catalog(&self) -> Result<()> {
        let system_schema = Arc::new(MemorySchemaProvider::new());
        system_schema.register_table(
            SYSTEM_CATALOG_TABLE_NAME.to_string(),
            self.system.information_schema.system.clone(),
        )?;
        let system_catalog = Arc::new(MemoryCatalogProvider::new());
        system_catalog.register_schema(INFORMATION_SCHEMA_NAME.to_string(), system_schema);
        self.catalogs
            .register_catalog(SYSTEM_CATALOG_NAME.to_string(), system_catalog);

        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        let default_schema = Arc::new(MemorySchemaProvider::new());

        // Add numbers table for test
        // TODO(hl): remove this registration
        let table = Arc::new(NumbersTable::default());
        default_schema.register_table("numbers".to_string(), table)?;

        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema);
        self.catalogs
            .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog);
        Ok(())
    }

    /// Processes records from system catalog table and returns the max table id persisted
    /// in system catalog table.
    async fn handle_system_catalog_entries(&self, records: RecordBatch) -> Result<TableId> {
        ensure!(
            records.df_recordbatch.columns().len() >= 6,
            SystemCatalogSnafu {
                msg: format!(
                    "Length mismatch: {}",
                    records.df_recordbatch.columns().len()
                )
            }
        );

        let entry_type = UInt8Vector::try_from_arrow_array(&records.df_recordbatch.columns()[0])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[ENTRY_TYPE_INDEX]
                    .data_type()
                    .clone(),
            })?;

        let key = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[1])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[KEY_INDEX]
                    .data_type()
                    .clone(),
            })?;

        let value = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[3])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[VALUE_INDEX]
                    .data_type()
                    .clone(),
            })?;

        let mut max_table_id = 0;
        for ((t, k), v) in entry_type
            .iter_data()
            .zip(key.iter_data())
            .zip(value.iter_data())
        {
            let entry = decode_system_catalog(t, k, v)?;
            match entry {
                Entry::Catalog(c) => {
                    self.catalogs.register_catalog_if_absent(
                        c.catalog_name.clone(),
                        Arc::new(MemoryCatalogProvider::new()),
                    );
                    info!("Register catalog: {}", c.catalog_name);
                }
                Entry::Schema(s) => {
                    let catalog =
                        self.catalogs
                            .catalog(&s.catalog_name)
                            .context(CatalogNotFoundSnafu {
                                catalog_name: &s.catalog_name,
                            })?;
                    catalog.register_schema(
                        s.schema_name.clone(),
                        Arc::new(MemorySchemaProvider::new()),
                    );
                    info!("Registered schema: {:?}", s);
                }
                Entry::Table(t) => {
                    debug!("t: {:?}", t);
                    self.open_and_register_table(&t).await?;
                    info!("Registered table: {:?}", t);
                    max_table_id = max_table_id.max(t.table_id);
                }
            }
        }
        Ok(max_table_id)
    }

    async fn open_and_register_table(&self, t: &TableEntry) -> Result<()> {
        let catalog = self
            .catalogs
            .catalog(&t.catalog_name)
            .context(CatalogNotFoundSnafu {
                catalog_name: &t.catalog_name,
            })?;
        let schema = catalog
            .schema(&t.schema_name)
            .context(SchemaNotFoundSnafu {
                schema_info: format!("{}.{}", &t.catalog_name, &t.schema_name),
            })?;

        let context = EngineContext {};
        let request = OpenTableRequest {
            catalog_name: t.catalog_name.clone(),
            schema_name: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
        };

        let option = self
            .engine
            .open_table(&context, request)
            .await
            .with_context(|_| OpenTableSnafu {
                table_info: format!(
                    "{}.{}.{}, id: {}",
                    &t.catalog_name, &t.schema_name, &t.table_name, t.table_id
                ),
            })?
            .with_context(|| TableNotFoundSnafu {
                table_info: format!(
                    "{}.{}.{}, id: {}",
                    &t.catalog_name, &t.schema_name, &t.table_name, t.table_id
                ),
            })?;

        schema.register_table(t.table_name.clone(), option)?;
        Ok(())
    }
}

impl CatalogList for LocalCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut res = self.catalogs.catalog_names();
        res.push(SYSTEM_CATALOG_NAME.to_string());
        res
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name.eq_ignore_ascii_case(SYSTEM_CATALOG_NAME) {
            Some(self.system.clone())
        } else {
            self.catalogs.catalog(name)
        }
    }
}

#[async_trait::async_trait]
impl CatalogManager for LocalCatalogManager {
    /// Start [MemoryCatalogManager] to load all information from system catalog table.
    /// Make sure table engine is initialized before starting [MemoryCatalogManager].
    async fn start(&self) -> Result<()> {
        self.init().await
    }

    #[inline]
    fn next_table_id(&self) -> TableId {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<usize> {
        let started = self.init_lock.lock().await;

        ensure!(
            *started,
            IllegalManagerStateSnafu {
                msg: "Catalog manager not started",
            }
        );

        let catalog_name = request
            .catalog
            .unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string());
        let schema_name = request
            .schema
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

        let catalog = self
            .catalogs
            .catalog(&catalog_name)
            .context(CatalogNotFoundSnafu {
                catalog_name: &catalog_name,
            })?;
        let schema = catalog
            .schema(&schema_name)
            .with_context(|| SchemaNotFoundSnafu {
                schema_info: format!("{}.{}", catalog_name, schema_name),
            })?;

        if schema.table_exist(&request.table_name) {
            return TableExistsSnafu {
                table: format_full_table_name(&catalog_name, &schema_name, &request.table_name),
            }
            .fail();
        }

        self.system
            .register_table(
                catalog_name,
                schema_name,
                request.table_name.clone(),
                request.table_id,
            )
            .await?;

        schema.register_table(request.table_name, request.table)?;
        Ok(1)
    }

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        ensure!(
            !*self.init_lock.lock().await,
            IllegalManagerStateSnafu {
                msg: "Catalog manager already started",
            }
        );

        let mut sys_table_requests = self.system_table_requests.lock().await;
        sys_table_requests.push(request);

        Ok(())
    }

    fn table(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        let catalog_name = catalog.unwrap_or(DEFAULT_CATALOG_NAME);
        let schema_name = schema.unwrap_or(DEFAULT_SCHEMA_NAME);

        let catalog = self
            .catalogs
            .catalog(catalog_name)
            .context(CatalogNotFoundSnafu { catalog_name })?;
        let schema = catalog
            .schema(schema_name)
            .with_context(|| SchemaNotFoundSnafu {
                schema_info: format!("{}.{}", catalog_name, schema_name),
            })?;
        Ok(schema.table(table_name))
    }
}
