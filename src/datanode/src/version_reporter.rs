use std::sync::Arc;

use async_trait::async_trait;
use common_meta::version_reporter::{
    GreptimeVersionReport, Mode as VersionReporterMode, Reporter, VersionReportTask,
    VERSION_REPORT_INTERVAL, VERSION_UUID_KEY,
};
use object_store::{ErrorKind, ObjectStore};
use servers::Mode;

use crate::datanode::DatanodeOptions;

fn get_uuid_file_name() -> String {
    format!("./.{}", VERSION_UUID_KEY)
}

async fn get_uuid(file_name: &str, object_store: ObjectStore) -> Option<String> {
    let mut uuid = None;
    let bs = object_store.clone().read(file_name).await;
    match bs {
        Ok(bytes) => {
            let result = String::from_utf8(bytes);
            if let Ok(s) = result {
                uuid = Some(s);
            }
        }
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                let bs = uuid::Uuid::new_v4().to_string().into_bytes();
                let write_result = object_store.clone().write(file_name, bs.clone()).await;
                if write_result.is_ok() {
                    uuid = Some(String::from_utf8(bs).unwrap());
                }
            }
            _ => {}
        },
    }
    return uuid;
}

pub async fn get_report_task(
    opts: &DatanodeOptions,
    object_store: ObjectStore,
) -> Option<Arc<VersionReportTask>> {
    let uuid_file_name = get_uuid_file_name();
    match opts.mode {
        Mode::Standalone => {
            struct FrontendVersionReport {
                object_store: ObjectStore,
                uuid: Option<String>,
                retry: i32,
                uuid_file_name: String,
            }
            #[async_trait]
            impl Reporter for FrontendVersionReport {
                async fn get_mode(&self) -> VersionReporterMode {
                    VersionReporterMode::Standalone
                }
                async fn get_nodes(&self) -> i32 {
                    1
                }
                async fn get_uuid(&mut self) -> String {
                    if let Some(uuid) = &self.uuid {
                        uuid.clone()
                    } else {
                        if self.retry > 3 {
                            return "".to_string();
                        }
                        let uuid =
                            get_uuid(self.uuid_file_name.as_str(), self.object_store.clone()).await;
                        if let Some(_uuid) = uuid {
                            self.uuid = Some(_uuid.clone());
                            return _uuid;
                        } else {
                            self.retry += 1;
                            return "".to_string();
                        }
                    }
                }
            }
            Some(Arc::new(VersionReportTask::new(
                *VERSION_REPORT_INTERVAL,
                Box::new(GreptimeVersionReport::new(Box::new(
                    FrontendVersionReport {
                        object_store: object_store.clone(),
                        uuid: get_uuid(uuid_file_name.as_str(), object_store.clone()).await,
                        retry: 0,
                        uuid_file_name: uuid_file_name,
                    },
                ))),
            )))
        }
        Mode::Distributed => None,
    }
}
