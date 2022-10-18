mod mock;
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::remote::{OpendalBackend, RemoteCatalogManager};
    use catalog::CatalogManager;

    use super::*;

    async fn create_opendal_backend(path: &str) -> Arc<OpendalBackend> {
        let accessor = opendal::services::fs::Builder::default()
            .root(path)
            .finish()
            .await
            .unwrap();
        Arc::new(OpendalBackend::new(accessor))
    }

    #[tokio::test]
    async fn test_create_backend() {
        let dir = tempdir::TempDir::new("opendal_test").unwrap();
        let backend = create_opendal_backend(dir.path().to_str().unwrap()).await;
        let engine = Arc::new(mock::MockTableEngine::default());
        let catalog_manager = RemoteCatalogManager::new(
            engine.clone(),
            "localhost".to_string(),
            backend.clone() as _,
        );
        catalog_manager.start().await.unwrap();
    }
}
