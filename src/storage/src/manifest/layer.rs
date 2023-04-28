use std::fmt::Debug;

use async_trait::async_trait;
use common_datasource::compression::CompressionType;

use crate::error::{CompressObjectSnafu, DecompressObjectSnafu, Result};

#[async_trait]
pub trait ObjectStoreLayer: Send + Sync + Debug {
    async fn forward(&self, data: Vec<u8>) -> Result<Vec<u8>>;
    async fn backward(&self, data: Vec<u8>) -> Result<Vec<u8>>;
}

/// Layer used to compress data
#[derive(Debug, Clone)]
pub struct CompressLayer {
    compress_type: CompressionType,
}

impl CompressLayer {
    pub fn new(compress_type: CompressionType) -> Self {
        Self { compress_type }
    }
}

#[async_trait]
impl ObjectStoreLayer for CompressLayer {
    async fn forward(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let compress_data = self.compress_type.encode(&data).await.map_err(|e| {
            CompressObjectSnafu {
                compress_type: self.compress_type,
                err_msg: e.to_string(),
            }
            .build()
        })?;
        Ok(compress_data)
    }

    async fn backward(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let decompress_data = self.compress_type.decode(data).await.map_err(|e| {
            DecompressObjectSnafu {
                compress_type: self.compress_type,
                err_msg: e.to_string(),
            }
            .build()
        })?;
        Ok(decompress_data)
    }
}

/// Layer to unify all other layers
#[derive(Debug)]
pub struct ComposeLayer {
    layers: Vec<Box<dyn ObjectStoreLayer>>,
}

impl ComposeLayer {
    pub fn new() -> Self {
        Self {
            layers: vec![Box::new(CompressLayer::new(CompressionType::ZSTD))],
        }
    }
}

#[async_trait]
impl ObjectStoreLayer for ComposeLayer {
    async fn forward(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let mut acc_data = data;
        for layer in &self.layers {
            acc_data = layer.forward(acc_data).await?;
        }
        Ok(acc_data)
    }

    async fn backward(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let mut acc_data = data;
        for layer in &self.layers {
            acc_data = layer.backward(acc_data).await?;
        }
        Ok(acc_data)
    }
}
