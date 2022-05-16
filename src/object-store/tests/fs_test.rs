use anyhow::Result;
use object_store::{backend::fs, util, Object, ObjectMode, ObjectStore, ObjectStreamer};
use tempdir::TempDir;

#[tokio::test]
async fn test_fs_backend_object_crud() -> Result<()> {
    let tmp_dir = TempDir::new("test_fs_backend_object_crud")?;
    // Init store service
    let store = ObjectStore::new(
        fs::Backend::build()
            .root(&tmp_dir.path().to_string_lossy())
            .finish()
            .await?,
    );

    // Create object handler.
    let object = store.object("test_file");

    // Write data info object;
    assert!(object.write("Hello, World!").await.is_ok());

    // Read data from object;
    let bs = object.read().await?;
    assert_eq!("Hello, World!", String::from_utf8(bs)?);

    // Read range from object;
    let bs = object.range_read(1..=11).await?;
    assert_eq!("ello, World", String::from_utf8(bs)?);

    // Get object's Metadata
    let meta = object.metadata().await?;
    assert!(meta.complete());
    assert_eq!("test_file", meta.path());
    assert_eq!(ObjectMode::FILE, meta.mode());
    assert_eq!(13, meta.content_length());

    // Delete object.
    assert!(object.delete().await.is_ok());
    assert!(object.read().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_fs_backend_object_list() -> Result<()> {
    let tmp_dir = TempDir::new("test_fs_backend_object_list")?;
    // Init store service
    let store = ObjectStore::new(
        fs::Backend::build()
            .root(&tmp_dir.path().to_string_lossy())
            .finish()
            .await?,
    );

    // Create  some object handlers.
    let o1 = store.object("test_file1");
    let o2 = store.object("test_file2");
    let o3 = store.object("test_file3");

    // Write something
    assert!(o1.write("Hello, object1!").await.is_ok());
    assert!(o2.write("Hello, object2!").await.is_ok());
    assert!(o3.write("Hello, object3!").await.is_ok());

    // List objects
    let o: Object = store.object("/");
    let obs: ObjectStreamer = o.list().await?;
    let objects = util::collect(obs).await?;
    assert_eq!(3, objects.len());

    // Delete o1, o3
    assert!(o1.delete().await.is_ok());
    assert!(o3.delete().await.is_ok());

    // List obejcts again
    let objects = util::collect(o.list().await?).await?;
    assert_eq!(1, objects.len());

    // Only o2 is exists
    let o2 = &objects[0];
    let bs = o2.read().await?;
    assert_eq!("Hello, object2!", String::from_utf8(bs)?);
    // Delete o2
    assert!(o2.delete().await.is_ok());

    let objects = util::collect(o.list().await?).await?;
    assert!(objects.is_empty());

    Ok(())
}
