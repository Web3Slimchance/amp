use amp_config::MetadataDbConfig;

pub async fn run(metadata_db_config: &MetadataDbConfig) -> Result<(), Error> {
    let _metadata_db = metadata_db::connect_pool_with_config(
        &metadata_db_config.url,
        metadata_db_config.clone(),
        true,
    )
    .await
    .map_err(Error)?;

    Ok(())
}

/// Failed to run database migrations on the metadata database.
#[derive(Debug, thiserror::Error)]
#[error("Failed to run migrations")]
pub struct Error(#[source] metadata_db::Error);
