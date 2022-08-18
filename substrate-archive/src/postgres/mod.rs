use super::{ArchiveService, BatchOptions};
use crate::entities::{Batch, Metadata, Status};
use crate::error::Error;
use crate::metrics::ObserverExt;
use sqlx::{Pool, Postgres};

mod utils;
mod selection;
mod fields;
mod serializer;
mod batch;

#[derive(Debug)]
pub struct PostgresArchive {
    pool: Pool<Postgres>,
}

#[async_trait::async_trait]
impl ArchiveService for PostgresArchive {
    type Batch = Batch;
    type BatchOptions = BatchOptions;
    type Metadata = Metadata;
    type Status = Status;
    type Error = Error;

    async fn batch(&self, options: &BatchOptions) -> Result<Vec<Self::Batch>, Self::Error> {
        let batch = options.loader(self.pool.clone()).load().await?;
        Ok(batch)
    }

    async fn metadata(&self) -> Result<Vec<Self::Metadata>, Self::Error> {
        let query = "SELECT id, spec_name, spec_version::int8, block_height::int8, block_hash, hex
            FROM metadata ORDER BY block_height";
        let metadata = sqlx::query_as::<_, Metadata>(query)
            .fetch_all(&self.pool)
            .observe_duration("metadata")
            .await?;
        Ok(metadata)
    }

    async fn metadata_by_id(&self, id: String) -> Result<Option<Self::Metadata>, Self::Error> {
        let query = "SELECT id, spec_name, spec_version::int8, block_height::int8, block_hash, hex
            FROM metadata WHERE id = $1";
        let metadata = sqlx::query_as::<_, Metadata>(query)
            .bind(id)
            .fetch_optional(&self.pool)
            .observe_duration("metadata")
            .await?;
        Ok(metadata)
    }

    async fn status(&self) -> Result<Status, Error> {
        let query = "SELECT height::int8 as head FROM block ORDER BY height DESC LIMIT 1";
        let status = sqlx::query_as::<_, Status>(query)
            .fetch_optional(&self.pool)
            .observe_duration("block")
            .await?
            .unwrap_or_else(|| Status { head: -1 });
        Ok(status)
    }

    #[tracing::instrument(target="query")]
    async fn test(&self) -> Result<Status, Error> {
        Ok(Status { head: 1 })
    }
}


impl PostgresArchive {
    pub fn new(pool: Pool<Postgres>) -> PostgresArchive {
        PostgresArchive { pool }
    }
}
