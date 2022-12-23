use self::controller::BatchController;
use super::{ArchiveService, BatchOptions};
use crate::entities::{Batch, Metadata, Status};
use crate::error::Error;
use crate::metrics::ObserverExt;
use sqlx::{Pool, Postgres};

mod batch;
mod controller;
mod fields;
mod limit;
mod partial;
mod selection;
mod serializer;
mod utils;

#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[derive(Clone, Debug)]
pub enum DatabaseType {
    Postgres,
    Cockroach,
}

pub struct BatchResponse {
    pub data: Vec<Batch>,
    pub next_block: Option<i32>,
}

pub struct PostgresArchive {
    pool: Pool<Postgres>,
    database_type: DatabaseType,
    scan_start_value: u16,
    scan_max_value: u32,
}

#[async_trait::async_trait]
impl ArchiveService for PostgresArchive {
    type Batch = BatchResponse;
    type BatchOptions = BatchOptions;
    type Metadata = Metadata;
    type Status = Status;
    type Error = Error;

    async fn batch(&self, options: &BatchOptions) -> Result<BatchResponse, Self::Error> {
        let controller = BatchController::new(
            self.pool.clone(), self.database_type.clone(),
            self.scan_start_value, self.scan_max_value
        );
        controller.load(options).await
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
}

impl PostgresArchive {
    pub fn new(
        pool: Pool<Postgres>,
        database_type: DatabaseType,
        scan_start_value: u16,
        scan_max_value: u32
    ) -> PostgresArchive {
        PostgresArchive {
            pool,
            database_type,
            scan_start_value,
            scan_max_value,
        }
    }
}
