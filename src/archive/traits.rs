#[async_trait::async_trait]
pub trait ArchiveService {
    type Batch;
    type BatchOptions;
    type Metadata;
    type Status;
    type Error;

    async fn batch(&self, options: &Self::BatchOptions) -> Result<Vec<Self::Batch>, Self::Error>;
    async fn metadata(&self) -> Result<Vec<Self::Metadata>, Self::Error>;
    async fn metadata_by_id(&self, id: String) -> Result<Option<Self::Metadata>, Self::Error>;
    async fn status(&self) -> Result<Self::Status, Self::Error>;
}
