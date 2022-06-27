#[async_trait::async_trait]
pub trait ArchiveService {
    type EvmLogSelection;
    type ContractsEventSelection;
    type EventSelection;
    type CallSelection;
    type Batch;
    type Metadata;
    type Status;
    type Error;

    async fn batch(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        evm_log_selections: &Vec<Self::EvmLogSelection>,
        contracts_event_selections: &Vec<Self::ContractsEventSelection>,
        event_selections: &Vec<Self::EventSelection>,
        call_selections: &Vec<Self::CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Self::Batch>, Self::Error>;
    async fn metadata(&self) -> Result<Vec<Self::Metadata>, Self::Error>;
    async fn metadata_by_id(&self, id: String) -> Result<Option<Self::Metadata>, Self::Error>;
    async fn status(&self) -> Result<Self::Status, Self::Error>;
}