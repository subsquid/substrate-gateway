#[async_trait::async_trait]
pub trait ArchiveService {
    type EvmLogSelection;
    type EthTransactSelection;
    type ContractsEventSelection;
    type GearMessageEnqueuedSelection;
    type GearUserMessageSentSelection;
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
        eth_transact_selections: &Vec<Self::EthTransactSelection>,
        contracts_event_selections: &Vec<Self::ContractsEventSelection>,
        gear_message_enqueued_selections: &Vec<Self::GearMessageEnqueuedSelection>,
        gear_user_message_sent_selections: &Vec<Self::GearUserMessageSentSelection>,
        event_selections: &Vec<Self::EventSelection>,
        call_selections: &Vec<Self::CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Self::Batch>, Self::Error>;
    async fn metadata(&self) -> Result<Vec<Self::Metadata>, Self::Error>;
    async fn metadata_by_id(&self, id: String) -> Result<Option<Self::Metadata>, Self::Error>;
    async fn status(&self) -> Result<Self::Status, Self::Error>;
}