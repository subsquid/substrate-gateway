use substrate_archive::{ArchiveService, BatchOptions};
use substrate_archive::selection::{
    EventSelection, CallSelection, EvmLogSelection,
    ContractsEventSelection, EthTransactSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection,
    EvmExecutedSelection,
};
use substrate_archive::error::Error;
use substrate_archive::entities::{Batch, Metadata, Status};
use async_graphql::{Context, Object, Result};
use inputs::{
    EventSelectionInput, CallSelectionInput, EthTransactSelectionInput,
    EvmLogSelectionInput, ContractsEventSelectionInput,
    GearMessageEnqueuedSelectionInput, GearUserMessageSentSelectionInput,
    EvmExecutedSelectionInput,
};
use objects::{StatusObject, MetadataObject, BatchObject};

mod inputs;
mod objects;

pub struct EvmSupport(pub bool);

fn is_evm_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<EvmSupport>().0
}

pub struct EvmPlusSupport(pub bool);

fn is_evm_plus_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<EvmPlusSupport>().0
}

pub struct ContractsSupport(pub bool);

fn is_contracts_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<ContractsSupport>().0
}

pub struct GearSupport(pub bool);

fn is_gear_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<GearSupport>().0
}

pub struct QueryRoot {
    pub archive: Box<dyn ArchiveService<
        Batch = Batch,
        BatchOptions = BatchOptions,
        Metadata = Metadata,
        Status = Status,
        Error = Error,
    > + Send + Sync>,
}

#[Object]
impl QueryRoot {
    #[allow(clippy::too_many_arguments)]
    async fn batch(
        &self,
        limit: i32,
        #[graphql(default = 0)]
        from_block: i32,
        to_block: Option<i32>,
        #[graphql(name = "evmLogs", visible = "is_evm_supported")]
        evm_log_selections: Option<Vec<EvmLogSelectionInput>>,
        #[graphql(name = "ethereumTransactions", visible = "is_evm_supported")]
        eth_transact_selections: Option<Vec<EthTransactSelectionInput>>,
        #[graphql(name = "contractsEvents", visible = "is_contracts_supported")]
        contracts_event_selections: Option<Vec<ContractsEventSelectionInput>>,
        #[graphql(name = "gearMessagesEnqueued", visible = "is_gear_supported")]
        gear_message_enqueued_selections: Option<Vec<GearMessageEnqueuedSelectionInput>>,
        #[graphql(name = "gearUserMessagesSent", visible = "is_gear_supported")]
        gear_user_message_sent_selections: Option<Vec<GearUserMessageSentSelectionInput>>,
        // #[graphql(name = "evmExecuted", visible = "is_evm_plus_supported")]
        // evm_executed_selections: Option<Vec<EvmExecutedSelectionInput>>,
        #[graphql(name = "events")]
        event_selections: Option<Vec<EventSelectionInput>>,
        #[graphql(name = "calls")]
        call_selections: Option<Vec<CallSelectionInput>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<BatchObject>> {
        let options = BatchOptions {
            limit,
            from_block,
            to_block,
            include_all_blocks: include_all_blocks.unwrap_or(false),
            call_selections: self.unwrap_selections::<CallSelectionInput, CallSelection>(call_selections),
            event_selections: self.unwrap_selections::<EventSelectionInput, EventSelection>(event_selections),
            evm_log_selections: self.unwrap_selections::<EvmLogSelectionInput, EvmLogSelection>(evm_log_selections),
            eth_transact_selections: self.unwrap_selections::<EthTransactSelectionInput, EthTransactSelection>(eth_transact_selections),
            contracts_event_selections: self.unwrap_selections::<ContractsEventSelectionInput, ContractsEventSelection>(contracts_event_selections),
            gear_message_enqueued_selections: self.unwrap_selections::<GearMessageEnqueuedSelectionInput, GearMessageEnqueuedSelection>(gear_message_enqueued_selections),
            gear_user_message_sent_selections: self.unwrap_selections::<GearUserMessageSentSelectionInput, GearUserMessageSentSelection>(gear_user_message_sent_selections),
            evm_executed_selections: vec![],
            // evm_executed_selections: self.unwrap_selections::<EvmExecutedSelectionInput, EvmExecutedSelection>(evm_executed_selections),
        };
        let batch = self.archive.batch(&options)
            .await?
            .into_iter()
            .map(|batch| batch.into())
            .collect();
        Ok(batch)
    }

    async fn metadata(&self) -> Result<Vec<MetadataObject>> {
        let metadata = self.archive.metadata().await?
            .into_iter()
            .map(|metadata| metadata.into())
            .collect();
        Ok(metadata)
    }

    async fn metadata_by_id(&self, id: String) -> Result<Option<MetadataObject>> {
        let metadata = self.archive
            .metadata_by_id(id)
            .await?
            .map(|metadata| metadata.into());
        Ok(metadata)
    }

    async fn status(&self) -> Result<StatusObject> {
        let status = self.archive.status().await?.into();
        Ok(status)
    }
}

impl QueryRoot {
    fn unwrap_selections<T, U: From<T>>(&self, selections: Option<Vec<T>>) -> Vec<U> {
        selections.map_or_else(Vec::new, |selections| {
            selections.into_iter()
                .map(|selection| U::from(selection))
                .collect()
        })
    }
}
