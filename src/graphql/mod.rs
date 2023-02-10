use async_graphql::{Context, Object, Result};
use inputs::{
    AcalaEvmEventSelectionInput, CallSelectionInput, ContractsEventSelectionInput,
    EthExecutedSelectionInput, EthTransactSelectionInput, EventSelectionInput,
    EvmLogSelectionInput, GearMessageEnqueuedSelectionInput, GearUserMessageSentSelectionInput,
};
use std::sync::{Arc, Mutex};
use substrate_archive::archive::{ArchiveService, BatchOptions, Selections};
use substrate_archive::entities::{Batch, Metadata, Status};
use substrate_archive::selection::{
    AcalaEvmEventSelection, CallSelection, ContractsEventSelection, EthExecutedSelection,
    EthTransactSelection, EventSelection, EvmLogSelection, GearMessageEnqueuedSelection,
    GearUserMessageSentSelection,
};

mod inputs;

pub struct EvmSupport(pub bool);

fn is_evm_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<EvmSupport>().0
}

pub struct AcalaSupport(pub bool);

fn is_acala_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<AcalaSupport>().0
}

pub struct ContractsSupport(pub bool);

fn is_contracts_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<ContractsSupport>().0
}

pub struct GearSupport(pub bool);

fn is_gear_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<GearSupport>().0
}

pub struct NextBlock(pub Option<i32>);

pub struct QueryRoot {
    pub archive: Box<dyn ArchiveService + Send + Sync>,
}

#[Object]
impl QueryRoot {
    #[allow(clippy::too_many_arguments)]
    async fn batch(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
        #[graphql(default = 0)] from_block: i32,
        to_block: Option<i32>,
        #[graphql(name = "evmLogs", visible = "is_evm_supported")] evm_log_selections: Option<
            Vec<EvmLogSelectionInput>,
        >,
        #[graphql(name = "ethereumTransactions", visible = "is_evm_supported")]
        eth_transact_selections: Option<Vec<EthTransactSelectionInput>>,
        #[graphql(name = "ethereumExecuted", visible = "is_evm_supported")]
        eth_executed_selections: Option<Vec<EthExecutedSelectionInput>>,
        #[graphql(name = "contractsEvents", visible = "is_contracts_supported")]
        contracts_event_selections: Option<Vec<ContractsEventSelectionInput>>,
        #[graphql(name = "gearMessagesEnqueued", visible = "is_gear_supported")]
        gear_message_enqueued_selections: Option<Vec<GearMessageEnqueuedSelectionInput>>,
        #[graphql(name = "gearUserMessagesSent", visible = "is_gear_supported")]
        gear_user_message_sent_selections: Option<Vec<GearUserMessageSentSelectionInput>>,
        #[graphql(name = "acalaEvmExecuted", visible = "is_acala_supported")]
        acala_evm_executed_selections: Option<Vec<AcalaEvmEventSelectionInput>>,
        #[graphql(name = "acalaEvmExecutedFailed", visible = "is_acala_supported")]
        acala_evm_executed_failed_selections: Option<Vec<AcalaEvmEventSelectionInput>>,
        #[graphql(name = "events")] event_selections: Option<Vec<EventSelectionInput>>,
        #[graphql(name = "calls")] call_selections: Option<Vec<CallSelectionInput>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<Batch>> {
        let next_block = ctx.data_unchecked::<Arc<Mutex<NextBlock>>>();
        let selections = Selections {
            call: self.unwrap_selections::<CallSelectionInput, CallSelection>(call_selections),
            event: self.unwrap_selections::<EventSelectionInput, EventSelection>(event_selections),
            evm_log: self.unwrap_selections::<EvmLogSelectionInput, EvmLogSelection>(evm_log_selections),
            eth_transact: self.unwrap_selections::<EthTransactSelectionInput, EthTransactSelection>(eth_transact_selections),
            eth_executed: self.unwrap_selections::<EthExecutedSelectionInput, EthExecutedSelection>(eth_executed_selections),
            contracts_event: self.unwrap_selections::<ContractsEventSelectionInput, ContractsEventSelection>(contracts_event_selections),
            gear_message_enqueued: self.unwrap_selections::<GearMessageEnqueuedSelectionInput, GearMessageEnqueuedSelection>(gear_message_enqueued_selections),
            gear_user_message_sent: self.unwrap_selections::<GearUserMessageSentSelectionInput, GearUserMessageSentSelection>(gear_user_message_sent_selections),
            acala_evm_executed: self.unwrap_selections::<AcalaEvmEventSelectionInput, AcalaEvmEventSelection>(acala_evm_executed_selections),
            acala_evm_executed_failed: self.unwrap_selections::<AcalaEvmEventSelectionInput, AcalaEvmEventSelection>(acala_evm_executed_failed_selections),
        };
        let options = BatchOptions {
            limit,
            from_block,
            to_block,
            include_all_blocks: include_all_blocks.unwrap_or(false),
            selections,
        };
        let resp = self.archive.batch(&options).await?;
        if let Some(next) = resp.next_block {
            let mut next_block = next_block.lock().unwrap();
            next_block.0 = Some(next);
        }
        Ok(resp.data)
    }

    async fn metadata(&self) -> Result<Vec<Metadata>> {
        let metadata = self.archive.metadata().await?;
        Ok(metadata)
    }

    async fn metadata_by_id(&self, id: String) -> Result<Option<Metadata>> {
        let metadata = self.archive.metadata_by_id(id).await?;
        Ok(metadata)
    }

    async fn status(&self) -> Result<Status> {
        let status = self.archive.status().await?;
        Ok(status)
    }
}

impl QueryRoot {
    fn unwrap_selections<T, U: From<T>>(&self, selections: Option<Vec<T>>) -> Vec<U> {
        selections.map_or_else(Vec::new, |selections| {
            selections
                .into_iter()
                .map(|selection| U::from(selection))
                .collect()
        })
    }
}
