use crate::archive::{ArchiveService, BatchOptions};
use crate::archive::selection::{
    EventSelection, CallSelection, EvmLogSelection,
    ContractsEventSelection, EthTransactSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection,
};
use crate::error::Error;
use crate::entities::{Batch, Metadata, Status};
use crate::metrics::DB_TIME_SPENT_SECONDS;
use serde_json::{Map, Value};
use convert_case::{Casing, Case};
use async_graphql::{Context, Object, Result};
use inputs::{
    EventSelectionInput, CallSelectionInput, EthTransactSelectionInput,
    EvmLogSelectionInput, ContractsEventSelectionInput,
    GearMessageEnqueuedSelectionInput, GearUserMessageSentSelectionInput,
};

mod inputs;

pub struct EvmSupport(pub bool);

fn is_evm_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<EvmSupport>().0
}

pub struct ContractsSupport(pub bool);

fn is_contracts_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<ContractsSupport>().0
}

pub struct GearSupport(pub bool);

fn is_gear_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<GearSupport>().0
}

fn keys_to_camel_case(map: &mut Map<String, Value>) {
    *map = std::mem::take(map)
        .into_iter()
        .map(|(k, v)| (k.to_case(Case::Camel), v))
        .collect();
}

fn batch_to_camel_case(batch: &mut Vec<Batch>) {
    for item in batch {
        for call in &mut item.calls {
            let map = call.as_object_mut().unwrap();
            keys_to_camel_case(map);
        }
        for event in &mut item.events {
            let map = event.as_object_mut().unwrap();
            keys_to_camel_case(map);
        }
        for extrinsic in &mut item.extrinsics {
            let map = extrinsic.as_object_mut().unwrap();
            keys_to_camel_case(map);
        }
    }
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
        #[graphql(name = "events")]
        event_selections: Option<Vec<EventSelectionInput>>,
        #[graphql(name = "calls")]
        call_selections: Option<Vec<CallSelectionInput>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<Batch>> {
        let events = self.unwrap_selections::<EventSelectionInput, EventSelection>(event_selections);
        let calls = self.unwrap_selections::<CallSelectionInput, CallSelection>(call_selections);
        let evm_logs = self.unwrap_selections::<EvmLogSelectionInput, EvmLogSelection>(evm_log_selections);
        let eth_transactions = self.unwrap_selections::<EthTransactSelectionInput, EthTransactSelection>(eth_transact_selections);
        let contracts_events = self.unwrap_selections::<ContractsEventSelectionInput, ContractsEventSelection>(contracts_event_selections);
        let gear_messages_enqueued = self.unwrap_selections::<GearMessageEnqueuedSelectionInput, GearMessageEnqueuedSelection>(gear_message_enqueued_selections);
        let gear_user_messages_sent = self.unwrap_selections::<GearUserMessageSentSelectionInput, GearUserMessageSentSelection>(gear_user_message_sent_selections);
        let include_all_blocks = include_all_blocks.unwrap_or(false);

        let options = BatchOptions::new()
            .limit(limit)
            .from_block(from_block)
            .to_block(to_block)
            .include_all_blocks(include_all_blocks)
            .call_selections(calls)
            .event_selections(events)
            .evm_log_selections(evm_logs)
            .eth_transact_selections(eth_transactions)
            .contracts_event_selections(contracts_events)
            .gear_message_enqueued_selections(gear_messages_enqueued)
            .gear_user_message_sent_selections(gear_user_messages_sent);
        let mut batch = self.archive.batch(&options).await?;
        batch_to_camel_case(&mut batch);
        Ok(batch)
    }

    async fn metadata(&self) -> Result<Vec<Metadata>> {
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["metadata"]).start_timer();
        let metadata = self.archive.metadata().await?;
        timer.observe_duration();
        Ok(metadata)
    }

    async fn metadata_by_id(&self, id: String) -> Result<Option<Metadata>> {
        let metadata = self.archive.metadata_by_id(id).await?;
        Ok(metadata)
    }

    async fn status(&self) -> Result<Status> {
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["block"]).start_timer();
        let status = self.archive.status().await?;
        timer.observe_duration();
        Ok(status)
    }
}

impl QueryRoot {
    fn unwrap_selections<T, U: From<T>>(&self, selections: Option<Vec<T>>) -> Option<Vec<U>> {
        selections.map(|selections| {
            selections.into_iter()
                .map(|selection| U::from(selection))
                .collect()
        })
    }
}
