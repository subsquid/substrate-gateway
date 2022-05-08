use crate::archive::ArchiveService;
use crate::archive::selection::{EventSelection, CallSelection, EvmLogSelection};
use crate::entities::{Batch, Metadata, Status};
use crate::metrics::DB_TIME_SPENT_SECONDS;
use serde_json::{Map, Value};
use convert_case::{Casing, Case};
use async_graphql::{Context, Object, Result};
use inputs::{EventSelectionInput, CallSelectionInput, EvmLogSelectionInput};

mod inputs;

pub struct EvmSupport(pub bool);

fn is_evm_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<EvmSupport>().0
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

pub struct QueryRoot<T: ArchiveService> {
    pub archive: T,
}

#[Object]
impl<T> QueryRoot<T>
    where T: ArchiveService + Send + Sync
{
    async fn batch(
        &self,
        limit: i32,
        #[graphql(default = 0)]
        from_block: i32,
        to_block: Option<i32>,
        #[graphql(name = "emvLogs", visible = "is_evm_supported")]
        evm_log_selections: Option<Vec<EvmLogSelectionInput>>,
        #[graphql(name = "events")]
        event_selections: Option<Vec<EventSelectionInput>>,
        #[graphql(name = "calls")]
        call_selections: Option<Vec<CallSelectionInput>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<Batch>> {
        let mut events = Vec::new();
        if let Some(selections) = event_selections {
            for selection in selections {
                events.push(EventSelection::from(selection));
            }
        }
        let mut calls = Vec::new();
        if let Some(selections) = call_selections {
            for selection in selections {
                calls.push(CallSelection::from(selection));
            }
        }
        let mut evm_logs = Vec::new();
        if let Some(selections) = evm_log_selections {
            for selection in selections {
                evm_logs.push(EvmLogSelection::from(selection));
            }
        }
        let include_all_blocks = include_all_blocks.unwrap_or(false);
        let mut batch = self.archive
            .batch(limit, from_block, to_block, &evm_logs, &events, &calls, include_all_blocks)
            .await?;
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
