use crate::archive::ArchiveService;
use crate::archive::cockroach::CockroachArchive;
use crate::archive::selection::{EventSelection, CallSelection, EvmLogSelection};
use crate::entities::{Batch, Metadata, Status};
use crate::metrics::DB_TIME_SPENT_SECONDS;
use sqlx::{Pool, Postgres};
use async_graphql::{Context, Object, Result};
use inputs::{EventSelectionInput, CallSelectionInput, EvmLogSelectionInput};

mod inputs;

pub struct EvmSupport(pub bool);

fn is_evm_supported(ctx: &Context<'_>) -> bool {
    ctx.data_unchecked::<EvmSupport>().0
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn batch(
        &self,
        ctx: &Context<'_>,
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
        let pool = ctx.data::<Pool<Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
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
        let batch = archive
            .batch(limit, from_block, to_block, &evm_logs, &events, &calls, include_all_blocks)
            .await?;
        Ok(batch)
    }

    async fn metadata(&self, ctx: &Context<'_>) -> Result<Vec<Metadata>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["metadata"]).start_timer();
        let metadata = archive.metadata().await?;
        timer.observe_duration();
        Ok(metadata)
    }

    async fn metadata_by_id(&self, ctx: &Context<'_>, id: String) -> Result<Option<Metadata>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
        let metadata = archive.metadata_by_id(id).await?;
        Ok(metadata)
    }

    async fn status(&self, ctx: &Context<'_>) -> Result<Status> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["block"]).start_timer();
        let status = archive.status().await?;
        timer.observe_duration();
        Ok(status)
    }
}
