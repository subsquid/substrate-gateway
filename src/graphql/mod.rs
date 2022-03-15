use crate::entities::{Block, BlockHeader, Metadata, Status};
use crate::repository::{get_blocks, get_metadata, get_status, EventSelection, CallSelection};
use crate::metrics::DB_TIME_SPENT_SECONDS;
use crate::server::DbTimer;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use sqlx::{Pool, Postgres};
use async_graphql::{Context, Object, Result};
use async_graphql::dataloader::DataLoader;
use loader::{ExtrinsicLoader, CallLoader, EventLoader};


pub mod loader;


struct BatchContext {
    call_loader: DataLoader<CallLoader>,
    extrinsic_loader: DataLoader<ExtrinsicLoader>,
    event_loader: DataLoader<EventLoader>,
}


impl BatchContext {
    fn new(
        call_loader: DataLoader<CallLoader>,
        extrinsic_loader: DataLoader<ExtrinsicLoader>,
        event_loader: DataLoader<EventLoader>
    ) -> Self {
        Self {
            call_loader,
            extrinsic_loader,
            event_loader,
        }
    }
}


struct Batch {
    block: Block,
    context: Arc<BatchContext>,
}


impl Batch {
    pub fn new(block: Block, context: Arc<BatchContext>) -> Self {
        Self {
            block,
            context
        }
    }
}


#[Object]
impl Batch {
    async fn header(&self, _ctx: &Context<'_>) -> &BlockHeader {
        &self.block.header
    }

    async fn extrinsics(&self, _ctx: &Context<'_>) -> Result<Vec<serde_json::Value>> {
        let extrinsics = self.context.extrinsic_loader
            .load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new)
            .iter()
            .map(|extrinsic| serde_json::to_value(extrinsic).unwrap())
            .collect();
        Ok(extrinsics)
    }

    async fn calls(&self, _ctx: &Context<'_>) -> Result<Vec<serde_json::Value>> {
        let calls = self.context.call_loader
            .load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new)
            .iter()
            .map(|call| serde_json::to_value(call).unwrap())
            .collect();
        Ok(calls)
    }

    async fn events(&self, _ctx: &Context<'_>) -> Result<Vec<serde_json::Value>> {
        let events = self.context.event_loader
            .load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new)
            .iter()
            .map(|event| serde_json::to_value(event).unwrap())
            .collect();
        Ok(events)
    }
}


pub struct QueryRoot;


#[Object]
impl QueryRoot {
    async fn batch(
        &self,
        ctx: &Context<'_>,
        limit: i32,
        #[graphql(default = 0)] from_block: i32,
        to_block: Option<i32>,
        events: Option<Vec<EventSelection>>,
        calls: Option<Vec<CallSelection>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<Batch>> {
        let pool = ctx.data::<Pool<Postgres>>()?;
        let db_timer = ctx.data::<Arc<Mutex<DbTimer>>>()?;
        let call_loader = DataLoader::new(
            CallLoader::new(pool.clone(), calls.clone(), events.clone(), db_timer.clone()),
            actix_web::rt::spawn
        );
        let extrinsic_loader = DataLoader::new(
            ExtrinsicLoader::new(pool.clone(), calls.clone(), events.clone(), db_timer.clone()),
            actix_web::rt::spawn
        );
        let event_loader = DataLoader::new(
            EventLoader::new(pool.clone(), events.clone(), db_timer.clone()),
            actix_web::rt::spawn
        );
        let batch_context = Arc::new(BatchContext::new(call_loader, extrinsic_loader, event_loader));
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["block"]).start_timer();
        let query_start = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let blocks = get_blocks(pool, limit, from_block, to_block, events, calls, include_all_blocks).await?;
        let query_finish = SystemTime::now().duration_since(UNIX_EPOCH)?;
        db_timer.lock().unwrap().add_interval((query_start, query_finish));
        let batch = blocks.into_iter()
            .map(|block| Batch::new(block, batch_context.clone()))
            .collect();
        timer.observe_duration();
        Ok(batch)
    }

    async fn metadata(&self, ctx: &Context<'_>) -> Result<Vec<Metadata>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["metadata"]).start_timer();
        let metadata = get_metadata(&pool).await?;
        timer.observe_duration();
        Ok(metadata)
    }

    async fn status(&self, ctx: &Context<'_>) -> Result<Status> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["block"]).start_timer();
        let status = get_status(&pool).await?;
        timer.observe_duration();
        Ok(status)
    }
}
